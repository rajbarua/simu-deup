package com.hazelcast.simudedupe.tests;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.simudedupe.jet.DedupRequest;
import com.hazelcast.simudedupe.jet.DedupResult;
import com.hazelcast.simudedupe.jet.DedupResultStatsAggregator;
import com.hazelcast.simudedupe.jet.JetDeduplicationJob;
import com.hazelcast.simudedupe.jet.OperationIdPrefixPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.simudedupe.jet.DedupResultStatsAggregator.ACCEPTED;
import static com.hazelcast.simudedupe.jet.DedupResultStatsAggregator.DUPLICATE;
import static com.hazelcast.simudedupe.jet.DedupResultStatsAggregator.INCORRECT;
import static com.hazelcast.simudedupe.jet.DedupResultStatsAggregator.RETRY;
import static com.hazelcast.simudedupe.jet.DedupResultStatsAggregator.TOTAL;

/**
 * Generates a payment-ID mix into an input IMap and measures the time until the
 * Jet decision appears in the result IMap.
 *
 * <p>The normal Simulator timestep measures input-map acknowledgement latency.
 * The {@code jetEndToEnd} probe is completed by a filtered result-map listener
 * and is excluded from Simulator throughput so each operation is counted once.
 * Final correctness comes from distributed input/result map counts rather than
 * listener delivery, which is intentionally only the latency signal.
 */
public class JetDeduplicationTest extends HazelcastTest {
    private static final Pattern WORKER_ADDRESS = Pattern.compile("A(\\d+)_W(\\d+)");

    /** Name of the event-journal-enabled map into which Simulator writes requests. */
    public String inputMapName = JetDeduplicationJob.DEFAULT_INPUT_MAP_NAME;

    /** Name of the map into which Jet writes one decision per operation ID. */
    public String resultMapName = JetDeduplicationJob.DEFAULT_RESULT_MAP_NAME;

    /** Name assigned to the long-running Jet job by its JobConfig. */
    public String jetJobName = JetDeduplicationJob.DEFAULT_JOB_NAME;

    /** Size of the preloaded payment-ID range in the deduplicate MapStore. */
    public long existingKeyDomain = 1_000_000L;

    /** First six-digit suffix in the preloaded payment-ID range. */
    public long existingStartId = 0L;

    /** Number of evenly distributed existing IDs cached as the duplicate hot set. */
    public int existingKeySampleSize = 10_000;

    /** Percentage of submissions selected from the existing duplicate hot set. */
    public int existingKeyPercentage = 50;

    /** Eighteen-digit prefix used by the 1M PostgreSQL seed. */
    public String existingIdPrefix = "100000000000000000";

    /** One-digit prefix for unique new 24-digit payment IDs. */
    public String newIdPrefix = "2";

    /**
     * One-digit prefix for unique 24-digit operation IDs. It must differ from
     * the payment-ID prefixes because operation IDs are correlation IDs, not
     * business deduplication keys.
     */
    public String operationIdPrefix = "9";

    /**
     * Six-digit namespace used by operation IDs and new payment IDs. Increment
     * before rerunning against a retained deduplicate map.
     */
    public int newKeyRunId = 1;

    /** Maximum time to wait for the deployed Jet job to reach RUNNING. */
    public int jobStartTimeoutSeconds = 600;

    /** Maximum post-run time to wait for all submitted operations to produce results. */
    public int resultDrainTimeoutSeconds = 120;

    /** Number of missed-listener correlations retrieved per fallback getAll call. */
    public int resultRecoveryBatchSize = 1_000;

    /**
     * Clears only the transient Jet input/result maps before the run. The
     * MapStore-backed deduplicate map and its PostgreSQL baseline are retained.
     */
    public boolean resetTransientMaps = true;

    private IMap<String, DedupRequest> inputMap;
    private IMap<String, DedupResult> resultMap;
    private Job jetJob;
    private UUID listenerRegistrationId;
    private LatencyProbe endToEndProbe;
    private String[] existingKeys;
    private String runOperationPrefix;
    private String producerOperationPrefix;
    private String newPaymentScopePrefix;
    private final AtomicInteger nextThreadIndex = new AtomicInteger();
    private final ConcurrentHashMap<String, PendingAttempt> pending = new ConcurrentHashMap<>();
    private final LongAdder submitted = new LongAdder();
    private final LongAdder completedByListener = new LongAdder();
    private final LongAdder completedByFallback = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder duplicates = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder incorrect = new LongAdder();
    private final LongAdder replayedResultEvents = new LongAdder();
    private final LongAdder lostResultEvents = new LongAdder();

    @Setup
    public void setup() {
        validateConfiguration();
        int[] workerIdentity = resolveWorkerIdentity();
        String run = zeroPadded(newKeyRunId, 6);
        String worker = zeroPadded(workerIdentity[0], 3) + zeroPadded(workerIdentity[1], 3);
        runOperationPrefix = operationIdPrefix + run;
        producerOperationPrefix = runOperationPrefix + worker;
        newPaymentScopePrefix = newIdPrefix + run + worker;

        inputMap = targetInstance.getMap(inputMapName);
        resultMap = targetInstance.getMap(resultMapName);
        existingKeys = generateExistingKeys();
        endToEndProbe = testContext.getLatencyProbe("jetEndToEnd", false);

        ResultListener resultListener = new ResultListener();
        listenerRegistrationId = resultMap.addEntryListener(
                resultListener,
                new OperationIdPrefixPredicate<DedupResult>(producerOperationPrefix),
                true);
        jetJob = awaitRunningJob();
    }

    /** Clears transient maps once, after all clients have installed result listeners. */
    @Prepare(global = true)
    public void resetTransientState() {
        if (resetTransientMaps) {
            inputMap.clear();
            resultMap.clear();
        }
        if (inputMap.size() != 0 || resultMap.size() != 0) {
            throw new IllegalStateException("Jet input/result maps must be empty before the benchmark");
        }
    }

    @TimeStep
    public void submit(ThreadState state) {
        boolean expectedDuplicate = state.randomInt(100) < existingKeyPercentage;
        long sequence = state.nextSequence();
        String operationId = state.operationId(sequence);
        String paymentId = expectedDuplicate
                ? existingKeys[state.randomInt(existingKeys.length)]
                : state.newPaymentId(sequence);

        PendingAttempt attempt = new PendingAttempt(System.nanoTime(), expectedDuplicate);
        if (pending.putIfAbsent(operationId, attempt) != null) {
            throw new IllegalStateException("Operation ID reused: " + operationId);
        }
        try {
            inputMap.set(operationId, new DedupRequest(operationId, paymentId, expectedDuplicate));
            submitted.increment();
        } catch (RuntimeException e) {
            pending.remove(operationId, attempt);
            throw e;
        }
    }

    /**
     * Waits locally for listener callbacks. If listener events were lost, a
     * post-measurement getAll fallback verifies the persisted results without
     * adding synthetic latency samples.
     */
    @Verify(global = false)
    public void verifyLocalCompletions() {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(resultDrainTimeoutSeconds);
        long previousPending = pending.size();
        long lastProgressNanos = System.nanoTime();

        while (!pending.isEmpty() && System.nanoTime() < deadlineNanos) {
            sleepMillis(200L);
            long currentPending = pending.size();
            if (currentPending < previousPending) {
                previousPending = currentPending;
                lastProgressNanos = System.nanoTime();
            } else if (System.nanoTime() - lastProgressNanos >= TimeUnit.SECONDS.toNanos(5)) {
                recoverAvailableResults();
                previousPending = pending.size();
                lastProgressNanos = System.nanoTime();
            }
        }

        recoverAvailableResults();
        if (!pending.isEmpty()) {
            throw new IllegalStateException("Timed out with " + pending.size()
                    + " Jet results missing for producer " + producerOperationPrefix);
        }
        if (incorrect.sum() != 0L) {
            throw new IllegalStateException("Observed " + incorrect.sum()
                    + " incorrect Jet dedup decisions for producer " + producerOperationPrefix);
        }
        long completed = completedByListener.sum() + completedByFallback.sum();
        long outcomes = accepted.sum() + duplicates.sum() + retries.sum();
        if (completed != submitted.sum() || outcomes != submitted.sum()) {
            throw new IllegalStateException("Local Jet result accounting mismatch for producer "
                    + producerOperationPrefix + ": submitted=" + submitted.sum()
                    + ", completed=" + completed + ", outcomes=" + outcomes);
        }

        logger.info("Jet producer results: submitted={}, listenerCompleted={}, fallbackCompleted={}, "
                        + "accepted={}, duplicates={}, retries={}, replayedResultEvents={}, lostResultEvents={}",
                submitted.sum(), completedByListener.sum(), completedByFallback.sum(), accepted.sum(),
                duplicates.sum(), retries.sum(), replayedResultEvents.sum(), lostResultEvents.sum());
    }

    /**
     * Final distributed verification. It scans each transient IMap once and
     * compares counts for this run, then verifies every stored classification.
     */
    @Verify(global = true)
    public void verifyInputAndResultMaps() {
        if (jetJob.getStatus() != JobStatus.RUNNING) {
            throw new IllegalStateException("Jet job is not RUNNING at verification: " + jetJob.getStatus());
        }

        long inputCount = inputMap.aggregate(
                Aggregators.<Map.Entry<String, DedupRequest>>count(),
                new OperationIdPrefixPredicate<DedupRequest>(runOperationPrefix));
        long[] resultStats = resultMap.aggregate(new DedupResultStatsAggregator(runOperationPrefix));

        if (inputCount == 0L) {
            throw new IllegalStateException("Jet input map contains no operations for run " + newKeyRunId);
        }
        if (inputCount != resultStats[TOTAL]) {
            throw new IllegalStateException("Jet input/result count mismatch for run " + newKeyRunId
                    + ": input=" + inputCount + ", result=" + resultStats[TOTAL]);
        }
        if (resultStats[INCORRECT] != 0L) {
            throw new IllegalStateException("Jet result map contains " + resultStats[INCORRECT]
                    + " incorrect classifications for run " + newKeyRunId);
        }
        if (resultStats[ACCEPTED] + resultStats[DUPLICATE] + resultStats[RETRY] != resultStats[TOTAL]) {
            throw new IllegalStateException("Jet result outcome counts do not add up for run " + newKeyRunId);
        }

        logger.info("Verified Jet IMap counts for run {}: input={}, result={}, accepted={}, duplicates={}, retries={}",
                newKeyRunId, inputCount, resultStats[TOTAL], resultStats[ACCEPTED],
                resultStats[DUPLICATE], resultStats[RETRY]);
    }

    @Teardown(global = false)
    public void removeResultListener() {
        if (listenerRegistrationId != null) {
            resultMap.removeEntryListener(listenerRegistrationId);
        }
    }

    private Job awaitRunningJob() {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(jobStartTimeoutSeconds);
        Job found;
        do {
            found = targetInstance.getJet().getJob(jetJobName);
            if (found != null && found.getStatus() == JobStatus.RUNNING) {
                return found;
            }
            sleepMillis(500L);
        } while (System.nanoTime() < deadlineNanos);

        throw new IllegalStateException("Jet job " + jetJobName + " did not reach RUNNING within "
                + jobStartTimeoutSeconds + " seconds; last status="
                + (found == null ? "not found" : found.getStatus()));
    }

    private void recoverAvailableResults() {
        List<String> operationIds = new ArrayList<>(pending.keySet());
        for (int offset = 0; offset < operationIds.size(); offset += resultRecoveryBatchSize) {
            int end = Math.min(offset + resultRecoveryBatchSize, operationIds.size());
            Set<String> batch = new HashSet<>(operationIds.subList(offset, end));
            Map<String, DedupResult> found = resultMap.getAll(batch);
            found.forEach((operationId, result) -> complete(operationId, result, false));
        }
    }

    private void complete(String operationId, DedupResult result, boolean listenerDelivery) {
        PendingAttempt attempt = pending.remove(operationId);
        if (attempt == null) {
            if (listenerDelivery) {
                replayedResultEvents.increment();
            }
            return;
        }

        if (listenerDelivery) {
            endToEndProbe.done(attempt.startNanos());
            completedByListener.increment();
        } else {
            completedByFallback.increment();
        }
        if (attempt.expectedDuplicate() != result.isExpectedDuplicate()
                || !operationId.equals(result.getOperationId())
                || !result.isClassificationCorrect()) {
            incorrect.increment();
        }

        switch (result.getOutcome()) {
            case ACCEPTED -> accepted.increment();
            case DUPLICATE -> duplicates.increment();
            case RETRY -> retries.increment();
        }
    }

    private String[] generateExistingKeys() {
        String[] keys = new String[existingKeySampleSize];
        char[] keyBuffer = new char[24];
        existingIdPrefix.getChars(0, existingIdPrefix.length(), keyBuffer, 0);
        for (int sample = 0; sample < keys.length; sample++) {
            long domainOffset = ((long) sample * existingKeyDomain) / existingKeySampleSize;
            writeZeroPadded(existingStartId + domainOffset, keyBuffer, 18, 6);
            keys[sample] = new String(keyBuffer);
        }
        return keys;
    }

    private int[] resolveWorkerIdentity() {
        String workerAddress = System.getenv("WORKER_ADDRESS");
        Matcher matcher = workerAddress == null ? null : WORKER_ADDRESS.matcher(workerAddress);
        if (matcher == null || !matcher.matches()) {
            throw new IllegalStateException("Expected Simulator WORKER_ADDRESS in A<n>_W<n> format, found: "
                    + workerAddress);
        }
        int agentIndex = Integer.parseInt(matcher.group(1));
        int workerIndex = Integer.parseInt(matcher.group(2));
        if (agentIndex > 999 || workerIndex > 999) {
            throw new IllegalArgumentException("Worker address components must fit three digits: " + workerAddress);
        }
        return new int[]{agentIndex, workerIndex};
    }

    private void validateConfiguration() {
        if (existingKeyPercentage < 0 || existingKeyPercentage > 100) {
            throw new IllegalArgumentException("existingKeyPercentage must be between 0 and 100");
        }
        if (!existingIdPrefix.matches("[1-9][0-9]{17}")) {
            throw new IllegalArgumentException("existingIdPrefix must contain exactly 18 numeric digits");
        }
        if (!newIdPrefix.matches("[1-9]") || !operationIdPrefix.matches("[1-9]")) {
            throw new IllegalArgumentException("newIdPrefix and operationIdPrefix must each be one non-zero digit");
        }
        if (existingIdPrefix.startsWith(newIdPrefix)
                || existingIdPrefix.startsWith(operationIdPrefix)
                || newIdPrefix.equals(operationIdPrefix)) {
            throw new IllegalArgumentException("existing, new, and operation ID prefixes must be distinct");
        }
        if (existingKeyDomain <= 0L || existingStartId < 0L) {
            throw new IllegalArgumentException("existing key range must be positive");
        }
        if (existingKeySampleSize <= 0 || existingKeySampleSize > existingKeyDomain) {
            throw new IllegalArgumentException("existingKeySampleSize must be between 1 and existingKeyDomain");
        }
        long lastExistingId = Math.addExact(existingStartId, existingKeyDomain - 1L);
        if (lastExistingId > 999_999L) {
            throw new IllegalArgumentException("existing key range must fit a six-digit suffix");
        }
        if (newKeyRunId < 0 || newKeyRunId > 999_999) {
            throw new IllegalArgumentException("newKeyRunId must fit six digits");
        }
        if (jobStartTimeoutSeconds <= 0 || resultDrainTimeoutSeconds <= 0 || resultRecoveryBatchSize <= 0) {
            throw new IllegalArgumentException("Jet timeouts and result recovery batch size must be positive");
        }
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Jet", e);
        }
    }

    private static String zeroPadded(long value, int width) {
        char[] result = new char[width];
        writeZeroPadded(value, result, 0, width);
        return new String(result);
    }

    private static void writeZeroPadded(long value, char[] target, int offset, int width) {
        if (value < 0L) {
            throw new IllegalArgumentException("Value must not be negative: " + value);
        }
        long remaining = value;
        for (int index = offset + width - 1; index >= offset; index--) {
            target[index] = (char) ('0' + (remaining % 10L));
            remaining /= 10L;
        }
        if (remaining != 0L) {
            throw new IllegalArgumentException("Value does not fit " + width + " digits: " + value);
        }
    }

    private record PendingAttempt(long startNanos, boolean expectedDuplicate) {
    }

    private final class ResultListener implements
            EntryAddedListener<String, DedupResult>,
            EntryUpdatedListener<String, DedupResult>,
            EventLostListener {

        @Override
        public void entryAdded(EntryEvent<String, DedupResult> event) {
            complete(event.getKey(), event.getValue(), true);
        }

        @Override
        public void entryUpdated(EntryEvent<String, DedupResult> event) {
            complete(event.getKey(), event.getValue(), true);
        }

        @Override
        public void eventLost(EventLostEvent event) {
            lostResultEvents.increment();
        }
    }

    public class ThreadState extends BaseThreadState {
        private final char[] operationIdBuffer = new char[24];
        private final char[] newPaymentIdBuffer = new char[24];
        private long sequence;

        public ThreadState() {
            producerOperationPrefix.getChars(0, producerOperationPrefix.length(), operationIdBuffer, 0);
            newPaymentScopePrefix.getChars(0, newPaymentScopePrefix.length(), newPaymentIdBuffer, 0);
            int threadIndex = nextThreadIndex.getAndIncrement();
            writeZeroPadded(threadIndex, operationIdBuffer, 13, 3);
            writeZeroPadded(threadIndex, newPaymentIdBuffer, 13, 3);
        }

        private long nextSequence() {
            return sequence++;
        }

        private String operationId(long value) {
            writeZeroPadded(value, operationIdBuffer, 16, 8);
            return new String(operationIdBuffer);
        }

        private String newPaymentId(long value) {
            writeZeroPadded(value, newPaymentIdBuffer, 16, 8);
            return new String(newPaymentIdBuffer);
        }
    }
}
