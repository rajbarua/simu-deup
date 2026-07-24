package com.hazelcast.simudedupe.tests;

import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Exercises payment-ID deduplication with a set of CPMaps distributed across
 * configurable CP groups.
 *
 * <p>Every code path uses {@code floorMod(key.hashCode(), cpGroupCount)} to
 * select the CP group, so test-side population and timed operations always
 * route a payment ID to the same CPMap. Existing and new IDs use the same
 * 24-digit layouts as {@link DedupPutIfAbsentTest}.
 */
public class DedupCpMapTest extends HazelcastTest {
    private static final Pattern WORKER_ADDRESS = Pattern.compile("A(\\d+)_W(\\d+)");
    private static final long MILLIS_PER_DAY = 86_400_000L;

    /**
     * Number of existing payment IDs populated before the timed workload.
     * The current six-digit suffix supports at most 1,000,000 IDs when
     * {@link #existingStartId} is zero.
     */
    public long existingKeyDomain = 1_000_000L;

    /**
     * First numeric suffix in the population range. Normally this remains
     * zero. The complete range must fit in six digits.
     */
    public long existingStartId = 0L;

    /**
     * Number of CP groups, and therefore CPMaps, used as key shards. A key is
     * assigned with {@code floorMod(String.hashCode(), cpGroupCount)}. Increase
     * this before a shard approaches the configured CPMap size limit.
     */
    public int cpGroupCount = 3;

    /**
     * Number of parallel population threads started by each Simulator client.
     * The clients share a CP-backed cursor, so each existing ID is claimed by
     * exactly one thread. Raise cautiously because every CPMap set is a Raft
     * operation.
     */
    public int populationThreadsPerClient = 8;

    /**
     * Number of consecutive IDs reserved with each CP cursor operation. Larger
     * claims reduce cursor overhead; smaller claims balance work more evenly
     * when population clients have different speeds.
     */
    public long populationClaimSize = 1_000L;

    /**
     * Maximum time a client may spend waiting for its population threads. The
     * default allows 30 minutes for the initial 1M-entry load.
     */
    public int populationTimeoutSeconds = 1_800;

    /**
     * Number of populated keys read by the global Prepare phase. CPMap has no
     * bulk size operation, so this sampled verification complements the exact
     * shared loaded-count check without reading all 1M entries.
     */
    public int populationVerificationSampleSize = 1_000;

    /**
     * Distributes preloaded first-seen timestamps across this many days. Values
     * are precomputed once per client, so timestamp generation is not part of
     * the per-entry population cost.
     */
    public int firstSeenSpreadDays = 365;

    /**
     * Number of existing IDs cached by each client as the duplicate hot set.
     * The IDs are evenly sampled from {@link #existingKeyDomain}; this avoids
     * generating or retaining all 1M keys on every client.
     */
    public int existingKeySampleSize = 10_000;

    /**
     * Percentage of timed operations selected from the populated hot set. The
     * remainder use unique new IDs. Valid range is 0 through 100.
     */
    public int existingKeyPercentage = 50;

    /**
     * Eighteen-digit prefix for populated IDs. Appending a six-digit suffix
     * creates a 24-digit payment ID.
     */
    public String existingIdPrefix = "100000000000000000";

    /**
     * One-digit prefix for new IDs. It must not match the first digit of
     * {@link #existingIdPrefix}, preventing overlap with populated IDs.
     */
    public String newIdPrefix = "2";

    /**
     * Six-digit namespace for new IDs and population coordination. Increment
     * this before rerunning against retained CP state. A successful run removes
     * its coordination objects, but incrementing also makes retries after an
     * interrupted Prepare unambiguous.
     */
    public int newKeyRunId = 1;

    /**
     * Maximum time each client waits for the elected setup owner to clear the
     * previous AP IMap/CPMaps and initialize the shared population controls.
     */
    public int setupTimeoutSeconds = 300;

    private CPMap<String, String>[] maps;
    private IAtomicLong resetOwner;
    private IAtomicLong resetComplete;
    private IAtomicLong loadCursor;
    private IAtomicLong loadedCount;
    private IAtomicLong populationEpochMillis;
    private final LongAdder accepted = new LongAdder();
    private final LongAdder duplicates = new LongAdder();
    private final LongAdder existingKeyAttempts = new LongAdder();
    private final LongAdder newKeyAttempts = new LongAdder();
    private final AtomicInteger nextThreadIndex = new AtomicInteger();
    private String[] existingKeys;
    private int[] existingKeyGroups;
    private String[] firstSeenValues;
    private LongAdder[] groupAttempts;
    private String newKeyScopePrefix;
    private String cpMapRunName;

    @Setup
    public void setup() {
        validateConfiguration();
        int[] workerIdentity = resolveWorkerIdentity();
        newKeyScopePrefix = newIdPrefix
                + zeroPadded(newKeyRunId, 6)
                + zeroPadded(workerIdentity[0], 3)
                + zeroPadded(workerIdentity[1], 3);

        CPSubsystem cpSubsystem = targetInstance.getCPSubsystem();
        String controlPrefix = name + "-cp-run-" + zeroPadded(newKeyRunId, 6);
        cpMapRunName = name + "-run-" + zeroPadded(newKeyRunId, 6);
        resetOwner = cpSubsystem.getAtomicLong(controlPrefix + "-reset-owner");
        resetComplete = cpSubsystem.getAtomicLong(controlPrefix + "-reset-complete");
        loadCursor = cpSubsystem.getAtomicLong(controlPrefix + "-load-cursor");
        loadedCount = cpSubsystem.getAtomicLong(controlPrefix + "-loaded-count");
        populationEpochMillis = cpSubsystem.getAtomicLong(controlPrefix + "-population-epoch");

        if (resetOwner.compareAndSet(0L, 1L)) {
            resetForPopulation(cpSubsystem);
        } else {
            awaitReset();
        }

        maps = createMaps(cpSubsystem);
        firstSeenValues = createFirstSeenValues(populationEpochMillis.get());
        generateExistingKeySample();
        groupAttempts = new LongAdder[cpGroupCount];
        for (int group = 0; group < cpGroupCount; group++) {
            groupAttempts[group] = new LongAdder();
        }
    }

    /**
     * Populates existing IDs in parallel across all Simulator clients. Local
     * Prepare methods complete on every client before the global verifier runs.
     */
    @Prepare(global = false)
    public void populate() {
        ExecutorService executor = Executors.newFixedThreadPool(populationThreadsPerClient);
        List<Future<Long>> futures = new ArrayList<>(populationThreadsPerClient);
        try {
            for (int thread = 0; thread < populationThreadsPerClient; thread++) {
                futures.add(executor.submit(this::populateClaims));
            }
            executor.shutdown();

            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(populationTimeoutSeconds);
            long clientLoaded = 0L;
            for (Future<Long> future : futures) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0L) {
                    throw new TimeoutException("CPMap population exceeded " + populationTimeoutSeconds + " seconds");
                }
                clientLoaded += future.get(remainingNanos, TimeUnit.NANOSECONDS);
            }
            logger.info("Client populated {} / {} existing IDs across {} CP groups",
                    clientLoaded, existingKeyDomain, cpGroupCount);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while populating CPMaps", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("CPMap population failed", e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Verifies the exact number of completed population writes and samples the
     * routed CPMaps before timed traffic starts.
     */
    @Prepare(global = true)
    public void verifyPopulation() {
        long actualLoaded = loadedCount.get();
        if (actualLoaded != existingKeyDomain) {
            throw new IllegalStateException("Expected " + existingKeyDomain
                    + " populated CPMap entries but completed " + actualLoaded);
        }

        for (int sample = 0; sample < populationVerificationSampleSize; sample++) {
            long offset = populationVerificationSampleSize == 1
                    ? 0L
                    : ((long) sample * (existingKeyDomain - 1)) / (populationVerificationSampleSize - 1);
            String key = existingKey(existingStartId + offset);
            int group = groupFor(key);
            if (maps[group].get(key) == null) {
                throw new IllegalStateException("Missing populated key " + key + " in CP group " + group);
            }
        }
        logger.info("Verified {} populated IDs using {} routed samples across {} CP groups",
                actualLoaded, populationVerificationSampleSize, cpGroupCount);
    }

    @TimeStep
    public void putIfAbsent(ThreadState state) {
        boolean useExistingKey = state.randomInt(100) < existingKeyPercentage;
        String key;
        int group;
        if (useExistingKey) {
            existingKeyAttempts.increment();
            int sample = state.randomInt(existingKeys.length);
            key = existingKeys[sample];
            group = existingKeyGroups[sample];
        } else {
            newKeyAttempts.increment();
            key = state.nextNewKey();
            group = groupFor(key);
        }

        groupAttempts[group].increment();
        String previous = maps[group].putIfAbsent(key, Long.toString(System.currentTimeMillis()));
        if (previous == null) {
            accepted.increment();
        } else {
            duplicates.increment();
        }
    }

    @Verify(global = false)
    public void logResults() {
        long[] attemptsPerGroup = new long[groupAttempts.length];
        for (int group = 0; group < attemptsPerGroup.length; group++) {
            attemptsPerGroup[group] = groupAttempts[group].sum();
        }
        logger.info("CPMap putIfAbsent results for {}: existingKeyAttempts={}, newKeyAttempts={}, "
                        + "accepted={}, duplicates={}, groupAttempts={}",
                name, existingKeyAttempts.sum(), newKeyAttempts.sum(), accepted.sum(), duplicates.sum(),
                java.util.Arrays.toString(attemptsPerGroup));
    }

    @Verify(global = true)
    public void cleanupPopulationControls() {
        for (CPMap<String, String> map : maps) {
            map.destroy();
        }
        resetOwner.destroy();
        resetComplete.destroy();
        loadCursor.destroy();
        loadedCount.destroy();
        populationEpochMillis.destroy();
    }

    private void resetForPopulation(CPSubsystem cpSubsystem) {
        // AP and CP data are mutually exclusive in this benchmark. Destroying
        // an existing AP object frees its native-memory data. Inspecting the
        // existing proxies avoids creating an EAGER MapStore-backed IMap in a
        // CP-only run merely so it can be destroyed.
        targetInstance.getDistributedObjects().stream()
                .filter(object -> object instanceof IMap<?, ?> && object.getName().equals(name))
                .forEach(object -> object.destroy());
        loadCursor.set(0L);
        loadedCount.set(0L);
        populationEpochMillis.set(System.currentTimeMillis());
        resetComplete.set(1L);
    }

    private void awaitReset() {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(setupTimeoutSeconds);
        while (resetComplete.get() != 1L) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new IllegalStateException("Timed out waiting for CPMap population reset. "
                        + "If an earlier run was interrupted, increment newKeyRunId.");
            }
            try {
                Thread.sleep(250L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for CPMap reset", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private CPMap<String, String>[] createMaps(CPSubsystem cpSubsystem) {
        CPMap<String, String>[] result = (CPMap<String, String>[]) new CPMap<?, ?>[cpGroupCount];
        for (int group = 0; group < cpGroupCount; group++) {
            result[group] = cpSubsystem.getMap(cpMapRunName + "@" + cpGroupName(group));
        }
        return result;
    }

    private String cpGroupName(int group) {
        return name + "-cp-" + group;
    }

    private long populateClaims() {
        long threadLoaded = 0L;
        char[] keyBuffer = new char[24];
        existingIdPrefix.getChars(0, existingIdPrefix.length(), keyBuffer, 0);

        while (true) {
            long offset = loadCursor.getAndAdd(populationClaimSize);
            if (offset >= existingKeyDomain) {
                return threadLoaded;
            }
            long endExclusive = Math.min(offset + populationClaimSize, existingKeyDomain);
            for (long index = offset; index < endExclusive; index++) {
                writeZeroPadded(existingStartId + index, keyBuffer, 18, 6);
                String key = new String(keyBuffer);
                maps[groupFor(key)].set(key, firstSeenValues[valueIndex(index)]);
            }
            long completed = endExclusive - offset;
            loadedCount.addAndGet(completed);
            threadLoaded += completed;
        }
    }

    private String[] createFirstSeenValues(long epochMillis) {
        String[] values = new String[firstSeenSpreadDays + 1];
        for (int day = 0; day < values.length; day++) {
            values[day] = Long.toString(epochMillis - day * MILLIS_PER_DAY);
        }
        return values;
    }

    private int valueIndex(long domainOffset) {
        if (existingKeyDomain == 1L) {
            return 0;
        }
        return (int) ((domainOffset * firstSeenSpreadDays) / (existingKeyDomain - 1));
    }

    private void generateExistingKeySample() {
        existingKeys = new String[existingKeySampleSize];
        existingKeyGroups = new int[existingKeySampleSize];
        char[] keyBuffer = new char[24];
        existingIdPrefix.getChars(0, existingIdPrefix.length(), keyBuffer, 0);
        for (int sample = 0; sample < existingKeys.length; sample++) {
            long domainOffset = ((long) sample * existingKeyDomain) / existingKeySampleSize;
            writeZeroPadded(existingStartId + domainOffset, keyBuffer, 18, 6);
            String key = new String(keyBuffer);
            existingKeys[sample] = key;
            existingKeyGroups[sample] = groupFor(key);
        }
    }

    private String existingKey(long id) {
        char[] result = new char[24];
        existingIdPrefix.getChars(0, existingIdPrefix.length(), result, 0);
        writeZeroPadded(id, result, 18, 6);
        return new String(result);
    }

    private int groupFor(String key) {
        return Math.floorMod(key.hashCode(), cpGroupCount);
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
        if (cpGroupCount <= 0) {
            throw new IllegalArgumentException("cpGroupCount must be positive");
        }
        if (populationThreadsPerClient <= 0 || populationClaimSize <= 0L) {
            throw new IllegalArgumentException("population threads and claim size must be positive");
        }
        if (populationTimeoutSeconds <= 0 || setupTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("population and setup timeouts must be positive");
        }
        if (firstSeenSpreadDays <= 0) {
            throw new IllegalArgumentException("firstSeenSpreadDays must be positive");
        }
        if (existingKeyPercentage < 0 || existingKeyPercentage > 100) {
            throw new IllegalArgumentException("existingKeyPercentage must be between 0 and 100");
        }
        if (!existingIdPrefix.matches("[1-9][0-9]{17}")) {
            throw new IllegalArgumentException("existingIdPrefix must contain exactly 18 numeric digits");
        }
        if (!newIdPrefix.matches("[1-9]") || existingIdPrefix.startsWith(newIdPrefix)) {
            throw new IllegalArgumentException("newIdPrefix must be one digit outside the existing-key range");
        }
        if (existingKeyDomain <= 0L || existingStartId < 0L) {
            throw new IllegalArgumentException("existing key range must be positive");
        }
        if (existingKeySampleSize <= 0 || existingKeySampleSize > existingKeyDomain) {
            throw new IllegalArgumentException("existingKeySampleSize must be between 1 and existingKeyDomain");
        }
        if (populationVerificationSampleSize <= 0 || populationVerificationSampleSize > existingKeyDomain) {
            throw new IllegalArgumentException(
                    "populationVerificationSampleSize must be between 1 and existingKeyDomain");
        }
        long lastExistingId = Math.addExact(existingStartId, existingKeyDomain - 1L);
        if (lastExistingId > 999_999L) {
            throw new IllegalArgumentException("existing key range must fit a six-digit suffix");
        }
        if (newKeyRunId < 0 || newKeyRunId > 999_999) {
            throw new IllegalArgumentException("newKeyRunId must fit six digits");
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

    public class ThreadState extends BaseThreadState {
        private final char[] newKeyBuffer = new char[24];
        private long nextSequence;

        public ThreadState() {
            newKeyScopePrefix.getChars(0, newKeyScopePrefix.length(), newKeyBuffer, 0);
            writeZeroPadded(nextThreadIndex.getAndIncrement(), newKeyBuffer, 13, 3);
        }

        private String nextNewKey() {
            writeZeroPadded(nextSequence++, newKeyBuffer, 16, 8);
            return new String(newKeyBuffer);
        }
    }
}
