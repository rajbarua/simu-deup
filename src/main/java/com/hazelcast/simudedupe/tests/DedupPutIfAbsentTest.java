package com.hazelcast.simudedupe.tests;

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Exercises payment-ID deduplication with a configurable mix of existing and
 * new 24-digit keys.
 *
 * <p>Existing keys use {@code existingIdPrefix + six-digit suffix}. New keys
 * use {@code newIdPrefix + run(6) + agent(3) + worker(3) + thread(3) + sequence(8)}.
 * Configure the public fields from the test block in {@code imap_postgres_tests.yaml}.
 */
public class DedupPutIfAbsentTest extends HazelcastTest {
    private static final Pattern WORKER_ADDRESS = Pattern.compile("A(\\d+)_W(\\d+)");

    /**
     * Size of the existing key range in PostgreSQL. This must match
     * {@code dedup_seed_record_count}; it does not control how many keys are
     * cached by each test client. The current six-digit suffix limits the value
     * to 1,000,000 when {@link #existingStartId} is zero.
     */
    public long existingKeyDomain = 1_000_000L;

    /**
     * First numeric suffix in the PostgreSQL seed range. Keep this aligned with
     * {@code dedup_seed_start_id}. Normally this remains zero.
     */
    public long existingStartId = 0L;

    /**
     * Number of existing IDs cached by each client as the duplicate hot set.
     * The IDs are evenly sampled from {@link #existingKeyDomain}. A smaller
     * value repeats fewer keys and uses less client memory; a larger value
     * distributes duplicate traffic across more partitions. Valid range is
     * 1 through {@code existingKeyDomain}.
     */
    public int existingKeySampleSize = 10_000;

    /**
     * Percentage of operations that select from the existing-key hot set.
     * Remaining operations create unique keys from the new-key range. With a
     * complete, unexpired seed this is also the expected duplicate percentage.
     * Valid range is 0 through 100.
     */
    public int existingKeyPercentage = 50;

    /**
     * Eighteen-digit prefix used by the PostgreSQL seed Job. Keep this equal to
     * {@code dedup_seed_id_prefix}; appending the six-digit suffix produces the
     * required 24-digit existing key.
     */
    public String existingIdPrefix = "100000000000000000";

    /**
     * One-digit prefix for newly accepted keys. It must differ from the first
     * digit of {@link #existingIdPrefix} so new traffic cannot overlap the
     * preloaded range.
     */
    public String newIdPrefix = "2";

    /**
     * Six-digit run namespace for new keys. Increment it before rerunning the
     * benchmark against the same cluster; otherwise the next run will reuse its
     * previous "new" keys. Valid range is 0 through 999,999.
     */
    public int newKeyRunId = 1;

    private IMap<String, String> map;
    private final LongAdder accepted = new LongAdder();
    private final LongAdder duplicates = new LongAdder();
    private final LongAdder existingKeyAttempts = new LongAdder();
    private final LongAdder newKeyAttempts = new LongAdder();
    private final AtomicInteger nextThreadIndex = new AtomicInteger();
    private String[] existingKeys;
    private String newKeyScopePrefix;

    @Setup
    public void setup() {
        validateConfiguration();
        int[] workerIdentity = resolveWorkerIdentity();
        newKeyScopePrefix = newIdPrefix
                + zeroPadded(newKeyRunId, 6)
                + zeroPadded(workerIdentity[0], 3)
                + zeroPadded(workerIdentity[1], 3);
        // Cache only the configured duplicate hot set. The persisted domain can
        // grow without increasing client memory or timed key-generation cost.
        existingKeys = generateExistingKeys();
        map = targetInstance.getMap(name);
    }

    @TimeStep
    public void putIfAbsent(ThreadState state) {
        boolean useExistingKey = state.randomInt(100) < existingKeyPercentage;
        (useExistingKey ? existingKeyAttempts : newKeyAttempts).increment();
        String key = useExistingKey ? existingKey(state) : newKey(state);
        String previous = map.putIfAbsent(key, Long.toString(System.currentTimeMillis()));
        if (previous == null) {
            accepted.increment();
        } else {
            duplicates.increment();
        }
    }

    @Verify(global = false)
    public void verify() {
        logger.info("putIfAbsent results for {}: existingKeyAttempts={}, newKeyAttempts={}, accepted={}, duplicates={}",
                name, existingKeyAttempts.sum(), newKeyAttempts.sum(), accepted.sum(), duplicates.sum());
    }

    private String existingKey(ThreadState state) {
        return existingKeys[state.randomInt(existingKeys.length)];
    }

    private String newKey(ThreadState state) {
        return state.nextNewKey();
    }

    private String[] generateExistingKeys() {
        String[] keys = new String[existingKeySampleSize];
        char[] keyBuffer = new char[24];
        existingIdPrefix.getChars(0, existingIdPrefix.length(), keyBuffer, 0);
        for (int i = 0; i < keys.length; i++) {
            long domainOffset = ((long) i * existingKeyDomain) / existingKeySampleSize;
            writeZeroPadded(existingStartId + domainOffset, keyBuffer, 18, 6);
            keys[i] = new String(keyBuffer);
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
        if (!newIdPrefix.matches("[1-9]") || existingIdPrefix.startsWith(newIdPrefix)) {
            throw new IllegalArgumentException("newIdPrefix must be one digit outside the existing-key range");
        }
        if (existingKeyDomain <= 0 || existingStartId < 0) {
            throw new IllegalArgumentException("existing key range must be positive");
        }
        if (existingKeySampleSize <= 0 || existingKeySampleSize > existingKeyDomain) {
            throw new IllegalArgumentException("existingKeySampleSize must be between 1 and existingKeyDomain");
        }
        long lastExistingId = Math.addExact(existingStartId, existingKeyDomain - 1);
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
        if (value < 0) {
            throw new IllegalArgumentException("Value must not be negative: " + value);
        }
        long remaining = value;
        for (int index = offset + width - 1; index >= offset; index--) {
            target[index] = (char) ('0' + (remaining % 10));
            remaining /= 10;
        }
        if (remaining != 0) {
            throw new IllegalArgumentException("Value does not fit " + width + " digits: " + value);
        }
    }

    public class ThreadState extends BaseThreadState {
        // New keys cannot be cached. Each thread mutates only its private
        // sequence digits and creates the String required by IMap serialization.
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
