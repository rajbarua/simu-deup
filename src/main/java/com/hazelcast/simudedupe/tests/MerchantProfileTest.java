package com.hazelcast.simudedupe.tests;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.simudedupe.merchantprofile.PostgresRunMarkerCountTask;
import com.hazelcast.simudedupe.merchantprofile.MerchantProfile;
import com.hazelcast.simudedupe.merchantprofile.MerchantProfileCompactSerializer;
import com.hazelcast.simudedupe.merchantprofile.MerchantProfiles;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

/** Runs a fixed-rate 70% read / 30% write workload over one million 10 KiB Compact merchant profiles. */
public class MerchantProfileTest extends HazelcastTest {
    /** Name of the MapStore-backed IMap under test; kept separate from Simulator's Java-safe test-case name. */
    public String mapName = "merchant-profile";

    /** Number of pre-populated merchant IDs available to both read and write operations. */
    public long keyDomain = 1_000_000L;

    /** First six-digit suffix in the pre-populated merchant-ID range; normally zero. */
    public long startId = 0L;

    /** Eighteen-digit prefix which, with a six-digit suffix, creates the 24-digit IMap key. */
    public String merchantIdPrefix = "300000000000000000";

    /** Number of prebuilt 10 KiB values per Simulator client; construction is excluded from timed operations. */
    public int valueCacheSize = 256;

    /** Exact expected Compact value size excluding the IMap key. Keep this at 10,240 for the stated workload. */
    public int expectedSerializedValueSizeBytes = MerchantProfileCompactSerializer.SERIALIZED_SIZE_BYTES;

    /** Positive marker written into updated values; increment this before rerunning against a retained database. */
    public long updateRunId = 1L;

    /** Name of the member-side JDBC data connection used by the merchant-profile MapStore. */
    public String databaseConnectionName = "dedup-postgres";

    /** PostgreSQL table backing the merchant profile IMap. */
    public String databaseTableName = "merchant_profile";

    /** Maximum time to wait for write-behind flush and the final member-side PostgreSQL count query. */
    public int databaseVerificationTimeoutSeconds = 600;

    private IMap<String, MerchantProfile> map;
    private MerchantProfile[] cachedValues;
    private final LongAdder reads = new LongAdder();
    private final LongAdder writes = new LongAdder();
    private final LongAdder missingReads = new LongAdder();

    @Setup
    public void setup() {
        validateConfiguration();
        map = targetInstance.getMap(mapName);
        cachedValues = new MerchantProfile[valueCacheSize];
        long createdAt = System.currentTimeMillis();
        for (int sample = 0; sample < cachedValues.length; sample++) {
            cachedValues[sample] = MerchantProfiles.create(updateRunId, sample, createdAt);
        }
    }

    /** Loads the million-row PostgreSQL seed entirely inside the VPC before warmup starts. */
    @Prepare(global = true)
    public void loadBaseline() {
        map.loadAll(true);
        long loaded = map.size();
        if (loaded != keyDomain) {
            throw new IllegalStateException("Expected " + keyDomain + " merchant profiles after loadAll, found " + loaded);
        }
        long oldRunEntries = countMapUpdates();
        if (oldRunEntries != 0L) {
            throw new IllegalStateException("merchant-profile updateRunId " + updateRunId + " already exists on "
                    + oldRunEntries + " entries; increment updateRunId before rerunning");
        }
        logger.info("Loaded {} 10 KiB merchant profiles from PostgreSQL", loaded);
    }

    @TimeStep(prob = 0.70)
    public void read(ThreadState state) {
        MerchantProfile value = map.get(state.randomKey());
        reads.increment();
        if (value == null) {
            missingReads.increment();
        }
    }

    @TimeStep(prob = 0.30)
    public void write(ThreadState state) {
        map.set(state.randomKey(), cachedValues[state.randomInt(cachedValues.length)]);
        writes.increment();
    }

    @Verify(global = false)
    public void verifyLocalOperations() {
        if (missingReads.sum() != 0L) {
            throw new IllegalStateException("Observed " + missingReads.sum() + " missing merchant-profile reads");
        }
        logger.info("merchant-profile client operations: reads={}, writes={}, missingReads={}",
                reads.sum(), writes.sum(), missingReads.sum());
    }

    /** Flushes the one-second write-behind queue and compares distinct updated keys in IMap and PostgreSQL. */
    @Verify(global = true)
    public void verifyMapAndDatabaseCounts() {
        long mapSize = map.size();
        if (mapSize != keyDomain) {
            throw new IllegalStateException("merchant-profile IMap size changed: expected=" + keyDomain + ", actual=" + mapSize);
        }
        long mapUpdateCount = countMapUpdates();
        if (mapUpdateCount == 0L) {
            throw new IllegalStateException("No merchant-profile values carry updateRunId " + updateRunId);
        }
        logger.info("Flushing merchant-profile write-behind entries before PostgreSQL verification");
        flushMapStore();
        logger.info("merchant-profile write-behind flush completed");
        long databaseUpdateCount = queryDatabaseUpdateCount();
        if (databaseUpdateCount != mapUpdateCount) {
            throw new IllegalStateException("merchant-profile IMap/PostgreSQL update count mismatch for run " + updateRunId
                    + ": map=" + mapUpdateCount + ", database=" + databaseUpdateCount);
        }
        logger.info("Verified merchant-profile run {}: mapSize={}, distinctUpdatedKeys={}, databaseUpdatedRows={}",
                updateRunId, mapSize, mapUpdateCount, databaseUpdateCount);
    }

    private long countMapUpdates() {
        return map.aggregate(
                Aggregators.<Map.Entry<String, MerchantProfile>>count(),
                Predicates.equal("updateRunId", updateRunId));
    }

    private void flushMapStore() {
        CompletableFuture<Void> flush = CompletableFuture.runAsync(map::flush);
        try {
            flush.get(databaseVerificationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while flushing merchant-profile MapStore", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("merchant-profile MapStore flush failed", e.getCause());
        } catch (TimeoutException e) {
            flush.cancel(true);
            throw new IllegalStateException("merchant-profile MapStore flush exceeded "
                    + databaseVerificationTimeoutSeconds + " seconds", e);
        }
    }

    private long queryDatabaseUpdateCount() {
        Future<Long> count = targetInstance.getExecutorService("merchant_profile-database-verification")
                .submit(new PostgresRunMarkerCountTask(databaseConnectionName, databaseTableName, updateRunId));
        try {
            return count.get(databaseVerificationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while verifying merchant-profile PostgreSQL count", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("merchant-profile PostgreSQL count verification failed", e.getCause());
        } catch (TimeoutException e) {
            count.cancel(true);
            throw new IllegalStateException("merchant-profile PostgreSQL count verification exceeded "
                    + databaseVerificationTimeoutSeconds + " seconds", e);
        }
    }

    private void validateConfiguration() {
        if (mapName == null || mapName.isBlank()) {
            throw new IllegalArgumentException("mapName must identify the MapStore-backed merchant-profile IMap");
        }
        if (keyDomain <= 0L || startId < 0L || startId + keyDomain > 1_000_000L) {
            throw new IllegalArgumentException("startId and keyDomain must fit the six-digit key suffix");
        }
        if (merchantIdPrefix == null || !merchantIdPrefix.matches("[1-9][0-9]{17}")) {
            throw new IllegalArgumentException("merchantIdPrefix must contain exactly 18 numeric digits");
        }
        if (valueCacheSize <= 0) {
            throw new IllegalArgumentException("valueCacheSize must be positive");
        }
        if (expectedSerializedValueSizeBytes != MerchantProfileCompactSerializer.SERIALIZED_SIZE_BYTES) {
            throw new IllegalArgumentException("expectedSerializedValueSizeBytes must be "
                    + MerchantProfileCompactSerializer.SERIALIZED_SIZE_BYTES);
        }
        if (updateRunId <= 0L) {
            throw new IllegalArgumentException("updateRunId must be positive");
        }
        if (databaseConnectionName == null || databaseConnectionName.isBlank()
                || databaseTableName == null || databaseTableName.isBlank()
                || databaseVerificationTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("PostgreSQL verification settings are invalid");
        }
    }

    private static void writeZeroPadded(long value, char[] target, int offset, int width) {
        long remaining = value;
        for (int index = offset + width - 1; index >= offset; index--) {
            target[index] = (char) ('0' + (remaining % 10));
            remaining /= 10;
        }
        if (remaining != 0L) {
            throw new IllegalArgumentException("Value does not fit " + width + " digits: " + value);
        }
    }

    public class ThreadState extends BaseThreadState {
        private final char[] keyBuffer = new char[24];

        public ThreadState() {
            merchantIdPrefix.getChars(0, merchantIdPrefix.length(), keyBuffer, 0);
        }

        private String randomKey() {
            writeZeroPadded(startId + randomLong(keyDomain), keyBuffer, 18, 6);
            return new String(keyBuffer);
        }
    }
}
