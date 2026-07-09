package com.hazelcast.simudedupe.tests;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;
import com.hazelcast.simudedupe.DedupKey;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

public class DedupLoadTest extends HazelcastTest {
    public long entryCount = 1_000_000_000L;
    public long startId = 0L;
    public long claimSize = 100_000L;
    public long progressLogInterval = 5_000_000L;
    public int firstSeenSpreadDays = 365;
    public String paymentIdPrefix = "PAY";
    public String services = "UPI";

    private IMap<String, String> map;
    private String[] serviceNames;
    private IAtomicLong loadCursor;
    private IAtomicLong loadedCount;

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);
        serviceNames = splitServices(services);
        loadCursor = getAtomicLong(testContext.getTestId() + "-" + name + "-loadCursor");
        loadedCount = getAtomicLong(testContext.getTestId() + "-" + name + "-loadedCount");
    }

    @Prepare(global = false)
    public void prepare() {
        long spreadMillis = Math.max(1, firstSeenSpreadDays) * 86_400_000L;
        long now = System.currentTimeMillis();
        long workerLoaded = 0;
        long nextLogAt = progressLogInterval;

        while (true) {
            long offset = loadCursor.getAndAdd(claimSize);
            if (offset >= entryCount) {
                break;
            }
            long endExclusive = Math.min(offset + claimSize, entryCount);
            Streamer<String, String> streamer = StreamerFactory.getInstance(map);
            for (long i = offset; i < endExclusive; i++) {
                long id = startId + i;
                streamer.pushEntry(keyFor(id), Long.toString(now - Math.floorMod(id, spreadMillis)));
                workerLoaded++;
            }
            streamer.await();

            long totalLoaded = loadedCount.addAndGet(endExclusive - offset);
            if (progressLogInterval > 0 && totalLoaded >= nextLogAt) {
                logger.info("Loaded {} / {} dedup ids into {}", totalLoaded, entryCount, name);
                nextLogAt = totalLoaded + progressLogInterval;
            }
        }
        logger.info("Worker loaded {} dedup ids into {}", workerLoaded, name);
    }

    @TimeStep
    public void loadedNoop() {
    }

    @Verify(global = true)
    public void verify() {
        logger.info("Dedup load complete. Claimed loadedCount={} target={}", loadedCount.get(), entryCount);
    }

    private String keyFor(long id) {
        String service = serviceNames[(int) Math.floorMod(id, serviceNames.length)];
        return DedupKey.key(service, paymentIdPrefix + id);
    }

    private static String[] splitServices(String services) {
        String[] raw = services.split(",");
        String[] result = new String[raw.length];
        for (int i = 0; i < raw.length; i++) {
            result[i] = DedupKey.normalizeService(raw[i]);
        }
        return result;
    }
}
