package com.hazelcast.simudedupe.tests;

import com.hazelcast.map.IMap;
import com.hazelcast.simudedupe.DedupKey;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.concurrent.atomic.LongAdder;

public class DedupPutIfAbsentTest extends HazelcastTest {
    public long existingKeyDomain = 1_000_000_000L;
    public long existingStartId = 0L;
    public int duplicatePercentage = 50;
    public String paymentIdPrefix = "PAY";
    public String newPaymentIdPrefix = "NEWPAY";
    public String services = "UPI";

    private IMap<String, String> map;
    private String[] serviceNames;
    private final LongAdder accepted = new LongAdder();
    private final LongAdder duplicates = new LongAdder();

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);
        serviceNames = splitServices(services);
    }

    @TimeStep
    public void putIfAbsent(ThreadState state) {
        boolean duplicateCandidate = state.randomInt(100) < duplicatePercentage;
        String key = duplicateCandidate ? existingKey(state) : newKey(state);
        String previous = map.putIfAbsent(key, Long.toString(System.currentTimeMillis()));
        if (previous == null) {
            accepted.increment();
        } else {
            duplicates.increment();
        }
    }

    @Verify(global = false)
    public void verify() {
        logger.info("putIfAbsent results for {}: accepted={}, duplicates={}", name, accepted.sum(), duplicates.sum());
    }

    private String existingKey(ThreadState state) {
        long id = existingStartId + state.randomLong(existingKeyDomain);
        return DedupKey.key(serviceFor(id), paymentIdPrefix + id);
    }

    private String newKey(ThreadState state) {
        long sequence = state.nextSequence++;
        String paymentId = newPaymentIdPrefix + "-" + state.workerId + "-" + state.threadId + "-" + sequence;
        return DedupKey.key(serviceFor(sequence), paymentId);
    }

    private String serviceFor(long id) {
        return serviceNames[(int) Math.floorMod(id, serviceNames.length)];
    }

    private String resolveWorkerId() {
        String workerAddress = System.getenv("WORKER_ADDRESS");
        if (workerAddress != null && !workerAddress.isBlank()) {
            return workerAddress.replaceAll("[^A-Za-z0-9_.-]", "_");
        }
        String workerName = System.getenv("WORKER_NAME");
        if (workerName != null && !workerName.isBlank()) {
            return workerName.replaceAll("[^A-Za-z0-9_.-]", "_");
        }
        String workerIndex = System.getenv("WORKER_INDEX");
        if (workerIndex != null && !workerIndex.isBlank()) {
            return "W" + workerIndex;
        }
        return "unknown";
    }

    private static String[] splitServices(String services) {
        String[] raw = services.split(",");
        String[] result = new String[raw.length];
        for (int i = 0; i < raw.length; i++) {
            result[i] = DedupKey.normalizeService(raw[i]);
        }
        return result;
    }

    public class ThreadState extends BaseThreadState {
        private final String workerId = resolveWorkerId();
        private final long threadId = Thread.currentThread().getId();
        private long nextSequence;
    }
}
