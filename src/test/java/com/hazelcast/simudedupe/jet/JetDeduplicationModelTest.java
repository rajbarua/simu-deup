package com.hazelcast.simudedupe.jet;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.hazelcast.simudedupe.jet.DedupResult.Outcome.ACCEPTED;
import static com.hazelcast.simudedupe.jet.DedupResult.Outcome.DUPLICATE;
import static com.hazelcast.simudedupe.jet.DedupResult.Outcome.RETRY;
import static com.hazelcast.simudedupe.jet.DedupResultStatsAggregator.INCORRECT;
import static com.hazelcast.simudedupe.jet.DedupResultStatsAggregator.TOTAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JetDeduplicationModelTest {
    @Test
    void distinguishesAcceptedRetryAndBusinessDuplicate() {
        DedupRequest newRequest = new DedupRequest("900000100100100000000001", "200000100100100000000001", false);

        DedupResult accepted = DedupResult.classify(newRequest, null);
        DedupResult retry = DedupResult.classify(newRequest, newRequest.getOperationId());
        DedupResult unexpectedDuplicate = DedupResult.classify(newRequest, "another-operation");

        assertEquals(ACCEPTED, accepted.getOutcome());
        assertEquals(RETRY, retry.getOutcome());
        assertEquals(DUPLICATE, unexpectedDuplicate.getOutcome());
        assertTrue(accepted.isClassificationCorrect());
        assertTrue(retry.isClassificationCorrect());
        assertFalse(unexpectedDuplicate.isClassificationCorrect());
    }

    @Test
    void aggregatesOnlyTheRequestedRun() {
        String runPrefix = "9000001";
        DedupRequest existing = new DedupRequest(
                runPrefix + "00100100000001", "100000000000000000000001", true);
        DedupRequest newRequest = new DedupRequest(
                runPrefix + "00100100000002", "200000100100100000000002", false);
        DedupRequest anotherRun = new DedupRequest(
                "900000200100100000000003", "200000200100100000000003", false);

        DedupResultStatsAggregator aggregator = new DedupResultStatsAggregator(runPrefix);
        aggregator.accumulate(Map.entry(existing.getOperationId(), DedupResult.classify(existing, "seed-timestamp")));
        aggregator.accumulate(Map.entry(newRequest.getOperationId(), DedupResult.classify(newRequest, null)));
        aggregator.accumulate(Map.entry(anotherRun.getOperationId(), DedupResult.classify(anotherRun, null)));

        long[] stats = aggregator.aggregate();
        assertEquals(2L, stats[TOTAL]);
        assertEquals(0L, stats[INCORRECT]);
    }

    @Test
    @SuppressWarnings("removal")
    void buildsTheEnterpriseJetPipeline() {
        var pipeline = JetDeduplicationJob.pipeline("input", "result", "dedup", 8);
        assertFalse(pipeline.isEmpty());
        assertNotNull(pipeline.toDag());
    }
}
