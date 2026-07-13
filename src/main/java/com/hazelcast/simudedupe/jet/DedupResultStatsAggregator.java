package com.hazelcast.simudedupe.jet;

import com.hazelcast.aggregation.Aggregator;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** Aggregates result correctness without returning millions of entries to Simulator. */
public class DedupResultStatsAggregator
        implements Aggregator<Map.Entry<String, DedupResult>, long[]> {
    public static final int TOTAL = 0;
    public static final int ACCEPTED = 1;
    public static final int DUPLICATE = 2;
    public static final int RETRY = 3;
    public static final int INCORRECT = 4;
    public static final int STAT_COUNT = 5;

    private final String operationIdPrefix;
    private final long[] counts;

    public DedupResultStatsAggregator(String operationIdPrefix) {
        this.operationIdPrefix = Objects.requireNonNull(operationIdPrefix, "operationIdPrefix");
        this.counts = new long[STAT_COUNT];
    }

    DedupResultStatsAggregator(String operationIdPrefix, long[] counts) {
        this.operationIdPrefix = Objects.requireNonNull(operationIdPrefix, "operationIdPrefix");
        if (counts == null || counts.length != STAT_COUNT) {
            throw new IllegalArgumentException("Expected " + STAT_COUNT + " result statistics");
        }
        this.counts = Arrays.copyOf(counts, counts.length);
    }

    @Override
    public void accumulate(Map.Entry<String, DedupResult> entry) {
        if (entry.getKey() == null || !entry.getKey().startsWith(operationIdPrefix) || entry.getValue() == null) {
            return;
        }

        DedupResult result = entry.getValue();
        counts[TOTAL]++;
        switch (result.getOutcome()) {
            case ACCEPTED -> counts[ACCEPTED]++;
            case DUPLICATE -> counts[DUPLICATE]++;
            case RETRY -> counts[RETRY]++;
        }
        if (!result.isClassificationCorrect()) {
            counts[INCORRECT]++;
        }
    }

    @Override
    public void combine(Aggregator aggregator) {
        DedupResultStatsAggregator other = (DedupResultStatsAggregator) aggregator;
        for (int index = 0; index < counts.length; index++) {
            counts[index] += other.counts[index];
        }
    }

    @Override
    public long[] aggregate() {
        return counts.clone();
    }

    String getOperationIdPrefix() {
        return operationIdPrefix;
    }

    long[] getCounts() {
        return counts;
    }
}
