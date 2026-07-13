package com.hazelcast.simudedupe.jet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/** Compact serializer for the final distributed result aggregator. */
public final class DedupResultStatsAggregatorCompactSerializer
        implements CompactSerializer<DedupResultStatsAggregator> {
    private static final String TYPE_NAME = "com.hazelcast.simudedupe.jet.DedupResultStatsAggregator";

    @Override
    public DedupResultStatsAggregator read(CompactReader reader) {
        return new DedupResultStatsAggregator(
                reader.readString("operationIdPrefix"),
                reader.readArrayOfInt64("counts"));
    }

    @Override
    public void write(CompactWriter writer, DedupResultStatsAggregator aggregator) {
        writer.writeString("operationIdPrefix", aggregator.getOperationIdPrefix());
        writer.writeArrayOfInt64("counts", aggregator.getCounts());
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public Class<DedupResultStatsAggregator> getCompactClass() {
        return DedupResultStatsAggregator.class;
    }
}
