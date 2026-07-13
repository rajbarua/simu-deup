package com.hazelcast.simudedupe.jet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/** Compact serializer for decisions stored in the Jet result IMap. */
public final class DedupResultCompactSerializer implements CompactSerializer<DedupResult> {
    private static final String TYPE_NAME = "com.hazelcast.simudedupe.jet.DedupResult";

    @Override
    public DedupResult read(CompactReader reader) {
        return new DedupResult(
                reader.readString("operationId"),
                reader.readString("paymentId"),
                reader.readBoolean("expectedDuplicate"),
                DedupResult.Outcome.valueOf(reader.readString("outcome")));
    }

    @Override
    public void write(CompactWriter writer, DedupResult result) {
        writer.writeString("operationId", result.getOperationId());
        writer.writeString("paymentId", result.getPaymentId());
        writer.writeBoolean("expectedDuplicate", result.isExpectedDuplicate());
        writer.writeString("outcome", result.getOutcome().name());
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public Class<DedupResult> getCompactClass() {
        return DedupResult.class;
    }
}
