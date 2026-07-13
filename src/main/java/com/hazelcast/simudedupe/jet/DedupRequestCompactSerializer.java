package com.hazelcast.simudedupe.jet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/** Compact serializer for requests stored in the Jet input IMap. */
public final class DedupRequestCompactSerializer implements CompactSerializer<DedupRequest> {
    private static final String TYPE_NAME = "com.hazelcast.simudedupe.jet.DedupRequest";

    @Override
    public DedupRequest read(CompactReader reader) {
        return new DedupRequest(
                reader.readString("operationId"),
                reader.readString("paymentId"),
                reader.readBoolean("expectedDuplicate"));
    }

    @Override
    public void write(CompactWriter writer, DedupRequest request) {
        writer.writeString("operationId", request.getOperationId());
        writer.writeString("paymentId", request.getPaymentId());
        writer.writeBoolean("expectedDuplicate", request.isExpectedDuplicate());
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public Class<DedupRequest> getCompactClass() {
        return DedupRequest.class;
    }
}
