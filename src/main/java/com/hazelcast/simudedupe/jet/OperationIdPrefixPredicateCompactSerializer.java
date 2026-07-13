package com.hazelcast.simudedupe.jet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/** Compact serializer for the run/producer predicate sent to members. */
public final class OperationIdPrefixPredicateCompactSerializer
        implements CompactSerializer<OperationIdPrefixPredicate> {
    private static final String TYPE_NAME = "com.hazelcast.simudedupe.jet.OperationIdPrefixPredicate";

    @Override
    public OperationIdPrefixPredicate read(CompactReader reader) {
        return new OperationIdPrefixPredicate<>(reader.readString("prefix"));
    }

    @Override
    public void write(CompactWriter writer, OperationIdPrefixPredicate predicate) {
        writer.writeString("prefix", predicate.getPrefix());
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public Class<OperationIdPrefixPredicate> getCompactClass() {
        return OperationIdPrefixPredicate.class;
    }
}
