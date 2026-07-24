package com.hazelcast.simudedupe.jet;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/** Compact serializer for the member-side PostgreSQL verification task. */
public final class PostgresPrefixCountTaskCompactSerializer implements CompactSerializer<PostgresPrefixCountTask> {
    private static final String TYPE_NAME = "com.hazelcast.simudedupe.jet.PostgresPrefixCountTask";

    @Override
    public PostgresPrefixCountTask read(CompactReader reader) {
        return new PostgresPrefixCountTask(
                reader.readString("connectionName"),
                reader.readString("tableName"),
                reader.readString("idPrefix"));
    }

    @Override
    public void write(CompactWriter writer, PostgresPrefixCountTask task) {
        writer.writeString("connectionName", task.getConnectionName());
        writer.writeString("tableName", task.getTableName());
        writer.writeString("idPrefix", task.getIdPrefix());
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public Class<PostgresPrefixCountTask> getCompactClass() {
        return PostgresPrefixCountTask.class;
    }
}
