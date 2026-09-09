package com.hazelcast.simudedupe.merchantprofile;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/** Compact serializer for member-side merchant-profile PostgreSQL verification. */
public final class PostgresRunMarkerCountTaskCompactSerializer
        implements CompactSerializer<PostgresRunMarkerCountTask> {
    private static final String TYPE_NAME = "com.hazelcast.simudedupe.merchantprofile.PostgresRunMarkerCountTask";

    @Override
    public PostgresRunMarkerCountTask read(CompactReader reader) {
        return new PostgresRunMarkerCountTask(
                reader.readString("connectionName"),
                reader.readString("tableName"),
                reader.readInt64("updateRunId"));
    }

    @Override
    public void write(CompactWriter writer, PostgresRunMarkerCountTask task) {
        writer.writeString("connectionName", task.getConnectionName());
        writer.writeString("tableName", task.getTableName());
        writer.writeInt64("updateRunId", task.getUpdateRunId());
    }

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public Class<PostgresRunMarkerCountTask> getCompactClass() {
        return PostgresRunMarkerCountTask.class;
    }
}
