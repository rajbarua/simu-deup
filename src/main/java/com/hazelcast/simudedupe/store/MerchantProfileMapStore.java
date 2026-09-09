package com.hazelcast.simudedupe.store;

import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.simudedupe.merchantprofile.MerchantProfile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/** PostgreSQL write-behind store for the UPI merchant profile map. */
public class MerchantProfileMapStore extends MetadataAwareMapStore<MerchantProfile> {
    @Override
    protected String serialize(MerchantProfile value) {
        try {
            return JsonUtil.toJson(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected MerchantProfile deserialize(String payload) {
        if (payload == null) {
            return null;
        }
        try {
            return JsonUtil.beanFrom(payload, MerchantProfile.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void storeAll(Map<String, MetadataAwareValue<MerchantProfile>> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        String upsert = "INSERT INTO " + tableName()
                + " (id, payload, updateRunId, expirationTime) VALUES (?, ?::jsonb, ?, ?) "
                + "ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, "
                + "updateRunId = EXCLUDED.updateRunId, expirationTime = EXCLUDED.expirationTime";

        try (Connection connection = jdbcDataConnection().getConnection();
             PreparedStatement statement = connection.prepareStatement(upsert)) {
            int pending = 0;
            for (Map.Entry<String, MetadataAwareValue<MerchantProfile>> entry : entries.entrySet()) {
                MetadataAwareValue<MerchantProfile> metadata = entry.getValue();
                if (entry.getKey() == null || metadata == null || metadata.getValue() == null) {
                    continue;
                }
                MerchantProfile profile = metadata.getValue();
                statement.setString(1, entry.getKey());
                statement.setString(2, serialize(profile));
                statement.setLong(3, profile.getUpdateRunId());
                statement.setLong(4, expirationTime(metadata));
                statement.addBatch();
                if (++pending % batchSize() == 0) {
                    statement.executeBatch();
                }
            }
            if (pending % batchSize() != 0) {
                statement.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException("storeAll failed for " + entries.size() + " merchant profiles", e);
        }
    }
}
