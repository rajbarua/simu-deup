package com.hazelcast.simudedupe.store;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.EntryStore;
import com.hazelcast.map.MapLoaderLifecycleSupport;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.regex.Pattern;

public abstract class MetadataAwareMapStore<V>
        implements EntryStore<String, V>, MapLoaderLifecycleSupport {
    private static final Pattern SQL_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final String NOW_MILLIS = "(EXTRACT(epoch FROM current_timestamp) * 1000)";

    private JdbcDataConnection jdbcDataConnection;
    private String tableName;
    private int batchSize;
    private long fallbackTtlMillis;
    private ILogger logger;

    protected abstract String serialize(V value);

    protected abstract V deserialize(String payload);

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        logger = hazelcastInstance.getLoggingService().getLogger(getClass());

        String connectionName = required(properties, "connectionName");
        jdbcDataConnection = hazelcastInstance.getDataConnectionService()
                .getAndRetainDataConnection(connectionName, JdbcDataConnection.class);
        tableName = checkedIdentifier(required(properties, "tableName"), "tableName");
        batchSize = intProperty(properties, "batchSize", 1_000);
        fallbackTtlMillis = longProperty(properties, "ttlSeconds", 31_622_400L) * 1_000L;

        logger.info("Initialized " + getClass().getSimpleName()
                + " map=" + mapName
                + " table=" + tableName
                + " connectionName=" + connectionName
                + " batchSize=" + batchSize);
    }

    @Override
    public void destroy() {
        if (jdbcDataConnection != null) {
            jdbcDataConnection.release();
            jdbcDataConnection = null;
        }
    }

    @Override
    public MetadataAwareValue<V> load(String key) {
        return null;
    }

    @Override
    public Map<String, MetadataAwareValue<V>> loadAll(Collection<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> list = keys instanceof List ? (List<String>) keys : new ArrayList<>(keys);
        Map<String, MetadataAwareValue<V>> result = new HashMap<>(list.size());
        final int chunkSize = 1_000;

        for (int offset = 0; offset < list.size(); offset += chunkSize) {
            List<String> chunk = list.subList(offset, Math.min(offset + chunkSize, list.size()));
            String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
            String select = "SELECT id, payload, expirationTime FROM " + tableName
                    + " WHERE id IN (" + placeholders + ")"
                    + " AND expirationTime > " + NOW_MILLIS;

            try (Connection connection = jdbcDataConnection.getConnection();
                 PreparedStatement statement = connection.prepareStatement(select)) {
                int parameter = 1;
                for (String key : chunk) {
                    statement.setString(parameter++, key);
                }
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        V value = deserialize(rs.getString(2));
                        if (value != null) {
                            result.put(rs.getString(1), new MetadataAwareValue<>(value, rs.getLong(3)));
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("loadAll failed for " + chunk.size() + " keys", e);
            }
        }
        return result;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return AllKeysIterator::new;
    }

    @Override
    public void store(String key, MetadataAwareValue<V> value) {
        storeAll(Collections.singletonMap(key, value));
    }

    @Override
    public void storeAll(Map<String, MetadataAwareValue<V>> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        String upsert = "INSERT INTO " + tableName + " (id, payload, expirationTime) VALUES (?, ?::jsonb, ?) "
                + "ON CONFLICT (id) DO UPDATE SET "
                + "payload = EXCLUDED.payload, "
                + "expirationTime = EXCLUDED.expirationTime";

        try (Connection connection = jdbcDataConnection.getConnection();
             PreparedStatement statement = connection.prepareStatement(upsert)) {
            int pending = 0;
            for (Map.Entry<String, MetadataAwareValue<V>> entry : entries.entrySet()) {
                MetadataAwareValue<V> metadataAwareValue = entry.getValue();
                if (entry.getKey() == null || metadataAwareValue == null || metadataAwareValue.getValue() == null) {
                    continue;
                }

                statement.setString(1, entry.getKey());
                statement.setString(2, serialize(metadataAwareValue.getValue()));
                statement.setLong(3, expirationTime(metadataAwareValue));
                statement.addBatch();
                if (++pending % batchSize == 0) {
                    statement.executeBatch();
                }
            }
            if (pending % batchSize != 0) {
                statement.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException("storeAll failed for " + entries.size() + " entries", e);
        }
    }

    @Override
    public void delete(String key) {
    }

    @Override
    public void deleteAll(Collection<String> keys) {
    }

    private long expirationTime(MetadataAwareValue<V> value) {
        long expirationTime = value.getExpirationTime();
        if (expirationTime == MetadataAwareValue.NO_TIME_SET) {
            return System.currentTimeMillis() + fallbackTtlMillis;
        }
        return expirationTime;
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing MapStore property: " + name);
        }
        return value.trim();
    }

    private static int intProperty(Properties properties, String name, int defaultValue) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value.trim());
    }

    private static long longProperty(Properties properties, String name, long defaultValue) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value.trim());
    }

    private static String checkedIdentifier(String value, String propertyName) {
        if (!SQL_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier in property " + propertyName + ": " + value);
        }
        return value;
    }

    private final class AllKeysIterator implements Iterator<String>, Closeable {
        private final Connection connection;
        private final PreparedStatement statement;
        private final ResultSet resultSet;
        private boolean checked;
        private boolean hasNext;

        private AllKeysIterator() {
            try {
                connection = jdbcDataConnection.getConnection();
                // PostgreSQL JDBC only uses a cursor when auto-commit is off.
                // Without this, an EAGER load buffers the complete keyset in
                // the driver before Hazelcast can start consuming it.
                connection.setAutoCommit(false);
                statement = connection.prepareStatement("SELECT id FROM " + tableName
                        + " WHERE expirationTime > " + NOW_MILLIS
                        + " ORDER BY id");
                statement.setFetchSize(batchSize);
                resultSet = statement.executeQuery();
            } catch (SQLException e) {
                close();
                throw new RuntimeException("loadAllKeys failed", e);
            }
        }

        @Override
        public boolean hasNext() {
            if (!checked) {
                try {
                    hasNext = resultSet.next();
                    if (!hasNext) {
                        close();
                    }
                    checked = true;
                } catch (SQLException e) {
                    close();
                    throw new RuntimeException(e);
                }
            }
            return hasNext;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                checked = false;
                return resultSet.getString(1);
            } catch (SQLException e) {
                close();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            closeQuietly(resultSet);
            closeQuietly(statement);
            closeQuietly(connection);
        }

        private void closeQuietly(AutoCloseable closeable) {
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }
}
