package com.hazelcast.simudedupe.store;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.simudedupe.DedupKey;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
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

public class PaymentDedupMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport {
    private static final Pattern SQL_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private String jdbcUrl;
    private String username;
    private String password;
    private String tableName;
    private int batchSize;
    private int loadBatchSize;
    private int loadFetchSize;
    private int loadWindowDays;
    private int retentionDays;
    private ILogger logger;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        logger = hazelcastInstance.getLoggingService().getLogger(getClass());
        jdbcUrl = required(properties, "jdbcUrl");
        username = required(properties, "username");
        password = required(properties, "password");
        tableName = checkedIdentifier(required(properties, "tableName"), "tableName");
        batchSize = intProperty(properties, "batchSize", 5_000);
        loadBatchSize = intProperty(properties, "loadBatchSize", 1_000);
        loadFetchSize = intProperty(properties, "loadFetchSize", 10_000);
        loadWindowDays = intProperty(properties, "loadWindowDays", 366);
        retentionDays = intProperty(properties, "retentionDays", 366);
        logger.info("Initialized PaymentDedupMapStore map=" + mapName
                + " table=" + tableName
                + " loadWindowDays=" + loadWindowDays
                + " retentionDays=" + retentionDays);
    }

    @Override
    public void destroy() {
    }

    @Override
    public String load(String key) {
        return null;
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> list = keys instanceof List ? (List<String>) keys : new ArrayList<>(keys);
        Map<String, String> result = new HashMap<>(list.size());
        String select = "SELECT id, first_seen_at FROM " + tableName
                + " WHERE id IN (%s)"
                + " AND first_seen_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))"
                + " AND expires_at > CURRENT_TIMESTAMP";

        for (int offset = 0; offset < list.size(); offset += loadBatchSize) {
            List<String> chunk = list.subList(offset, Math.min(offset + loadBatchSize, list.size()));
            String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(String.format(select, placeholders))) {
                int parameter = 1;
                for (String key : chunk) {
                    statement.setString(parameter++, key);
                }
                statement.setInt(parameter, loadWindowDays);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        Timestamp firstSeen = rs.getTimestamp(2);
                        result.put(rs.getString(1), Long.toString(firstSeen.toInstant().toEpochMilli()));
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
        return KeyIterator::new;
    }

    @Override
    public void store(String key, String value) {
        storeAll(Collections.singletonMap(key, value));
    }

    @Override
    public void storeAll(Map<String, String> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        String upsert = "INSERT INTO " + tableName
                + " (id, service_name, payment_id, first_seen_at, last_seen_at, expires_at)"
                + " VALUES (?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (id) DO UPDATE SET"
                + " last_seen_at = EXCLUDED.last_seen_at,"
                + " expires_at = GREATEST(" + tableName + ".expires_at, EXCLUDED.expires_at),"
                + " seen_count = " + tableName + ".seen_count + 1";

        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(upsert)) {
            int pending = 0;
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                long firstSeenMillis = firstSeenMillis(entry.getValue());
                Timestamp firstSeen = Timestamp.from(Instant.ofEpochMilli(firstSeenMillis));
                Timestamp lastSeen = Timestamp.from(Instant.now());
                Timestamp expiresAt = Timestamp.from(Instant.ofEpochMilli(firstSeenMillis)
                        .plusSeconds(retentionDays * 86_400L));

                String key = entry.getKey();
                statement.setString(1, key);
                statement.setString(2, DedupKey.serviceName(key));
                statement.setString(3, DedupKey.paymentId(key));
                statement.setTimestamp(4, firstSeen);
                statement.setTimestamp(5, lastSeen);
                statement.setTimestamp(6, expiresAt);
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

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    private long firstSeenMillis(String value) {
        if (value == null || value.isBlank()) {
            return System.currentTimeMillis();
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return System.currentTimeMillis();
        }
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

    private static String checkedIdentifier(String value, String propertyName) {
        if (!SQL_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier in property " + propertyName + ": " + value);
        }
        return value;
    }

    private final class KeyIterator implements Iterator<String>, AutoCloseable {
        private final Connection connection;
        private final PreparedStatement statement;
        private final ResultSet resultSet;
        private boolean checked;
        private boolean hasNext;

        private KeyIterator() {
            try {
                connection = connection();
                connection.setAutoCommit(false);
                statement = connection.prepareStatement("SELECT id FROM " + tableName
                        + " WHERE first_seen_at >= (CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))"
                        + " AND expires_at > CURRENT_TIMESTAMP"
                        + " ORDER BY id");
                statement.setFetchSize(loadFetchSize);
                statement.setInt(1, loadWindowDays);
                resultSet = statement.executeQuery();
            } catch (SQLException e) {
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
