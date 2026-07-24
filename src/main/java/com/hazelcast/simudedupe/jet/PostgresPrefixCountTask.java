package com.hazelcast.simudedupe.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/** Counts active PostgreSQL rows for one fixed-width payment-ID prefix on a Hazelcast member. */
public final class PostgresPrefixCountTask implements Callable<Long>, HazelcastInstanceAware {
    private static final Pattern SQL_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern NUMERIC_PREFIX = Pattern.compile("[0-9]{1,24}");
    private static final String NOW_MILLIS = "(EXTRACT(epoch FROM current_timestamp) * 1000)";

    private final String connectionName;
    private final String tableName;
    private final String idPrefix;
    private transient HazelcastInstance hazelcastInstance;

    public PostgresPrefixCountTask(String connectionName, String tableName, String idPrefix) {
        this.connectionName = requireNonBlank(connectionName, "connectionName");
        this.tableName = checkedTableName(tableName);
        this.idPrefix = checkedIdPrefix(idPrefix);
    }

    @Override
    public Long call() throws Exception {
        if (hazelcastInstance == null) {
            throw new IllegalStateException("HazelcastInstance was not injected into PostgreSQL verification task");
        }

        JdbcDataConnection dataConnection = hazelcastInstance.getDataConnectionService()
                .getAndRetainDataConnection(connectionName, JdbcDataConnection.class);
        try {
            String sql = "SELECT COUNT(*) FROM " + tableName
                    + " WHERE id LIKE ? AND expirationTime > " + NOW_MILLIS;
            try (Connection connection = dataConnection.getConnection();
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, idPrefix + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new IllegalStateException("PostgreSQL count query returned no row");
                    }
                    return resultSet.getLong(1);
                }
            }
        } finally {
            dataConnection.release();
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = Objects.requireNonNull(hazelcastInstance, "hazelcastInstance");
    }

    String getConnectionName() {
        return connectionName;
    }

    String getTableName() {
        return tableName;
    }

    String getIdPrefix() {
        return idPrefix;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static String checkedTableName(String value) {
        String tableName = requireNonBlank(value, "tableName");
        if (!SQL_IDENTIFIER.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid PostgreSQL table name: " + tableName);
        }
        return tableName;
    }

    private static String checkedIdPrefix(String value) {
        String idPrefix = requireNonBlank(value, "idPrefix");
        if (!NUMERIC_PREFIX.matcher(idPrefix).matches()) {
            throw new IllegalArgumentException("PostgreSQL verification prefix must contain 1 to 24 digits");
        }
        return idPrefix;
    }
}
