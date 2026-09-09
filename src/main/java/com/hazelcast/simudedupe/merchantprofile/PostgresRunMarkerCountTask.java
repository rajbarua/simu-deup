package com.hazelcast.simudedupe.merchantprofile;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/** Counts active PostgreSQL rows written by one merchant-profile benchmark run. */
public final class PostgresRunMarkerCountTask implements Callable<Long>, HazelcastInstanceAware {
    private static final Pattern SQL_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final String NOW_MILLIS = "(EXTRACT(epoch FROM current_timestamp) * 1000)";

    private final String connectionName;
    private final String tableName;
    private final long updateRunId;
    private transient HazelcastInstance hazelcastInstance;

    public PostgresRunMarkerCountTask(String connectionName, String tableName, long updateRunId) {
        if (connectionName == null || connectionName.isBlank()) {
            throw new IllegalArgumentException("connectionName must not be blank");
        }
        if (tableName == null || !SQL_IDENTIFIER.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid PostgreSQL table name: " + tableName);
        }
        if (updateRunId <= 0) {
            throw new IllegalArgumentException("updateRunId must be positive");
        }
        this.connectionName = connectionName.trim();
        this.tableName = tableName;
        this.updateRunId = updateRunId;
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
                    + " WHERE updateRunId = ? AND expirationTime > " + NOW_MILLIS;
            try (Connection connection = dataConnection.getConnection();
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, updateRunId);
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

    long getUpdateRunId() {
        return updateRunId;
    }
}
