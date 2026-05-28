/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;

/**
 * JDBC wrapper around a single connection to a Databricks SQL warehouse.
 *
 * <p>Adds three things the bare JDBC API doesn't provide:
 * <ol>
 *   <li>Lazy + thread-safe open: the first {@link #connection()} call drives the auth flow.</li>
 *   <li>Liveness probe + automatic reconnect: every {@link #connection()} call quickly tests
 *       the existing handle with {@link Connection#isValid} and rebuilds if it has gone stale
 *       (warehouse idle-shutdown, server-side disconnect, network drop).</li>
 *   <li>Retry-on-transient: query helpers run inside {@link #withRetry} which transparently
 *       reopens the connection and retries once on connection-level SQLExceptions.</li>
 * </ol>
 */
public class DatabricksConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksConnection.class);
    private static final int VALIDITY_TIMEOUT_SECONDS = 2;

    private final DatabricksConnectionFactory factory;
    private final DatabricksConnectorConfig config;
    private final AtomicReference<Connection> conn = new AtomicReference<>();
    private final AtomicLong reconnects = new AtomicLong();
    private final Object openLock = new Object();

    public DatabricksConnection(DatabricksConnectorConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = new DatabricksConnectionFactory(config);
    }

    public DatabricksConnection(DatabricksConnectionFactory factory) {
        this.factory = Objects.requireNonNull(factory, "factory");
        this.config = factory.config();
    }

    /**
     * Returns a live connection, reopening if the previous handle is closed or
     * no longer valid.
     */
    public Connection connection() throws SQLException {
        Connection existing = conn.get();
        if (existing != null && isLive(existing)) {
            return existing;
        }
        return openOrReopen();
    }

    private Connection openOrReopen() throws SQLException {
        synchronized (openLock) {
            Connection existing = conn.get();
            if (existing != null && isLive(existing)) {
                return existing;
            }
            if (existing != null) {
                closeQuietly(existing);
                reconnects.incrementAndGet();
                LOG.info("Databricks JDBC connection went stale; reopening");
            }
            try {
                Class.forName(factory.driverClass());
            }
            catch (ClassNotFoundException e) {
                throw new SQLException("Databricks JDBC driver class not found on classpath: " + factory.driverClass(), e);
            }
            String url = factory.jdbcUrl();
            LOG.info("Opening Databricks JDBC connection to {}", factory.jdbcUrlForDisplay());
            Connection c = java.sql.DriverManager.getConnection(url, new Properties());
            conn.set(c);
            return c;
        }
    }

    private static boolean isLive(Connection c) {
        try {
            if (c.isClosed()) {
                return false;
            }
            return c.isValid(VALIDITY_TIMEOUT_SECONDS);
        }
        catch (SQLException e) {
            return false;
        }
    }

    /**
     * Number of times the underlying JDBC connection has been re-opened after going stale.
     * Useful for ops dashboards / tests.
     */
    public long reconnectCount() {
        return reconnects.get();
    }

    /**
     * Tests connectivity and required SELECT permissions on the workspace.
     *
     * @return a one-line summary suitable for startup logging
     */
    public String selfTest() throws SQLException {
        return withRetry("self-test", () -> {
            try (Statement st = connection().createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds());
                try (ResultSet rs = st.executeQuery(
                        "SELECT current_user() AS u, current_catalog() AS c, current_schema() AS s")) {
                    if (!rs.next()) {
                        throw new SQLException("Self-test query returned no rows");
                    }
                    String user = rs.getString("u");
                    String catalog = rs.getString("c");
                    String schema = rs.getString("s");
                    return "user=" + user + " catalog=" + catalog + " schema=" + schema;
                }
            }
        });
    }

    /**
     * Executes a query and processes each row with the given consumer.
     */
    public void query(String sql, Consumer<ResultSet> rowHandler) throws SQLException {
        withRetry("query", () -> {
            try (Statement st = connection().createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds());
                try (ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {
                        rowHandler.accept(rs);
                    }
                }
            }
            return null;
        });
    }

    /**
     * Executes a parameterized query.
     */
    public void preparedQuery(String sql, Consumer<PreparedStatement> binder, Consumer<ResultSet> rowHandler) throws SQLException {
        withRetry("preparedQuery", () -> {
            try (PreparedStatement ps = connection().prepareStatement(sql)) {
                ps.setQueryTimeout(queryTimeoutSeconds());
                if (binder != null) {
                    binder.accept(ps);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rowHandler.accept(rs);
                    }
                }
            }
            return null;
        });
    }

    /**
     * Executes an arbitrary statement (DDL/DML used by IT setup only).
     */
    public void execute(String sql) throws SQLException {
        withRetry("execute", () -> {
            try (Statement st = connection().createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds());
                st.execute(sql);
            }
            return null;
        });
    }

    public DatabricksConnectionFactory factory() {
        return factory;
    }

    @Override
    public void close() {
        Connection c = conn.getAndSet(null);
        if (c != null) {
            closeQuietly(c);
        }
    }

    private int queryTimeoutSeconds() {
        return (int) Math.max(1L, Duration.ofMillis(config.queryTimeoutMs()).toSeconds());
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        }
        catch (SQLException e) {
            LOG.warn("Error closing Databricks JDBC connection: {}", e.getMessage());
        }
    }

    /**
     * Wraps a SQL operation with a single retry on transient connection errors.
     * Reopens the connection between attempts.
     */
    private <T> T withRetry(String op, SqlBlock<T> block) throws SQLException {
        try {
            return block.run();
        }
        catch (SQLException e) {
            if (!isRetriable(e)) {
                throw e;
            }
            LOG.warn("Databricks {} failed with retriable error ({}); reconnecting + retrying once",
                    op, e.getMessage());
            // Force reopen.
            Connection stale = conn.getAndSet(null);
            if (stale != null) {
                closeQuietly(stale);
            }
            try {
                openOrReopen();
            }
            catch (SQLException reopenError) {
                SQLException combined = new SQLException(
                        "Failed to reopen Databricks connection during retry: " + reopenError.getMessage(), e);
                combined.addSuppressed(reopenError);
                throw combined;
            }
            return block.run();
        }
    }

    private static boolean isRetriable(SQLException e) {
        if (e instanceof SQLNonTransientConnectionException
                || e instanceof SQLRecoverableException
                || e instanceof SQLTransientException) {
            return true;
        }
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase();
        return lower.contains("connection is closed")
                || lower.contains("connection reset")
                || lower.contains("connection refused")
                || lower.contains("broken pipe")
                || lower.contains("warehouse is stopped");
    }

    @FunctionalInterface
    private interface SqlBlock<T> {
        T run() throws SQLException;
    }
}
