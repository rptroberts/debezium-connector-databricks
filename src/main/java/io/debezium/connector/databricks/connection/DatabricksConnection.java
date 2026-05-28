/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;

/**
 * Thin JDBC wrapper around a single connection to a Databricks SQL warehouse.
 *
 * <p>Not extended from Debezium's {@code JdbcConnection} on purpose: the
 * Databricks driver does not implement every {@code DatabaseMetaData} call
 * Debezium's relational machinery expects (catalog enumeration semantics
 * differ for Unity Catalog). We use plain JDBC + Unity Catalog
 * {@code information_schema} queries instead.
 */
public class DatabricksConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksConnection.class);

    private final DatabricksConnectionFactory factory;
    private final DatabricksConnectorConfig config;
    private final AtomicReference<Connection> conn = new AtomicReference<>();

    public DatabricksConnection(DatabricksConnectorConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = new DatabricksConnectionFactory(config);
    }

    public DatabricksConnection(DatabricksConnectionFactory factory) {
        this.factory = Objects.requireNonNull(factory, "factory");
        this.config = factory.config();
    }

    /**
     * Opens a connection if not already open. Idempotent.
     */
    public Connection connection() throws SQLException {
        Connection existing = conn.get();
        if (existing != null && !existing.isClosed()) {
            return existing;
        }
        synchronized (conn) {
            existing = conn.get();
            if (existing != null && !existing.isClosed()) {
                return existing;
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

    /**
     * Tests connectivity and required SELECT permissions on the workspace.
     *
     * @return a one-line summary suitable for startup logging
     */
    public String selfTest() throws SQLException {
        try (Statement st = connection().createStatement()) {
            st.setQueryTimeout((int) Duration.ofMillis(config.queryTimeoutMs()).toSeconds());
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
    }

    /**
     * Executes a query and processes each row with the given consumer.
     */
    public void query(String sql, Consumer<ResultSet> rowHandler) throws SQLException {
        try (Statement st = connection().createStatement()) {
            st.setQueryTimeout((int) Duration.ofMillis(config.queryTimeoutMs()).toSeconds());
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    rowHandler.accept(rs);
                }
            }
        }
    }

    /**
     * Executes a parameterized query.
     */
    public void preparedQuery(String sql, Consumer<PreparedStatement> binder, Consumer<ResultSet> rowHandler) throws SQLException {
        try (PreparedStatement ps = connection().prepareStatement(sql)) {
            ps.setQueryTimeout((int) Duration.ofMillis(config.queryTimeoutMs()).toSeconds());
            if (binder != null) {
                binder.accept(ps);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rowHandler.accept(rs);
                }
            }
        }
    }

    /**
     * Executes an arbitrary statement (DDL/DML used by IT setup only).
     */
    public void execute(String sql) throws SQLException {
        try (Statement st = connection().createStatement()) {
            st.setQueryTimeout((int) Duration.ofMillis(config.queryTimeoutMs()).toSeconds());
            st.execute(sql);
        }
    }

    public DatabricksConnectionFactory factory() {
        return factory;
    }

    @Override
    public void close() {
        Connection c = conn.getAndSet(null);
        if (c != null) {
            try {
                c.close();
            }
            catch (SQLException e) {
                LOG.warn("Error closing Databricks JDBC connection", e);
            }
        }
    }
}
