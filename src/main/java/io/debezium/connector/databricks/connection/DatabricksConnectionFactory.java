/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.connector.databricks.DatabricksConnectorConfig;
import io.debezium.connector.databricks.connection.auth.AuthProvider;

/**
 * Assembles JDBC connection URLs and property maps for the Databricks JDBC driver.
 *
 * <p>Output URL form:
 * {@code jdbc:databricks://<host>:443;httpPath=<path>;<auth-params>;<extra>}.
 */
public final class DatabricksConnectionFactory {

    private static final String JDBC_PREFIX = "jdbc:databricks://";
    private static final int DEFAULT_PORT = 443;
    private static final String DRIVER_CLASS = "com.databricks.client.jdbc.Driver";

    private final DatabricksConnectorConfig config;
    private final AuthProvider authProvider;

    public DatabricksConnectionFactory(DatabricksConnectorConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.authProvider = AuthProvider.from(config);
    }

    public String driverClass() {
        return DRIVER_CLASS;
    }

    public AuthProvider authProvider() {
        return authProvider;
    }

    /**
     * Builds the complete JDBC URL. Secrets are inlined as connection
     * parameters in the URL — keep this string out of logs.
     */
    public String jdbcUrl() {
        StringBuilder sb = new StringBuilder(JDBC_PREFIX)
                .append(config.workspaceHost())
                .append(":")
                .append(DEFAULT_PORT);
        appendParam(sb, "httpPath", config.warehouseHttpPath());
        appendParam(sb, "EnableArrow", "1");
        appendParam(sb, "EnableQueryResultLZ4Compression", "1");
        appendParam(sb, "RowsFetchedPerBlock", String.valueOf(config.jdbcRowsFetchedPerBlock()));
        appendParam(sb, "SSL", "1");
        // ConnTimeout/SocketTimeout are seconds. Round sub-second configs up to 1s so the driver
        // doesn't interpret 0 as "wait forever".
        appendParam(sb, "ConnTimeout", String.valueOf(toSecondsCeil(config.httpConnectTimeoutMs())));
        appendParam(sb, "SocketTimeout", String.valueOf(toSecondsCeil(config.httpSocketTimeoutMs())));
        for (Map.Entry<String, String> e : authProvider.jdbcProperties().entrySet()) {
            appendParam(sb, e.getKey(), e.getValue());
        }
        if (config.httpProxyHost() != null && !config.httpProxyHost().isBlank()) {
            appendParam(sb, "UseProxy", "1");
            appendParam(sb, "ProxyHost", config.httpProxyHost());
            appendParam(sb, "ProxyPort", String.valueOf(config.httpProxyPort()));
            if (config.httpProxyUser() != null && !config.httpProxyUser().isBlank()) {
                appendParam(sb, "ProxyUID", config.httpProxyUser());
                appendParam(sb, "ProxyPWD", config.httpProxyPassword());
            }
        }
        return sb.toString();
    }

    /**
     * URL with secrets redacted, safe for logging.
     */
    public String jdbcUrlForDisplay() {
        return JDBC_PREFIX + config.workspaceHost() + ":" + DEFAULT_PORT +
                ";httpPath=" + config.warehouseHttpPath() +
                ";auth=" + authProvider.describe();
    }

    public Map<String, String> connectionProperties() {
        Map<String, String> p = new LinkedHashMap<>(authProvider.jdbcProperties());
        p.put("httpPath", config.warehouseHttpPath());
        return p;
    }

    public DatabricksConnectorConfig config() {
        return config;
    }

    private static void appendParam(StringBuilder sb, String k, String v) {
        if (v == null || v.isEmpty()) {
            return;
        }
        sb.append(';').append(k).append('=').append(v);
    }

    private static long toSecondsCeil(long ms) {
        if (ms <= 0) {
            return 0;
        }
        return Math.max(1L, (ms + 999L) / 1000L);
    }
}
