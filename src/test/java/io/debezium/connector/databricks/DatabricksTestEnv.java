/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves Databricks credentials for integration tests.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>Process environment variables (e.g. {@code DATABRICKS_HOST})</li>
 *   <li>{@code .env} file at the project root (skipped in CI)</li>
 * </ol>
 *
 * <p>If any required value is missing, integration tests should be skipped.
 */
public final class DatabricksTestEnv {

    public static final String HOST = "DATABRICKS_HOST";
    public static final String TOKEN = "DATABRICKS_TOKEN";
    public static final String HTTP_PATH = "DATABRICKS_HTTP_PATH";
    public static final String CATALOG = "DATABRICKS_CATALOG";
    public static final String SCHEMA = "DATABRICKS_SCHEMA";

    private final Map<String, String> values;

    private DatabricksTestEnv(Map<String, String> values) {
        this.values = values;
    }

    public static DatabricksTestEnv load() {
        Map<String, String> resolved = new HashMap<>();
        for (String key : new String[] { HOST, TOKEN, HTTP_PATH, CATALOG, SCHEMA }) {
            String v = System.getenv(key);
            if (v != null && !v.isBlank()) {
                resolved.put(key, v);
            }
        }
        if (resolved.size() < 5) {
            for (Path candidate : new Path[] { Paths.get(".env"), Paths.get("../.env") }) {
                if (Files.exists(candidate)) {
                    parseDotEnv(candidate).forEach(resolved::putIfAbsent);
                    break;
                }
            }
        }
        return new DatabricksTestEnv(resolved);
    }

    public boolean isComplete() {
        return values.containsKey(HOST) && values.containsKey(TOKEN)
                && values.containsKey(HTTP_PATH) && values.containsKey(CATALOG)
                && values.containsKey(SCHEMA);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(values.get(key));
    }

    public String require(String key) {
        return get(key).orElseThrow(() -> new IllegalStateException("Missing " + key));
    }

    public String host() {
        return require(HOST);
    }

    public String token() {
        return require(TOKEN);
    }

    public String httpPath() {
        return require(HTTP_PATH);
    }

    public String catalog() {
        return require(CATALOG);
    }

    public String schema() {
        return require(SCHEMA);
    }

    private static Map<String, String> parseDotEnv(Path path) {
        Map<String, String> out = new HashMap<>();
        try {
            for (String line : Files.readAllLines(path)) {
                String t = line.trim();
                if (t.isEmpty() || t.startsWith("#")) {
                    continue;
                }
                int eq = t.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String k = t.substring(0, eq).trim();
                String v = t.substring(eq + 1).trim();
                // strip optional matching quotes
                if (v.length() >= 2 && ((v.startsWith("\"") && v.endsWith("\"")) || (v.startsWith("'") && v.endsWith("'")))) {
                    v = v.substring(1, v.length() - 1);
                }
                out.put(k, v);
            }
        }
        catch (IOException e) {
            // best-effort
        }
        return out;
    }
}
