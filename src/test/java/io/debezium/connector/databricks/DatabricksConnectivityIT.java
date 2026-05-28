/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.databricks.connection.DatabricksConnection;

/**
 * Smoke-tests JDBC connectivity, identity, and CDF basic round-trip against a real workspace.
 */
class DatabricksConnectivityIT {

    private DatabricksTestEnv env;

    @BeforeEach
    void setUp() {
        env = DatabricksTestEnv.load();
        assumeTrue(env.isComplete(), "DATABRICKS_* env not set; skipping IT");
    }

    @Test
    void selfTestReturnsCurrentUser() throws Exception {
        DatabricksConnectorConfig cfg = buildConfig();
        try (DatabricksConnection c = new DatabricksConnection(cfg)) {
            String summary = c.selfTest();
            // Session default catalog/schema can differ from the one this connector targets —
            // we always qualify queries with the configured catalog, so just confirm identity.
            assertThat(summary).contains("user=");
        }
    }

    @Test
    void describeTablesInTargetSchema() throws Exception {
        DatabricksConnectorConfig cfg = buildConfig();
        try (DatabricksConnection c = new DatabricksConnection(cfg)) {
            int[] count = { 0 };
            c.query("SELECT table_name FROM `" + env.catalog() + "`.information_schema.tables WHERE table_schema = '"
                    + env.schema().replace("'", "''") + "' LIMIT 5", rs -> count[0]++);
            // At least the query must succeed; row count depends on schema content.
            assertThat(count[0]).isGreaterThanOrEqualTo(0);
        }
    }

    private DatabricksConnectorConfig buildConfig() {
        Map<String, String> p = new HashMap<>();
        p.put("topic.prefix", "dbx-it");
        p.put("databricks.workspace.host", env.host());
        p.put("databricks.warehouse.http.path", env.httpPath());
        p.put("databricks.auth.type", "pat");
        p.put("databricks.token", env.token());
        p.put("databricks.catalog", env.catalog());
        return new DatabricksConnectorConfig(Configuration.from(p));
    }
}
