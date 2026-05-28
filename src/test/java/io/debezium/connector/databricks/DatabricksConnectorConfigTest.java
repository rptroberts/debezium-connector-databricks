/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.databricks.connection.auth.AuthMode;
import io.debezium.relational.TableId;

class DatabricksConnectorConfigTest {

    @Test
    void defaultsAreSensible() {
        DatabricksConnectorConfig c = build(Map.of(
                "topic.prefix", "p",
                "databricks.workspace.host", "h",
                "databricks.warehouse.http.path", "/path",
                "databricks.catalog", "cat",
                "databricks.token", "t"));
        assertThat(c.authMode()).isEqualTo(AuthMode.PAT);
        assertThat(c.pollInterval().toMillis()).isEqualTo(5000L);
        assertThat(c.cdfBatchMaxVersions()).isEqualTo(100L);
        assertThat(c.snapshotMode()).isEqualTo(DatabricksConnectorConfig.SnapshotMode.INITIAL);
        assertThat(c.disruptiveOpHandling()).isEqualTo(DatabricksConnectorConfig.DisruptiveOpHandling.TRUNCATE);
    }

    @Test
    void rejectsEntraConfigWithoutSecret() {
        Configuration cfg = Configuration.from(Map.of(
                "topic.prefix", "p",
                "databricks.workspace.host", "h",
                "databricks.warehouse.http.path", "/path",
                "databricks.catalog", "cat",
                "databricks.auth.type", "azure-entra-sp",
                "databricks.entra.tenant.id", "t",
                "databricks.entra.client.id", "c"));
        assertThatThrownBy(() -> DatabricksConnectorConfig.requireAuthFieldsPresent(cfg))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("databricks.entra.client.secret");
    }

    @Test
    void rejectsMiConfigWithoutResourceId() {
        Configuration cfg = Configuration.from(Map.of(
                "topic.prefix", "p",
                "databricks.workspace.host", "h",
                "databricks.warehouse.http.path", "/path",
                "databricks.catalog", "cat",
                "databricks.auth.type", "azure-mi"));
        assertThatThrownBy(() -> DatabricksConnectorConfig.requireAuthFieldsPresent(cfg))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("databricks.azure.workspace.resource.id");
    }

    @Test
    void tableIncludeFilterIsRegex() {
        DatabricksConnectorConfig c = build(Map.of(
                "topic.prefix", "p",
                "databricks.workspace.host", "h",
                "databricks.warehouse.http.path", "/path",
                "databricks.catalog", "cat",
                "databricks.token", "t",
                "table.include.list", "main\\.banking\\.(customer|account|transaction)"));
        assertThat(c.isTableIncluded(new TableId("main", "banking", "customer"))).isTrue();
        assertThat(c.isTableIncluded(new TableId("main", "banking", "account"))).isTrue();
        assertThat(c.isTableIncluded(new TableId("main", "banking", "ledger"))).isFalse();
        assertThat(c.isTableIncluded(new TableId("other", "banking", "customer"))).isFalse();
    }

    @Test
    void excludeOverridesInclude() {
        DatabricksConnectorConfig c = build(Map.of(
                "topic.prefix", "p",
                "databricks.workspace.host", "h",
                "databricks.warehouse.http.path", "/path",
                "databricks.catalog", "cat",
                "databricks.token", "t",
                "table.include.list", "main\\..*\\..*",
                "table.exclude.list", "main\\.audit\\..*"));
        assertThat(c.isTableIncluded(new TableId("main", "banking", "customer"))).isTrue();
        assertThat(c.isTableIncluded(new TableId("main", "audit", "log"))).isFalse();
    }

    private static DatabricksConnectorConfig build(Map<String, String> overrides) {
        Map<String, String> base = new HashMap<>(overrides);
        return new DatabricksConnectorConfig(Configuration.from(base));
    }
}
