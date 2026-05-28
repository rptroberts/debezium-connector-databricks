/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.databricks.DatabricksConnectorConfig;

class DatabricksConnectionFactoryTest {

    @Test
    void patUrlContainsRequiredKeys() {
        DatabricksConnectorConfig c = buildConfig(Map.of(
                "topic.prefix", "test",
                "databricks.workspace.host", "adb-1.0.azuredatabricks.net",
                "databricks.warehouse.http.path", "/sql/1.0/warehouses/abc",
                "databricks.auth.type", "pat",
                "databricks.token", "dapi-xyz",
                "databricks.catalog", "main"));
        String url = new DatabricksConnectionFactory(c).jdbcUrl();
        assertThat(url).startsWith("jdbc:databricks://adb-1.0.azuredatabricks.net:443");
        assertThat(url).contains("httpPath=/sql/1.0/warehouses/abc");
        assertThat(url).contains("AuthMech=3");
        assertThat(url).contains("PWD=dapi-xyz");
        assertThat(url).contains("EnableArrow=1");
    }

    @Test
    void displayUrlOmitsSecrets() {
        DatabricksConnectorConfig c = buildConfig(Map.of(
                "topic.prefix", "test",
                "databricks.workspace.host", "adb-1.0.azuredatabricks.net",
                "databricks.warehouse.http.path", "/sql/1.0/warehouses/abc",
                "databricks.auth.type", "pat",
                "databricks.token", "DAPI-VERY-SECRET",
                "databricks.catalog", "main"));
        String url = new DatabricksConnectionFactory(c).jdbcUrlForDisplay();
        assertThat(url).doesNotContain("DAPI-VERY-SECRET");
        assertThat(url).contains("PAT(token=***)");
    }

    @Test
    void entraUrlContainsDiscoveryEndpoint() {
        DatabricksConnectorConfig c = buildConfig(Map.of(
                "topic.prefix", "test",
                "databricks.workspace.host", "adb-1.0.azuredatabricks.net",
                "databricks.warehouse.http.path", "/sql/1.0/warehouses/abc",
                "databricks.auth.type", "azure-entra-sp",
                "databricks.entra.tenant.id", "tid",
                "databricks.entra.client.id", "cid",
                "databricks.entra.client.secret", "sec",
                "databricks.catalog", "main"));
        String url = new DatabricksConnectionFactory(c).jdbcUrl();
        assertThat(url).contains("AuthMech=11");
        assertThat(url).contains("Auth_Flow=1");
        assertThat(url).contains("login.microsoftonline.com/tid");
    }

    @Test
    void subSecondTimeoutsRoundUpToOneSecond() {
        // Regression: integer division previously produced ConnTimeout=0 (driver: wait forever).
        DatabricksConnectorConfig c = buildConfig(Map.of(
                "topic.prefix", "test",
                "databricks.workspace.host", "h",
                "databricks.warehouse.http.path", "/p",
                "databricks.catalog", "main",
                "databricks.token", "t",
                "databricks.http.connect.timeout.ms", "500",
                "databricks.http.socket.timeout.ms", "999"));
        String url = new DatabricksConnectionFactory(c).jdbcUrl();
        assertThat(url).contains("ConnTimeout=1");
        assertThat(url).contains("SocketTimeout=1");
        assertThat(url).doesNotContain("ConnTimeout=0");
        assertThat(url).doesNotContain("SocketTimeout=0");
    }

    @Test
    void miUrlIncludesWorkspaceResourceId() {
        DatabricksConnectorConfig c = buildConfig(Map.of(
                "topic.prefix", "test",
                "databricks.workspace.host", "adb-1.0.azuredatabricks.net",
                "databricks.warehouse.http.path", "/sql/1.0/warehouses/abc",
                "databricks.auth.type", "azure-mi",
                "databricks.azure.workspace.resource.id", "/subscriptions/x/.../workspaces/w",
                "databricks.catalog", "main"));
        String url = new DatabricksConnectionFactory(c).jdbcUrl();
        assertThat(url).contains("AuthMech=11").contains("Auth_Flow=3");
        assertThat(url).contains("Azure_workspace_resource_id=/subscriptions/x/.../workspaces/w");
    }

    private static DatabricksConnectorConfig buildConfig(Map<String, String> overrides) {
        Map<String, String> base = new HashMap<>(overrides);
        return new DatabricksConnectorConfig(Configuration.from(base));
    }
}
