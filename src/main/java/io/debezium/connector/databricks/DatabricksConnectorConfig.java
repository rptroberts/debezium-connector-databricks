/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.databricks.connection.auth.AuthMode;
import io.debezium.relational.TableId;

/**
 * Strongly-typed configuration for the Databricks connector.
 *
 * <p>Extends {@link CommonConnectorConfig} rather than
 * {@code RelationalDatabaseConnectorConfig} because the connector does not use
 * Debezium's full relational schema machinery (Delta CDF returns rows under
 * current schema; there is no DDL log to historize).
 */
public class DatabricksConnectorConfig extends CommonConnectorConfig {

    // ----- Snapshot mode -----

    public enum SnapshotMode implements EnumeratedValue {
        NEVER("never"),
        INITIAL("initial"),
        INITIAL_ONLY("initial_only"),
        WHEN_NEEDED("when_needed");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static SnapshotMode parse(String s, String defaultValue) {
            for (SnapshotMode m : values()) {
                if (m.value.equalsIgnoreCase(s)) {
                    return m;
                }
            }
            for (SnapshotMode m : values()) {
                if (m.value.equalsIgnoreCase(defaultValue)) {
                    return m;
                }
            }
            return INITIAL;
        }
    }

    public enum DisruptiveOpHandling implements EnumeratedValue {
        WARN("warn"),
        TRUNCATE("truncate"),
        RESNAPSHOT("resnapshot"),
        FAIL("fail");

        private final String value;

        DisruptiveOpHandling(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static DisruptiveOpHandling parse(String s) {
            if (s == null) {
                return TRUNCATE;
            }
            for (DisruptiveOpHandling m : values()) {
                if (m.value.equalsIgnoreCase(s.trim())) {
                    return m;
                }
            }
            return TRUNCATE;
        }
    }

    // ----- Connection fields -----

    public static final Field WORKSPACE_HOST = Field.create("databricks.workspace.host")
            .withDisplayName("Workspace host")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("Azure Databricks workspace hostname, e.g. adb-XXXX.X.azuredatabricks.net (no scheme).");

    public static final Field WAREHOUSE_HTTP_PATH = Field.create("databricks.warehouse.http.path")
            .withDisplayName("SQL warehouse HTTP path")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("HTTP path of the SQL warehouse to query, e.g. /sql/1.0/warehouses/abc123.");

    public static final Field AUTH_TYPE = Field.create("databricks.auth.type")
            .withDisplayName("Authentication type")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault("pat")
            .withDescription("One of: pat, azure-entra-sp, azure-mi.");

    public static final Field TOKEN = Field.create("databricks.token")
            .withDisplayName("Databricks PAT")
            .withType(Type.PASSWORD)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Personal Access Token (required when databricks.auth.type=pat).");

    public static final Field ENTRA_TENANT_ID = Field.create("databricks.entra.tenant.id")
            .withDisplayName("Entra tenant ID")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Microsoft Entra ID tenant UUID (required when databricks.auth.type=azure-entra-sp).");

    public static final Field ENTRA_CLIENT_ID = Field.create("databricks.entra.client.id")
            .withDisplayName("Entra client ID")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Microsoft Entra ID app registration client ID.");

    public static final Field ENTRA_CLIENT_SECRET = Field.create("databricks.entra.client.secret")
            .withDisplayName("Entra client secret")
            .withType(Type.PASSWORD)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Microsoft Entra ID app registration client secret.");

    public static final Field MI_CLIENT_ID = Field.create("databricks.mi.client.id")
            .withDisplayName("Managed Identity client ID")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Client ID for a user-assigned managed identity (omit for system-assigned).");

    public static final Field AZURE_WORKSPACE_RESOURCE_ID = Field.create("databricks.azure.workspace.resource.id")
            .withDisplayName("Azure workspace ARM resource ID")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Full ARM resource ID of the Databricks workspace (required for managed identity auth).");

    public static final Field CATALOG = Field.create("databricks.catalog")
            .withDisplayName("Unity Catalog name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("Unity Catalog containing the tables to capture.");

    public static final Field SCHEMA_INCLUDE_LIST = Field.create("schema.include.list")
            .withDisplayName("Schema include regex list")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Comma-separated regular expressions matching <catalog>.<schema> to capture.");

    public static final Field SCHEMA_EXCLUDE_LIST = Field.create("schema.exclude.list")
            .withDisplayName("Schema exclude regex list")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Comma-separated regular expressions matching <catalog>.<schema> to exclude.");

    public static final Field TABLE_INCLUDE_LIST = Field.create("table.include.list")
            .withDisplayName("Table include regex list")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Comma-separated regular expressions matching <catalog>.<schema>.<table> to capture.");

    public static final Field TABLE_EXCLUDE_LIST = Field.create("table.exclude.list")
            .withDisplayName("Table exclude regex list")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Comma-separated regular expressions matching <catalog>.<schema>.<table> to exclude.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(SnapshotMode.INITIAL.getValue())
            .withDescription("One of: never, initial, initial_only, when_needed.");

    // ----- CDF tuning -----

    public static final Field CDF_POLL_INTERVAL_MS = Field.create("cdf.poll.interval.ms")
            .withDisplayName("CDF poll interval (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(5000L)
            .withDescription("Delay between CDF range polls when caught up.");

    public static final Field CDF_BATCH_MAX_VERSIONS = Field.create("cdf.batch.max.versions")
            .withDisplayName("Max _commit_version span per batch")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(100L)
            .withDescription("Upper bound on the (endVersion - startVersion) span passed to table_changes() per poll.");

    public static final Field CDF_BATCH_MAX_ROWS = Field.create("cdf.batch.max.rows")
            .withDisplayName("Max rows per batch (soft)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(50000L)
            .withDescription("Soft cap on rows fetched per polling cycle.");

    public static final Field CDF_QUERY_TIMEOUT_MS = Field.create("cdf.query.timeout.ms")
            .withDisplayName("Per-query timeout (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(60000L)
            .withDescription("Timeout applied to all SQL statements issued by the connector.");

    public static final Field CDF_TABLES_METADATA_REFRESH_MS = Field.create("cdf.tables.metadata.refresh.ms")
            .withDisplayName("Table metadata refresh (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(300_000L)
            .withDescription("How often to refresh per-table column metadata / PK from information_schema.");

    public static final Field CDF_DISRUPTIVE_OP_HANDLING = Field.create("cdf.disruptive.operation.handling")
            .withDisplayName("Disruptive op handling")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(DisruptiveOpHandling.TRUNCATE.getValue())
            .withDescription("How to react when DESCRIBE HISTORY shows TRUNCATE / REPLACE / RESTORE: warn, truncate, resnapshot, fail.");

    public static final Field CDF_LAG_ALERT_VERSIONS = Field.create("cdf.lag.alert.versions")
            .withDisplayName("Lag alert (versions)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0L)
            .withDescription("Emit a WARN log when the connector falls behind the table head by this many _commit_versions (0 to disable).");

    // ----- JDBC / HTTP tuning -----

    public static final Field JDBC_ROWS_FETCHED_PER_BLOCK = Field.create("databricks.jdbc.rows.fetched.per.block")
            .withDisplayName("Rows per Arrow block")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(100_000)
            .withDescription("Tuning for the Databricks JDBC driver's Arrow result fetch size.");

    public static final Field HTTP_CONNECT_TIMEOUT_MS = Field.create("databricks.http.connect.timeout.ms")
            .withDisplayName("HTTP connect timeout")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(30_000L);

    public static final Field HTTP_SOCKET_TIMEOUT_MS = Field.create("databricks.http.socket.timeout.ms")
            .withDisplayName("HTTP socket timeout")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(120_000L);

    public static final Field HTTP_PROXY_HOST = Field.create("databricks.http.proxy.host")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW);

    public static final Field HTTP_PROXY_PORT = Field.create("databricks.http.proxy.port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0);

    public static final Field HTTP_PROXY_USER = Field.create("databricks.http.proxy.user")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW);

    public static final Field HTTP_PROXY_PASSWORD = Field.create("databricks.http.proxy.password")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW);

    // ----- Definition -----

    public static final ConfigDefinition CONFIG_DEFINITION = CommonConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Databricks")
            .connector(
                    WORKSPACE_HOST,
                    WAREHOUSE_HTTP_PATH,
                    AUTH_TYPE,
                    TOKEN,
                    ENTRA_TENANT_ID,
                    ENTRA_CLIENT_ID,
                    ENTRA_CLIENT_SECRET,
                    MI_CLIENT_ID,
                    AZURE_WORKSPACE_RESOURCE_ID,
                    CATALOG,
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    TABLE_INCLUDE_LIST,
                    TABLE_EXCLUDE_LIST,
                    SNAPSHOT_MODE,
                    CDF_POLL_INTERVAL_MS,
                    CDF_BATCH_MAX_VERSIONS,
                    CDF_BATCH_MAX_ROWS,
                    CDF_QUERY_TIMEOUT_MS,
                    CDF_TABLES_METADATA_REFRESH_MS,
                    CDF_DISRUPTIVE_OP_HANDLING,
                    CDF_LAG_ALERT_VERSIONS,
                    JDBC_ROWS_FETCHED_PER_BLOCK,
                    HTTP_CONNECT_TIMEOUT_MS,
                    HTTP_SOCKET_TIMEOUT_MS,
                    HTTP_PROXY_HOST,
                    HTTP_PROXY_PORT,
                    HTTP_PROXY_USER,
                    HTTP_PROXY_PASSWORD)
            .create();

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public static Iterable<Field> allFields() {
        return CONFIG_DEFINITION.all();
    }

    private final Configuration raw;

    public DatabricksConnectorConfig(Configuration config) {
        super(config, 0);
        this.raw = config;
    }

    public Configuration rawConfig() {
        return raw;
    }

    // ----- Typed accessors -----

    public String workspaceHost() {
        return raw.getString(WORKSPACE_HOST);
    }

    public String warehouseHttpPath() {
        return raw.getString(WAREHOUSE_HTTP_PATH);
    }

    public AuthMode authMode() {
        return AuthMode.parse(raw.getString(AUTH_TYPE));
    }

    public String token() {
        return raw.getString(TOKEN);
    }

    public String entraTenantId() {
        return raw.getString(ENTRA_TENANT_ID);
    }

    public String entraClientId() {
        return raw.getString(ENTRA_CLIENT_ID);
    }

    public String entraClientSecret() {
        return raw.getString(ENTRA_CLIENT_SECRET);
    }

    public String miClientId() {
        return raw.getString(MI_CLIENT_ID);
    }

    public String azureWorkspaceResourceId() {
        return raw.getString(AZURE_WORKSPACE_RESOURCE_ID);
    }

    public String catalog() {
        return raw.getString(CATALOG);
    }

    public List<Pattern> schemaIncludePatterns() {
        return parsePatternList(raw.getString(SCHEMA_INCLUDE_LIST));
    }

    public List<Pattern> schemaExcludePatterns() {
        return parsePatternList(raw.getString(SCHEMA_EXCLUDE_LIST));
    }

    public List<Pattern> tableIncludePatterns() {
        return parsePatternList(raw.getString(TABLE_INCLUDE_LIST));
    }

    public List<Pattern> tableExcludePatterns() {
        return parsePatternList(raw.getString(TABLE_EXCLUDE_LIST));
    }

    public SnapshotMode snapshotMode() {
        return SnapshotMode.parse(raw.getString(SNAPSHOT_MODE), SnapshotMode.INITIAL.getValue());
    }

    public Duration pollInterval() {
        return Duration.ofMillis(raw.getLong(CDF_POLL_INTERVAL_MS));
    }

    public long cdfBatchMaxVersions() {
        return raw.getLong(CDF_BATCH_MAX_VERSIONS);
    }

    public long cdfBatchMaxRows() {
        return raw.getLong(CDF_BATCH_MAX_ROWS);
    }

    public long queryTimeoutMs() {
        return raw.getLong(CDF_QUERY_TIMEOUT_MS);
    }

    public Duration metadataRefreshInterval() {
        return Duration.ofMillis(raw.getLong(CDF_TABLES_METADATA_REFRESH_MS));
    }

    public DisruptiveOpHandling disruptiveOpHandling() {
        return DisruptiveOpHandling.parse(raw.getString(CDF_DISRUPTIVE_OP_HANDLING));
    }

    public long lagAlertVersions() {
        return raw.getLong(CDF_LAG_ALERT_VERSIONS);
    }

    public int jdbcRowsFetchedPerBlock() {
        return raw.getInteger(JDBC_ROWS_FETCHED_PER_BLOCK);
    }

    public long httpConnectTimeoutMs() {
        return raw.getLong(HTTP_CONNECT_TIMEOUT_MS);
    }

    public long httpSocketTimeoutMs() {
        return raw.getLong(HTTP_SOCKET_TIMEOUT_MS);
    }

    public String httpProxyHost() {
        return raw.getString(HTTP_PROXY_HOST);
    }

    public int httpProxyPort() {
        return raw.getInteger(HTTP_PROXY_PORT);
    }

    public String httpProxyUser() {
        return raw.getString(HTTP_PROXY_USER);
    }

    public String httpProxyPassword() {
        return raw.getString(HTTP_PROXY_PASSWORD);
    }

    public String getLogicalName() {
        return getLogicalName(raw);
    }

    public static String getLogicalName(Configuration config) {
        return config.getString(CommonConnectorConfig.TOPIC_PREFIX);
    }

    // ----- Helpers -----

    public boolean isTableIncluded(TableId tid) {
        String full = tid.identifier();
        String schemaQualified = tid.catalog() + "." + tid.schema();
        return matchesAny(full, tableIncludePatterns(), true)
                && !matchesAny(full, tableExcludePatterns(), false)
                && matchesAny(schemaQualified, schemaIncludePatterns(), true)
                && !matchesAny(schemaQualified, schemaExcludePatterns(), false);
    }

    private static boolean matchesAny(String s, List<Pattern> patterns, boolean defaultWhenEmpty) {
        if (patterns.isEmpty()) {
            return defaultWhenEmpty;
        }
        for (Pattern p : patterns) {
            if (p.matcher(s).matches()) {
                return true;
            }
        }
        return false;
    }

    private static List<Pattern> parsePatternList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Pattern::compile)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public EnumeratedValue getSnapshotMode() {
        return snapshotMode();
    }

    @Override
    public java.util.Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        // Delta uses MVCC — no source-side locking is needed for a consistent snapshot.
        return java.util.Optional.empty();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new DatabricksSourceInfoStructMaker();
    }

    public static Map<String, String> requireAuthFieldsPresent(Configuration cfg) {
        AuthMode mode = AuthMode.parse(cfg.getString(AUTH_TYPE));
        switch (mode) {
            case PAT:
                ensure(cfg, TOKEN);
                break;
            case AZURE_ENTRA_SP:
                ensure(cfg, ENTRA_TENANT_ID);
                ensure(cfg, ENTRA_CLIENT_ID);
                ensure(cfg, ENTRA_CLIENT_SECRET);
                break;
            case AZURE_MI:
                ensure(cfg, AZURE_WORKSPACE_RESOURCE_ID);
                break;
        }
        return Map.of();
    }

    private static void ensure(Configuration cfg, Field f) {
        String v = cfg.getString(f);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException(
                    "Configuration '" + f.name() + "' is required for the selected databricks.auth.type");
        }
    }
}
