/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.databricks.cdf.DeltaTableInspector;
import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.connector.databricks.source.DatabricksPoller;
import io.debezium.relational.TableId;

/**
 * Full CDF round-trip: creates a Delta table with CDF enabled, inserts/updates/deletes rows,
 * runs the connector's poller, asserts the right Debezium-envelope records are emitted.
 */
class DatabricksCdfIT {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksCdfIT.class);

    private DatabricksTestEnv env;
    private DatabricksConnectorConfig config;
    private DatabricksConnection connection;
    private TableId testTable;

    @BeforeEach
    void setUp() throws Exception {
        env = DatabricksTestEnv.load();
        assumeTrue(env.isComplete(), "DATABRICKS_* env not set; skipping IT");

        String tableName = "_dbz_it_" + System.currentTimeMillis() + "_" + (int) (Math.random() * 1000);
        testTable = new TableId(env.catalog(), env.schema(), tableName);
        config = buildConfig(testTable);
        connection = new DatabricksConnection(config);
        connection.selfTest();

        // Drop if exists (idempotent), create with CDF enabled and a primary key so
        // pre/post-image pairing actually fires for the UPDATE event.
        connection.execute("DROP TABLE IF EXISTS `" + testTable.catalog() + "`.`" + testTable.schema() + "`.`" + testTable.table() + "`");
        connection.execute("CREATE TABLE `" + testTable.catalog() + "`.`" + testTable.schema() + "`.`" + testTable.table() + "` (" +
                " id BIGINT NOT NULL, name STRING, amount DECIMAL(18,2), " +
                " CONSTRAINT " + testTable.table() + "_pk PRIMARY KEY (id)) USING DELTA " +
                "TBLPROPERTIES (delta.enableChangeDataFeed = true)");
        LOG.info("Created test table {} with CDF enabled", testTable.identifier());
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (connection != null && testTable != null) {
                connection.execute("DROP TABLE IF EXISTS `" + testTable.catalog() + "`.`" + testTable.schema() + "`.`" + testTable.table() + "`");
            }
        }
        finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    void snapshotEmitsExistingRowsThenStreamingPicksUpChanges() throws Exception {
        // Pre-populate table BEFORE starting the poller (these rows will arrive as op=r in the snapshot).
        connection.execute("INSERT INTO `" + testTable.catalog() + "`.`" + testTable.schema() + "`.`" + testTable.table() +
                "` VALUES (1, 'alpha', 100.00), (2, 'beta', 200.00)");

        DatabricksSchema schema = new DatabricksSchema(config);
        DatabricksPartition partition = new DatabricksPartition(config.getLogicalName());
        DatabricksOffsetContext offset = new DatabricksOffsetContext(config, Map.of());
        DatabricksPoller poller = new DatabricksPoller(config, connection, schema, partition, offset);

        DeltaTableInspector inspector = new DeltaTableInspector(connection, config);
        var meta = inspector.fetchMetadata(testTable);
        assertThat(meta.cdfEnabled()).as("CDF must be enabled on test table").isTrue();
        poller.setTables(List.of(testTable));

        List<SourceRecord> all = new ArrayList<>();

        // Phase 1: snapshot picks up the pre-existing rows.
        AtomicReference<List<SourceRecord>> firstBatch = new AtomicReference<>();
        Awaitility.await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    List<SourceRecord> batch = poller.pollOnce();
                    all.addAll(batch);
                    if (firstBatch.get() == null && !batch.isEmpty()) {
                        firstBatch.set(batch);
                    }
                    assertThat(opCount(all, "r")).isGreaterThanOrEqualTo(2);
                });

        // Phase 2: stream new INSERT/UPDATE/DELETE.
        connection.execute("INSERT INTO `" + testTable.catalog() + "`.`" + testTable.schema() + "`.`" + testTable.table() +
                "` VALUES (3, 'gamma', 300.00)");
        connection.execute("UPDATE `" + testTable.catalog() + "`.`" + testTable.schema() + "`.`" + testTable.table() +
                "` SET amount = 999.00 WHERE id = 2");
        connection.execute("DELETE FROM `" + testTable.catalog() + "`.`" + testTable.schema() + "`.`" + testTable.table() +
                "` WHERE id = 1");

        Awaitility.await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    all.addAll(poller.pollOnce());
                    assertThat(opCount(all, "c")).as("expect at least one create").isGreaterThanOrEqualTo(1);
                    assertThat(opCount(all, "u")).as("expect at least one update").isGreaterThanOrEqualTo(1);
                    assertThat(opCount(all, "d")).as("expect at least one delete").isGreaterThanOrEqualTo(1);
                });

        // Validate one UPDATE has correctly paired before+after.
        SourceRecord update = all.stream().filter(r -> "u".equals(opOf(r))).findFirst().orElseThrow();
        Struct env = (Struct) update.value();
        Struct before = (Struct) env.get("before");
        Struct after = (Struct) env.get("after");
        assertThat(after).isNotNull();
        if (before != null) {
            assertThat(before.get("id")).isEqualTo(2L);
        }
        assertThat(after.get("id")).isEqualTo(2L);
        assertThat(after.get("amount")).isNotNull();

        // Validate source struct carries Delta metadata.
        Struct source = (Struct) env.get("source");
        assertThat(source.getString("catalog")).isEqualTo(testTable.catalog());
        assertThat(source.getString("schema")).isEqualTo(testTable.schema());
        assertThat(source.getString("table")).isEqualTo(testTable.table());
        assertThat(source.getInt64("commit_version")).isNotNull();

        LOG.info("Test passed. Total records: {} (r={}, c={}, u={}, d={})",
                all.size(), opCount(all, "r"), opCount(all, "c"), opCount(all, "u"), opCount(all, "d"));
    }

    private static long opCount(List<SourceRecord> records, String op) {
        return records.stream().filter(r -> op.equals(opOf(r))).count();
    }

    private static String opOf(SourceRecord r) {
        if (r.value() instanceof Struct s) {
            try {
                return s.getString("op");
            }
            catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    private DatabricksConnectorConfig buildConfig(TableId table) {
        Map<String, String> p = new HashMap<>();
        p.put("topic.prefix", "dbx-it");
        p.put("databricks.workspace.host", env.host());
        p.put("databricks.warehouse.http.path", env.httpPath());
        p.put("databricks.auth.type", "pat");
        p.put("databricks.token", env.token());
        p.put("databricks.catalog", env.catalog());
        p.put("table.include.list", java.util.regex.Pattern.quote(table.identifier()));
        p.put("snapshot.mode", "initial");
        p.put("cdf.poll.interval.ms", "2000");
        p.put("cdf.batch.max.versions", "200");
        return new DatabricksConnectorConfig(Configuration.from(p));
    }
}
