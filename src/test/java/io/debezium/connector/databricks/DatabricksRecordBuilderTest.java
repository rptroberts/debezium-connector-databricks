/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.databricks.cdf.ChangeRow;
import io.debezium.connector.databricks.cdf.ChangeType;
import io.debezium.connector.databricks.cdf.DeltaColumn;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.relational.TableId;

class DatabricksRecordBuilderTest {

    @Test
    void buildSnapshotRoundTripsThroughJsonConverter() {
        DatabricksConnectorConfig cfg = buildConfig();
        DatabricksSchema schema = new DatabricksSchema(cfg);
        DatabricksPartition partition = new DatabricksPartition(cfg.getLogicalName());
        DatabricksOffsetContext offset = new DatabricksOffsetContext(cfg, Map.of());
        DatabricksRecordBuilder builder = new DatabricksRecordBuilder(cfg, schema, partition);

        TableId tid = new TableId("main", "banking", "customer");
        DeltaTableMetadata meta = new DeltaTableMetadata(tid, List.of(
                new DeltaColumn("id", "bigint", false, 1, true),
                new DeltaColumn("name", "string", true, 2, false)), List.of("id"), true);
        DatabricksSchema.CachedSchema cached = schema.ensureSchema(meta);

        Map<String, Object> values = new HashMap<>();
        values.put("id", 1L);
        values.put("name", "Alice");
        SourceRecord rec = builder.buildSnapshot(cached, values, 1L, Instant.ofEpochMilli(1716_000_000_000L), offset);

        // Verify the value struct shape
        Struct env = (Struct) rec.value();
        assertThat(env.getString("op")).isEqualTo("r");
        Struct after = (Struct) env.get("after");
        assertThat(after.get("id")).isEqualTo(1L);
        assertThat(after.get("name")).isEqualTo("Alice");

        // Round-trip through JsonConverter to ensure no required-field-null issues
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", "false", "converter.type", "value"), false);
        byte[] bytes = converter.fromConnectData(rec.topic(), rec.valueSchema(), rec.value());
        assertThat(bytes).isNotEmpty();
        String json = new String(bytes);
        assertThat(json).contains("\"op\":\"r\"").contains("\"after\"").contains("\"source\"");
    }

    @Test
    void buildCreateUpdateDeleteRoundTrip() {
        DatabricksConnectorConfig cfg = buildConfig();
        DatabricksSchema schema = new DatabricksSchema(cfg);
        DatabricksPartition partition = new DatabricksPartition(cfg.getLogicalName());
        DatabricksOffsetContext offset = new DatabricksOffsetContext(cfg, Map.of());
        DatabricksRecordBuilder builder = new DatabricksRecordBuilder(cfg, schema, partition);

        TableId tid = new TableId("main", "banking", "customer");
        DeltaTableMetadata meta = new DeltaTableMetadata(tid, List.of(
                new DeltaColumn("id", "bigint", false, 1, true),
                new DeltaColumn("name", "string", true, 2, false)), List.of("id"), true);
        DatabricksSchema.CachedSchema cached = schema.ensureSchema(meta);

        // Build a batch with create + (pre+post update) + delete
        Instant ts1 = Instant.ofEpochMilli(1716_000_000_000L);
        Instant ts2 = Instant.ofEpochMilli(1716_000_001_000L);
        List<ChangeRow> rows = List.of(
                row(tid, ChangeType.INSERT, 10L, ts1, Map.of("id", 1L, "name", "A")),
                row(tid, ChangeType.UPDATE_PREIMAGE, 11L, ts2, Map.of("id", 2L, "name", "Bold")),
                row(tid, ChangeType.UPDATE_POSTIMAGE, 11L, ts2, Map.of("id", 2L, "name", "Bnew")),
                row(tid, ChangeType.DELETE, 12L, ts2, Map.of("id", 3L, "name", "C")));

        List<SourceRecord> out = builder.buildBatch(cached, rows, offset);
        assertThat(out).hasSize(3); // pre+post collapse to one update

        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", "false", "converter.type", "value"), false);
        for (SourceRecord rec : out) {
            byte[] bytes = converter.fromConnectData(rec.topic(), rec.valueSchema(), rec.value());
            String json = new String(bytes);
            assertThat(json).contains("\"source\"");
        }
        List<String> ops = out.stream().map(r -> ((Struct) r.value()).getString("op")).toList();
        assertThat(ops).containsExactly("c", "u", "d");

        // Verify update has both before and after
        SourceRecord upd = out.get(1);
        Struct env = (Struct) upd.value();
        assertThat(env.get("before")).isNotNull();
        assertThat(env.get("after")).isNotNull();
        assertThat(((Struct) env.get("before")).get("name")).isEqualTo("Bold");
        assertThat(((Struct) env.get("after")).get("name")).isEqualTo("Bnew");
    }

    @Test
    void buildTruncateProducesTruncateOp() {
        DatabricksConnectorConfig cfg = buildConfig();
        DatabricksSchema schema = new DatabricksSchema(cfg);
        DatabricksPartition partition = new DatabricksPartition(cfg.getLogicalName());
        DatabricksOffsetContext offset = new DatabricksOffsetContext(cfg, Map.of());
        DatabricksRecordBuilder builder = new DatabricksRecordBuilder(cfg, schema, partition);

        TableId tid = new TableId("main", "banking", "customer");
        DeltaTableMetadata meta = new DeltaTableMetadata(tid, List.of(
                new DeltaColumn("id", "bigint", false, 1, true)), List.of("id"), true);
        DatabricksSchema.CachedSchema cached = schema.ensureSchema(meta);
        SourceRecord rec = builder.buildTruncate(cached, 42L, Instant.now(), offset);
        Struct env = (Struct) rec.value();
        assertThat(env.getString("op")).isEqualTo("t");
    }

    private static ChangeRow row(TableId tid, ChangeType type, long ver, Instant ts, Map<String, Object> values) {
        return new ChangeRow(tid, type, ver, ts, values);
    }

    private static DatabricksConnectorConfig buildConfig() {
        Map<String, String> p = new HashMap<>();
        p.put("topic.prefix", "test");
        p.put("databricks.workspace.host", "h");
        p.put("databricks.warehouse.http.path", "/p");
        p.put("databricks.catalog", "main");
        p.put("databricks.token", "t");
        return new DatabricksConnectorConfig(Configuration.from(p));
    }
}
