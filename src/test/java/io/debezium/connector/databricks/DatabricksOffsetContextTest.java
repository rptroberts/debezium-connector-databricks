/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.databricks.DatabricksOffsetContext.TableOffset;
import io.debezium.relational.TableId;

class DatabricksOffsetContextTest {

    @Test
    void offsetRoundTripsThroughLoader() {
        DatabricksConnectorConfig config = buildConfig();
        DatabricksOffsetContext ctx = new DatabricksOffsetContext(config, Map.of());
        TableId t1 = new TableId("main", "banking", "customer");
        TableId t2 = new TableId("main", "banking", "account");

        ctx.recordCommit(t1, 42L, Instant.ofEpochMilli(1716_000_000_000L));
        ctx.recordCommit(t2, 99L, Instant.ofEpochMilli(1717_000_000_000L));
        ctx.markSnapshotStarted(t2);

        Map<String, ?> persisted = ctx.getOffset();
        DatabricksOffsetContext restored = new DatabricksOffsetContext.Loader(config).load(persisted);

        assertThat(restored.offsetFor(t1).commitVersion()).isEqualTo(42L);
        assertThat(restored.offsetFor(t2).commitVersion()).isEqualTo(99L);
        assertThat(restored.offsetFor(t2).snapshotting()).isTrue();
    }

    @Test
    void loaderReturnsEmptyForNullOffset() {
        DatabricksConnectorConfig config = buildConfig();
        DatabricksOffsetContext ctx = new DatabricksOffsetContext.Loader(config).load(null);
        TableId t = new TableId("main", "banking", "customer");
        TableOffset o = ctx.offsetFor(t);
        assertThat(o.commitVersion()).isEqualTo(-1L);
        assertThat(o.snapshotCompleted()).isFalse();
    }

    @Test
    void recordCommitMonotonicMax() {
        DatabricksConnectorConfig config = buildConfig();
        DatabricksOffsetContext ctx = new DatabricksOffsetContext(config, Map.of());
        TableId t = new TableId("main", "banking", "customer");
        ctx.recordCommit(t, 10L, null);
        ctx.recordCommit(t, 5L, null); // out-of-order should NOT regress
        assertThat(ctx.offsetFor(t).commitVersion()).isEqualTo(10L);
    }

    @Test
    void resetForResnapshotRevertsCommitVersionAndCompletion() {
        DatabricksConnectorConfig config = buildConfig();
        DatabricksOffsetContext ctx = new DatabricksOffsetContext(config, Map.of());
        TableId t = new TableId("main", "banking", "customer");
        ctx.markSnapshotStarted(t);
        ctx.recordCommit(t, 100L, Instant.now());
        ctx.markSnapshotCompleted(t);
        // Sanity: snapshot was completed
        assertThat(ctx.offsetFor(t).snapshotCompleted()).isTrue();
        // Reset back to "needs initial snapshot"
        ctx.resetForResnapshot(t);
        TableOffset o = ctx.offsetFor(t);
        assertThat(o.commitVersion()).isEqualTo(-1L);
        assertThat(o.snapshotCompleted()).isFalse();
        assertThat(o.snapshotting()).isFalse();
    }

    @Test
    void outOfOrderRecordCommitDoesNotPairOldVersionWithNewTimestamp() {
        // Regression: previous merger always took newV.commitTimestamp, even when oldV won on version.
        DatabricksConnectorConfig config = buildConfig();
        DatabricksOffsetContext ctx = new DatabricksOffsetContext(config, Map.of());
        TableId t = new TableId("main", "banking", "customer");
        Instant ts10 = Instant.ofEpochMilli(1_000_000L);
        Instant ts9 = Instant.ofEpochMilli(500_000L);
        ctx.recordCommit(t, 10L, ts10);
        ctx.recordCommit(t, 9L, ts9); // out-of-order — should not regress

        TableOffset o = ctx.offsetFor(t);
        assertThat(o.commitVersion()).isEqualTo(10L);
        assertThat(o.commitTimestamp()).isEqualTo(ts10);
    }

    @Test
    void markSnapshotCompletedClearsSnapshotting() {
        DatabricksConnectorConfig config = buildConfig();
        DatabricksOffsetContext ctx = new DatabricksOffsetContext(config, Map.of());
        TableId t = new TableId("main", "banking", "customer");
        ctx.markSnapshotStarted(t);
        ctx.recordCommit(t, 100L, Instant.now());
        ctx.markSnapshotCompleted(t);
        assertThat(ctx.offsetFor(t).snapshotting()).isFalse();
        assertThat(ctx.offsetFor(t).snapshotCompleted()).isTrue();
    }

    private static DatabricksConnectorConfig buildConfig() {
        Map<String, String> p = new HashMap<>();
        p.put("topic.prefix", "test");
        p.put("databricks.workspace.host", "h");
        p.put("databricks.warehouse.http.path", "/p");
        p.put("databricks.catalog", "main");
        p.put("databricks.token", "tok");
        return new DatabricksConnectorConfig(Configuration.from(p));
    }
}
