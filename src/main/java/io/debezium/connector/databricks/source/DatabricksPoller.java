/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.source;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;
import io.debezium.connector.databricks.DatabricksConnectorConfig.SnapshotMode;
import io.debezium.connector.databricks.DatabricksOffsetContext;
import io.debezium.connector.databricks.DatabricksOffsetContext.TableOffset;
import io.debezium.connector.databricks.DatabricksPartition;
import io.debezium.connector.databricks.DatabricksRecordBuilder;
import io.debezium.connector.databricks.DatabricksSchema;
import io.debezium.connector.databricks.cdf.DeltaCdfReader;
import io.debezium.connector.databricks.cdf.DeltaTableInspector;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.connector.databricks.cdf.HeadVersionCache;
import io.debezium.connector.databricks.cdf.HistoryScanner;
import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.relational.TableId;

/**
 * Thin orchestrator over {@link TableSetManager} (discovery + metadata cache),
 * {@link TableSnapshotter} (initial snapshot via Delta time travel),
 * {@link CdfStreamer} ({@code table_changes} loop), and
 * {@link DisruptiveOpDetector} (TRUNCATE / REPLACE / RESTORE handling).
 *
 * <p>Each poll cycle: refresh head versions for all tables in a single batched
 * query, then for each table run its snapshot or streaming phase as
 * appropriate. The snapshot path is one-shot per table; streaming continues
 * for the life of the task.
 */
public class DatabricksPoller {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksPoller.class);

    private final DatabricksConnectorConfig config;
    private final DatabricksOffsetContext offset;
    private final TableSetManager tableSet;
    private final DatabricksSchema schema;
    private final TableSnapshotter snapshotter;
    private final CdfStreamer streamer;
    private final DisruptiveOpDetector disruptiveOps;
    private final HeadVersionCache headCache;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public DatabricksPoller(DatabricksConnectorConfig config,
                            DatabricksConnection connection,
                            DatabricksSchema schema,
                            DatabricksPartition partition,
                            DatabricksOffsetContext offset) {
        this.config = config;
        this.offset = offset;
        this.schema = schema;

        DeltaTableInspector inspector = new DeltaTableInspector(connection, config);
        this.tableSet = new TableSetManager(config, inspector);
        this.headCache = new HeadVersionCache(inspector, config.pollInterval());
        DatabricksRecordBuilder recordBuilder = new DatabricksRecordBuilder(config, schema, partition);
        this.snapshotter = new TableSnapshotter(connection, recordBuilder);
        this.streamer = new CdfStreamer(config, new DeltaCdfReader(connection), recordBuilder, headCache);
        this.disruptiveOps = new DisruptiveOpDetector(config, new HistoryScanner(connection), recordBuilder);
    }

    /**
     * Test/IT hook: pre-populate the table list, bypassing discovery.
     */
    public void setTables(List<TableId> tables) throws SQLException {
        tableSet.setTablesForTest(tables);
        // Warm the head-version cache so the first poll has fresh data.
        headCache.refreshAll(tables);
    }

    /**
     * Performs one polling cycle over all enabled tables.
     */
    public List<SourceRecord> pollOnce() throws SQLException, InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        List<TableId> tables = tableSet.currentTables();
        if (tables.isEmpty()) {
            return out;
        }
        // Refresh head versions in one batched query so per-table reads can skip work for caught-up tables.
        try {
            headCache.refreshAll(tables);
        }
        catch (SQLException e) {
            LOG.warn("Batched head-version refresh failed; falling back to per-table on demand: {}", e.getMessage());
        }
        for (TableId tid : tables) {
            if (stopped.get() || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            DeltaTableMetadata meta = tableSet.metadataFor(tid);
            if (meta == null || !meta.cdfEnabled()) {
                continue;
            }
            DatabricksSchema.CachedSchema cached = schema.ensureSchema(meta);
            pollTable(meta, cached, out);
        }
        return out;
    }

    private void pollTable(DeltaTableMetadata meta, DatabricksSchema.CachedSchema cached,
                           List<SourceRecord> out) throws SQLException {
        TableId tid = meta.tableId();
        TableOffset state = offset.offsetFor(tid);

        if (needsSnapshot(state)) {
            long head = headCache.get(tid);
            long snapshotVersion = Math.max(0L, head);
            offset.markSnapshotStarted(tid);
            snapshotter.run(meta, cached, snapshotVersion, offset, out);
            offset.recordCommit(tid, snapshotVersion, Instant.now());
            offset.markSnapshotCompleted(tid);
            if (config.snapshotMode() == SnapshotMode.INITIAL_ONLY) {
                return;
            }
            // Streaming will continue from snapshotVersion+1 on the next polling pass.
            return;
        }

        // Disruptive-op detection: scan the soon-to-be-streamed range first.
        long head = headCache.get(tid);
        long lastSeen = state.commitVersion();
        if (head > lastSeen) {
            long end = Math.min(head, lastSeen + config.cdfBatchMaxVersions());
            boolean reSnapshotTriggered = disruptiveOps.scanAndEmit(tid, cached, lastSeen, end, offset, out);
            if (reSnapshotTriggered) {
                return; // Next poll cycle will run the re-snapshot.
            }
        }

        streamer.run(meta, cached, offset, out);
    }

    private boolean needsSnapshot(TableOffset state) {
        SnapshotMode mode = config.snapshotMode();
        if (mode == SnapshotMode.NEVER) {
            return false;
        }
        return !state.snapshotCompleted();
    }

    public void stop() {
        stopped.set(true);
    }

    // ----- Test hooks -----

    public TableSetManager tableSetManager() {
        return tableSet;
    }

    public HeadVersionCache headVersionCache() {
        return headCache;
    }

    public CdfStreamer streamer() {
        return streamer;
    }
}
