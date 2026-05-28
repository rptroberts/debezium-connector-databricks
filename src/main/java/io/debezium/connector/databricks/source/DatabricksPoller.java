/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.source;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;
import io.debezium.connector.databricks.DatabricksConnectorConfig.DisruptiveOpHandling;
import io.debezium.connector.databricks.DatabricksConnectorConfig.SnapshotMode;
import io.debezium.connector.databricks.DatabricksOffsetContext;
import io.debezium.connector.databricks.DatabricksOffsetContext.TableOffset;
import io.debezium.connector.databricks.DatabricksPartition;
import io.debezium.connector.databricks.DatabricksRecordBuilder;
import io.debezium.connector.databricks.DatabricksSchema;
import io.debezium.connector.databricks.cdf.ChangeRow;
import io.debezium.connector.databricks.cdf.DeltaCdfReader;
import io.debezium.connector.databricks.cdf.DeltaTableInspector;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.connector.databricks.cdf.HistoryScanner;
import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.relational.TableId;

/**
 * Drives both snapshot and streaming reads for the configured tables.
 *
 * <p>State machine per table:
 * <ol>
 *   <li>If snapshot is required and not yet completed, drive a paged
 *       {@code VERSION AS OF v0} read; mark snapshot completed and set the
 *       offset to {@code v0}.</li>
 *   <li>Otherwise issue {@code table_changes(t, last+1, head)} against the
 *       current head version.</li>
 *   <li>Detect TRUNCATE/REPLACE/RESTORE via {@link HistoryScanner} and emit a
 *       synthetic truncate event before the affected range.</li>
 * </ol>
 */
public class DatabricksPoller {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksPoller.class);

    private final DatabricksConnectorConfig config;
    private final DatabricksConnection connection;
    private final DatabricksSchema schema;
    private final DatabricksPartition partition;
    private final DatabricksOffsetContext offset;
    private final DatabricksRecordBuilder recordBuilder;
    private final DeltaTableInspector inspector;
    private final DeltaCdfReader cdfReader;
    private final HistoryScanner historyScanner;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private long lastMetadataRefreshNs = 0L;
    private List<TableId> currentTables = List.of();
    private final Map<TableId, DeltaTableMetadata> metadataCache = new LinkedHashMap<>();

    public DatabricksPoller(DatabricksConnectorConfig config,
                            DatabricksConnection connection,
                            DatabricksSchema schema,
                            DatabricksPartition partition,
                            DatabricksOffsetContext offset) {
        this.config = config;
        this.connection = connection;
        this.schema = schema;
        this.partition = partition;
        this.offset = offset;
        this.recordBuilder = new DatabricksRecordBuilder(config, schema, partition);
        this.inspector = new DeltaTableInspector(connection, config);
        this.cdfReader = new DeltaCdfReader(connection);
        this.historyScanner = new HistoryScanner(connection);
    }

    /**
     * Test/IT hook: pre-populate the list of tables to capture, bypassing discovery.
     * Also fetches per-table metadata immediately so the poller can read the schema.
     */
    public void setTables(List<TableId> tables) throws SQLException {
        Map<TableId, DeltaTableMetadata> fresh = new LinkedHashMap<>();
        for (TableId tid : tables) {
            fresh.put(tid, inspector.fetchMetadata(tid));
        }
        metadataCache.clear();
        metadataCache.putAll(fresh);
        this.currentTables = List.copyOf(tables);
        this.lastMetadataRefreshNs = System.nanoTime();
    }

    /**
     * Performs one polling cycle over all enabled tables. Returns a (possibly empty) batch.
     */
    public List<SourceRecord> pollOnce() throws SQLException, InterruptedException {
        refreshTablesIfDue();
        List<SourceRecord> out = new ArrayList<>();
        for (TableId tid : currentTables) {
            if (stopped.get() || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            DeltaTableMetadata meta = metadataCache.get(tid);
            if (meta == null) {
                continue;
            }
            if (!meta.cdfEnabled()) {
                LOG.warn("Skipping {} — CDF is not enabled (set TBLPROPERTIES delta.enableChangeDataFeed = true).",
                        tid.identifier());
                continue;
            }
            DatabricksSchema.CachedSchema cached = schema.ensureSchema(meta);
            try {
                pollTable(tid, meta, cached, out);
            }
            catch (SQLException e) {
                LOG.error("CDF poll failed for {}: {}", tid.identifier(), e.getMessage());
                throw e;
            }
        }
        return out;
    }

    private void pollTable(TableId tid, DeltaTableMetadata meta, DatabricksSchema.CachedSchema cached, List<SourceRecord> out)
            throws SQLException {
        TableOffset state = offset.offsetFor(tid);
        boolean needSnapshot = needsSnapshot(state);
        long head = inspector.currentVersion(tid);

        if (needSnapshot) {
            long snapshotVersion = Math.max(0L, head);
            int rowCount = runSnapshot(meta, cached, snapshotVersion, out);
            offset.markSnapshotCompleted(tid);
            offset.recordCommit(tid, snapshotVersion, Instant.now(), false);
            LOG.info("Initial snapshot of {} complete at version {}: {} rows", tid.identifier(), snapshotVersion, rowCount);
            if (config.snapshotMode() == SnapshotMode.INITIAL_ONLY) {
                return;
            }
            state = offset.offsetFor(tid);
        }

        long start = state.commitVersion() + 1;
        if (start > head) {
            // Caught up — note the lag (zero) for ops.
            maybeWarnLag(tid, head, state.commitVersion());
            return;
        }
        long end = Math.min(head, start + config.cdfBatchMaxVersions() - 1);
        maybeWarnLag(tid, head, state.commitVersion());

        // Detect disruptive ops in this range.
        List<HistoryScanner.DisruptiveOp> disruptive = historyScanner.scan(tid, state.commitVersion(), end);
        if (!disruptive.isEmpty()) {
            handleDisruptive(tid, cached, disruptive, out, end);
        }

        List<ChangeRow> rows = new ArrayList<>();
        cdfReader.readRange(meta, start, end, rows::add);
        if (!rows.isEmpty()) {
            out.addAll(recordBuilder.buildBatch(cached, rows, offset));
            LOG.debug("Streamed {} rows from {} for versions [{},{}]", rows.size(), tid.identifier(), start, end);
        }
        offset.recordCommit(tid, end, rows.isEmpty() ? null : rows.get(rows.size() - 1).commitTimestamp(), false);
    }

    private int runSnapshot(DeltaTableMetadata meta, DatabricksSchema.CachedSchema cached, long v0, List<SourceRecord> out) throws SQLException {
        TableId tid = meta.tableId();
        String cols = String.join(",", meta.columnNames().stream().map(DeltaTableInspector::quoteIdent).toList());
        if (cols.isEmpty()) {
            cols = "*";
        }
        String sql = "SELECT " + cols + " FROM " + DeltaTableInspector.quoteTable(tid) + " VERSION AS OF " + v0;
        int[] rowCount = { 0 };
        connection.query(sql, rs -> {
            try {
                Map<String, Object> values = new LinkedHashMap<>();
                for (var col : meta.columns()) {
                    Object raw = rs.getObject(col.name());
                    values.put(col.name(), io.debezium.connector.databricks.cdf.DeltaTypes.convert(col, raw));
                }
                SourceRecord rec = recordBuilder.buildSnapshot(cached, values, v0, Instant.now(), offset);
                if (rec != null) {
                    out.add(rec);
                    rowCount[0]++;
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return rowCount[0];
    }

    private boolean needsSnapshot(TableOffset state) {
        SnapshotMode mode = config.snapshotMode();
        if (mode == SnapshotMode.NEVER) {
            return false;
        }
        return !state.snapshotCompleted();
    }

    private void handleDisruptive(TableId tid, DatabricksSchema.CachedSchema cached,
                                  List<HistoryScanner.DisruptiveOp> ops, List<SourceRecord> out, long endVersion) {
        DisruptiveOpHandling handling = config.disruptiveOpHandling();
        for (HistoryScanner.DisruptiveOp op : ops) {
            switch (handling) {
                case WARN:
                    LOG.warn("Detected disruptive op {} at {}:v{}: {}", op.operation(), tid.identifier(), op.version(), op.parameters());
                    break;
                case TRUNCATE:
                    LOG.info("Emitting synthetic TRUNCATE for {}:v{} ({})", tid.identifier(), op.version(), op.operation());
                    out.add(recordBuilder.buildTruncate(cached, op.version(), op.timestamp() == null ? Instant.now() : op.timestamp(), offset));
                    break;
                case RESNAPSHOT:
                    LOG.warn("Re-snapshot requested for {} after {}:v{}: clearing offset to trigger fresh snapshot",
                            tid.identifier(), op.operation(), op.version());
                    offset.resetForResnapshot(tid);
                    return; // skip the rest of this table's poll cycle; next poll will run snapshot
                case FAIL:
                    throw new IllegalStateException("Disruptive Delta operation '" + op.operation() +
                            "' at " + tid.identifier() + ":v" + op.version() + " — failing per cdf.disruptive.operation.handling=fail");
            }
        }
    }

    private void maybeWarnLag(TableId tid, long head, long lastSeen) {
        long threshold = config.lagAlertVersions();
        if (threshold > 0 && (head - lastSeen) >= threshold) {
            LOG.warn("CDF lag for {}: lastSeen=v{}, head=v{}, lag={} versions (threshold={})",
                    tid.identifier(), lastSeen, head, head - lastSeen, threshold);
        }
    }

    private void refreshTablesIfDue() throws SQLException {
        long now = System.nanoTime();
        if (currentTables.isEmpty() || now - lastMetadataRefreshNs > config.metadataRefreshInterval().toNanos()) {
            try {
                List<TableId> discovered = inspector.discoverTables();
                Map<TableId, DeltaTableMetadata> fresh = new LinkedHashMap<>();
                for (TableId tid : discovered) {
                    DeltaTableMetadata m = inspector.fetchMetadata(tid);
                    if (!m.cdfEnabled()) {
                        LOG.warn("Table {} matches include list but does not have CDF enabled; skipping.", tid.identifier());
                        continue;
                    }
                    fresh.put(tid, m);
                }
                metadataCache.clear();
                metadataCache.putAll(fresh);
                currentTables = List.copyOf(fresh.keySet());
                lastMetadataRefreshNs = now;
                LOG.info("Table set refreshed: {} table(s) under capture: {}", currentTables.size(),
                        currentTables.stream().map(TableId::identifier).toList());
            }
            catch (SQLException e) {
                if (currentTables.isEmpty()) {
                    throw e;
                }
                LOG.warn("Table-set refresh failed; continuing with stale set of {}: {}", currentTables.size(), e.getMessage());
            }
        }
    }

    public void stop() {
        stopped.set(true);
    }
}
