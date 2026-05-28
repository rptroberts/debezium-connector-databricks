/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Offset context carrying per-table {@code _commit_version} watermarks.
 *
 * <p>The partition key is server-wide; per-table positions live in the offset
 * value as a nested map. On restart the loader rehydrates the full map.
 */
public class DatabricksOffsetContext extends CommonOffsetContext<SourceInfo> {

    public static final String PER_TABLE_KEY = "tables";
    public static final String COMMIT_VERSION_KEY = "commit_version";
    public static final String COMMIT_TIMESTAMP_KEY = "commit_timestamp_ms";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Map<TableId, TableOffset> tableOffsets = new ConcurrentHashMap<>();
    private final TransactionContext transactionContext;

    public DatabricksOffsetContext(DatabricksConnectorConfig config, Map<TableId, TableOffset> initial) {
        super(new SourceInfo(config));
        if (initial != null) {
            tableOffsets.putAll(initial);
        }
        this.transactionContext = new TransactionContext();
    }

    public TableOffset offsetFor(TableId tableId) {
        return tableOffsets.computeIfAbsent(tableId, k -> TableOffset.empty());
    }

    public void recordCommit(TableId tableId, long commitVersion, Instant commitTimestamp, boolean snapshotting) {
        tableOffsets.merge(tableId,
                new TableOffset(commitVersion, commitTimestamp, snapshotting, !snapshotting),
                (oldV, newV) -> {
                    long winnerVersion = Math.max(oldV.commitVersion, newV.commitVersion);
                    // Keep the timestamp paired with the winning version, not the most recent write.
                    Instant winnerTs;
                    if (newV.commitVersion >= oldV.commitVersion && newV.commitTimestamp != null) {
                        winnerTs = newV.commitTimestamp;
                    }
                    else {
                        winnerTs = oldV.commitTimestamp != null ? oldV.commitTimestamp : newV.commitTimestamp;
                    }
                    return new TableOffset(winnerVersion, winnerTs, newV.snapshotting,
                            newV.snapshotCompleted || oldV.snapshotCompleted);
                });
    }

    /** Forcefully reset to "needs initial snapshot" state — used by the RESNAPSHOT disruptive-op handler. */
    public void resetForResnapshot(TableId tableId) {
        tableOffsets.put(tableId, TableOffset.empty());
    }

    public void markSnapshotCompleted(TableId tableId) {
        TableOffset cur = offsetFor(tableId);
        tableOffsets.put(tableId, new TableOffset(cur.commitVersion, cur.commitTimestamp, false, true));
    }

    @Override
    public Map<String, ?> getOffset() {
        // The Embedded Engine's offset backing store (and Kafka Connect's) accepts only
        // primitive types in the offset value, so we flatten per-table offsets to a single
        // serialized field: one string entry per (commit_version | commit_timestamp_ms | snapshot | snapshot_completed)
        // keyed by `<catalog>.<schema>.<table>.<field>`.
        Map<String, Object> out = new HashMap<>();
        for (Map.Entry<TableId, TableOffset> e : tableOffsets.entrySet()) {
            String prefix = PER_TABLE_KEY + "." + e.getKey().identifier() + ".";
            TableOffset off = e.getValue();
            out.put(prefix + COMMIT_VERSION_KEY, off.commitVersion);
            if (off.commitTimestamp != null) {
                out.put(prefix + COMMIT_TIMESTAMP_KEY, off.commitTimestamp.toEpochMilli());
            }
            out.put(prefix + SNAPSHOT_KEY, off.snapshotting);
            out.put(prefix + SNAPSHOT_COMPLETED_KEY, off.snapshotCompleted);
        }
        return out;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return tableOffsets.values().stream().anyMatch(t -> t.snapshotting);
    }

    @Override
    public void preSnapshotStart(boolean onDemand) {
        sourceInfo.setSnapshot(onDemand ? SnapshotRecord.INCREMENTAL : SnapshotRecord.TRUE);
    }

    @Override
    public void preSnapshotCompletion() {
        // no-op
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        if (collectionId instanceof TableId tid) {
            TableOffset off = offsetFor(tid);
            sourceInfo.update(tid, off.commitVersion, off.commitTimestamp != null ? off.commitTimestamp : timestamp, null);
        }
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public Map<TableId, TableOffset> tableOffsets() {
        return Map.copyOf(tableOffsets);
    }

    /** Exposes the inner SourceInfo for record builders. */
    public SourceInfo getSource() {
        return sourceInfo;
    }

    /** Per-table offset record. */
    public record TableOffset(long commitVersion, Instant commitTimestamp, boolean snapshotting, boolean snapshotCompleted) {

        public static TableOffset empty() {
            return new TableOffset(-1L, null, false, false);
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new HashMap<>();
            m.put(COMMIT_VERSION_KEY, commitVersion);
            if (commitTimestamp != null) {
                m.put(COMMIT_TIMESTAMP_KEY, commitTimestamp.toEpochMilli());
            }
            m.put(SNAPSHOT_KEY, snapshotting);
            m.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
            return m;
        }

        @SuppressWarnings("unchecked")
        public static TableOffset fromMap(Map<String, ?> m) {
            if (m == null) {
                return empty();
            }
            Object vRaw = m.get(COMMIT_VERSION_KEY);
            long v = vRaw == null ? -1L : ((Number) vRaw).longValue();
            Object tsRaw = m.get(COMMIT_TIMESTAMP_KEY);
            Instant ts = tsRaw == null ? null : Instant.ofEpochMilli(((Number) tsRaw).longValue());
            boolean snap = Boolean.TRUE.equals(m.get(SNAPSHOT_KEY));
            boolean done = Boolean.TRUE.equals(m.get(SNAPSHOT_COMPLETED_KEY));
            return new TableOffset(v, ts, snap, done);
        }
    }

    /** Re-hydrates the offset context from persisted state. */
    public static class Loader implements OffsetContext.Loader<DatabricksOffsetContext> {

        private final DatabricksConnectorConfig config;

        public Loader(DatabricksConnectorConfig config) {
            this.config = config;
        }

        @Override
        public DatabricksOffsetContext load(Map<String, ?> offset) {
            Map<TableId, TableOffset> rehydrated = new HashMap<>();
            if (offset != null) {
                String prefix = PER_TABLE_KEY + ".";
                // Group flat keys "<prefix>.<table-id>.<field>" back into per-table maps.
                Map<String, Map<String, Object>> tablesByName = new HashMap<>();
                for (Map.Entry<String, ?> e : offset.entrySet()) {
                    String k = e.getKey();
                    if (!k.startsWith(prefix)) {
                        continue;
                    }
                    String rest = k.substring(prefix.length());
                    int lastDot = rest.lastIndexOf('.');
                    if (lastDot < 0) {
                        continue;
                    }
                    String tableId = rest.substring(0, lastDot);
                    String field = rest.substring(lastDot + 1);
                    tablesByName
                            .computeIfAbsent(tableId, x -> new HashMap<>())
                            .put(field, e.getValue());
                }
                for (Map.Entry<String, Map<String, Object>> e : tablesByName.entrySet()) {
                    TableId tid = TableId.parse(e.getKey());
                    rehydrated.put(tid, TableOffset.fromMap(e.getValue()));
                }
            }
            return new DatabricksOffsetContext(config, rehydrated);
        }
    }
}
