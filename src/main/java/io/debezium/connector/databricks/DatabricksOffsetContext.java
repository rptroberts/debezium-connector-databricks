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
 * value as flat {@code tables||<tableId>||<field>} entries (so the Embedded
 * Engine's primitive-only offset store accepts them, and a {@code .} inside an
 * identifier can't collide with the delimiter).
 */
public class DatabricksOffsetContext extends CommonOffsetContext<SourceInfo> {

    public static final String PER_TABLE_KEY = "tables";
    /**
     * Separator placed between the {@link #PER_TABLE_KEY} prefix and the table id,
     * and between the table id and the field name. Two pipes were chosen
     * because they cannot appear in any Spark/Delta identifier (the pipe is
     * reserved by SQL) — this makes round-tripping safe for arbitrary names.
     */
    private static final String DELIMITER = "||";
    public static final String COMMIT_VERSION_KEY = "commit_version";
    public static final String COMMIT_TIMESTAMP_KEY = "commit_timestamp_ms";
    public static final String SNAPSHOT_PHASE_KEY = "snapshot_phase";

    /** Phase a table is in with respect to its initial snapshot. */
    public enum SnapshotPhase {
        /** No snapshot has been started; either snapshot.mode says not to, or we just started. */
        NOT_STARTED,
        /** Snapshot is running; rows being emitted as op=r. */
        IN_PROGRESS,
        /** Snapshot completed; CDF streaming is active. */
        COMPLETED;

        public boolean isSnapshotting() {
            return this == IN_PROGRESS;
        }

        public boolean isCompleted() {
            return this == COMPLETED;
        }
    }

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

    /**
     * Advance the table's commit version + timestamp. Does not touch
     * {@link SnapshotPhase} — callers use {@link #markSnapshotStarted},
     * {@link #markSnapshotCompleted}, or {@link #resetForResnapshot} for phase
     * transitions.
     */
    public void recordCommit(TableId tableId, long commitVersion, Instant commitTimestamp) {
        tableOffsets.merge(tableId,
                new TableOffset(commitVersion, commitTimestamp, SnapshotPhase.NOT_STARTED),
                (oldV, newV) -> {
                    long winnerVersion = Math.max(oldV.commitVersion, newV.commitVersion);
                    Instant winnerTs;
                    if (newV.commitVersion >= oldV.commitVersion && newV.commitTimestamp != null) {
                        winnerTs = newV.commitTimestamp;
                    }
                    else {
                        winnerTs = oldV.commitTimestamp != null ? oldV.commitTimestamp : newV.commitTimestamp;
                    }
                    return new TableOffset(winnerVersion, winnerTs, oldV.phase);
                });
    }

    public void markSnapshotStarted(TableId tableId) {
        TableOffset cur = offsetFor(tableId);
        tableOffsets.put(tableId, new TableOffset(cur.commitVersion, cur.commitTimestamp, SnapshotPhase.IN_PROGRESS));
    }

    public void markSnapshotCompleted(TableId tableId) {
        TableOffset cur = offsetFor(tableId);
        tableOffsets.put(tableId, new TableOffset(cur.commitVersion, cur.commitTimestamp, SnapshotPhase.COMPLETED));
    }

    /** Forcefully reset to "needs initial snapshot" state — used by the RESNAPSHOT disruptive-op handler. */
    public void resetForResnapshot(TableId tableId) {
        tableOffsets.put(tableId, TableOffset.empty());
    }

    @Override
    public Map<String, ?> getOffset() {
        // Embedded Engine's offset stores accept only primitive types. Flatten the per-table map
        // to entries `tables||<tableId>||<field>` to survive both the Kafka offset writer and
        // the file-backed embedded store.
        Map<String, Object> out = new HashMap<>();
        for (Map.Entry<TableId, TableOffset> e : tableOffsets.entrySet()) {
            String prefix = PER_TABLE_KEY + DELIMITER + e.getKey().identifier() + DELIMITER;
            TableOffset off = e.getValue();
            out.put(prefix + COMMIT_VERSION_KEY, off.commitVersion);
            if (off.commitTimestamp != null) {
                out.put(prefix + COMMIT_TIMESTAMP_KEY, off.commitTimestamp.toEpochMilli());
            }
            out.put(prefix + SNAPSHOT_PHASE_KEY, off.phase.name());
        }
        return out;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return tableOffsets.values().stream().anyMatch(t -> t.phase == SnapshotPhase.IN_PROGRESS);
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
    public record TableOffset(long commitVersion, Instant commitTimestamp, SnapshotPhase phase) {

        public static TableOffset empty() {
            return new TableOffset(-1L, null, SnapshotPhase.NOT_STARTED);
        }

        /** True if the table is currently being snapshotted. */
        public boolean snapshotting() {
            return phase == SnapshotPhase.IN_PROGRESS;
        }

        /** True iff initial snapshot has finished. */
        public boolean snapshotCompleted() {
            return phase == SnapshotPhase.COMPLETED;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new HashMap<>();
            m.put(COMMIT_VERSION_KEY, commitVersion);
            if (commitTimestamp != null) {
                m.put(COMMIT_TIMESTAMP_KEY, commitTimestamp.toEpochMilli());
            }
            m.put(SNAPSHOT_PHASE_KEY, phase.name());
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
            SnapshotPhase phase = parsePhase(m);
            return new TableOffset(v, ts, phase);
        }

        private static SnapshotPhase parsePhase(Map<String, ?> m) {
            Object raw = m.get(SNAPSHOT_PHASE_KEY);
            if (raw != null) {
                try {
                    return SnapshotPhase.valueOf(raw.toString());
                }
                catch (IllegalArgumentException ignored) {
                }
            }
            // Backwards-compat with the pre-0.2 boolean encoding.
            Object oldSnapshotting = m.get("snapshot");
            Object oldCompleted = m.get("snapshot_completed");
            if (Boolean.TRUE.equals(oldCompleted)) {
                return SnapshotPhase.COMPLETED;
            }
            if (Boolean.TRUE.equals(oldSnapshotting)) {
                return SnapshotPhase.IN_PROGRESS;
            }
            return SnapshotPhase.NOT_STARTED;
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
                String prefix = PER_TABLE_KEY + DELIMITER;
                Map<String, Map<String, Object>> tablesByName = new HashMap<>();
                for (Map.Entry<String, ?> e : offset.entrySet()) {
                    String k = e.getKey();
                    if (!k.startsWith(prefix)) {
                        continue;
                    }
                    String rest = k.substring(prefix.length());
                    int sep = rest.lastIndexOf(DELIMITER);
                    if (sep < 0) {
                        continue;
                    }
                    String tableId = rest.substring(0, sep);
                    String field = rest.substring(sep + DELIMITER.length());
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
