/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksSchema.CachedSchema;
import io.debezium.connector.databricks.cdf.ChangeRow;
import io.debezium.connector.databricks.cdf.ChangeType;
import io.debezium.relational.TableId;

/**
 * Builds Debezium-style {@link SourceRecord} envelopes from CDF rows.
 *
 * <p>For UPDATE operations Delta emits two rows ({@code update_preimage} +
 * {@code update_postimage}) with the same {@code _commit_version}. They are
 * paired here by primary key before emission.
 */
public class DatabricksRecordBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksRecordBuilder.class);

    private final DatabricksConnectorConfig config;
    private final DatabricksSchema schema;
    private final DatabricksPartition partition;
    // Tables for which we've already warned about no-PK update pairing.
    private final Set<TableId> warnedTables = ConcurrentHashMap.newKeySet();

    public DatabricksRecordBuilder(DatabricksConnectorConfig config, DatabricksSchema schema, DatabricksPartition partition) {
        this.config = Objects.requireNonNull(config);
        this.schema = Objects.requireNonNull(schema);
        this.partition = Objects.requireNonNull(partition);
    }

    /**
     * Pairs pre/post-image rows and emits one record per logical change.
     * The {@code rows} list MUST be from a single {@code table_changes()} batch
     * for one table (i.e. one {@code _commit_version} sequence).
     */
    public List<SourceRecord> buildBatch(CachedSchema cached, List<ChangeRow> rows, DatabricksOffsetContext offset) {
        List<SourceRecord> out = new ArrayList<>(rows.size());

        // Bucket by commit version to keep ordering and pair pre/post within a version.
        Map<Long, List<ChangeRow>> byVersion = new LinkedHashMap<>();
        for (ChangeRow r : rows) {
            byVersion.computeIfAbsent(r.commitVersion(), v -> new ArrayList<>()).add(r);
        }

        boolean hasPk = !cached.metadata.primaryKey().isEmpty();
        if (!hasPk && warnedTables.add(cached.metadata.tableId())) {
            // Tables without a primary key cannot reliably pair pre/post-image. Postimages
            // arrive as op=u with `before=null` so downstream consumers still see the change,
            // but the previous value is unrecoverable from CDF alone for these tables.
            LOG.warn("Table {} has no primary key — UPDATE events will be emitted without a paired before-image. " +
                    "Set message.key.columns or add a PRIMARY KEY constraint to enable pairing.",
                    cached.metadata.tableId().identifier());
        }

        for (Map.Entry<Long, List<ChangeRow>> e : byVersion.entrySet()) {
            List<ChangeRow> commitRows = e.getValue();
            // Index update_preimage rows by PK so we can attach them to their postimage.
            Map<Object, ChangeRow> preimagesByKey = new LinkedHashMap<>();
            Iterator<ChangeRow> it = commitRows.iterator();
            while (it.hasNext()) {
                ChangeRow row = it.next();
                if (row.changeType() == ChangeType.UPDATE_PREIMAGE) {
                    if (hasPk) {
                        preimagesByKey.put(keyTuple(cached, row), row);
                        it.remove();
                    }
                    // No PK: leave the preimage in the row list — it will be emitted on its own
                    // as op=u with `after=null`, preserving data without falsely pairing.
                }
            }

            for (ChangeRow row : commitRows) {
                ChangeRow preimage = null;
                if (row.changeType() == ChangeType.UPDATE_POSTIMAGE && hasPk) {
                    preimage = preimagesByKey.remove(keyTuple(cached, row));
                }
                SourceRecord rec = build(cached, row, preimage, offset);
                if (rec != null) {
                    out.add(rec);
                }
            }
        }
        return out;
    }

    /**
     * Builds a single record for a snapshot row (op=r).
     */
    public SourceRecord buildSnapshot(CachedSchema cached, Map<String, Object> values, long version, Instant ts, DatabricksOffsetContext offset) {
        offset.markSnapshotStarted(cached.metadata.tableId());
        offset.recordCommit(cached.metadata.tableId(), version, ts);
        Struct source = buildSource(cached.metadata.tableId(), version, ts, "read", offset);
        Struct after = toValueStruct(cached, values);
        Struct envelope = cached.envelope.read(after, source, ts);
        Struct keyStruct = keyStruct(cached, values);
        return new SourceRecord(
                partition.getSourcePartition(),
                offset.getOffset(),
                cached.topic,
                null,
                cached.keySchema,
                keyStruct,
                cached.envelope.schema(),
                envelope,
                ts == null ? null : ts.toEpochMilli());
    }

    private SourceRecord build(CachedSchema cached, ChangeRow row, ChangeRow preimage, DatabricksOffsetContext offset) {
        TableId tid = cached.metadata.tableId();
        offset.recordCommit(tid, row.commitVersion(), row.commitTimestamp());
        Struct source = buildSource(tid, row.commitVersion(), row.commitTimestamp(), row.changeType().value(), offset);
        Instant ts = row.commitTimestamp();
        Struct envelope;
        switch (row.changeType()) {
            case INSERT: {
                Struct after = toValueStruct(cached, row.values());
                envelope = cached.envelope.create(after, source, ts);
                break;
            }
            case DELETE: {
                Struct before = toValueStruct(cached, row.values());
                envelope = cached.envelope.delete(before, source, ts);
                break;
            }
            case UPDATE_POSTIMAGE: {
                Struct after = toValueStruct(cached, row.values());
                Struct before = preimage != null ? toValueStruct(cached, preimage.values()) : null;
                envelope = cached.envelope.update(before, after, source, ts);
                break;
            }
            case UPDATE_PREIMAGE: {
                // PK-less table path: emit the preimage as an op=u with only `before` set,
                // so downstream consumers see the prior state. With PK, preimages are removed
                // from the row list before this point and never reach here.
                Struct before = toValueStruct(cached, row.values());
                envelope = cached.envelope.update(before, null, source, ts);
                break;
            }
            default:
                return null;
        }
        Struct keyStruct = keyStruct(cached, row.values());
        return new SourceRecord(
                partition.getSourcePartition(),
                offset.getOffset(),
                cached.topic,
                null,
                cached.keySchema,
                keyStruct,
                cached.envelope.schema(),
                envelope,
                ts == null ? null : ts.toEpochMilli());
    }

    /**
     * Emits a synthetic truncate event when a disruptive Delta op (TRUNCATE/REPLACE/RESTORE/OVERWRITE) is detected.
     */
    public SourceRecord buildTruncate(CachedSchema cached, long version, Instant ts, DatabricksOffsetContext offset) {
        TableId tid = cached.metadata.tableId();
        offset.recordCommit(tid, version, ts);
        Struct source = buildSource(tid, version, ts, "truncate", offset);
        Struct envelope = cached.envelope.truncate(source, ts);
        return new SourceRecord(
                partition.getSourcePartition(),
                offset.getOffset(),
                cached.topic,
                null,
                null, // truncate carries no key
                null,
                cached.envelope.schema(),
                envelope,
                ts == null ? null : ts.toEpochMilli());
    }

    private Struct toValueStruct(CachedSchema cached, Map<String, Object> values) {
        Struct s = new Struct(cached.valueSchema);
        for (var col : cached.metadata.columns()) {
            s.put(col.name(), values.get(col.name()));
        }
        return s;
    }

    private Struct keyStruct(CachedSchema cached, Map<String, Object> values) {
        if (cached.keySchema == null) {
            return null;
        }
        Struct k = new Struct(cached.keySchema);
        for (String pk : cached.metadata.primaryKey()) {
            k.put(pk, values.get(pk));
        }
        return k;
    }

    private Object keyTuple(CachedSchema cached, ChangeRow row) {
        // Callers only invoke this when a primary key is present (PK-less pairing is
        // structurally avoided in buildBatch). Defensive guard kept for callers that
        // might bypass that invariant.
        List<String> pkCols = cached.metadata.primaryKey();
        if (pkCols.isEmpty()) {
            throw new IllegalStateException("keyTuple called on PK-less table " + cached.metadata.tableId().identifier());
        }
        List<Object> tuple = new ArrayList<>(pkCols.size());
        for (String pk : pkCols) {
            tuple.add(row.values().get(pk));
        }
        return tuple;
    }

    /**
     * Test/inspection hook: tables for which a "no PK" warning has been emitted.
     */
    Set<TableId> warnedTables() {
        return new HashSet<>(warnedTables);
    }

    private Struct buildSource(TableId tid, long version, Instant ts, String changeType, DatabricksOffsetContext offset) {
        SourceInfo info = offset.getSource();
        info.update(tid, version, ts, changeType);
        return schema.sourceInfoStructMaker().struct(info);
    }
}
