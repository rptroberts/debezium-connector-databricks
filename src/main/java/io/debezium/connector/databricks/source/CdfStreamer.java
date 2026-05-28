/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.source;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;
import io.debezium.connector.databricks.DatabricksOffsetContext;
import io.debezium.connector.databricks.DatabricksOffsetContext.TableOffset;
import io.debezium.connector.databricks.DatabricksRecordBuilder;
import io.debezium.connector.databricks.DatabricksSchema;
import io.debezium.connector.databricks.cdf.ChangeRow;
import io.debezium.connector.databricks.cdf.DeltaCdfReader;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.connector.databricks.cdf.HeadVersionCache;
import io.debezium.relational.TableId;

/**
 * Reads {@code table_changes()} for one table and emits Debezium-style change
 * events.
 *
 * <p>Steady-state cost is one {@code table_changes} query per polling cycle
 * per table that has new commits; tables whose head hasn't advanced past
 * {@code last_committed_version} are skipped entirely thanks to the
 * {@link HeadVersionCache}.
 */
public class CdfStreamer {

    private static final Logger LOG = LoggerFactory.getLogger(CdfStreamer.class);

    private final DatabricksConnectorConfig config;
    private final DeltaCdfReader reader;
    private final DatabricksRecordBuilder recordBuilder;
    private final HeadVersionCache headCache;
    private final AtomicLong emptyPolls = new AtomicLong();

    public CdfStreamer(DatabricksConnectorConfig config,
                       DeltaCdfReader reader,
                       DatabricksRecordBuilder recordBuilder,
                       HeadVersionCache headCache) {
        this.config = config;
        this.reader = reader;
        this.recordBuilder = recordBuilder;
        this.headCache = headCache;
    }

    /**
     * Reads new CDF rows in {@code (last_committed, end]} where {@code end} is
     * bounded by the configured batch size. Returns the number of records
     * appended.
     */
    public int run(DeltaTableMetadata meta, DatabricksSchema.CachedSchema cached,
                   DatabricksOffsetContext offset, List<SourceRecord> out) throws SQLException {
        TableId tid = meta.tableId();
        TableOffset state = offset.offsetFor(tid);
        long head = headCache.get(tid);
        long lastSeen = state.commitVersion();

        if (head <= lastSeen) {
            // No new data — alert on excessive lag if configured.
            maybeWarnLag(tid, head, lastSeen);
            emptyPolls.incrementAndGet();
            return 0;
        }

        long start = lastSeen + 1;
        long end = Math.min(head, start + config.cdfBatchMaxVersions() - 1);
        maybeWarnLag(tid, head, lastSeen);

        List<ChangeRow> rows = new ArrayList<>();
        reader.readRange(meta, start, end, rows::add);

        if (rows.isEmpty()) {
            // The range has versions but no CDF rows (insert-only commits at metadata level can
            // appear empty in CDF). Advance the offset to `end` ONLY through a record-bearing
            // path — empty advance would leave the persisted offset behind the in-memory one.
            // We let the next poll re-discover this same empty range; cheap and idempotent.
            LOG.debug("CDF range [{},{}] on {} produced 0 rows; offset unchanged", start, end, tid.identifier());
            emptyPolls.incrementAndGet();
            return 0;
        }

        List<SourceRecord> recs = recordBuilder.buildBatch(cached, rows, offset);
        out.addAll(recs);
        long lastVersion = rows.get(rows.size() - 1).commitVersion();
        headCache.observe(tid, lastVersion);
        LOG.debug("Streamed {} rows from {} for versions [{},{}]", rows.size(), tid.identifier(), start, end);
        return recs.size();
    }

    /** Test/inspection: empty-poll counter. */
    public long emptyPolls() {
        return emptyPolls.get();
    }

    private void maybeWarnLag(TableId tid, long head, long lastSeen) {
        long threshold = config.lagAlertVersions();
        if (threshold > 0 && head > lastSeen && (head - lastSeen) >= threshold) {
            LOG.warn("CDF lag for {}: lastSeen=v{}, head=v{}, lag={} versions (threshold={})",
                    tid.identifier(), lastSeen, head, head - lastSeen, threshold);
        }
    }
}
