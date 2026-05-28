/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.source;

import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksOffsetContext;
import io.debezium.connector.databricks.DatabricksRecordBuilder;
import io.debezium.connector.databricks.DatabricksSchema;
import io.debezium.connector.databricks.cdf.DeltaTableInspector;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.connector.databricks.cdf.DeltaTypes;
import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.relational.TableId;

/**
 * Runs an initial snapshot of a single Delta table via Delta time travel
 * ({@code SELECT * FROM t VERSION AS OF v0}).
 *
 * <p>The snapshot is pinned at a single version captured at start-of-snapshot.
 * Restarts that occur mid-snapshot will re-read from version 0 of the
 * snapshot — Delta guarantees consistent time-travel reads, so duplicates are
 * possible but data loss is not.
 */
public class TableSnapshotter {

    private static final Logger LOG = LoggerFactory.getLogger(TableSnapshotter.class);

    private final DatabricksConnection connection;
    private final DatabricksRecordBuilder recordBuilder;

    public TableSnapshotter(DatabricksConnection connection, DatabricksRecordBuilder recordBuilder) {
        this.connection = connection;
        this.recordBuilder = recordBuilder;
    }

    /**
     * Snapshots the table at the given version, appending records to {@code out}.
     *
     * @return number of rows emitted
     */
    public int run(DeltaTableMetadata meta, DatabricksSchema.CachedSchema cached, long version,
                   DatabricksOffsetContext offset, List<SourceRecord> out) throws SQLException {
        TableId tid = meta.tableId();
        String cols = String.join(",", meta.columnNames().stream().map(DeltaTableInspector::quoteIdent).toList());
        if (cols.isEmpty()) {
            cols = "*";
        }
        String sql = "SELECT " + cols + " FROM " + DeltaTableInspector.quoteTable(tid) + " VERSION AS OF " + version;
        int[] rowCount = { 0 };
        connection.query(sql, rs -> {
            try {
                Map<String, Object> values = new LinkedHashMap<>();
                for (var col : meta.columns()) {
                    Object raw = rs.getObject(col.name());
                    values.put(col.name(), DeltaTypes.convert(col, raw));
                }
                SourceRecord rec = recordBuilder.buildSnapshot(cached, values, version, Instant.now(), offset);
                if (rec != null) {
                    out.add(rec);
                    rowCount[0]++;
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        LOG.info("Snapshot of {} at version {}: {} rows", tid.identifier(), version, rowCount[0]);
        return rowCount[0];
    }
}
