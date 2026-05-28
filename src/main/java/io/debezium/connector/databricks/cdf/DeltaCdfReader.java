/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.relational.TableId;

/**
 * Reads change-data-feed rows for a given table and version range using
 * {@code SELECT * FROM table_changes(...)}.
 */
public class DeltaCdfReader {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaCdfReader.class);

    private static final String CHANGE_TYPE_COL = "_change_type";
    private static final String COMMIT_VERSION_COL = "_commit_version";
    private static final String COMMIT_TIMESTAMP_COL = "_commit_timestamp";

    private final DatabricksConnection connection;

    public DeltaCdfReader(DatabricksConnection connection) {
        this.connection = connection;
    }

    /**
     * Read rows in the inclusive range {@code [startVersion, endVersion]} for the table.
     * Calls {@code rowHandler} for each row in result order.
     */
    public void readRange(DeltaTableMetadata meta, long startVersion, long endVersion, Consumer<ChangeRow> rowHandler) throws SQLException {
        if (startVersion > endVersion) {
            return;
        }
        String tableLit = meta.tableId().catalog() + "." + meta.tableId().schema() + "." + meta.tableId().table();
        String sql = "SELECT * FROM table_changes('" + tableLit.replace("'", "''") +
                "', " + startVersion + ", " + endVersion + ")";
        LOG.debug("CDF query: {}", sql);
        connection.query(sql, rs -> {
            try {
                ChangeRow row = projectRow(meta, rs);
                rowHandler.accept(row);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Reads rows starting from {@code startVersion} (inclusive) up to the table's current head.
     */
    public void readFrom(DeltaTableMetadata meta, long startVersion, Consumer<ChangeRow> rowHandler) throws SQLException {
        String tableLit = meta.tableId().catalog() + "." + meta.tableId().schema() + "." + meta.tableId().table();
        String sql = "SELECT * FROM table_changes('" + tableLit.replace("'", "''") + "', " + startVersion + ")";
        connection.query(sql, rs -> {
            try {
                rowHandler.accept(projectRow(meta, rs));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ChangeRow projectRow(DeltaTableMetadata meta, ResultSet rs) throws SQLException {
        ResultSetMetaData rsm = rs.getMetaData();
        Map<String, Object> values = new LinkedHashMap<>();
        String changeType = null;
        long commitVersion = -1L;
        Instant commitTimestamp = null;

        // Case-insensitive lookup from label → canonical column name so we don't lose data when the
        // driver's getColumnLabel() returns a different case than information_schema.columns.
        Map<String, String> canonicalByLowerLabel = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String name : meta.columnNames()) {
            canonicalByLowerLabel.put(name, name);
        }

        int colCount = rsm.getColumnCount();
        for (int i = 1; i <= colCount; i++) {
            String label = rsm.getColumnLabel(i);
            if (label == null) {
                continue;
            }
            String lower = label.toLowerCase(Locale.ROOT);
            if (CHANGE_TYPE_COL.equals(lower)) {
                changeType = rs.getString(i);
            }
            else if (COMMIT_VERSION_COL.equals(lower)) {
                commitVersion = rs.getLong(i);
            }
            else if (COMMIT_TIMESTAMP_COL.equals(lower)) {
                java.sql.Timestamp ts = rs.getTimestamp(i);
                if (ts != null) {
                    commitTimestamp = ts.toInstant();
                }
            }
            else {
                String canonical = canonicalByLowerLabel.get(label);
                if (canonical != null) {
                    DeltaColumn dc = meta.column(canonical);
                    Object raw = rs.getObject(i);
                    values.put(canonical, dc != null ? DeltaTypes.convert(dc, raw) : raw);
                }
            }
        }

        TableId tid = meta.tableId();
        return new ChangeRow(tid, ChangeType.from(changeType), commitVersion, commitTimestamp, values);
    }
}
