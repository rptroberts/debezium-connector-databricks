/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.relational.TableId;

/**
 * Detects disruptive operations (TRUNCATE, REPLACE, RESTORE, OVERWRITE) in a
 * Delta table's history. CDF does not emit per-row deletes for these
 * operations, so the connector must spot them via {@code DESCRIBE HISTORY}.
 */
public class HistoryScanner {

    private static final Set<String> DISRUPTIVE_OPS = Set.of(
            "TRUNCATE",
            "REPLACE TABLE AS SELECT",
            "REPLACE TABLE",
            "RESTORE",
            "OVERWRITE",
            "CREATE OR REPLACE TABLE",
            "CREATE OR REPLACE TABLE AS SELECT");
    // Note: plain "WRITE" is the normal append op; WRITE-with-overwrite is detected via params in scan()

    private final DatabricksConnection connection;

    public HistoryScanner(DatabricksConnection connection) {
        this.connection = connection;
    }

    /**
     * Returns every disruptive operation in the inclusive range
     * {@code (fromExclusive, toInclusive]}.
     */
    public List<DisruptiveOp> scan(TableId tid, long fromExclusive, long toInclusive) throws SQLException {
        if (fromExclusive >= toInclusive) {
            return List.of();
        }
        String sql = "SELECT version, timestamp, operation, operationParameters " +
                "FROM (DESCRIBE HISTORY " + DeltaTableInspector.quoteTable(tid) + ") " +
                "WHERE version > " + fromExclusive + " AND version <= " + toInclusive +
                " ORDER BY version";
        List<DisruptiveOp> out = new ArrayList<>();
        connection.query(sql, rs -> {
            try {
                long version = rs.getLong(1);
                java.sql.Timestamp ts = rs.getTimestamp(2);
                String op = rs.getString(3);
                String params = rs.getString(4);
                boolean disruptive = DISRUPTIVE_OPS.contains(op);
                if ("WRITE".equals(op) && params != null && params.toLowerCase().contains("overwrite")) {
                    disruptive = true;
                }
                if (disruptive) {
                    out.add(new DisruptiveOp(tid, version, ts == null ? null : ts.toInstant(), op, params));
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return out;
    }

    /**
     * Represents one disruptive Delta log entry.
     */
    public record DisruptiveOp(TableId tableId, long version, java.time.Instant timestamp, String operation, String parameters) {
    }
}
