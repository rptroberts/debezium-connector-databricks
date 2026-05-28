/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;
import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.relational.TableId;

/**
 * Reads Unity Catalog {@code information_schema} to discover tables and their
 * schemas, plus per-table CDF enablement.
 */
public class DeltaTableInspector {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaTableInspector.class);

    private final DatabricksConnection connection;
    private final DatabricksConnectorConfig config;

    public DeltaTableInspector(DatabricksConnection connection, DatabricksConnectorConfig config) {
        this.connection = connection;
        this.config = config;
    }

    /**
     * Lists user tables in the configured catalog whose {@code <catalog>.<schema>.<table>}
     * identifier passes the configured include/exclude regex filters.
     */
    public List<TableId> discoverTables() throws SQLException {
        String catalog = config.catalog();
        String sql = "SELECT table_catalog, table_schema, table_name " +
                "FROM " + quoteIdent(catalog) + ".information_schema.tables " +
                "WHERE table_type IN ('MANAGED','EXTERNAL') " +
                "AND table_schema <> 'information_schema'";
        List<TableId> all = new ArrayList<>();
        connection.query(sql, rs -> {
            try {
                all.add(new TableId(rs.getString(1), rs.getString(2), rs.getString(3)));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        List<TableId> filtered = new ArrayList<>();
        for (TableId tid : all) {
            if (config.isTableIncluded(tid)) {
                filtered.add(tid);
            }
        }
        LOG.info("Discovered {} table(s); {} after include/exclude filtering", all.size(), filtered.size());
        return filtered;
    }

    /**
     * Reads column list, types, nullability, and primary key for a single table.
     */
    public DeltaTableMetadata fetchMetadata(TableId tid) throws SQLException {
        List<DeltaColumn> cols = new ArrayList<>();
        String colSql = "SELECT column_name, full_data_type, is_nullable, ordinal_position " +
                "FROM " + quoteIdent(tid.catalog()) + ".information_schema.columns " +
                "WHERE table_schema = '" + sqlEscape(tid.schema()) + "' " +
                "AND table_name = '" + sqlEscape(tid.table()) + "' " +
                "ORDER BY ordinal_position";

        final List<String> pkSnapshot = fetchPrimaryKey(tid);
        Set<String> pkSet = new LinkedHashSet<>(pkSnapshot);

        connection.query(colSql, rs -> {
            try {
                String name = rs.getString(1);
                String type = rs.getString(2);
                boolean nullable = "YES".equalsIgnoreCase(rs.getString(3));
                int pos = rs.getInt(4);
                cols.add(new DeltaColumn(name, type, nullable, pos, pkSet.contains(name)));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        boolean cdfEnabled = isCdfEnabled(tid);
        return new DeltaTableMetadata(tid, cols, pkSnapshot, cdfEnabled);
    }

    private List<String> fetchPrimaryKey(TableId tid) throws SQLException {
        // Unity Catalog primary keys are exposed via information_schema constraint tables.
        String catalogQuoted = quoteIdent(tid.catalog());
        String sql = "SELECT kcu.column_name " +
                "FROM " + catalogQuoted + ".information_schema.table_constraints tc " +
                "JOIN " + catalogQuoted + ".information_schema.key_column_usage kcu " +
                "  ON tc.constraint_catalog = kcu.constraint_catalog " +
                " AND tc.constraint_schema  = kcu.constraint_schema " +
                " AND tc.constraint_name    = kcu.constraint_name " +
                "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                "  AND tc.table_schema = '" + sqlEscape(tid.schema()) + "' " +
                "  AND tc.table_name   = '" + sqlEscape(tid.table()) + "' " +
                "ORDER BY kcu.ordinal_position";
        List<String> pk = new ArrayList<>();
        try {
            connection.query(sql, rs -> {
                try {
                    pk.add(rs.getString(1));
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (SQLException e) {
            // Some workspace configurations may not expose key_column_usage; log and continue with empty PK.
            LOG.debug("Primary-key lookup failed for {}: {}", tid.identifier(), e.getMessage());
        }
        return pk;
    }

    /**
     * Checks whether the table has CDF enabled via {@code SHOW TBLPROPERTIES}.
     * Falls back to {@code false} on any error.
     */
    public boolean isCdfEnabled(TableId tid) throws SQLException {
        String sql = "SHOW TBLPROPERTIES " + quoteTable(tid) + " (delta.enableChangeDataFeed)";
        boolean[] enabled = { false };
        try {
            connection.query(sql, rs -> {
                try {
                    String key = rs.getString(1);
                    String v = rs.getString(2);
                    // Guard against future prefix-match behavior — only accept exact property key.
                    if ("delta.enableChangeDataFeed".equalsIgnoreCase(key)
                            && v != null && v.equalsIgnoreCase("true")) {
                        enabled[0] = true;
                    }
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (SQLException e) {
            LOG.debug("Failed to read CDF flag for {}: {}", tid.identifier(), e.getMessage());
            return false;
        }
        return enabled[0];
    }

    /**
     * Returns the latest {@code _commit_version} for the table (head version).
     */
    public long currentVersion(TableId tid) throws SQLException {
        // Cheap: limit DESCRIBE HISTORY to one row.
        String sql = "SELECT version FROM (DESCRIBE HISTORY " + quoteTable(tid) + ") ORDER BY version DESC LIMIT 1";
        long[] v = { -1L };
        connection.query(sql, rs -> {
            try {
                v[0] = rs.getLong(1);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return v[0];
    }

    private static String sqlEscape(String s) {
        return s == null ? "" : s.replace("'", "''");
    }

    /** Backtick-quote an identifier and double any backticks inside it (per Spark SQL). */
    public static String quoteIdent(String s) {
        if (s == null) {
            return "``";
        }
        return "`" + s.replace("`", "``") + "`";
    }

    /** Fully-qualified backtick-quoted table reference. */
    public static String quoteTable(TableId tid) {
        return quoteIdent(tid.catalog()) + "." + quoteIdent(tid.schema()) + "." + quoteIdent(tid.table());
    }
}
