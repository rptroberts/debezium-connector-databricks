/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.source;

import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;
import io.debezium.connector.databricks.cdf.DeltaTableInspector;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.relational.TableId;

/**
 * Owns the working set of tables under capture, plus their cached metadata,
 * refreshed on a configured cadence.
 *
 * <p>Refresh is best-effort: if discovery fails after the initial population,
 * the previous table list + metadata is retained and the failure is logged
 * (the steady-state poll then continues to work even during a transient
 * metadata outage).
 */
public class TableSetManager {

    private static final Logger LOG = LoggerFactory.getLogger(TableSetManager.class);

    private final DatabricksConnectorConfig config;
    private final DeltaTableInspector inspector;
    private final Map<TableId, DeltaTableMetadata> metadataCache = new LinkedHashMap<>();
    private long lastRefreshNs = 0L;

    public TableSetManager(DatabricksConnectorConfig config, DeltaTableInspector inspector) {
        this.config = config;
        this.inspector = inspector;
    }

    /**
     * Returns the currently-captured tables (in stable discovery order). May
     * trigger a refresh if the cache is empty or the configured TTL has passed.
     */
    public List<TableId> currentTables() throws SQLException {
        refreshIfDue();
        return List.copyOf(metadataCache.keySet());
    }

    public DeltaTableMetadata metadataFor(TableId tid) {
        return metadataCache.get(tid);
    }

    /**
     * Force-set the table list. Test/IT-only.
     */
    public void setTablesForTest(List<TableId> tables) throws SQLException {
        Map<TableId, DeltaTableMetadata> fresh = inspector.fetchMetadataBatch(tables);
        metadataCache.clear();
        metadataCache.putAll(fresh);
        lastRefreshNs = System.nanoTime();
    }

    private void refreshIfDue() throws SQLException {
        long now = System.nanoTime();
        if (!metadataCache.isEmpty() && (now - lastRefreshNs) <= config.metadataRefreshInterval().toNanos()) {
            return;
        }
        try {
            List<TableId> discovered = inspector.discoverTables();
            Map<TableId, DeltaTableMetadata> fresh = inspector.fetchMetadataBatch(discovered);
            // Drop tables whose CDF is not enabled — they can't be captured.
            Map<TableId, DeltaTableMetadata> captureable = new LinkedHashMap<>();
            for (Map.Entry<TableId, DeltaTableMetadata> e : fresh.entrySet()) {
                if (e.getValue().cdfEnabled()) {
                    captureable.put(e.getKey(), e.getValue());
                }
                else {
                    LOG.warn("Skipping {} — CDF not enabled (set TBLPROPERTIES delta.enableChangeDataFeed = true).",
                            e.getKey().identifier());
                }
            }
            metadataCache.clear();
            metadataCache.putAll(captureable);
            lastRefreshNs = now;
            LOG.info("Table set refreshed: {} table(s) under capture", metadataCache.size());
        }
        catch (SQLException e) {
            if (metadataCache.isEmpty()) {
                throw e;
            }
            LOG.warn("Table-set refresh failed; continuing with stale set of {}: {}",
                    metadataCache.size(), e.getMessage());
        }
    }

    /** Test inspection. */
    Map<TableId, DeltaTableMetadata> metadataCacheSnapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(metadataCache));
    }
}
