/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.time.Instant;

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Per-event source metadata for the Databricks connector.
 *
 * <p>The Delta change-data-feed offset is a monotonic {@code _commit_version}
 * per table; {@code _commit_timestamp} is captured for human/operator
 * inspection but is not the authoritative ordering key.
 */
public class SourceInfo extends BaseSourceInfo {

    public static final String COMMIT_VERSION_KEY = "commit_version";
    public static final String COMMIT_TIMESTAMP_KEY = "commit_timestamp";
    public static final String CHANGE_TYPE_KEY = "change_type";
    public static final String CATALOG_KEY = "catalog";
    public static final String SCHEMA_KEY = "schema";
    public static final String TABLE_KEY = "table";

    private long commitVersion = -1L;
    private Instant commitTimestamp;
    private String changeType;
    private TableId tableId;

    public SourceInfo(DatabricksConnectorConfig config) {
        super(config);
    }

    public long commitVersion() {
        return commitVersion;
    }

    public Instant commitTimestamp() {
        return commitTimestamp;
    }

    public String changeType() {
        return changeType;
    }

    public TableId tableId() {
        return tableId;
    }

    @Override
    protected Instant timestamp() {
        return commitTimestamp != null ? commitTimestamp : Instant.EPOCH;
    }

    @Override
    protected String database() {
        return tableId != null ? tableId.catalog() : null;
    }

    public void update(TableId tableId, long commitVersion, Instant commitTimestamp, String changeType) {
        this.tableId = tableId;
        this.commitVersion = commitVersion;
        this.commitTimestamp = commitTimestamp;
        this.changeType = changeType;
    }
}
