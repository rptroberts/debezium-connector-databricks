/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.relational.TableId;

/**
 * One row returned by {@code table_changes()}. The {@code values} map carries
 * the user-defined columns; the three CDF metadata columns are projected onto
 * the top-level fields.
 */
public final class ChangeRow {

    private final TableId tableId;
    private final ChangeType changeType;
    private final long commitVersion;
    private final Instant commitTimestamp;
    private final Map<String, Object> values;

    public ChangeRow(TableId tableId, ChangeType changeType, long commitVersion, Instant commitTimestamp, Map<String, Object> values) {
        this.tableId = Objects.requireNonNull(tableId);
        this.changeType = Objects.requireNonNull(changeType);
        this.commitVersion = commitVersion;
        this.commitTimestamp = commitTimestamp;
        // Map.copyOf rejects null values, so we cannot use it — Delta columns are routinely null.
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public TableId tableId() {
        return tableId;
    }

    public ChangeType changeType() {
        return changeType;
    }

    public long commitVersion() {
        return commitVersion;
    }

    public Instant commitTimestamp() {
        return commitTimestamp;
    }

    public Map<String, Object> values() {
        return values;
    }

    @Override
    public String toString() {
        return "ChangeRow{table=" + tableId.identifier() + ", op=" + changeType.value() +
                ", v=" + commitVersion + ", ts=" + commitTimestamp + "}";
    }
}
