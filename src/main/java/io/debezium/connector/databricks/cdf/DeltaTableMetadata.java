/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.debezium.relational.TableId;

/**
 * Snapshot of a Delta table's column list, primary key, and CDF enablement state
 * at a particular point in time. Used by the schema cache.
 */
public final class DeltaTableMetadata {

    private final TableId tableId;
    private final List<DeltaColumn> columns;
    private final List<String> primaryKey;
    private final boolean cdfEnabled;

    public DeltaTableMetadata(TableId tableId, List<DeltaColumn> columns, List<String> primaryKey, boolean cdfEnabled) {
        this.tableId = Objects.requireNonNull(tableId);
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        this.cdfEnabled = cdfEnabled;
    }

    public TableId tableId() {
        return tableId;
    }

    public List<DeltaColumn> columns() {
        return columns;
    }

    public List<String> primaryKey() {
        return primaryKey;
    }

    public boolean cdfEnabled() {
        return cdfEnabled;
    }

    public DeltaColumn column(String name) {
        return columns.stream().filter(c -> c.name().equalsIgnoreCase(name)).findFirst().orElse(null);
    }

    public List<String> columnNames() {
        return columns.stream().map(DeltaColumn::name).collect(Collectors.toUnmodifiableList());
    }

    /**
     * Best-effort equality for change detection: same columns (name + canonical type + nullable) and same PK.
     */
    public boolean schemaEquivalentTo(DeltaTableMetadata other) {
        if (other == null) {
            return false;
        }
        if (!primaryKey.equals(other.primaryKey)) {
            return false;
        }
        if (columns.size() != other.columns.size()) {
            return false;
        }
        for (int i = 0; i < columns.size(); i++) {
            DeltaColumn a = columns.get(i);
            DeltaColumn b = other.columns.get(i);
            if (!a.name().equals(b.name())
                    || !a.canonicalType().equals(b.canonicalType())
                    || a.nullable() != b.nullable()) {
                return false;
            }
        }
        return true;
    }
}
