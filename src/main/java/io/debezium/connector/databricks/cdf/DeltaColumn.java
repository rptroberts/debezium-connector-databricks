/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

/**
 * Metadata for a single Delta table column, sufficient for Kafka Connect schema construction.
 */
public record DeltaColumn(String name, String typeName, boolean nullable, int position, boolean primaryKey) {

    /** Returns the lower-cased canonical Spark/Delta type name, e.g. "string", "bigint", "decimal(38,18)". */
    public String canonicalType() {
        return typeName == null ? "string" : typeName.toLowerCase().trim();
    }
}
