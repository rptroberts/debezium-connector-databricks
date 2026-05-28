/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

/**
 * Values of the {@code _change_type} pseudo-column returned by Delta CDF.
 */
public enum ChangeType {

    INSERT("insert"),
    UPDATE_PREIMAGE("update_preimage"),
    UPDATE_POSTIMAGE("update_postimage"),
    DELETE("delete");

    private final String value;

    ChangeType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static ChangeType from(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("_change_type is null");
        }
        for (ChangeType t : values()) {
            if (t.value.equals(raw)) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown _change_type value: " + raw);
    }
}
