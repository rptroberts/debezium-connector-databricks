/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

class ChangeRowTest {

    @Test
    void acceptsNullColumnValues() {
        // Regression: Map.copyOf rejects nulls, so previously any null Delta column NPE'd here.
        Map<String, Object> values = new HashMap<>();
        values.put("id", 1L);
        values.put("name", null);
        ChangeRow row = new ChangeRow(
                new TableId("c", "s", "t"),
                ChangeType.INSERT,
                42L,
                Instant.now(),
                values);
        assertThat(row.values()).containsEntry("id", 1L);
        assertThat(row.values()).containsKey("name");
        assertThat(row.values().get("name")).isNull();
    }

    @Test
    void valuesMapIsUnmodifiable() {
        ChangeRow row = new ChangeRow(
                new TableId("c", "s", "t"),
                ChangeType.INSERT,
                1L,
                Instant.now(),
                Map.of("k", "v"));
        assertThatThrownBy(() -> row.values().put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }
}
