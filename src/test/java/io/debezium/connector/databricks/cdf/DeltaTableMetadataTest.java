/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

class DeltaTableMetadataTest {

    private static final TableId TID = new TableId("main", "banking", "customer");

    @Test
    void schemaEquivalentToWhenSame() {
        DeltaTableMetadata a = build(List.of(col("id", "bigint", false, 1), col("name", "string", true, 2)), List.of("id"));
        DeltaTableMetadata b = build(List.of(col("id", "bigint", false, 1), col("name", "string", true, 2)), List.of("id"));
        assertThat(a.schemaEquivalentTo(b)).isTrue();
    }

    @Test
    void notEquivalentWhenColumnAdded() {
        DeltaTableMetadata a = build(List.of(col("id", "bigint", false, 1)), List.of("id"));
        DeltaTableMetadata b = build(List.of(col("id", "bigint", false, 1), col("name", "string", true, 2)), List.of("id"));
        assertThat(a.schemaEquivalentTo(b)).isFalse();
    }

    @Test
    void notEquivalentWhenTypeChanged() {
        DeltaTableMetadata a = build(List.of(col("id", "int", false, 1)), List.of("id"));
        DeltaTableMetadata b = build(List.of(col("id", "bigint", false, 1)), List.of("id"));
        assertThat(a.schemaEquivalentTo(b)).isFalse();
    }

    @Test
    void notEquivalentWhenPkChanged() {
        DeltaTableMetadata a = build(List.of(col("id", "bigint", false, 1), col("ssn", "string", false, 2)), List.of("id"));
        DeltaTableMetadata b = build(List.of(col("id", "bigint", false, 1), col("ssn", "string", false, 2)), List.of("ssn"));
        assertThat(a.schemaEquivalentTo(b)).isFalse();
    }

    @Test
    void columnLookupIsCaseInsensitive() {
        DeltaTableMetadata m = build(List.of(col("Id", "bigint", false, 1)), List.of());
        assertThat(m.column("id")).isNotNull();
        assertThat(m.column("ID")).isNotNull();
    }

    @Test
    void quoteIdentEscapesBackticks() {
        assertThat(DeltaTableInspector.quoteIdent("normal")).isEqualTo("`normal`");
        assertThat(DeltaTableInspector.quoteIdent("with`tick")).isEqualTo("`with``tick`");
        assertThat(DeltaTableInspector.quoteIdent(null)).isEqualTo("``");
    }

    @Test
    void quoteTableQuotesAllThreeParts() {
        assertThat(DeltaTableInspector.quoteTable(new TableId("c", "s", "t"))).isEqualTo("`c`.`s`.`t`");
        // Adversarial: identifier containing a backtick must be escaped, not break out
        assertThat(DeltaTableInspector.quoteTable(new TableId("c", "s", "t`;DROP")))
                .isEqualTo("`c`.`s`.`t``;DROP`");
    }

    private static DeltaColumn col(String name, String type, boolean nullable, int pos) {
        return new DeltaColumn(name, type, nullable, pos, false);
    }

    private static DeltaTableMetadata build(List<DeltaColumn> cols, List<String> pk) {
        return new DeltaTableMetadata(TID, cols, pk, true);
    }
}
