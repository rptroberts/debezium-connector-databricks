/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

class DeltaTypesTest {

    @Test
    void mapsScalarTypes() {
        assertThat(schemaOf("bigint").type()).isEqualTo(Schema.Type.INT64);
        assertThat(schemaOf("int").type()).isEqualTo(Schema.Type.INT32);
        assertThat(schemaOf("smallint").type()).isEqualTo(Schema.Type.INT16);
        assertThat(schemaOf("tinyint").type()).isEqualTo(Schema.Type.INT8);
        assertThat(schemaOf("double").type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(schemaOf("float").type()).isEqualTo(Schema.Type.FLOAT32);
        assertThat(schemaOf("boolean").type()).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(schemaOf("string").type()).isEqualTo(Schema.Type.STRING);
        assertThat(schemaOf("binary").type()).isEqualTo(Schema.Type.BYTES);
    }

    @Test
    void mapsDecimalWithScaleFromTypeString() {
        Schema s = schemaOf("decimal(18,4)");
        assertThat(s.type()).isEqualTo(Schema.Type.BYTES);
        assertThat(s.name()).isEqualTo(Decimal.LOGICAL_NAME);
        assertThat(s.parameters().get(Decimal.SCALE_FIELD)).isEqualTo("4");
    }

    @Test
    void mapsDateAsLogicalDays() {
        Schema s = schemaOf("date");
        assertThat(s.name()).isEqualTo("io.debezium.time.Date");
    }

    @Test
    void mapsTimestampAsMicros() {
        Schema s = schemaOf("timestamp");
        assertThat(s.name()).isEqualTo("io.debezium.time.MicroTimestamp");
    }

    @Test
    void convertsBigDecimal() {
        DeltaColumn c = new DeltaColumn("amt", "decimal(18,4)", true, 1, false);
        Object out = DeltaTypes.convert(c, BigDecimal.valueOf(12345, 4));
        assertThat(out).isInstanceOf(BigDecimal.class);
    }

    @Test
    void convertsDate() {
        DeltaColumn c = new DeltaColumn("dob", "date", true, 1, false);
        int days = (int) DeltaTypes.convert(c, LocalDate.of(2026, 1, 1));
        assertThat(days).isEqualTo((int) LocalDate.of(2026, 1, 1).toEpochDay());
    }

    @Test
    void convertsTimestampToMicros() {
        DeltaColumn c = new DeltaColumn("ts", "timestamp", true, 1, false);
        long micros = (long) DeltaTypes.convert(c, Timestamp.from(Instant.ofEpochSecond(1700000000L, 123_456_000)));
        assertThat(micros).isEqualTo(1700000000L * 1_000_000L + 123_456L);
    }

    @Test
    void convertsBinaryToByteBuffer() {
        DeltaColumn c = new DeltaColumn("b", "binary", true, 1, false);
        Object out = DeltaTypes.convert(c, new byte[] { 1, 2, 3 });
        assertThat(out).isInstanceOf(ByteBuffer.class);
    }

    @Test
    void unknownTypesFallBackToString() {
        Schema s = schemaOf("array<int>");
        assertThat(s.type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void stringFallbackPreservesMicroPrecision() {
        // Regression: string fallback used toEpochMilli() * 1000 which dropped sub-ms precision.
        DeltaColumn c = new DeltaColumn("ts", "timestamp", true, 1, false);
        long micros = (long) DeltaTypes.convert(c, "2026-05-28T12:00:00.123456Z");
        // Sub-ms must survive: full micros, not ms-quantized * 1000.
        assertThat(micros).isEqualTo(1779_969_600_123_456L);
        assertThat(micros % 1000).isEqualTo(456L); // proves sub-ms didn't get rounded
    }

    @Test
    void preservesNullability() {
        Schema nullable = schemaOf("string");
        assertThat(nullable.isOptional()).isTrue();
        Schema required = DeltaTypes.toConnectSchema(new DeltaColumn("c", "string", false, 1, false));
        assertThat(required.isOptional()).isFalse();
    }

    private static Schema schemaOf(String type) {
        return DeltaTypes.toConnectSchema(new DeltaColumn("c", type, true, 1, false));
    }
}
