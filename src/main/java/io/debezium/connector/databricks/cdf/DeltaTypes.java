/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Locale;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Mapping between Delta/Spark SQL type names and Kafka Connect schemas / values.
 *
 * <p>Limitations:
 * <ul>
 *   <li>{@code ARRAY}, {@code MAP}, {@code STRUCT}, and {@code VARIANT} are flattened
 *       to their JSON string representation. Nested struct handling is a v0.2 target.</li>
 *   <li>{@code TIMESTAMP} is mapped to {@code INT64} micros from epoch (UTC).</li>
 *   <li>{@code TIMESTAMP_NTZ} is mapped to {@code INT64} micros wall-clock (no zone).</li>
 *   <li>{@code DECIMAL} is encoded as Kafka Connect {@link Decimal} with the column's scale.</li>
 * </ul>
 */
public final class DeltaTypes {

    private DeltaTypes() {
    }

    /**
     * Returns the Connect schema for a column. The schema is optional iff the
     * column is nullable.
     */
    public static Schema toConnectSchema(DeltaColumn col) {
        SchemaBuilder b = baseBuilder(col.canonicalType());
        if (col.nullable()) {
            b.optional();
        }
        return b.build();
    }

    private static SchemaBuilder baseBuilder(String type) {
        String t = type.toLowerCase(Locale.ROOT).trim();

        // Decimal: decimal(p,s)
        if (t.startsWith("decimal")) {
            int scale = 0;
            int open = t.indexOf('(');
            int close = t.indexOf(')');
            if (open > 0 && close > open) {
                String[] parts = t.substring(open + 1, close).split(",");
                if (parts.length == 2) {
                    try {
                        scale = Integer.parseInt(parts[1].trim());
                    }
                    catch (NumberFormatException ignored) {
                    }
                }
            }
            return Decimal.builder(scale);
        }

        switch (t) {
            case "tinyint":
            case "byte":
                return SchemaBuilder.int8();
            case "smallint":
            case "short":
                return SchemaBuilder.int16();
            case "int":
            case "integer":
                return SchemaBuilder.int32();
            case "bigint":
            case "long":
                return SchemaBuilder.int64();
            case "float":
            case "real":
                return SchemaBuilder.float32();
            case "double":
                return SchemaBuilder.float64();
            case "boolean":
            case "bool":
                return SchemaBuilder.bool();
            case "binary":
                return SchemaBuilder.bytes();
            case "date":
                return io.debezium.time.Date.builder();
            case "timestamp":
            case "timestamp_ltz":
                return io.debezium.time.MicroTimestamp.builder();
            case "timestamp_ntz":
                return io.debezium.time.MicroTimestamp.builder();
            case "string":
            case "varchar":
            case "char":
                return SchemaBuilder.string();
            case "void":
                return SchemaBuilder.string().optional();
            case "variant":
                return io.debezium.data.Json.builder();
            default:
                // Complex types — array<...>, map<...>, struct<...>, interval ... — and unknowns.
                return SchemaBuilder.string();
        }
    }

    /**
     * Converts a JDBC-returned value to the Connect representation matching {@link #toConnectSchema}.
     */
    public static Object convert(DeltaColumn col, Object raw) {
        if (raw == null) {
            return null;
        }
        String t = col.canonicalType();

        if (t.startsWith("decimal")) {
            return raw instanceof BigDecimal bd ? bd : new BigDecimal(raw.toString());
        }
        switch (t) {
            case "tinyint":
            case "byte":
                return ((Number) raw).byteValue();
            case "smallint":
            case "short":
                return ((Number) raw).shortValue();
            case "int":
            case "integer":
                return ((Number) raw).intValue();
            case "bigint":
            case "long":
                return raw instanceof BigInteger bi ? bi.longValueExact() : ((Number) raw).longValue();
            case "float":
            case "real":
                return ((Number) raw).floatValue();
            case "double":
                return ((Number) raw).doubleValue();
            case "boolean":
            case "bool":
                return raw instanceof Boolean b ? b : Boolean.parseBoolean(raw.toString());
            case "binary":
                return raw instanceof byte[] bs ? ByteBuffer.wrap(bs) : ByteBuffer.wrap(raw.toString().getBytes());
            case "date":
                return toDays(raw);
            case "timestamp":
            case "timestamp_ltz":
                return toMicros(raw);
            case "timestamp_ntz":
                return toMicrosNoTz(raw);
            case "string":
            case "varchar":
            case "char":
            case "void":
                return raw.toString();
            case "variant":
                return raw.toString(); // JSON string
            default:
                return raw.toString();
        }
    }

    private static int toDays(Object raw) {
        if (raw instanceof Date d) {
            return (int) d.toLocalDate().toEpochDay();
        }
        if (raw instanceof LocalDate ld) {
            return (int) ld.toEpochDay();
        }
        return (int) LocalDate.parse(raw.toString()).toEpochDay();
    }

    private static long toMicros(Object raw) {
        if (raw instanceof Timestamp ts) {
            return ts.toInstant().getEpochSecond() * 1_000_000L + ts.getNanos() / 1000L;
        }
        if (raw instanceof Instant in) {
            return in.getEpochSecond() * 1_000_000L + in.getNano() / 1000L;
        }
        Instant parsed = Instant.parse(raw.toString());
        return parsed.getEpochSecond() * 1_000_000L + parsed.getNano() / 1000L;
    }

    private static long toMicrosNoTz(Object raw) {
        if (raw instanceof Timestamp ts) {
            LocalDateTime ldt = ts.toLocalDateTime();
            return ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000L;
        }
        if (raw instanceof LocalDateTime ldt) {
            return ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000L;
        }
        // String fallback
        String s = raw.toString();
        // expected ISO local datetime
        LocalDateTime ldt = LocalDateTime.parse(s.replace(' ', 'T'));
        return ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000L;
    }

    @SuppressWarnings("unused")
    private static long timeToMicros(Object raw) {
        if (raw instanceof Time t) {
            return t.toLocalTime().toNanoOfDay() / 1000L;
        }
        if (raw instanceof LocalTime lt) {
            return lt.toNanoOfDay() / 1000L;
        }
        return LocalTime.parse(raw.toString()).toNanoOfDay() / 1000L;
    }
}
