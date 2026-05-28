/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.cdf.DeltaColumn;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.connector.databricks.cdf.DeltaTypes;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;

/**
 * Per-table Kafka Connect schema cache.
 *
 * <p>Builds and memoizes:
 * <ul>
 *   <li>the value (record) schema, with one optional/required field per Delta column,</li>
 *   <li>the primary-key schema (the connector's record key), iff a PK exists,</li>
 *   <li>the Debezium envelope schema, combining the above with source/op/ts fields.</li>
 * </ul>
 *
 * <p>Caches are invalidated when a refreshed {@link DeltaTableMetadata} is not
 * {@linkplain DeltaTableMetadata#schemaEquivalentTo(DeltaTableMetadata) schema-equivalent}
 * to the cached one.
 */
public class DatabricksSchema {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksSchema.class);

    private final DatabricksConnectorConfig config;
    private final DatabricksSourceInfoStructMaker sourceInfoStructMaker;
    private final ConcurrentHashMap<TableId, CachedSchema> byId = new ConcurrentHashMap<>();

    public DatabricksSchema(DatabricksConnectorConfig config) {
        this.config = Objects.requireNonNull(config);
        this.sourceInfoStructMaker = new DatabricksSourceInfoStructMaker();
        this.sourceInfoStructMaker.init(Module.name(), Module.version(), config);
    }

    public DatabricksSourceInfoStructMaker sourceInfoStructMaker() {
        return sourceInfoStructMaker;
    }

    /**
     * Returns the cached schema for the table, building it if absent. If the
     * cached schema is no longer equivalent to {@code current}, the cache is
     * rebuilt and the old schema is dropped.
     */
    public CachedSchema ensureSchema(DeltaTableMetadata current) {
        return byId.compute(current.tableId(), (id, existing) -> {
            if (existing == null) {
                return buildSchema(current);
            }
            if (existing.metadata.schemaEquivalentTo(current)) {
                return existing;
            }
            LOG.info("Detected schema change for {}, rebuilding Connect schema", id.identifier());
            return buildSchema(current);
        });
    }

    public CachedSchema get(TableId id) {
        return byId.get(id);
    }

    public void clear() {
        byId.clear();
    }

    private CachedSchema buildSchema(DeltaTableMetadata meta) {
        String topicName = topicName(meta.tableId());
        // Value schema is OPTIONAL because Envelope reuses it as both `before` and `after`,
        // and at least one of those is always null (insert→no before, delete→no after).
        SchemaBuilder rowBuilder = SchemaBuilder.struct().name(topicName + ".Value").optional();
        for (DeltaColumn c : meta.columns()) {
            rowBuilder.field(c.name(), DeltaTypes.toConnectSchema(c));
        }
        Schema valueSchema = rowBuilder.build();

        Schema keySchema = null;
        if (!meta.primaryKey().isEmpty()) {
            SchemaBuilder kb = SchemaBuilder.struct().name(topicName + ".Key");
            for (String pkCol : meta.primaryKey()) {
                DeltaColumn dc = meta.column(pkCol);
                if (dc != null) {
                    // PK fields must be non-null in the key schema.
                    Schema fieldSchema = DeltaTypes.toConnectSchema(new DeltaColumn(
                            dc.name(), dc.typeName(), false, dc.position(), true));
                    kb.field(pkCol, fieldSchema);
                }
            }
            keySchema = kb.build();
        }

        Schema sourceSchema = sourceInfoStructMaker.schema();
        Envelope envelope = Envelope.defineSchema()
                .withName(topicName + ".Envelope")
                .withRecord(valueSchema)
                .withSource(sourceSchema)
                .build();

        return new CachedSchema(meta, topicName, keySchema, valueSchema, envelope);
    }

    public String topicName(TableId id) {
        return config.getLogicalName() + "." + id.catalog() + "." + id.schema() + "." + id.table();
    }

    /**
     * Cached Connect schemas + the metadata they were built from.
     */
    public static final class CachedSchema {
        public final DeltaTableMetadata metadata;
        public final String topic;
        public final Schema keySchema; // nullable when the table has no PK
        public final Schema valueSchema;
        public final Envelope envelope;

        CachedSchema(DeltaTableMetadata metadata, String topic, Schema keySchema, Schema valueSchema, Envelope envelope) {
            this.metadata = metadata;
            this.topic = topic;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
            this.envelope = envelope;
        }
    }
}
