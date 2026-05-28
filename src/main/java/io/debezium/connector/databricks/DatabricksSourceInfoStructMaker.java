/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfoStructMaker;

public class DatabricksSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, io.debezium.config.CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        this.schema = SchemaBuilder.struct()
                .name("io.debezium.connector.databricks.Source")
                .field(SourceInfo.CATALOG_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.SCHEMA_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.TABLE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_VERSION_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.COMMIT_TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.CHANGE_TYPE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo source) {
        Struct s = new Struct(schema);
        if (source.tableId() != null) {
            s.put(SourceInfo.CATALOG_KEY, source.tableId().catalog());
            s.put(SourceInfo.SCHEMA_KEY, source.tableId().schema());
            s.put(SourceInfo.TABLE_KEY, source.tableId().table());
        }
        s.put(SourceInfo.COMMIT_VERSION_KEY, source.commitVersion());
        if (source.commitTimestamp() != null) {
            s.put(SourceInfo.COMMIT_TIMESTAMP_KEY, source.commitTimestamp().toEpochMilli());
        }
        if (source.changeType() != null) {
            s.put(SourceInfo.CHANGE_TYPE_KEY, source.changeType());
        }
        return s;
    }
}
