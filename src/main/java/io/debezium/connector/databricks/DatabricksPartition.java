/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;

/**
 * Single-partition addressing: keyed by the connector's {@code topic.prefix}.
 *
 * <p>All tables observed by a task share this partition; per-table offsets are
 * carried inside the {@link DatabricksOffsetContext} value, not in the partition
 * key. This mirrors the MongoDB connector pattern and keeps offset reads cheap
 * for connectors that watch many tables.
 */
public class DatabricksPartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public DatabricksPartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DatabricksPartition other)) {
            return false;
        }
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }

    @Override
    public String toString() {
        return "DatabricksPartition{server='" + serverName + "'}";
    }

    /**
     * Single-partition provider — Databricks CDC is logically server-wide:
     * one connector instance, many tables, one offset namespace.
     */
    public static class Provider implements Partition.Provider<DatabricksPartition> {

        private final DatabricksConnectorConfig config;

        public Provider(DatabricksConnectorConfig config) {
            this.config = config;
        }

        @Override
        public Set<DatabricksPartition> getPartitions() {
            return Collections.singleton(new DatabricksPartition(config.getLogicalName()));
        }
    }
}
