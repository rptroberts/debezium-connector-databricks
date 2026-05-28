/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.connector.databricks.source.DatabricksPoller;
import io.debezium.relational.TableId;

/**
 * Single-task implementation: discovers tables, runs snapshot + CDF poll loop,
 * emits Debezium-envelope {@link SourceRecord}s.
 */
public class DatabricksConnectorTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksConnectorTask.class);

    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);

    private DatabricksConnectorConfig config;
    private DatabricksConnection connection;
    private DatabricksSchema schema;
    private DatabricksPartition partition;
    private DatabricksOffsetContext offset;
    private DatabricksPoller poller;
    private long lastEmptyPollAtNs;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        if (!state.compareAndSet(State.STOPPED, State.STARTING)) {
            return;
        }
        try {
            Configuration cfg = Configuration.from(props);
            this.config = new DatabricksConnectorConfig(cfg);
            DatabricksConnectorConfig.requireAuthFieldsPresent(cfg);

            this.connection = new DatabricksConnection(config);
            String summary = connection.selfTest();
            LOG.info("Databricks connection OK: {}", summary);

            this.schema = new DatabricksSchema(config);
            this.partition = new DatabricksPartition(config.getLogicalName());
            this.offset = restoreOffsetContext();
            this.poller = new DatabricksPoller(config, connection, schema, partition, offset);
            state.set(State.RUNNING);
            LOG.info("Databricks task started (topic.prefix={}, catalog={})", config.getLogicalName(), config.catalog());
        }
        catch (Exception e) {
            state.set(State.STOPPED);
            closeQuietly();
            throw new RuntimeException("Failed to start Databricks connector task: " + e.getMessage(), e);
        }
    }

    private DatabricksOffsetContext restoreOffsetContext() {
        DatabricksOffsetContext.Loader loader = new DatabricksOffsetContext.Loader(config);
        Map<String, ?> sp = partitionMap();
        Map<String, Object> persisted = (context != null && context.offsetStorageReader() != null)
                ? loadOffset(sp)
                : null;
        return loader.load(persisted);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadOffset(Map<String, ?> sourcePartition) {
        try {
            Map<String, Object> o = (Map<String, Object>) context.offsetStorageReader()
                    .offset((Map<String, Object>) sourcePartition);
            return o;
        }
        catch (Exception e) {
            LOG.warn("Failed to load persisted offsets — starting from scratch: {}", e.getMessage());
            return null;
        }
    }

    private Map<String, ?> partitionMap() {
        return Map.of("server", config.getLogicalName());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (state.get() != State.RUNNING) {
            return Collections.emptyList();
        }
        try {
            List<SourceRecord> batch = poller.pollOnce();
            if (batch.isEmpty()) {
                long now = System.nanoTime();
                if (now - lastEmptyPollAtNs >= config.pollInterval().toNanos()) {
                    lastEmptyPollAtNs = now;
                }
                Thread.sleep(config.pollInterval().toMillis());
                return Collections.emptyList();
            }
            return batch;
        }
        catch (SQLException e) {
            LOG.error("Poll cycle failed: {}", e.getMessage());
            // Surface as a retriable error to the engine driver.
            throw new org.apache.kafka.connect.errors.RetriableException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (RuntimeException e) {
            LOG.error("Unexpected error in poll cycle", e);
            throw e;
        }
    }

    @Override
    public void stop() {
        State prev = state.getAndSet(State.STOPPED);
        if (prev != State.RUNNING && prev != State.STARTING) {
            return;
        }
        if (poller != null) {
            poller.stop();
        }
        closeQuietly();
        LOG.info("Databricks task stopped");
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (Exception ignored) {
            }
        }
    }

    // ----- Test hooks -----

    DatabricksPoller poller() {
        return poller;
    }

    DatabricksOffsetContext offsetContext() {
        return offset;
    }

    DatabricksConnection connection() {
        return connection;
    }

    DatabricksConnectorConfig config() {
        return config;
    }

    /** Test hook: pre-populate the table list, bypassing discovery. */
    void setTables(List<TableId> tables) throws SQLException {
        if (poller != null) {
            poller.setTables(tables);
        }
    }

    private enum State {
        STOPPED, STARTING, RUNNING
    }
}
