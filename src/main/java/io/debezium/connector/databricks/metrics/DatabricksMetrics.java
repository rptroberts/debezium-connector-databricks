/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.metrics;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal JMX-exposed counters for ops visibility.
 *
 * <p>Registers a single MBean under
 * {@code io.debezium.connector.databricks:type=connector-metrics,context=<topic.prefix>}.
 * Counts are atomic and lock-free; reads are eventually consistent.
 */
public class DatabricksMetrics implements DatabricksMetricsMXBean {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksMetrics.class);

    private final ObjectName objectName;
    private final AtomicLong snapshotRows = new AtomicLong();
    private final AtomicLong streamingRows = new AtomicLong();
    private final AtomicLong queriesIssued = new AtomicLong();
    private final AtomicLong queriesFailed = new AtomicLong();
    private final AtomicLong batches = new AtomicLong();
    private final AtomicLong lagVersions = new AtomicLong();
    private final AtomicLong truncateEvents = new AtomicLong();
    private final AtomicReference<Instant> lastPollAt = new AtomicReference<>();
    private final AtomicReference<String> lastError = new AtomicReference<>();

    public DatabricksMetrics(String topicPrefix) {
        try {
            this.objectName = new ObjectName(
                    "io.debezium.connector.databricks:type=connector-metrics,context=" + topicPrefix);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void register() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (!server.isRegistered(objectName)) {
                server.registerMBean(this, objectName);
            }
        }
        catch (Exception e) {
            LOG.warn("Failed to register MBean {}: {}", objectName, e.getMessage());
        }
    }

    public void unregister() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        }
        catch (Exception e) {
            LOG.warn("Failed to unregister MBean {}: {}", objectName, e.getMessage());
        }
    }

    public void onSnapshotRow() {
        snapshotRows.incrementAndGet();
    }

    public void onStreamingRow() {
        streamingRows.incrementAndGet();
    }

    public void onQueryIssued() {
        queriesIssued.incrementAndGet();
    }

    public void onQueryFailed(String message) {
        queriesFailed.incrementAndGet();
        lastError.set(message);
    }

    public void onBatchCompleted() {
        batches.incrementAndGet();
        lastPollAt.set(Instant.now());
    }

    public void onTruncate() {
        truncateEvents.incrementAndGet();
    }

    public void setLagVersions(long lag) {
        lagVersions.set(lag);
    }

    @Override
    public long getSnapshotRows() {
        return snapshotRows.get();
    }

    @Override
    public long getStreamingRows() {
        return streamingRows.get();
    }

    @Override
    public long getQueriesIssued() {
        return queriesIssued.get();
    }

    @Override
    public long getQueriesFailed() {
        return queriesFailed.get();
    }

    @Override
    public long getBatches() {
        return batches.get();
    }

    @Override
    public long getLagVersions() {
        return lagVersions.get();
    }

    @Override
    public long getTruncateEvents() {
        return truncateEvents.get();
    }

    @Override
    public String getLastPollAt() {
        Instant i = lastPollAt.get();
        return i == null ? "" : i.toString();
    }

    @Override
    public String getLastError() {
        String s = lastError.get();
        return s == null ? "" : s;
    }
}
