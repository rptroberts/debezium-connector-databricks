/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.metrics;

/** JMX surface for the Databricks connector. */
public interface DatabricksMetricsMXBean {

    long getSnapshotRows();

    long getStreamingRows();

    long getQueriesIssued();

    long getQueriesFailed();

    long getBatches();

    long getLagVersions();

    long getTruncateEvents();

    String getLastPollAt();

    String getLastError();
}
