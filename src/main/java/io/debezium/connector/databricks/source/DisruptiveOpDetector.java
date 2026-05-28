/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.source;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.DatabricksConnectorConfig;
import io.debezium.connector.databricks.DatabricksConnectorConfig.DisruptiveOpHandling;
import io.debezium.connector.databricks.DatabricksOffsetContext;
import io.debezium.connector.databricks.DatabricksRecordBuilder;
import io.debezium.connector.databricks.DatabricksSchema;
import io.debezium.connector.databricks.cdf.DeltaTableMetadata;
import io.debezium.connector.databricks.cdf.HistoryScanner;
import io.debezium.relational.TableId;

/**
 * Detects disruptive Delta operations (TRUNCATE, REPLACE TABLE, RESTORE,
 * WRITE-with-overwrite) that CDF does NOT emit per-row deletes for, and
 * surfaces them according to the configured policy.
 */
public class DisruptiveOpDetector {

    private static final Logger LOG = LoggerFactory.getLogger(DisruptiveOpDetector.class);

    private final DatabricksConnectorConfig config;
    private final HistoryScanner historyScanner;
    private final DatabricksRecordBuilder recordBuilder;

    public DisruptiveOpDetector(DatabricksConnectorConfig config,
                                HistoryScanner historyScanner,
                                DatabricksRecordBuilder recordBuilder) {
        this.config = config;
        this.historyScanner = historyScanner;
        this.recordBuilder = recordBuilder;
    }

    /**
     * Scans Delta history for disruptive operations in {@code (fromExclusive, toInclusive]}.
     * Returns true if the caller should SKIP the rest of this table's poll cycle
     * (i.e. a re-snapshot was triggered).
     */
    public boolean scanAndEmit(TableId tid, DatabricksSchema.CachedSchema cached,
                               long fromExclusive, long toInclusive,
                               DatabricksOffsetContext offset, List<SourceRecord> out) throws SQLException {
        List<HistoryScanner.DisruptiveOp> ops = historyScanner.scan(tid, fromExclusive, toInclusive);
        if (ops.isEmpty()) {
            return false;
        }
        DisruptiveOpHandling handling = config.disruptiveOpHandling();
        for (HistoryScanner.DisruptiveOp op : ops) {
            switch (handling) {
                case WARN:
                    LOG.warn("Detected disruptive op {} at {}:v{}: {}",
                            op.operation(), tid.identifier(), op.version(), op.parameters());
                    break;
                case TRUNCATE:
                    LOG.info("Emitting synthetic TRUNCATE for {}:v{} ({})",
                            tid.identifier(), op.version(), op.operation());
                    out.add(recordBuilder.buildTruncate(cached, op.version(),
                            op.timestamp() == null ? Instant.now() : op.timestamp(), offset));
                    break;
                case RESNAPSHOT:
                    LOG.warn("Re-snapshot requested for {} after {}:v{}: clearing offset to trigger fresh snapshot",
                            tid.identifier(), op.operation(), op.version());
                    offset.resetForResnapshot(tid);
                    return true;
                case FAIL:
                    throw new IllegalStateException("Disruptive Delta operation '" + op.operation() +
                            "' at " + tid.identifier() + ":v" + op.version() +
                            " — failing per cdf.disruptive.operation.handling=fail");
            }
        }
        return false;
    }
}
