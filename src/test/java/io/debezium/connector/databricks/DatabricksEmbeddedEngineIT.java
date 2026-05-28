/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.databricks.connection.DatabricksConnection;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.TableId;

/**
 * End-to-end smoke test through the Debezium Embedded Engine: drives the
 * connector via {@link DebeziumEngine} with a {@code FileOffsetBackingStore},
 * inserts a row in Databricks, asserts a JSON change event arrives.
 *
 * <p>This is the deliverable acceptance test for the "embedded engine is the
 * primary runtime" constraint.
 */
class DatabricksEmbeddedEngineIT {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksEmbeddedEngineIT.class);

    private DatabricksTestEnv env;
    private DatabricksConnection setupConnection;
    private DatabricksConnectorConfig setupConfig;
    private TableId table;
    private Path offsetsFile;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        env = DatabricksTestEnv.load();
        assumeTrue(env.isComplete(), "DATABRICKS_* env not set; skipping IT");

        String tableName = "_dbz_engine_" + System.currentTimeMillis() + "_" + (int) (Math.random() * 1000);
        table = new TableId(env.catalog(), env.schema(), tableName);
        offsetsFile = Files.createTempFile("dbz-offsets-", ".dat");
        // FileOffsetBackingStore requires the file to NOT exist initially OR be empty/proper.
        Files.deleteIfExists(offsetsFile);

        setupConfig = buildBaseConfig();
        setupConnection = new DatabricksConnection(setupConfig);
        setupConnection.selfTest();
        setupConnection.execute("CREATE TABLE `" + table.catalog() + "`.`" + table.schema() + "`.`" + table.table() +
                "` (id BIGINT NOT NULL, name STRING, " +
                " CONSTRAINT " + table.table() + "_pk PRIMARY KEY (id)) " +
                "USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)");
        setupConnection.execute("INSERT INTO `" + table.catalog() + "`.`" + table.schema() + "`.`" + table.table() +
                "` VALUES (1, 'seed-row')");
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (engine != null) {
                engine.close();
            }
        }
        catch (Exception ignored) {
        }
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        if (setupConnection != null && table != null) {
            try {
                setupConnection.execute("DROP TABLE IF EXISTS `" + table.catalog() + "`.`" + table.schema() + "`.`" + table.table() + "`");
            }
            finally {
                setupConnection.close();
            }
        }
        if (offsetsFile != null) {
            Files.deleteIfExists(offsetsFile);
        }
    }

    @Test
    void engineDeliversChangeEvents() throws Exception {
        Properties props = new Properties();
        props.setProperty("name", "dbx-engine-it");
        props.setProperty("connector.class", DatabricksConnector.class.getName());

        // Offsets via filesystem — proves the connector works without Kafka.
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetsFile.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "1000");

        // Json converter tuning — schemas.enable adds the schema to every record (large)
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");
        props.setProperty("topic.prefix", "dbx-engine-it");
        props.setProperty("databricks.workspace.host", env.host());
        props.setProperty("databricks.warehouse.http.path", env.httpPath());
        props.setProperty("databricks.auth.type", "pat");
        props.setProperty("databricks.token", env.token());
        props.setProperty("databricks.catalog", env.catalog());
        props.setProperty("table.include.list", java.util.regex.Pattern.quote(table.identifier()));
        props.setProperty("snapshot.mode", "initial");
        props.setProperty("cdf.poll.interval.ms", "2000");

        CopyOnWriteArrayList<ChangeEvent<String, String>> received = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .using((success, message, error) -> {
                    if (!success) {
                        failure.set(error);
                        LOG.error("Engine completed unsuccessfully: {}", message, error);
                    }
                })
                .notifying((java.util.function.Consumer<ChangeEvent<String, String>>) received::add)
                .build();
        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        // Assert snapshot delivers the seed row.
        Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    assertThat(failure.get()).as("engine error").isNull();
                    assertThat(received).as("seed row delivered as r").anyMatch(e -> e.value() != null && e.value().contains("\"op\":\"r\""));
                });

        // Insert a new row → streaming should pick it up.
        setupConnection.execute("INSERT INTO `" + table.catalog() + "`.`" + table.schema() + "`.`" + table.table() +
                "` VALUES (2, 'streamed-row')");

        Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    assertThat(failure.get()).isNull();
                    assertThat(received).anyMatch(e -> e.value() != null && e.value().contains("\"op\":\"c\"")
                            && e.value().contains("streamed-row"));
                });

        LOG.info("Embedded engine IT received {} change events", received.size());
    }

    private DatabricksConnectorConfig buildBaseConfig() {
        java.util.Map<String, String> p = new java.util.HashMap<>();
        p.put("topic.prefix", "dbx-engine-it");
        p.put("databricks.workspace.host", env.host());
        p.put("databricks.warehouse.http.path", env.httpPath());
        p.put("databricks.auth.type", "pat");
        p.put("databricks.token", env.token());
        p.put("databricks.catalog", env.catalog());
        return new DatabricksConnectorConfig(io.debezium.config.Configuration.from(p));
    }
}
