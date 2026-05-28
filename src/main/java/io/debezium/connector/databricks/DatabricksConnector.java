/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

/**
 * Kafka Connect {@link SourceConnector} entrypoint for the Databricks connector.
 *
 * <p>Embedded-engine compatibility note: this class extends only the public
 * Kafka Connect API. No dependency on Debezium's full source-connector base
 * classes; the engine driver instantiates and lifecycles the
 * {@link DatabricksConnectorTask} directly.
 */
public class DatabricksConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksConnector.class);

    private Map<String, String> props;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = new HashMap<>(props);
        LOG.info("Starting Databricks connector v{}", version());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DatabricksConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // v0.1.x: single-task. Tables are multiplexed inside the task.
        List<Map<String, String>> out = new ArrayList<>();
        out.add(new HashMap<>(props));
        return out;
    }

    @Override
    public void stop() {
        // No connector-level state to release.
    }

    @Override
    public ConfigDef config() {
        return DatabricksConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        // Cross-field validation: auth fields must match auth.type
        try {
            Configuration cfg = Configuration.from(connectorConfigs);
            DatabricksConnectorConfig.requireAuthFieldsPresent(cfg);
        }
        catch (IllegalArgumentException e) {
            // Attach the error to the auth.type field for visibility.
            for (ConfigValue cv : config.configValues()) {
                if (DatabricksConnectorConfig.AUTH_TYPE.name().equals(cv.name())) {
                    cv.addErrorMessage(e.getMessage());
                    break;
                }
            }
        }
        return config;
    }

    @SuppressWarnings("unused")
    private static List<String> errors(Config c) {
        return c.configValues().stream()
                .flatMap(cv -> cv.errorMessages().stream())
                .collect(Collectors.toList());
    }
}
