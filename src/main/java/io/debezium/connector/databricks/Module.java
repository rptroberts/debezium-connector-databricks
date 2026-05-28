/*
 * Copyright 2026 RecordPoint and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.databricks;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Provides the connector's name and version, sourced from {@code Module.properties}.
 */
public final class Module {

    public static final String NAME = "databricks";
    public static final String CONTEXT_NAME = "databricks-connector";

    private static final Properties INFO = loadInfo();

    private Module() {
    }

    public static String version() {
        return INFO.getProperty("version", "unknown");
    }

    public static String name() {
        return NAME;
    }

    public static String contextName() {
        return CONTEXT_NAME;
    }

    private static Properties loadInfo() {
        Properties p = new Properties();
        try (InputStream in = Module.class.getResourceAsStream("Module.properties")) {
            if (in != null) {
                p.load(in);
            }
        }
        catch (IOException e) {
            // best-effort
        }
        return p;
    }
}
