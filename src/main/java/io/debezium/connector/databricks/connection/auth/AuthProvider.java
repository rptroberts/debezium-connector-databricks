/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection.auth;

import java.util.Map;

import io.debezium.connector.databricks.DatabricksConnectorConfig;

/**
 * Builds JDBC connection properties for a specific authentication mode.
 *
 * <p>The Databricks JDBC driver itself handles token acquisition and refresh
 * for OAuth/MI modes; this interface only emits the connection-property values
 * (AuthMech, Auth_Flow, OAuth2ClientID, etc.) that drive that behavior.
 */
public sealed interface AuthProvider permits PatProvider, EntraSpProvider, AzureMiProvider {

    /**
     * Returns the JDBC connection properties for this auth mode.
     * Keys map to Databricks JDBC driver connection parameters.
     */
    Map<String, String> jdbcProperties();

    /**
     * Returns a human-readable description of this provider's effective config,
     * with secrets redacted. Used for startup logging.
     */
    String describe();

    /**
     * Factory: build the right provider for the configured auth mode.
     */
    static AuthProvider from(DatabricksConnectorConfig config) {
        AuthMode mode = config.authMode();
        return switch (mode) {
            case PAT -> new PatProvider(config.token());
            case AZURE_ENTRA_SP -> new EntraSpProvider(
                    config.entraTenantId(),
                    config.entraClientId(),
                    config.entraClientSecret());
            case AZURE_MI -> new AzureMiProvider(
                    config.azureWorkspaceResourceId(),
                    config.miClientId());
        };
    }
}
