/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection.auth;

import java.util.Arrays;
import java.util.Locale;

/**
 * Supported authentication modes for Azure Databricks.
 *
 * <ul>
 *   <li>{@link #PAT} — Personal Access Token. Dev/test only; long-lived secret, no refresh.</li>
 *   <li>{@link #AZURE_ENTRA_SP} — Microsoft Entra (Azure AD) service principal with a client secret.
 *       Token refresh is handled by the Databricks JDBC driver via OIDC discovery.</li>
 *   <li>{@link #AZURE_MI} — Azure Managed Identity (system- or user-assigned).
 *       Only works when the connector runs on Azure compute with IMDS access.</li>
 * </ul>
 */
public enum AuthMode {

    PAT("pat"),
    AZURE_ENTRA_SP("azure-entra-sp"),
    AZURE_MI("azure-mi");

    private final String value;

    AuthMode(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static AuthMode parse(String s) {
        if (s == null) {
            throw new IllegalArgumentException("databricks.auth.type must not be null");
        }
        String norm = s.toLowerCase(Locale.ROOT).trim();
        return Arrays.stream(values())
                .filter(m -> m.value.equals(norm))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown databricks.auth.type '" + s + "'. Allowed: " +
                                Arrays.toString(Arrays.stream(values()).map(AuthMode::value).toArray())));
    }
}
