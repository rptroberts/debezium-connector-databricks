/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection.auth;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Azure Managed Identity auth (system- or user-assigned).
 *
 * <p>JDBC params: {@code AuthMech=11;Auth_Flow=3;Azure_workspace_resource_id=<arm-id>}.
 * For user-assigned identities, supply {@code OAuth2ClientID} as the MI client id.
 * Token retrieval is performed by the driver against the Azure Instance Metadata
 * Service ({@code http://169.254.169.254}) — connector must run on Azure compute.
 */
public final class AzureMiProvider implements AuthProvider {

    private final String workspaceResourceId;
    private final String userAssignedClientId;

    public AzureMiProvider(String workspaceResourceId, String userAssignedClientId) {
        this.workspaceResourceId = requireNonBlank(workspaceResourceId, "databricks.azure.workspace.resource.id");
        this.userAssignedClientId = userAssignedClientId; // optional
    }

    @Override
    public Map<String, String> jdbcProperties() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("AuthMech", "11");
        p.put("Auth_Flow", "3");
        p.put("Azure_workspace_resource_id", workspaceResourceId);
        if (userAssignedClientId != null && !userAssignedClientId.isBlank()) {
            p.put("OAuth2ClientID", userAssignedClientId);
        }
        return p;
    }

    @Override
    public String describe() {
        return "AzureMi(workspaceResourceId=" + workspaceResourceId +
                (userAssignedClientId != null && !userAssignedClientId.isBlank()
                        ? ", userAssignedClientId=" + userAssignedClientId
                        : ", systemAssigned")
                + ")";
    }

    private static String requireNonBlank(String v, String name) {
        Objects.requireNonNull(v, name + " must not be null");
        if (v.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return v;
    }
}
