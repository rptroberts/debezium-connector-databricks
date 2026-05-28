/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection.auth;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Microsoft Entra ID (Azure AD) service principal auth.
 *
 * <p>JDBC params: {@code AuthMech=11;Auth_Flow=1;EnableOIDCDiscovery=true;OAuth2ClientID=<id>;OAuth2Secret=<secret>;OIDCDiscoveryEndpoint=https://login.microsoftonline.com/<tenant>/v2.0/.well-known/openid-configuration}.
 * The driver acquires tokens against the Azure Databricks first-party resource
 * (application ID {@code 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d}) and refreshes
 * them transparently.
 */
public final class EntraSpProvider implements AuthProvider {

    private static final String DISCOVERY_TEMPLATE =
            "https://login.microsoftonline.com/%s/v2.0/.well-known/openid-configuration";

    private final String tenantId;
    private final String clientId;
    private final String clientSecret;

    public EntraSpProvider(String tenantId, String clientId, String clientSecret) {
        this.tenantId = requireNonBlank(tenantId, "databricks.entra.tenant.id");
        this.clientId = requireNonBlank(clientId, "databricks.entra.client.id");
        this.clientSecret = requireNonBlank(clientSecret, "databricks.entra.client.secret");
    }

    @Override
    public Map<String, String> jdbcProperties() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("AuthMech", "11");
        p.put("Auth_Flow", "1");
        p.put("EnableOIDCDiscovery", "true");
        p.put("OAuth2ClientID", clientId);
        p.put("OAuth2Secret", clientSecret);
        p.put("OIDCDiscoveryEndpoint", String.format(DISCOVERY_TEMPLATE, tenantId));
        return p;
    }

    @Override
    public String describe() {
        return "EntraSp(tenant=" + tenantId + ", clientId=" + clientId + ", secret=***)";
    }

    private static String requireNonBlank(String v, String name) {
        Objects.requireNonNull(v, name + " must not be null");
        if (v.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return v;
    }
}
