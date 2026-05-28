/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

class AuthProvidersTest {

    @Test
    void patProviderEmitsAuthMech3() {
        Map<String, String> p = new PatProvider("dapi-secret").jdbcProperties();
        assertThat(p).containsEntry("AuthMech", "3").containsEntry("UID", "token").containsEntry("PWD", "dapi-secret");
    }

    @Test
    void patProviderRedactsInDescribe() {
        assertThat(new PatProvider("dapi-secret").describe()).doesNotContain("dapi-secret");
    }

    @Test
    void patProviderRejectsBlank() {
        assertThatThrownBy(() -> new PatProvider("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PatProvider(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void entraSpProviderEmitsAuthMech11WithDiscoveryUrl() {
        Map<String, String> p = new EntraSpProvider("tenant-uuid", "client-uuid", "client-secret").jdbcProperties();
        assertThat(p).containsEntry("AuthMech", "11").containsEntry("Auth_Flow", "1");
        assertThat(p).containsEntry("OAuth2ClientID", "client-uuid").containsEntry("OAuth2Secret", "client-secret");
        assertThat(p.get("OIDCDiscoveryEndpoint")).contains("login.microsoftonline.com/tenant-uuid");
        assertThat(p).containsEntry("EnableOIDCDiscovery", "true");
    }

    @Test
    void entraSpProviderRedactsSecret() {
        String s = new EntraSpProvider("t", "c", "very-secret").describe();
        assertThat(s).doesNotContain("very-secret");
    }

    @Test
    void azureMiSystemAssignedOmitsClientId() {
        Map<String, String> p = new AzureMiProvider("/subscriptions/abc/.../workspaces/ws", null).jdbcProperties();
        assertThat(p).containsEntry("AuthMech", "11").containsEntry("Auth_Flow", "3");
        assertThat(p).containsEntry("Azure_workspace_resource_id", "/subscriptions/abc/.../workspaces/ws");
        assertThat(p).doesNotContainKey("OAuth2ClientID");
    }

    @Test
    void azureMiUserAssignedIncludesClientId() {
        Map<String, String> p = new AzureMiProvider("/subscriptions/abc/.../workspaces/ws", "mi-client").jdbcProperties();
        assertThat(p).containsEntry("OAuth2ClientID", "mi-client");
    }

    @Test
    void authModeParseRejectsUnknown() {
        assertThatThrownBy(() -> AuthMode.parse("oauth2")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void authModeParseAcceptsKnown() {
        assertThat(AuthMode.parse("pat")).isEqualTo(AuthMode.PAT);
        assertThat(AuthMode.parse("PAT")).isEqualTo(AuthMode.PAT);
        assertThat(AuthMode.parse("azure-entra-sp")).isEqualTo(AuthMode.AZURE_ENTRA_SP);
        assertThat(AuthMode.parse("azure-mi")).isEqualTo(AuthMode.AZURE_MI);
    }
}
