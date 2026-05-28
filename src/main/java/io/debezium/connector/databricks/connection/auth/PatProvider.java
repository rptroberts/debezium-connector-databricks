/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.connection.auth;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Personal Access Token auth. Sets {@code AuthMech=3;UID=token;PWD=<pat>}.
 */
public final class PatProvider implements AuthProvider {

    private final String token;

    public PatProvider(String token) {
        this.token = Objects.requireNonNull(token, "PAT token must not be null");
        if (token.isBlank()) {
            throw new IllegalArgumentException("PAT token must not be blank");
        }
    }

    @Override
    public Map<String, String> jdbcProperties() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("AuthMech", "3");
        p.put("UID", "token");
        p.put("PWD", token);
        return p;
    }

    @Override
    public String describe() {
        return "PAT(token=***)";
    }
}
