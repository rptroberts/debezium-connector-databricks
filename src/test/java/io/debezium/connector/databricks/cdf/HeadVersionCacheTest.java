/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

class HeadVersionCacheTest {

    @Test
    void observeUpdatesCacheWithoutQuery() throws Exception {
        // Inspector is never called for cache hits, so any inspector throwing on currentVersion
        // proves we don't issue an SQL round trip.
        DeltaTableInspector throwingInspector = new DeltaTableInspector(null, null) {
            @Override
            public long currentVersion(TableId tid) {
                throw new AssertionError("inspector should not be queried");
            }
        };
        HeadVersionCache cache = new HeadVersionCache(throwingInspector, Duration.ofMinutes(10));
        TableId t = new TableId("c", "s", "t");
        cache.observe(t, 100L);
        assertThat(cache.get(t)).isEqualTo(100L);
    }

    @Test
    void getRefreshesAfterTtlExpiry() throws Exception {
        AtomicCallCount counter = new AtomicCallCount();
        DeltaTableInspector counting = new DeltaTableInspector(null, null) {
            @Override
            public long currentVersion(TableId tid) {
                return counter.next();
            }
        };
        HeadVersionCache cache = new HeadVersionCache(counting, Duration.ofNanos(1));
        TableId t = new TableId("c", "s", "t");
        long v1 = cache.get(t);
        // Sleep past the 1ns TTL.
        Thread.sleep(1);
        long v2 = cache.get(t);
        assertThat(v1).isEqualTo(1L);
        assertThat(v2).isEqualTo(2L);
    }

    @Test
    void observeDoesNotRegressVersion() throws SQLException {
        DeltaTableInspector throwingInspector = new DeltaTableInspector(null, null) {
            @Override
            public long currentVersion(TableId tid) {
                throw new AssertionError();
            }
        };
        HeadVersionCache cache = new HeadVersionCache(throwingInspector, Duration.ofMinutes(10));
        TableId t = new TableId("c", "s", "t");
        cache.observe(t, 100L);
        cache.observe(t, 50L); // out-of-order
        assertThat(cache.get(t)).isEqualTo(100L);
    }

    private static class AtomicCallCount {
        private long n = 0;

        synchronized long next() {
            return ++n;
        }
    }
}
