/*
 * Copyright 2026 RecordPoint and contributors
 * Licensed under the Apache License, Version 2.0
 */
package io.debezium.connector.databricks.cdf;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.debezium.relational.TableId;

/**
 * TTL cache of the most-recently observed head {@code _commit_version} per
 * table. Eliminates redundant {@code DESCRIBE HISTORY ... LIMIT 1} round
 * trips during steady-state polling.
 *
 * <p>Refresh is opportunistic: callers that have just read CDF up to version
 * {@code N} should call {@link #observe(TableId, long)} to update the cache
 * without a round trip. {@link #get(TableId)} performs a batched
 * {@code DESCRIBE HISTORY} only when the cached entry is missing or older
 * than the configured TTL.
 */
public class HeadVersionCache {

    private final DeltaTableInspector inspector;
    private final Duration ttl;
    private final Map<TableId, Entry> cache = new ConcurrentHashMap<>();

    public HeadVersionCache(DeltaTableInspector inspector, Duration ttl) {
        this.inspector = inspector;
        this.ttl = ttl;
    }

    /**
     * Returns the head version for the table, refreshing if the cached entry is
     * missing or older than the TTL.
     */
    public long get(TableId tid) throws SQLException {
        Entry e = cache.get(tid);
        long now = System.nanoTime();
        if (e != null && (now - e.timestampNs) < ttl.toNanos()) {
            return e.version;
        }
        long fresh = inspector.currentVersion(tid);
        cache.put(tid, new Entry(fresh, now));
        return fresh;
    }

    /**
     * Bulk-refresh head versions for all listed tables in a single round trip.
     * Useful at the start of a polling cycle.
     */
    public void refreshAll(List<TableId> tables) throws SQLException {
        Map<TableId, Long> heads = inspector.currentVersionsBatch(tables);
        long now = System.nanoTime();
        for (Map.Entry<TableId, Long> e : heads.entrySet()) {
            cache.put(e.getKey(), new Entry(e.getValue(), now));
        }
    }

    /**
     * Record an observed version from a CDF read result. Useful to keep the
     * cache up-to-date without an extra metadata query when the caller has
     * already learned the head from a {@code table_changes()} response.
     */
    public void observe(TableId tid, long version) {
        Entry existing = cache.get(tid);
        if (existing == null || version > existing.version) {
            cache.put(tid, new Entry(version, System.nanoTime()));
        }
    }

    public void invalidate(TableId tid) {
        cache.remove(tid);
    }

    public void clear() {
        cache.clear();
    }

    /** Test/inspection. */
    public Map<TableId, Long> snapshot() {
        Map<TableId, Long> out = new HashMap<>();
        cache.forEach((k, v) -> out.put(k, v.version));
        return out;
    }

    private record Entry(long version, long timestampNs) {
    }
}
