package com.example.fulfillment;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters and gauges for this worker, logged every few seconds as a STATS line.
 *
 * A stream doesn't know how many orders are waiting or in flight, so this consumer
 * has to measure that itself. Each STATS line is also written in CloudWatch embedded
 * metric format (EMF): CloudWatch Logs turns the fields into custom metrics, which
 * the "before" dashboard plots. They're billed as custom metrics. A queue publishes
 * the equivalent metrics on its own.
 */
final class Stats {
    static final String NAMESPACE = "KinesisQueuesDemo/KclFulfillment";

    final AtomicLong fulfilled = new AtomicLong();
    final AtomicLong duplicates = new AtomicLong();
    final AtomicLong retries = new AtomicLong();
    final AtomicLong sidelined = new AtomicLong();
    final AtomicLong skipped = new AtomicLong();
    /** Records handed to processRecords and not yet covered by a checkpoint. */
    final AtomicInteger inFlight = new AtomicInteger();
    private final Set<String> shards = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> millisBehindLatest = new ConcurrentHashMap<>();

    private final String applicationName;
    private long lastFulfilled;
    private long lastSidelined;
    private long lastRetries;
    private long lastAt = System.currentTimeMillis();

    Stats(String applicationName) {
        this.applicationName = applicationName;
    }

    void shardAcquired(String shardId) {
        shards.add(shardId);
    }

    void shardReleased(String shardId) {
        shards.remove(shardId);
        millisBehindLatest.remove(shardId);
    }

    void behindLatest(String shardId, Long millis) {
        if (millis != null) {
            millisBehindLatest.put(shardId, millis);
        }
    }

    synchronized void report() {
        long now = System.currentTimeMillis();
        double seconds = Math.max(1, now - lastAt) / 1000.0;
        long f = fulfilled.get();
        long s = sidelined.get();
        long r = retries.get();
        long behind = millisBehindLatest.values().stream().mapToLong(Long::longValue).max().orElse(0);
        int inFlightNow = inFlight.get();
        List<String> held = List.copyOf(new TreeSet<>(shards));

        var emf = JsonLog.fields(
                "Timestamp", now,
                "CloudWatchMetrics", List.of(
                        Map.of("Namespace", NAMESPACE,
                                "Dimensions", List.of(List.of("Application")),
                                "Metrics", List.of(
                                        Map.of("Name", "OrdersFulfilled", "Unit", "Count"),
                                        Map.of("Name", "OrdersSidelined", "Unit", "Count"),
                                        Map.of("Name", "RetryAttempts", "Unit", "Count"))),
                        // A gauge per task, so a dashboard can add them up across the fleet.
                        Map.of("Namespace", NAMESPACE,
                                "Dimensions", List.of(List.of("Application", "Task")),
                                "Metrics", List.of(Map.of("Name", "RecordsInFlight", "Unit", "Count")))));

        JsonLog.info("STATS", JsonLog.fields(
                "shards", held,
                "fulfilledPerSecond", Math.round((f - lastFulfilled) / seconds * 100) / 100.0,
                "intervalSeconds", Math.round(seconds),
                "inFlight", inFlightNow,
                "millisBehindLatest", behind,
                "total", JsonLog.fields(
                        "fulfilled", f, "duplicate", duplicates.get(), "retries", r,
                        "sidelined", s, "skipped", skipped.get()),
                // Embedded metric format: CloudWatch extracts the fields below as custom metrics.
                "_aws", emf,
                "Application", applicationName,
                "Task", JsonLog.TASK_ID,
                "OrdersFulfilled", f - lastFulfilled,
                "OrdersSidelined", s - lastSidelined,
                "RetryAttempts", r - lastRetries,
                "RecordsInFlight", inFlightNow));
        lastFulfilled = f;
        lastSidelined = s;
        lastRetries = r;
        lastAt = now;
    }
}
