package com.example.fraud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Order-velocity fraud signal: a customer who places VELOCITY_THRESHOLD orders within
 * VELOCITY_WINDOW_SECONDS is flagged.
 *
 * <p>This is a stream job, not a queue job. The signal depends on seeing every order
 * of a customer, in order. The checkout uses customerId as the partition key, so all
 * of a customer's orders land on one shard, in the order they were placed, and KCL
 * gives each shard to one processor at a time. One processor therefore sees a
 * customer's whole history and can keep it in memory.
 *
 * <p>The window is kept in memory and isn't checkpointed, so after a restart or a
 * lease move it starts empty. A production job would keep it in a state store, or
 * use Amazon Managed Service for Apache Flink.
 */
final class FraudSignalsProcessor implements ShardRecordProcessor {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Consumer<String> output;
    private final Consumer<Throwable> onFailure;
    private final Duration window;
    private final int threshold;
    /** Order times per customer, oldest first, within the window. */
    private final Map<String, Deque<Instant>> recentOrders = new HashMap<>();
    private String shardId;
    private boolean failed;
    private boolean leaseLost;
    private boolean processedRecords;

    FraudSignalsProcessor(Consumer<String> output, Consumer<Throwable> onFailure, Duration window, int threshold) {
        this.output = output;
        this.onFailure = onFailure;
        this.window = window;
        this.threshold = threshold;
    }

    @Override
    public void initialize(InitializationInput input) {
        shardId = input.shardId();
        System.err.printf("Processing shard %s from %s%n", shardId, input.extendedSequenceNumber());
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        if (failed || leaseLost || input.records().isEmpty()) {
            return;
        }
        try {
            for (KinesisClientRecord record : input.records()) {
                score(record);
            }
            processedRecords = true;
            checkpoint(input.checkpointer());
        } catch (RuntimeException e) {
            fail(e);
        }
    }

    private void score(KinesisClientRecord record) {
        JsonNode event;
        try {
            event = JSON.readTree(StandardCharsets.UTF_8.decode(record.data().asReadOnlyBuffer()).toString());
        } catch (Exception e) {
            // Malformed events are the queues' problem to dead-letter; a stream job skips them.
            output.accept("SKIPPED shard=" + shardId + " reason=malformed sequence=" + record.sequenceNumber());
            return;
        }
        if (event == null || !"OrderPlaced".equals(event.path("eventType").asText())) {
            return;
        }
        String orderId = event.path("orderId").asText("");
        String customerId = event.path("customerId").asText("");
        if (orderId.isEmpty() || customerId.isEmpty()) {
            return;
        }
        Instant placedAt = parseTime(event.path("createdAt").asText(""), record.approximateArrivalTimestamp());

        Deque<Instant> times = recentOrders.computeIfAbsent(customerId, k -> new ArrayDeque<>());
        times.addLast(placedAt);
        Instant cutoff = placedAt.minus(window);
        while (!times.isEmpty() && times.peekFirst().isBefore(cutoff)) {
            times.removeFirst();
        }
        int inWindow = times.size();
        output.accept("ORDER_SCORED shard=" + shardId + " orderId=" + orderId + " customerId=" + customerId
                + " ordersInWindow=" + inWindow);
        // Signal once when the threshold is crossed, not on every order after it.
        if (inWindow == threshold) {
            output.accept("FRAUD_SIGNAL shard=" + shardId + " customerId=" + customerId + " orders=" + inWindow
                    + " withinSeconds=" + window.toSeconds() + " lastOrderId=" + orderId);
        }
        if (recentOrders.size() > 100_000) {
            // Bound memory: drop customers whose newest order has left the window.
            recentOrders.values().removeIf(d -> d.isEmpty() || d.peekLast().isBefore(cutoff));
        }
    }

    private static Instant parseTime(String text, Instant fallback) {
        try {
            return Instant.parse(text);
        } catch (Exception e) {
            return fallback;
        }
    }

    @Override
    public void leaseLost(LeaseLostInput input) {
        leaseLost = true;
        System.err.printf("Lease lost for %s; no final checkpoint.%n", shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput input) {
        if (failed || leaseLost) {
            throw new IllegalStateException("Cannot complete shard " + shardId + " after failure or lease loss.");
        }
        if (!checkpoint(input.checkpointer())) {
            throw new IllegalStateException("Could not checkpoint the end of shard " + shardId);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput input) {
        if (!failed && !leaseLost && processedRecords) {
            checkpoint(input.checkpointer());
        }
    }

    private boolean checkpoint(RecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
            return true;
        } catch (ShutdownException e) {
            leaseLost = true;
            return false;
        } catch (Exception e) {
            fail(e);
            return false;
        }
    }

    private void fail(Throwable error) {
        if (!failed) {
            failed = true;
            onFailure.accept(error);
        }
    }
}
