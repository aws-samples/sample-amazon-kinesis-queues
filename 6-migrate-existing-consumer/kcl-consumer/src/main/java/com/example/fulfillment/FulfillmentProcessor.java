package com.example.fulfillment;

import java.util.List;
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
 * Order fulfillment as a KCL record processor: the stateless consumer this scenario
 * migrates to a queue. KCL creates one of these per shard lease, and only the task
 * holding the lease reads that shard. Everything that makes per-order processing safe
 * on a stream is built here, by the team that owns the consumer. The queue consumer
 * (worker/src/main.ts) needs none of it:
 *
 * - Retries. An order that fails is retried in place, because KCL only moves past a
 *   record when the processor does. Every order behind it on the shard waits.
 * - Deciding what's poison. After maxAttempts failures, this task alone decides that
 *   the order can't be processed, and sidelines it. No other task can try it first:
 *   they don't read this shard. And the task can't tell a poison order from a broken
 *   dependency of its own.
 * - A dead-letter queue. Sidelined orders go to an SQS queue, with an audit trail, and
 *   a separate consumer (the sideline Lambda function) works through them.
 * - Progress. A checkpoint is one position per shard, so it can only say "everything
 *   up to here is done". After a lease moves or a task stops, the new owner replays
 *   every record after the checkpoint, including ones already fulfilled. The orders
 *   table absorbs those replays.
 * - Metrics. Records in flight, throughput and sidelined orders are measured here and
 *   published as custom metrics (see Stats).
 */
final class FulfillmentProcessor implements ShardRecordProcessor {
    private final Settings settings;
    private final OrderFulfillment fulfillment;
    private final Sideline sideline;
    private final Stats stats;
    private final Consumer<Throwable> onFailure;
    private String shardId;
    private boolean stopping;
    private boolean failed;
    /** The last record whose work is finished, and the last one a checkpoint covers. */
    private KinesisClientRecord lastProcessed;
    private KinesisClientRecord lastCheckpointed;

    FulfillmentProcessor(Settings settings, OrderFulfillment fulfillment, Sideline sideline, Stats stats,
            Consumer<Throwable> onFailure) {
        this.settings = settings;
        this.fulfillment = fulfillment;
        this.sideline = sideline;
        this.stats = stats;
        this.onFailure = onFailure;
    }

    @Override
    public void initialize(InitializationInput input) {
        shardId = input.shardId();
        stats.shardAcquired(shardId);
        JsonLog.info("LEASE_ACQUIRED", JsonLog.fields(
                "shardId", shardId, "resumeFrom", String.valueOf(input.extendedSequenceNumber())));
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        List<KinesisClientRecord> records = input.records();
        if (failed || stopping || records.isEmpty()) {
            return;
        }
        stats.behindLatest(shardId, input.millisBehindLatest());
        stats.inFlight.addAndGet(records.size());
        try {
            // One order at a time, in shard order. A retry holds up everything behind it.
            for (KinesisClientRecord record : records) {
                handle(record);
                lastProcessed = record;
            }
            // Sequential checkpointing: one position for the whole shard.
            checkpoint(input.checkpointer());
        } catch (RuntimeException e) {
            fail(e);
        } finally {
            stats.inFlight.addAndGet(-records.size());
        }
    }

    private void handle(KinesisClientRecord record) {
        Orders.Parsed parsed = Orders.parse(record.data());
        switch (parsed.kind()) {
            case OTHER -> stats.skipped.incrementAndGet();
            case INVALID -> {
                // Retrying can't fix a malformed event. Park it so the shard can move on.
                sideline.send(record, shardId, 1, "malformed: " + parsed.detail());
                stats.sidelined.incrementAndGet();
                JsonLog.warn("SIDELINED", JsonLog.fields(
                        "partitionKey", record.partitionKey(), "shardId", shardId, "reason", parsed.detail()));
            }
            case ORDER -> fulfillWithRetries(record, parsed.order());
        }
    }

    private void fulfillWithRetries(KinesisClientRecord record, Orders.Order order) {
        String lastError = "";
        for (int attempt = 1; attempt <= settings.maxAttempts(); attempt++) {
            try {
                OrderFulfillment.Outcome outcome = fulfillment.fulfill(order, attempt);
                if (outcome instanceof OrderFulfillment.Fulfilled f) {
                    stats.fulfilled.incrementAndGet();
                    JsonLog.info("FULFILLED", JsonLog.fields(
                            "orderId", order.orderId(), "shardId", shardId, "attempt", attempt,
                            "processingMs", f.processingMs(), "ageMs", ageMs(record)));
                } else if (outcome instanceof OrderFulfillment.Duplicate d) {
                    // Already shipped: a replay, or the queue consumer was first during the cutover.
                    stats.duplicates.incrementAndGet();
                    JsonLog.info(d.skipped() ? "DUPLICATE_SKIPPED" : "DUPLICATE_DISCARDED",
                            JsonLog.fields("orderId", order.orderId(), "shardId", shardId));
                }
                return;
            } catch (Exception e) {
                lastError = JsonLog.errorMessage(e);
                if (attempt < settings.maxAttempts()) {
                    stats.retries.incrementAndGet();
                    // Retry in place, after 1 s and then 2 s. Nothing else on this shard
                    // moves until this order does.
                    long backoffMs = 1000L << (attempt - 1);
                    JsonLog.warn("RETRY_IN_PLACE", JsonLog.fields(
                            "orderId", order.orderId(), "shardId", shardId, "attempt", attempt,
                            "retryInMs", backoffMs, "error", lastError));
                    sleep(backoffMs);
                }
            }
        }
        // This task decides on its own that the order can't be processed.
        sideline.send(record, shardId, settings.maxAttempts(), lastError);
        stats.sidelined.incrementAndGet();
        JsonLog.warn("SIDELINED", JsonLog.fields(
                "orderId", order.orderId(), "shardId", shardId, "attempts", settings.maxAttempts(), "reason", lastError));
    }

    @Override
    public void leaseLost(LeaseLostInput input) {
        stopping = true;
        stats.shardReleased(shardId);
        JsonLog.info("LEASE_LOST", JsonLog.fields("shardId", shardId, "note", "the next owner replays from the last checkpoint"));
    }

    @Override
    public void shardEnded(ShardEndedInput input) {
        stats.shardReleased(shardId);
        if (failed) {
            throw new IllegalStateException("Cannot complete shard " + shardId + " after a failure.");
        }
        try {
            input.checkpointer().checkpoint();
        } catch (Exception e) {
            throw new IllegalStateException("Could not checkpoint the end of shard " + shardId, e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput input) {
        // A graceful lease handoff or task shutdown: record how far this task got.
        stopping = true;
        if (!failed) {
            checkpoint(input.checkpointer());
        }
        stats.shardReleased(shardId);
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        KinesisClientRecord record = lastProcessed;
        if (record == null || record == lastCheckpointed) {
            return;
        }
        try {
            // Never checkpoint past a record whose work isn't finished.
            checkpointer.checkpoint(record.sequenceNumber(), record.subSequenceNumber());
            lastCheckpointed = record;
        } catch (ShutdownException e) {
            // Another task owns the shard now and replays from the previous checkpoint.
            stopping = true;
        } catch (Exception e) {
            fail(e);
        }
    }

    private void fail(Throwable error) {
        if (!failed) {
            // Stop checkpointing, so a restart replays from the durable position.
            failed = true;
            onFailure.accept(error);
        }
    }

    private static Long ageMs(KinesisClientRecord record) {
        return record.approximateArrivalTimestamp() == null ? null
                : System.currentTimeMillis() - record.approximateArrivalTimestamp().toEpochMilli();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
