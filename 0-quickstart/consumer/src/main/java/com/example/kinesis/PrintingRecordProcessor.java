package com.example.kinesis;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;

final class PrintingRecordProcessor implements ShardRecordProcessor {
    private final Consumer<String> output;
    private final Consumer<Throwable> onFailure;
    private String shardId;
    private boolean failed;
    private boolean leaseLost;
    private boolean processedRecords;

    PrintingRecordProcessor(Consumer<String> output, Consumer<Throwable> onFailure) {
        this.output = output;
        this.onFailure = onFailure;
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
            for (var record : input.records()) {
                String data = StandardCharsets.UTF_8.decode(record.data().asReadOnlyBuffer()).toString();
                output.accept("RECORD shard=" + shardId + " sequence=" + record.sequenceNumber() + " data=" + data);
            }
            processedRecords = true;
            checkpoint(input.checkpointer());
        } catch (RuntimeException e) {
            fail(e);
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
            // Do not mark a shard complete if any of its output failed.
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
            // Another worker owns this shard now. It will resume from the durable checkpoint.
            leaseLost = true;
            return false;
        } catch (Exception e) {
            // Stop on an unrecovered checkpoint error; restarting may replay printed records.
            fail(e);
            return false;
        }
    }

    private void fail(Throwable error) {
        if (!failed) {
            // KCL does not redeliver a batch just because processRecords throws.
            // Block all later checkpoints so a restart can replay from the durable position.
            failed = true;
            onFailure.accept(error);
        }
    }
}
