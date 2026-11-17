package com.example.fulfillment;

import java.time.Instant;
import java.util.Base64;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * The dead-letter queue this team built for its stream consumer: an SQS queue where
 * a worker parks the records it has given up on, so they stop blocking the shard.
 *
 * Each message is the record plus an audit trail (shard, sequence number, attempts,
 * reason, which worker gave up), because nothing else records what happened. The
 * records can't go back onto the stream: every consumer of the stream would process
 * them again. So a separate consumer reads this queue (the sideline Lambda function).
 */
final class Sideline {
    private final SqsAsyncClient sqs;
    private final String queueUrl;

    Sideline(SqsAsyncClient sqs, String queueUrl) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
    }

    /** Throws if SQS doesn't accept the message: the shard must not checkpoint past this record. */
    void send(KinesisClientRecord record, String shardId, int attempts, String reason) {
        var envelope = JsonLog.MAPPER.createObjectNode();
        byte[] data = new byte[record.data().remaining()];
        record.data().asReadOnlyBuffer().get(data);
        envelope.put("data", Base64.getEncoder().encodeToString(data));
        envelope.put("partitionKey", record.partitionKey());
        envelope.put("shardId", shardId);
        envelope.put("sequenceNumber", record.sequenceNumber());
        envelope.put("arrivedAt", record.approximateArrivalTimestamp() == null ? null : record.approximateArrivalTimestamp().toString());
        envelope.put("attempts", attempts);
        envelope.put("reason", reason);
        envelope.put("sidelinedBy", JsonLog.TASK_ID);
        envelope.put("sidelinedAt", Instant.now().toString());
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody(envelope.toString())).join();
    }
}
