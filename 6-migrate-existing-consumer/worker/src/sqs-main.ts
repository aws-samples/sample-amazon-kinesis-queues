// Order fulfillment as an SQS consumer: the relay setup before the migration.
//
// An EventBridge pipe copies every record from the stream into an SQS queue, and this
// consumer reads that SQS queue. It has the same structure as the queue consumer in
// main.ts, which replaces it. Compare the two: the loop, the fulfillment rules and the
// orders table are the same, and only the calls change:
//
//   SQS (this file)                    Kinesis queue (main.ts)
//   ReceiveMessage                     ReceiveQueueRecords
//   DeleteMessageBatch                 AcknowledgeQueueRecords
//   ChangeMessageVisibility(Batch)     UpdateQueueRecordsVisibility
//   dead-letter queue, redrive policy  the queue's built-in dead-letter records
//
// The difference is upstream: with a Kinesis queue there is no pipe and no second copy
// of the data.
import { setTimeout as sleep } from 'node:timers/promises';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DeleteMessageBatchCommand, ReceiveMessageCommand, SQSClient, type Message } from '@aws-sdk/client-sqs';
import { readSqsWorkerConfig, type SqsWorkerConfig } from './config';
import { Fulfillment, type StreamOrigin } from './fulfillment';
import { errorMessage, log, setConsumerLabel } from './log';
import { parseRecord } from '../../../shared/src/orders';

// Service limits: up to 10 messages per receive/delete call, wait time 0-20 seconds.
const MAX_MESSAGES = 10;
const WAIT_TIME_SECONDS = 20;

type Outcome = 'fulfilled' | 'duplicate' | 'skipped' | 'retry';

const stats = { fulfilled: 0, duplicate: 0, skipped: 0, failed: 0, invalid: 0, deleteFailed: 0 };

/** The fields of a Kinesis record that the pipe puts in each SQS message body. */
interface RelayedRecord {
  data?: string;
  partitionKey?: string;
  sequenceNumber?: string;
  /** Seconds since the epoch, with a fraction. */
  approximateArrivalTimestamp?: number;
  /** <shardId>:<sequenceNumber> */
  eventID?: string;
}

/** The stream record a message carries, and where it came from in the stream. */
function unwrap(message: Message): { data?: Uint8Array; partitionKey?: string; origin?: StreamOrigin } {
  const body = JSON.parse(message.Body ?? '{}') as RelayedRecord;
  const data = body.data ? Buffer.from(body.data, 'base64') : undefined;
  const shardId = body.eventID?.split(':')[0];
  const origin = shardId && body.sequenceNumber && body.approximateArrivalTimestamp !== undefined
    ? { shardId, sequenceNumber: body.sequenceNumber, arrivedAt: new Date(body.approximateArrivalTimestamp * 1000) }
    : undefined;
  return { data, partitionKey: body.partitionKey, origin };
}

class SqsWorker {
  private readonly sqs: SQSClient;
  private readonly fulfillment: Fulfillment;
  private readonly abort = new AbortController();
  private stopping = false;

  constructor(private readonly config: SqsWorkerConfig) {
    this.sqs = new SQSClient({ region: config.region });
    this.fulfillment = new Fulfillment(new DynamoDBClient({ region: config.region }), config, 'sqs');
  }

  stop(): void {
    if (this.stopping) return;
    this.stopping = true;
    log('INFO', 'STOPPING', { note: 'finishing in-flight orders, then exiting' });
    this.abort.abort(); // cancels idle long polls; orders already received are still finished
  }

  async run(): Promise<void> {
    log('INFO', 'STARTED', {
      queueUrl: this.config.queueUrl,
      concurrency: this.config.concurrency,
      visibilityTimeoutSeconds: this.config.visibilityTimeoutSeconds,
    });
    await Promise.all(Array.from({ length: this.config.concurrency }, (_, i) => this.pollLoop(i)));
    this.sqs.destroy();
  }

  private async pollLoop(loop: number): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      let messages: Message[];
      try {
        const out = await this.sqs.send(new ReceiveMessageCommand({
          QueueUrl: this.config.queueUrl,
          MaxNumberOfMessages: MAX_MESSAGES,
          WaitTimeSeconds: WAIT_TIME_SECONDS,
          VisibilityTimeout: this.config.visibilityTimeoutSeconds,
          MessageSystemAttributeNames: ['ApproximateReceiveCount'],
        }), { abortSignal: this.abort.signal });
        messages = out.Messages ?? [];
        failures = 0;
      } catch (error) {
        if (this.stopping) break;
        failures += 1;
        const backoffMs = Math.min(30_000, 500 * 2 ** failures) * (0.5 + Math.random() / 2);
        log('WARN', 'RECEIVE_FAILED', { loop, error: errorMessage(error), retryInMs: Math.round(backoffMs) });
        await sleep(backoffMs);
        continue;
      }
      if (messages.length === 0) continue;

      const outcomes = await Promise.all(messages.map((message) => this.handle(message)));
      await this.delete(messages.filter((_, i) => outcomes[i] !== 'retry'));
    }
  }

  private async handle(message: Message): Promise<Outcome> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
    let record: ReturnType<typeof unwrap>;
    try {
      record = unwrap(message);
    } catch (error) {
      // Not a message from the pipe. Left for the SQS queue's own dead-letter queue.
      stats.invalid += 1;
      log('ERROR', 'INVALID_MESSAGE', { messageId: message.MessageId, error: errorMessage(error), receiveCount });
      return 'retry';
    }
    const parsed = parseRecord(record.data);
    if (parsed.kind === 'other') {
      stats.skipped += 1; // other event types share the stream, and the pipe copies them all
      return 'skipped';
    }
    if (parsed.kind === 'invalid') {
      // Not deleted. After maxReceiveCount receives SQS moves it to the dead-letter queue.
      stats.invalid += 1;
      log('ERROR', 'INVALID_ORDER', { reason: parsed.reason, partitionKey: record.partitionKey, receiveCount });
      return 'retry';
    }
    const { order } = parsed;
    try {
      const result = await this.fulfillment.fulfill(order, receiveCount, record.origin);
      if (result.status !== 'FULFILLED') {
        stats.duplicate += 1;
        log('INFO', result.status, { orderId: order.orderId, receiveCount });
        return 'duplicate';
      }
      stats.fulfilled += 1;
      const ageMs = record.origin ? Date.now() - record.origin.arrivedAt.getTime() : undefined;
      log('INFO', 'FULFILLED', { orderId: order.orderId, receiveCount, processingMs: result.processingMs, ageMs });
      return 'fulfilled';
    } catch (error) {
      // Not deleted: SQS delivers it again after the visibility timeout.
      stats.failed += 1;
      log('WARN', 'FULFILLMENT_FAILED', { orderId: order.orderId, receiveCount, error: errorMessage(error) });
      return 'retry';
    }
  }

  private async delete(messages: Message[]): Promise<void> {
    if (messages.length === 0) return;
    try {
      const { Failed } = await this.sqs.send(new DeleteMessageBatchCommand({
        QueueUrl: this.config.queueUrl,
        Entries: messages.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle })),
      }));
      // HTTP 200 doesn't mean every entry succeeded; check Failed on every call.
      for (const f of Failed ?? []) {
        stats.deleteFailed += 1;
        log('WARN', 'DELETE_FAILED', { code: f.Code, message: f.Message, messageId: messages[Number(f.Id)]?.MessageId });
      }
    } catch (error) {
      stats.deleteFailed += messages.length;
      log('WARN', 'DELETE_CALL_FAILED', { messages: messages.length, error: errorMessage(error) });
    }
  }
}

function startStats(intervalMs: number): () => void {
  let last = { ...stats };
  let lastAt = Date.now();
  const report = () => {
    const now = Date.now();
    const seconds = (now - lastAt) / 1000;
    log('INFO', 'STATS', {
      fulfilledPerSecond: Number(((stats.fulfilled - last.fulfilled) / seconds).toFixed(2)),
      intervalSeconds: Math.round(seconds),
      total: { ...stats },
    });
    last = { ...stats };
    lastAt = now;
  };
  const timer = setInterval(report, intervalMs);
  return () => {
    clearInterval(timer);
    report();
  };
}

async function main(): Promise<void> {
  setConsumerLabel('sqs');
  const config = readSqsWorkerConfig();
  const worker = new SqsWorker(config);
  const stopStats = startStats(config.statsIntervalMs);
  process.once('SIGTERM', () => worker.stop());
  process.once('SIGINT', () => worker.stop());
  await worker.run();
  stopStats();
  log('INFO', 'STOPPED');
}

main().catch((error) => {
  log('ERROR', 'CRASHED', { error: errorMessage(error) });
  process.exit(1);
});
