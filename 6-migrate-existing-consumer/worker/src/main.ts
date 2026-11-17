// Order fulfillment as a queue consumer: the consumer after the migration.
//
// The fulfillment rules and the orders table are the same as the KCL consumer's.
// What's gone is everything around them: no lease table, no checkpoints, no
// retry-in-place loop, no sideline queue and no custom metrics. Tasks aren't tied to
// shards and don't coordinate. Each one runs CONCURRENCY poll loops, so it can keep
// many orders in progress at once.
import { setTimeout as sleep } from 'node:timers/promises';
import {
  AcknowledgeQueueRecordsCommand, KinesisClient, ReceiveQueueRecordsCommand, type QueueRecord,
} from '@aws-sdk/client-kinesis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { readWorkerConfig, type WorkerConfig } from './config';
import { Fulfillment } from './fulfillment';
import { errorMessage, log } from './log';
import { parseRecord } from '../../../shared/src/orders';
import { matchFailed } from '../../../shared/src/batch';

// Service limits: up to 10 records per receive/acknowledge call, wait time 1-20 seconds.
const MAX_RECORDS = 10;
const WAIT_TIME_SECONDS = 20;

type Outcome = 'fulfilled' | 'duplicate' | 'skipped' | 'retry';

const stats = { fulfilled: 0, duplicate: 0, skipped: 0, failed: 0, invalid: 0, ackFailed: 0 };

class Worker {
  private readonly kinesis: KinesisClient;
  private readonly fulfillment: Fulfillment;
  private readonly abort = new AbortController();
  private stopping = false;

  constructor(private readonly config: WorkerConfig) {
    // Keep connections open, with enough sockets for every poll loop's receive and acknowledge calls.
    this.kinesis = new KinesisClient({
      region: config.region,
      requestHandler: new NodeHttpHandler({ httpsAgent: { keepAlive: true, maxSockets: config.concurrency * 2 } }),
    });
    this.fulfillment = new Fulfillment(new DynamoDBClient({ region: config.region }), config, 'queue');
  }

  stop(): void {
    if (this.stopping) return;
    this.stopping = true;
    log('INFO', 'STOPPING', { note: 'finishing in-flight orders, then exiting' });
    this.abort.abort(); // cancels idle long polls; orders already received are still finished
  }

  async run(): Promise<void> {
    log('INFO', 'STARTED', {
      queueArn: this.config.queueArn,
      concurrency: this.config.concurrency,
      visibilityTimeoutSeconds: this.config.visibilityTimeoutSeconds,
    });
    await Promise.all(Array.from({ length: this.config.concurrency }, (_, i) => this.pollLoop(i)));
    this.kinesis.destroy();
  }

  private async pollLoop(loop: number): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      let records: QueueRecord[];
      try {
        const out = await this.kinesis.send(new ReceiveQueueRecordsCommand({
          QueueARN: this.config.queueArn,
          Limit: MAX_RECORDS,
          WaitTimeSeconds: WAIT_TIME_SECONDS,
          VisibilityTimeoutSeconds: this.config.visibilityTimeoutSeconds,
        }), { abortSignal: this.abort.signal });
        records = out.QueueRecords ?? [];
        failures = 0;
      } catch (error) {
        if (this.stopping) break;
        failures += 1;
        const backoffMs = Math.min(30_000, 500 * 2 ** failures) * (0.5 + Math.random() / 2);
        log('WARN', 'RECEIVE_FAILED', { loop, error: errorMessage(error), retryInMs: Math.round(backoffMs) });
        await sleep(backoffMs);
        continue;
      }
      // A receive can return fewer records than requested, including none.
      if (records.length === 0) continue;

      // Orders are independent, so they're fulfilled in parallel. Nothing waits for order.
      const outcomes = await Promise.all(records.map((record) => this.handle(record)));
      await this.acknowledge(records.filter((_, i) => outcomes[i] !== 'retry'));
    }
  }

  private async handle(record: QueueRecord): Promise<Outcome> {
    const receiveCount = record.ReceiveCount ?? 1;
    const parsed = parseRecord(record.Data);
    if (parsed.kind === 'other') {
      stats.skipped += 1; // other event types share the stream
      return 'skipped';
    }
    if (parsed.kind === 'invalid') {
      // Not acknowledged. After MaxReceiveCount receives the queue moves it to its
      // dead-letter records. No sideline queue, and no worker decides on its own.
      stats.invalid += 1;
      log('ERROR', 'INVALID_ORDER', { reason: parsed.reason, partitionKey: record.PartitionKey, receiveCount });
      return 'retry';
    }
    const { order } = parsed;
    try {
      const result = await this.fulfillment.fulfill(order, receiveCount);
      if (result.status !== 'FULFILLED') {
        // Already shipped: by an earlier delivery, or by the KCL consumer during the cutover.
        stats.duplicate += 1;
        log('INFO', result.status, { orderId: order.orderId, receiveCount });
        return 'duplicate';
      }
      stats.fulfilled += 1;
      const ageMs = record.ApproximateArrivalTimestamp ? Date.now() - record.ApproximateArrivalTimestamp.getTime() : undefined;
      log('INFO', 'FULFILLED', { orderId: order.orderId, receiveCount, processingMs: result.processingMs, ageMs });
      return 'fulfilled';
    } catch (error) {
      // Not acknowledged: the queue delivers it again after the visibility timeout,
      // to whichever task polls next. A broken task doesn't get the final say: the
      // queue counts receives across every task before it dead-letters an order.
      stats.failed += 1;
      log('WARN', 'FULFILLMENT_FAILED', { orderId: order.orderId, receiveCount, error: errorMessage(error) });
      return 'retry';
    }
  }

  private async acknowledge(records: QueueRecord[]): Promise<void> {
    if (records.length === 0) return;
    try {
      const { Failed } = await this.kinesis.send(new AcknowledgeQueueRecordsCommand({
        QueueARN: this.config.queueArn,
        Entries: records.map((r) => ({ ReceiptHandle: r.ReceiptHandle })),
      }));
      // HTTP 200 doesn't mean every entry succeeded; check the Failed list on every call.
      for (const { item: record, failure: f } of matchFailed(records, Failed)) {
        stats.ackFailed += 1;
        log('WARN', 'ACK_FAILED', { code: f.ErrorCode, message: f.ErrorMessage, receiveCount: record?.ReceiveCount });
      }
    } catch (error) {
      // The orders are delivered again; the orders table recognizes them as duplicates.
      stats.ackFailed += records.length;
      log('WARN', 'ACK_CALL_FAILED', { records: records.length, error: errorMessage(error) });
    }
  }
}

/** Logs this task's throughput regularly; scripts/status.ts sums it across tasks. */
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
  const config = readWorkerConfig();
  const worker = new Worker(config);
  const stopStats = startStats(config.statsIntervalMs);
  // ECS sends SIGTERM on scale-in and deployments, then SIGKILL after the stop timeout.
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
