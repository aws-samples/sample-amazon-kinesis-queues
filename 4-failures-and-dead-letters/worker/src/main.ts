// Order fulfillment worker: receive -> fulfill -> acknowledge.
//
// Tasks are stateless and don't coordinate with each other: no shard leases, no
// checkpoint table. To fulfill orders faster, run more tasks in the ECS service.
// Inside each task, CONCURRENCY poll loops (default 4) run side by side to keep
// the CPU busy while orders wait on the (simulated) warehouse API.
import { setTimeout as sleep } from 'node:timers/promises';
import {
  AcknowledgeQueueRecordsCommand, KinesisClient, ReceiveQueueRecordsCommand, type QueueRecord,
} from '@aws-sdk/client-kinesis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { readWorkerConfig, type WorkerConfig } from './config';
import { Fulfillment, PermanentError, TransientError, WarehouseUnavailableError } from './fulfillment';
import { errorMessage, log } from './log';
import { parseRecord } from '../../../shared/src/orders';
import { matchFailed } from '../../../shared/src/batch';

// Service limits: up to 10 records per receive/acknowledge call, wait time 1-20 seconds.
const MAX_RECORDS = 10;
const WAIT_TIME_SECONDS = 20;

type Outcome = 'fulfilled' | 'duplicate' | 'skipped' | 'invalid' | 'retry';

const stats: Record<Outcome, number> & { rejected: number; ackFailed: number } = {
  fulfilled: 0, duplicate: 0, skipped: 0, invalid: 0, retry: 0, rejected: 0, ackFailed: 0,
};

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
    this.fulfillment = new Fulfillment(new DynamoDBClient({ region: config.region }), config);
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
      processingMs: [this.config.processingMsMin, this.config.processingMsMax],
      catalogParameter: this.config.catalogParameter,
      visibilityTimeout: 'queue setting',
    });
    await Promise.all(Array.from({ length: this.config.concurrency }, (_, i) => this.pollLoop(i)));
    this.kinesis.destroy();
  }

  private async pollLoop(loop: number): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      let records: QueueRecord[];
      try {
        // VisibilityTimeoutSeconds omitted: the queue's configured timeout applies.
        const out = await this.kinesis.send(
          new ReceiveQueueRecordsCommand({ QueueARN: this.config.queueArn, Limit: MAX_RECORDS, WaitTimeSeconds: WAIT_TIME_SECONDS }),
          { abortSignal: this.abort.signal },
        );
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

      const outcomes = await Promise.all(records.map((record) => this.handle(record)));
      const done = records.filter((_, i) => outcomes[i] !== 'retry');
      await this.acknowledge(done);
    }
  }

  private async handle(record: QueueRecord): Promise<Outcome> {
    const receiveCount = record.ReceiveCount ?? 1;
    const parsed = parseRecord(record.Data);

    if (parsed.kind === 'other') {
      // Other event types share the stream; this consumer only fulfills orders.
      stats.skipped += 1;
      return 'skipped';
    }
    if (parsed.kind === 'invalid') {
      // Retrying can't fix a malformed event, but the worker doesn't decide that on
      // its own: it leaves the record unacknowledged, and after MaxReceiveCount
      // receives the queue moves it to its dead-letter state for an operator.
      stats.invalid += 1;
      log('ERROR', 'INVALID_ORDER', { reason: parsed.reason, partitionKey: record.PartitionKey, receiveCount });
      return 'retry';
    }

    const { order } = parsed;
    try {
      const result = await this.fulfillment.fulfill(order, receiveCount);
      const deliveryMs = record.ApproximateArrivalTimestamp ? Date.now() - record.ApproximateArrivalTimestamp.getTime() : undefined;
      if (result.status === 'DUPLICATE') {
        stats.duplicate += 1;
        log('INFO', 'DUPLICATE_SKIPPED', { orderId: order.orderId, receiveCount });
        return 'duplicate';
      }
      stats.fulfilled += 1;
      log('INFO', 'FULFILLED', {
        orderId: order.orderId, customerId: order.customerId, shipmentId: result.shipmentId,
        receiveCount, processingMs: result.processingMs, ageMs: deliveryMs,
      });
      return 'fulfilled';
    } catch (error) {
      // Not acknowledged: the queue delivers it again after the visibility timeout,
      // to whichever task polls next, without holding up any other order.
      // Permanent rejections take the same path: the queue, not the worker, decides
      // when a record has failed often enough to become a dead-letter record.
      const event = error instanceof WarehouseUnavailableError ? 'WAREHOUSE_UNAVAILABLE'
        : error instanceof TransientError ? 'TRANSIENT_FAILURE'
        : error instanceof PermanentError ? 'REJECTED' : 'FULFILLMENT_FAILED';
      if (error instanceof PermanentError) stats.rejected += 1;
      else stats.retry += 1;
      log('WARN', event, { orderId: order.orderId, receiveCount, error: errorMessage(error) });
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
      // The orders become visible again and are delivered again. The orders table
      // makes that safe: they are recognized as duplicates and acknowledged then.
      stats.ackFailed += records.length;
      log('WARN', 'ACK_CALL_FAILED', { records: records.length, error: errorMessage(error) });
    }
  }
}

function startStats(intervalMs: number): () => void {
  let last = { ...stats };
  let lastAt = Date.now();
  const report = () => {
    const now = Date.now();
    const fulfilled = stats.fulfilled - last.fulfilled;
    log('INFO', 'STATS', {
      intervalSeconds: Math.round((now - lastAt) / 1000),
      fulfilledPerSecond: Number((fulfilled / ((now - lastAt) / 1000)).toFixed(2)),
      interval: Object.fromEntries(Object.entries(stats).map(([k, v]) => [k, v - last[k as keyof typeof stats]])),
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
