// At-least-once order fulfillment worker: receive -> fulfill (idempotent) -> acknowledge,
// with a heartbeat that extends visibility while orders are still being processed.
//
// The queue delivers every record at least once. A record is delivered again when
// its visibility timeout expires before it's acknowledged: the worker crashed, the
// acknowledgment didn't arrive, or processing took too long. This worker handles
// that in two ways:
// - Idempotent fulfillment (fulfillment.ts): a second delivery of an order can't
//   fulfill it twice.
// - A heartbeat (heartbeat.ts): long-running orders stay hidden from other workers
//   for as long as this worker is processing them.
import { setTimeout as sleep } from 'node:timers/promises';
import {
  AcknowledgeQueueRecordsCommand, KinesisClient, ReceiveQueueRecordsCommand, type QueueRecord,
} from '@aws-sdk/client-kinesis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { readWorkerConfig, type WorkerConfig } from './config';
import { Fulfillment } from './fulfillment';
import { Heartbeat } from './heartbeat';
import { errorMessage, log } from './log';
import { parseRecord } from '../../../shared/src/orders';
import { matchFailed } from '../../../shared/src/batch';

// Service limits: up to 10 records per receive/acknowledge call, wait time 1-20 seconds.
const MAX_RECORDS = 10;
const WAIT_TIME_SECONDS = 20;

/** `ack` acknowledges the record; `leave` lets its visibility timeout expire so it's delivered again. */
type Decision = 'ack' | 'leave';

const stats = {
  fulfilled: 0, duplicatesSkipped: 0, duplicatesDiscarded: 0, ackLostSimulated: 0,
  skipped: 0, invalid: 0, failed: 0, ackFailed: 0, visibilityExtended: 0,
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
      requestHandler: new NodeHttpHandler({ httpsAgent: { keepAlive: true, maxSockets: config.concurrency * 3 } }),
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
      visibilityTimeoutSeconds: this.config.visibilityTimeoutSeconds,
      heartbeatSeconds: this.config.heartbeatSeconds,
      maxHoldSeconds: this.config.maxHoldSeconds,
      slowProcessingSeconds: this.config.slowProcessingMs / 1000,
    });
    await Promise.all(Array.from({ length: this.config.concurrency }, (_, i) => this.pollLoop(i)));
    this.kinesis.destroy();
  }

  private async pollLoop(loop: number): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      let records: QueueRecord[];
      try {
        // The worker sets the visibility timeout on each receive, so the heartbeat
        // knows how long it holds the records.
        const out = await this.kinesis.send(
          new ReceiveQueueRecordsCommand({
            QueueARN: this.config.queueArn,
            Limit: MAX_RECORDS,
            WaitTimeSeconds: WAIT_TIME_SECONDS,
            VisibilityTimeoutSeconds: this.config.visibilityTimeoutSeconds,
          }),
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
      await this.processBatch(records);
    }
  }

  private async processBatch(records: QueueRecord[]): Promise<void> {
    const heartbeat = new Heartbeat(this.kinesis, this.config, (n) => { stats.visibilityExtended += n; });
    try {
      const decisions = await Promise.all(records.map((record, i) => this.handle(String(i), record, heartbeat)));
      await this.acknowledge(records.filter((_, i) => decisions[i] === 'ack'));
    } finally {
      // Acknowledged records are gone. Records left unacknowledged reappear when
      // their visibility timeout expires.
      heartbeat.stop();
    }
  }

  private async handle(key: string, record: QueueRecord, heartbeat: Heartbeat): Promise<Decision> {
    const receiveCount = record.ReceiveCount ?? 1;
    const parsed = parseRecord(record.Data);

    if (parsed.kind === 'other') {
      // Other event types share the stream; this consumer only fulfills orders.
      stats.skipped += 1;
      return 'ack';
    }
    if (parsed.kind === 'invalid') {
      // Left unacknowledged: after MaxReceiveCount receives the queue dead-letters it.
      // Scenario 4 shows what to do with dead-letter records.
      stats.invalid += 1;
      log('ERROR', 'INVALID_ORDER', { reason: parsed.reason, receiveCount });
      return 'leave';
    }

    const { order } = parsed;
    // `slow-no-heartbeat` shows what happens without the heartbeat: the record expires
    // while it's still being processed, and another worker receives it.
    if (order.simulate !== 'slow-no-heartbeat') heartbeat.hold(key, record, order.orderId);
    const fields = { orderId: order.orderId, receiveCount, simulate: order.simulate };
    try {
      const result = await this.fulfillment.fulfill(order, receiveCount);
      switch (result.status) {
        case 'DUPLICATE_SKIPPED':
          stats.duplicatesSkipped += 1;
          log('INFO', 'DUPLICATE_SKIPPED', fields);
          return 'ack';
        case 'DUPLICATE_DISCARDED':
          stats.duplicatesDiscarded += 1;
          log('WARN', 'DUPLICATE_DISCARDED', {
            ...fields, processingMs: result.processingMs,
            note: 'another delivery of this order was fulfilled while this one was working',
          });
          return 'ack';
        case 'FULFILLED':
          stats.fulfilled += 1;
          log('INFO', 'FULFILLED', { ...fields, shipmentId: result.shipmentId, processingMs: result.processingMs });
          if (order.simulate === 'ack-lost' && receiveCount === 1) {
            // Simulates a worker that crashes, or loses its connection, after the side
            // effect and before the acknowledgment. Nothing extends the record any more,
            // so the queue delivers it again once its visibility timeout expires.
            stats.ackLostSimulated += 1;
            heartbeat.release(key);
            log('WARN', 'ACK_LOST_SIMULATED', { ...fields, redeliveryInSeconds: this.config.visibilityTimeoutSeconds });
            return 'leave';
          }
          return 'ack';
      }
    } catch (error) {
      // Not acknowledged: delivered again after the visibility timeout.
      stats.failed += 1;
      heartbeat.release(key);
      log('WARN', 'FULFILLMENT_FAILED', { ...fields, error: errorMessage(error) });
      return 'leave';
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
      // A failed acknowledgment means the record will be delivered again, and the
      // orders table recognizes it as a duplicate then.
      for (const { item: record, failure: f } of matchFailed(records, Failed)) {
        stats.ackFailed += 1;
        log('WARN', 'ACK_FAILED', { code: f.ErrorCode, message: f.ErrorMessage, receiveCount: record?.ReceiveCount });
      }
    } catch (error) {
      stats.ackFailed += records.length;
      log('WARN', 'ACK_CALL_FAILED', { records: records.length, error: errorMessage(error) });
    }
  }
}

function startStats(intervalMs: number): () => void {
  let last = { ...stats };
  const report = () => {
    const interval = Object.fromEntries(Object.entries(stats).map(([k, v]) => [k, v - last[k as keyof typeof stats]]));
    if (Object.values(interval).some((v) => v !== 0)) log('INFO', 'STATS', { interval, total: { ...stats } });
    last = { ...stats };
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
