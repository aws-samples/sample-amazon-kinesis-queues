// Order fulfillment worker for the scale-out scenario: receive -> fulfill -> acknowledge.
//
// Every task is identical and stateless. Tasks aren't assigned to shards and don't
// coordinate with each other; each one takes the next available orders from the
// queue. To drain a backlog faster, run more tasks.
import { randomInt, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  AcknowledgeQueueRecordsCommand, KinesisClient, ReceiveQueueRecordsCommand, type QueueRecord,
} from '@aws-sdk/client-kinesis';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { parseRecord } from '../../../shared/src/orders';
import { matchFailed } from '../../../shared/src/batch';

// Service limits: up to 10 records per receive/acknowledge call, wait time 1-20 seconds.
const MAX_RECORDS = 10;
const WAIT_TIME_SECONDS = 20;
const TASK_ID = randomUUID().slice(0, 8);

interface Config {
  region: string;
  queueArn: string;
  concurrency: number;
  processingMsMin: number;
  processingMsMax: number;
  statsIntervalMs: number;
}

function env(name: string, fallback?: string): string {
  const value = process.env[name]?.trim() || fallback;
  if (value === undefined) throw new Error(`Environment variable ${name} is required.`);
  return value;
}

function envNumber(name: string, fallback: number, min: number, max: number): number {
  const value = Number(env(name, String(fallback)));
  if (!Number.isFinite(value) || value < min || value > max) throw new Error(`${name} must be from ${min} to ${max}.`);
  return value;
}

function readConfig(): Config {
  const config: Config = {
    region: env('AWS_REGION'),
    queueArn: env('QUEUE_ARN'),
    concurrency: Math.trunc(envNumber('CONCURRENCY', 4, 1, 32)),
    processingMsMin: envNumber('PROCESSING_MS_MIN', 300, 0, 60_000),
    processingMsMax: envNumber('PROCESSING_MS_MAX', 800, 0, 60_000),
    statsIntervalMs: envNumber('STATS_INTERVAL_SECONDS', 10, 5, 3600) * 1000,
  };
  if (config.processingMsMax < config.processingMsMin) throw new Error('PROCESSING_MS_MAX must be >= PROCESSING_MS_MIN.');
  return config;
}

/** One JSON object per line, easy to query in CloudWatch Logs Insights. */
function log(level: 'INFO' | 'WARN' | 'ERROR', event: string, fields: Record<string, unknown> = {}): void {
  const line = JSON.stringify({ time: new Date().toISOString(), level, task: TASK_ID, event, ...fields });
  if (level === 'INFO') console.log(line);
  else console.error(line);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

const counts = { fulfilled: 0, skipped: 0, ackFailed: 0 };

class Worker {
  private readonly kinesis: KinesisClient;
  private readonly abort = new AbortController();
  private stopping = false;

  constructor(private readonly config: Config) {
    // Keep connections open, with enough sockets for every poll loop's receive and acknowledge calls.
    this.kinesis = new KinesisClient({
      region: config.region,
      requestHandler: new NodeHttpHandler({ httpsAgent: { keepAlive: true, maxSockets: config.concurrency * 2 } }),
    });
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
    });
    await Promise.all(Array.from({ length: this.config.concurrency }, (_, i) => this.pollLoop(i)));
    this.kinesis.destroy();
  }

  private async pollLoop(loop: number): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      let records: QueueRecord[];
      try {
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

      // The orders in a batch are independent, so fulfill them in parallel.
      await Promise.all(records.map((record) => this.fulfill(record)));
      await this.acknowledge(records);
    }
  }

  /** Stand-in for reserving stock and booking a shipment with a warehouse API. */
  private async fulfill(record: QueueRecord): Promise<void> {
    const parsed = parseRecord(record.Data);
    if (parsed.kind !== 'order') {
      // Not an order this scenario handles; scenario 4 covers failing records.
      counts.skipped += 1;
      return;
    }
    const { processingMsMin: min, processingMsMax: max } = this.config;
    await sleep(max > min ? randomInt(min, max + 1) : min);
    counts.fulfilled += 1;
  }

  private async acknowledge(records: QueueRecord[]): Promise<void> {
    try {
      const { Failed } = await this.kinesis.send(new AcknowledgeQueueRecordsCommand({
        QueueARN: this.config.queueArn,
        Entries: records.map((r) => ({ ReceiptHandle: r.ReceiptHandle })),
      }));
      // HTTP 200 doesn't mean every entry succeeded; check the Failed list on every call.
      for (const { item: record, failure: f } of matchFailed(records, Failed)) {
        counts.ackFailed += 1;
        log('WARN', 'ACK_FAILED', { code: f.ErrorCode, message: f.ErrorMessage, receiveCount: record?.ReceiveCount });
      }
    } catch (error) {
      // The orders become visible again after the visibility timeout and are redelivered.
      counts.ackFailed += records.length;
      log('WARN', 'ACK_CALL_FAILED', { records: records.length, error: errorMessage(error) });
    }
  }
}

/** Logs the task's throughput regularly; scripts/status.ts sums it across tasks. */
function startStats(intervalMs: number): () => void {
  let last = { ...counts };
  let lastAt = Date.now();
  const report = () => {
    const now = Date.now();
    const seconds = (now - lastAt) / 1000;
    log('INFO', 'STATS', {
      fulfilledPerSecond: Number(((counts.fulfilled - last.fulfilled) / seconds).toFixed(2)),
      intervalSeconds: Math.round(seconds),
      total: { ...counts },
    });
    last = { ...counts };
    lastAt = now;
  };
  const timer = setInterval(report, intervalMs);
  return () => {
    clearInterval(timer);
    report();
  };
}

async function main(): Promise<void> {
  const config = readConfig();
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
