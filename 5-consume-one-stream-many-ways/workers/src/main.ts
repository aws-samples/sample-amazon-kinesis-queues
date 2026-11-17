// Queue worker for scenario 5. The same code runs as two consumers, each on its own
// queue on the same stream:
// - ROLE=fulfillment: books the shipment for each order.
// - ROLE=notifications: sends the customer an order confirmation.
//
// Both are queue jobs: each order is a unit of work on its own, and once it's done
// and acknowledged, that queue doesn't deliver it again. Each queue receives every
// record on the stream and tracks its own progress, so the two consumers, and the
// KCL fraud-signals consumer reading the stream directly, don't affect each other.
import { randomInt, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  AcknowledgeQueueRecordsCommand, KinesisClient, ReceiveQueueRecordsCommand, type QueueRecord,
} from '@aws-sdk/client-kinesis';
import { ConditionalCheckFailedException, DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { TASK_ID, errorMessage, log } from './log';
import { parseRecord, type OrderPlaced } from '../../../shared/src/orders';
import { matchFailed } from '../../../shared/src/batch';

// Service limits: up to 10 records per receive/acknowledge call, wait time 1-20 seconds.
const MAX_RECORDS = 10;
const WAIT_TIME_SECONDS = 20;

type Role = 'fulfillment' | 'notifications';

interface Config {
  role: Role;
  region: string;
  queueArn: string;
  table: string;
}

function readConfig(): Config {
  const env = (name: string) => {
    const value = process.env[name]?.trim();
    if (!value) throw new Error(`Environment variable ${name} is required.`);
    return value;
  };
  const role = env('ROLE');
  if (role !== 'fulfillment' && role !== 'notifications') throw new Error('ROLE must be fulfillment or notifications.');
  return { role, region: env('AWS_REGION'), queueArn: env('QUEUE_ARN'), table: env('TABLE_NAME') };
}

/** The work for one order, by role. Returns the event to log and the item to record. */
async function work(role: Role, order: OrderPlaced): Promise<{ event: string; fields: Record<string, string> }> {
  // Stand-ins for a warehouse API and an email service.
  await sleep(randomInt(100, 400));
  if (role === 'fulfillment') return { event: 'FULFILLED', fields: { shipmentId: `shp-${randomUUID().slice(0, 12)}` } };
  return { event: 'CONFIRMATION_SENT', fields: { channel: 'email', messageId: `msg-${randomUUID().slice(0, 12)}` } };
}

class Worker {
  private readonly kinesis: KinesisClient;
  private readonly dynamo: DynamoDBClient;
  private readonly abort = new AbortController();
  private stopping = false;

  constructor(private readonly config: Config) {
    // Keep connections open between receive and acknowledge calls.
    this.kinesis = new KinesisClient({ region: config.region, requestHandler: new NodeHttpHandler({ httpsAgent: { keepAlive: true } }) });
    this.dynamo = new DynamoDBClient({ region: config.region });
  }

  stop(): void {
    if (this.stopping) return;
    this.stopping = true;
    log('INFO', 'STOPPING', { role: this.config.role });
    this.abort.abort();
  }

  async run(): Promise<void> {
    log('INFO', 'STARTED', { role: this.config.role, queueArn: this.config.queueArn });
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
        log('WARN', 'RECEIVE_FAILED', { error: errorMessage(error), retryInMs: Math.round(backoffMs) });
        await sleep(backoffMs);
        continue;
      }
      if (records.length === 0) continue;
      const done = await Promise.all(records.map((r) => this.handle(r)));
      await this.acknowledge(records.filter((_, i) => done[i]));
    }
    this.kinesis.destroy();
    this.dynamo.destroy();
  }

  /** Returns true when the record can be acknowledged. */
  private async handle(record: QueueRecord): Promise<boolean> {
    const receiveCount = record.ReceiveCount ?? 1;
    const parsed = parseRecord(record.Data);
    if (parsed.kind === 'other') return true;
    if (parsed.kind === 'invalid') {
      // Left unacknowledged: dead-lettered after MaxReceiveCount receives (scenario 4).
      log('ERROR', 'INVALID_ORDER', { role: this.config.role, reason: parsed.reason, receiveCount });
      return false;
    }
    const { order } = parsed;
    const ageMs = record.ApproximateArrivalTimestamp ? Date.now() - record.ApproximateArrivalTimestamp.getTime() : undefined;
    try {
      const { event, fields } = await work(this.config.role, order);
      // Idempotent: one item per orderId, so a redelivery doesn't do the work twice
      // on the record (scenario 2 covers this in depth).
      try {
        await this.dynamo.send(new PutItemCommand({
          TableName: this.config.table,
          Item: {
            orderId: { S: order.orderId },
            customerId: { S: order.customerId },
            doneAt: { S: new Date().toISOString() },
            doneBy: { S: TASK_ID },
            receiveCount: { N: String(receiveCount) },
            ...Object.fromEntries(Object.entries(fields).map(([k, v]) => [k, { S: v }])),
          },
          ConditionExpression: 'attribute_not_exists(orderId)',
        }));
      } catch (error) {
        if (!(error instanceof ConditionalCheckFailedException)) throw error;
        log('INFO', 'DUPLICATE_SKIPPED', { role: this.config.role, orderId: order.orderId, receiveCount });
        return true;
      }
      log('INFO', event, { role: this.config.role, orderId: order.orderId, customerId: order.customerId, receiveCount, ageMs, ...fields });
      return true;
    } catch (error) {
      log('WARN', 'FAILED', { role: this.config.role, orderId: order.orderId, receiveCount, error: errorMessage(error) });
      return false;
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
        log('WARN', 'ACK_FAILED', { role: this.config.role, code: f.ErrorCode, message: f.ErrorMessage, receiveCount: record?.ReceiveCount });
      }
    } catch (error) {
      log('WARN', 'ACK_CALL_FAILED', { role: this.config.role, records: records.length, error: errorMessage(error) });
    }
  }
}

async function main(): Promise<void> {
  const worker = new Worker(readConfig());
  process.once('SIGTERM', () => worker.stop());
  process.once('SIGINT', () => worker.stop());
  await worker.run();
  log('INFO', 'STOPPED');
}

main().catch((error) => {
  log('ERROR', 'CRASHED', { error: errorMessage(error) });
  process.exit(1);
});
