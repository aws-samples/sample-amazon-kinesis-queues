// A minimal queue consumer: receive, process, acknowledge.
// No KCL, lease table or shard logic. Run several copies to see them share the queue.
//
// The job: send an order confirmation for each OrderPlaced event. Each order is a
// unit of work on its own. Once its confirmation is sent and the record is
// acknowledged, the order is done for this queue and no consumer of the queue sees
// it again. The record stays in the stream, so stream consumers and other queues
// still read it.
import { randomUUID } from 'node:crypto';
import {
  AcknowledgeQueueRecordsCommand,
  KinesisClient,
  ReceiveQueueRecordsCommand,
  type QueueRecord,
} from '@aws-sdk/client-kinesis';
import { parseArgs } from 'node:util';
import { matchFailed } from '../../shared/src/batch';
import { DEFAULT_QUEUE_NAME, commonArgs, createClient, errorMessage, handleHelp, resolveQueueArn } from '../../shared/src/common';
import { parseRecord } from '../../shared/src/orders';

const USAGE = `Usage: npm run queue:consume -- [options]

  --queue-name NAME  Queue to read, looked up in shared/queue-outputs.json (default: ${DEFAULT_QUEUE_NAME})
  --queue-arn ARN    Queue to read, instead of --queue-name
                     With neither, the QUEUE_ARN environment variable is used if set
  --count N          Stop after acknowledging N records (default: run until Ctrl+C)
  --region REGION    Default: us-east-2
  --profile PROFILE  AWS profile; otherwise the standard credential chain`;

// Service limits: up to 10 records per call, wait time 1-20 seconds.
const MAX_RECORDS = 10;
const WAIT_TIME_SECONDS = 20;

/**
 * The work for one record. Returns normally when the record is done and can be
 * acknowledged; throws to leave it unacknowledged, so it's delivered again.
 */
function processRecord(record: QueueRecord, consumerId: string): void {
  const receiveCount = record.ReceiveCount ?? '?';
  const parsed = parseRecord(record.Data);
  if (parsed.kind === 'other') {
    // Every record on the stream reaches every queue. Events this consumer doesn't
    // handle are acknowledged, so they don't come back.
    console.log(`SKIPPED consumer=${consumerId} eventType=${parsed.eventType}`);
    return;
  }
  if (parsed.kind === 'invalid') throw new Error(`malformed order event: ${parsed.reason}`);

  const { order } = parsed;
  const latencyMs = record.ApproximateArrivalTimestamp ? Date.now() - record.ApproximateArrivalTimestamp.getTime() : undefined;
  // Stand-in for calling an email or notification service. A real consumer would
  // pass orderId as the idempotency key, because a record can be delivered more
  // than once (scenario 2).
  console.log(
    `CONFIRMATION_SENT consumer=${consumerId} orderId=${order.orderId} customerId=${order.customerId}`
    + ` total=${order.total.toFixed(2)} ${order.currency} receiveCount=${receiveCount} latencyMs=${latencyMs ?? '?'}`,
  );
}

async function acknowledge(client: KinesisClient, queueArn: string, records: QueueRecord[]): Promise<number> {
  if (records.length === 0) return 0;
  const entries = records.map((r) => ({ ReceiptHandle: r.ReceiptHandle }));
  const { Successful = [], Failed } = await client.send(new AcknowledgeQueueRecordsCommand({ QueueARN: queueArn, Entries: entries }));
  // The call returns HTTP 200 even when individual entries fail, so always check
  // Failed. Each failed entry is matched to its record by ReceiptHandle. A record
  // that isn't acknowledged is delivered again after its visibility timeout.
  for (const { item: record, failure: f } of matchFailed(records, Failed)) {
    console.warn(`ACK_FAILED partitionKey=${record?.PartitionKey ?? '?'} code=${f.ErrorCode} message=${f.ErrorMessage ?? ''}`);
  }
  return Successful.length;
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: {
      ...commonArgs,
      'queue-arn': { type: 'string' },
      'queue-name': { type: 'string' },
      count: { type: 'string' },
    },
  });
  handleHelp(args.help, USAGE);
  // The CDK stack's Fargate task passes the queue through QUEUE_ARN.
  const fromEnv = args['queue-name'] === undefined ? process.env.QUEUE_ARN : undefined;
  const queueArn = resolveQueueArn(args['queue-arn'] ?? fromEnv, args['queue-name'] ?? DEFAULT_QUEUE_NAME);
  const limit = args.count === undefined ? Infinity : Number(args.count);
  if (!(limit > 0) || (limit !== Infinity && !Number.isInteger(limit))) throw new Error('--count must be a positive whole number');

  const consumerId = randomUUID().slice(0, 8);
  const client = createClient({ region: args.region, profile: args.profile });
  const controller = new AbortController();
  let stopping = false;
  const stop = () => {
    if (stopping) return;
    stopping = true;
    console.log('Stopping after the current batch...');
    controller.abort();
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);

  console.log(`Consumer ${consumerId} polling ${queueArn} (visibility timeout: queue default)`);
  let acknowledged = 0;
  try {
    while (!stopping && acknowledged < limit) {
      let records: QueueRecord[];
      try {
        // VisibilityTimeoutSeconds omitted: the queue's configured timeout applies.
        const out = await client.send(
          new ReceiveQueueRecordsCommand({ QueueARN: queueArn, Limit: MAX_RECORDS, WaitTimeSeconds: WAIT_TIME_SECONDS }),
          { abortSignal: controller.signal },
        );
        records = out.QueueRecords ?? [];
      } catch (error) {
        if (stopping) break; // the abort canceled the long poll
        throw error;
      }
      // A receive can return fewer records than requested, including none.
      if (records.length === 0) continue;

      const done: QueueRecord[] = [];
      for (const record of records) {
        try {
          processRecord(record, consumerId);
          done.push(record);
        } catch (error) {
          // Not acknowledged: it becomes visible again when its visibility timeout expires.
          console.warn(`PROCESS_FAILED receiveCount=${record.ReceiveCount ?? '?'}: ${errorMessage(error)}`);
        }
      }
      acknowledged += await acknowledge(client, queueArn, done);
    }
    console.log(`Consumer ${consumerId} acknowledged ${acknowledged} record(s).`);
  } catch (error) {
    console.error(`consume failed: ${errorMessage(error)}`);
    process.exitCode = 1;
  } finally {
    client.destroy();
  }
}

main().catch((error) => {
  console.error(errorMessage(error));
  process.exitCode = 1;
});
