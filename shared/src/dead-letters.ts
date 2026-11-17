// Operator tool for a queue's dead-letter records: records that were received
// MaxReceiveCount times without an acknowledgment. Regular ReceiveQueueRecords
// calls never return them, so workers don't see them.
//
//   list     show dead-letter records (default)
//   restore  make them visible in the queue again with a fresh receive count, so
//            the existing workers process them; fix the cause first
//   discard  acknowledge them, which removes them from this queue only; the records
//            stay in the stream, for its other consumers, until its retention ends
import { parseArgs } from 'node:util';
import {
  AcknowledgeDeadLetterQueueRecordsCommand, ReceiveDeadLetterQueueRecordsCommand, RestoreDeadLetterQueueRecordsCommand,
  type KinesisClient, type QueueRecord,
} from '@aws-sdk/client-kinesis';
import { DEFAULT_QUEUE_NAME, commonArgs, createClient, errorMessage, handleHelp, resolveQueueArn } from './common';
import { matchFailed } from './batch';

const USAGE = `Usage: npm run queue:dead-letters -- [list|restore|discard] [selection] [options]

Selection (restore and discard need one; list shows everything by default):
  --order-id ID      Dead-letter records whose payload has this orderId (repeatable)
  --sku SKU          Dead-letter records for orders containing this SKU (repeatable)
  --malformed        Dead-letter records whose payload isn't valid JSON
  --all              Every dead-letter record

Options:
  --dry-run          Show what restore or discard would do, change nothing
  --max N            Look at no more than N dead-letter records (default: 100)
  --queue-name NAME  Queue, looked up in shared/queue-outputs.json (default: ${DEFAULT_QUEUE_NAME})
  --queue-arn ARN    Queue, instead of --queue-name
  --region REGION    Default: us-east-2
  --profile PROFILE  AWS profile; otherwise the standard credential chain

Reading dead-letter records hides them from other dead-letter reads for a short time,
like a normal receive. Records that don't match the selection stay dead-letter records.`;

/**
 * How long records read by this tool stay hidden from other dead-letter reads.
 * Must be above 0: with 0 the service hands the same dead-letter record back several
 * times in one call (and every read raises its receive count). Restore and
 * discard need the receipt handles to stay valid until their call completes.
 */
const HOLD_SECONDS = { list: 10, act: 30 };
const BATCH = 10;

type Command = 'list' | 'restore' | 'discard';

interface DeadLetter {
  record: QueueRecord;
  orderId?: string;
  skus: string[];
  malformed: boolean;
  preview: string;
}

function inspect(record: QueueRecord): DeadLetter {
  const text = record.Data ? Buffer.from(record.Data).toString('utf8') : '';
  try {
    const event = JSON.parse(text) as { orderId?: string; items?: { sku?: string }[] };
    const skus = (event.items ?? []).map((i) => i.sku).filter((s): s is string => typeof s === 'string');
    return { record, orderId: event.orderId, skus, malformed: false, preview: `orderId=${event.orderId ?? '?'} skus=${skus.join(',')}` };
  } catch {
    return { record, skus: [], malformed: true, preview: `malformed: ${JSON.stringify(text.slice(0, 50))}` };
  }
}

function line(d: DeadLetter): string {
  const r = d.record;
  const arrived = r.ApproximateArrivalTimestamp ? r.ApproximateArrivalTimestamp.toISOString().slice(0, 19) : '?';
  return `receiveCount=${r.ReceiveCount ?? '?'} arrived=${arrived} partitionKey=${r.PartitionKey ?? ''} recordId=${r.RecordId ?? '?'} ${d.preview}`;
}

async function applyBatch(client: KinesisClient, command: 'restore' | 'discard', queueArn: string, batch: DeadLetter[]) {
  const items = batch.map((d) => ({ deadLetter: d, ReceiptHandle: d.record.ReceiptHandle }));
  const input = { QueueARN: queueArn, Entries: items.map(({ ReceiptHandle }) => ({ ReceiptHandle })) };
  const result = command === 'restore'
    ? await client.send(new RestoreDeadLetterQueueRecordsCommand(input))
    : await client.send(new AcknowledgeDeadLetterQueueRecordsCommand(input));
  // Like the other batch calls: HTTP 200 can still carry per-entry failures.
  const failed = new Map<DeadLetter, string>();
  for (const { item, failure: f } of matchFailed(items, result.Failed)) {
    if (item) failed.set(item.deadLetter, `${f.ErrorCode}: ${f.ErrorMessage ?? ''}`);
  }
  return batch.map((d) => ({ deadLetter: d, error: failed.get(d) }));
}

async function main(): Promise<void> {
  const { values: args, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      ...commonArgs,
      'queue-arn': { type: 'string' },
      'queue-name': { type: 'string', default: DEFAULT_QUEUE_NAME },
      'order-id': { type: 'string', multiple: true, default: [] },
      sku: { type: 'string', multiple: true, default: [] },
      malformed: { type: 'boolean', default: false },
      all: { type: 'boolean', default: false },
      'dry-run': { type: 'boolean', default: false },
      max: { type: 'string', default: '100' },
    },
  });
  handleHelp(args.help, USAGE);
  const command = (positionals[0] ?? 'list') as Command;
  if (!['list', 'restore', 'discard'].includes(command) || positionals.length > 1) {
    throw new Error(`Unknown command "${positionals.join(' ')}". Use list, restore or discard.`);
  }
  const max = Number(args.max);
  if (!Number.isInteger(max) || max < 1) throw new Error('--max must be a positive whole number');

  const orderIds = new Set(args['order-id']);
  const skus = new Set(args.sku);
  const hasSelection = args.all || args.malformed || orderIds.size > 0 || skus.size > 0;
  if (command !== 'list' && !hasSelection) {
    throw new Error(`${command} needs a selection: --order-id, --sku, --malformed or --all.`);
  }
  const matches = (d: DeadLetter) => !hasSelection || args.all
    || (args.malformed && d.malformed)
    || (d.orderId !== undefined && orderIds.has(d.orderId))
    || d.skus.some((s) => skus.has(s));

  const queueArn = resolveQueueArn(args['queue-arn'], args['queue-name']);
  const client = createClient({ region: args.region, profile: args.profile });
  const acting = command !== 'list' && !args['dry-run'];
  const hold = acting ? HOLD_SECONDS.act : HOLD_SECONDS.list;
  // Records held by an earlier run of this tool are skipped until their hold ends.

  const seen = new Set<string>();
  let examined = 0;
  let selected = 0;
  let done = 0;
  let failed = 0;
  const verb = command === 'restore' ? 'RESTORED' : 'DISCARDED';

  while (examined < max) {
    const { QueueRecords = [] } = await client.send(new ReceiveDeadLetterQueueRecordsCommand({
      QueueARN: queueArn,
      Limit: Math.min(BATCH, max - examined),
      WaitTimeSeconds: 1,
      VisibilityTimeoutSeconds: hold,
    }));
    // Keep one copy per record, even if a batch repeats one; stop once nothing is new.
    const fresh: QueueRecord[] = [];
    for (const r of QueueRecords) {
      const key = r.RecordId ?? r.ReceiptHandle ?? '';
      if (seen.has(key)) continue;
      seen.add(key);
      fresh.push(r);
    }
    if (fresh.length === 0) break;
    examined += fresh.length;

    const batch = fresh.map(inspect).filter(matches);
    selected += batch.length;
    if (!acting) {
      for (const d of batch) console.log(`${command === 'list' ? '' : `WOULD ${command.toUpperCase()} `}${line(d)}`);
      continue;
    }
    if (batch.length === 0) continue;
    for (const { deadLetter, error } of await applyBatch(client, command as 'restore' | 'discard', queueArn, batch)) {
      if (error) {
        failed += 1;
        console.log(`FAILED ${line(deadLetter)} (${error})`);
      } else {
        done += 1;
        console.log(`${verb} ${line(deadLetter)}`);
      }
    }
  }

  const scope = examined >= max ? ` (stopped at --max ${max})` : '';
  if (command === 'list') console.log(`${selected} dead-letter record(s) on ${queueArn}${scope}`);
  else if (!acting) console.log(`Dry run: ${selected} of ${examined} dead-letter record(s) would be ${command === 'restore' ? 'restored' : 'discarded'}${scope}.`);
  else {
    console.log(`${done} ${verb.toLowerCase()}, ${failed} failed, ${examined - selected} left as dead-letter records${scope}.`);
    if (command === 'restore' && done > 0) console.log('Restored records are visible in the queue again; the workers pick them up with a fresh receive count.');
    if (failed > 0) process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(`dead-letters failed: ${errorMessage(error)}`);
  process.exitCode = 1;
});
