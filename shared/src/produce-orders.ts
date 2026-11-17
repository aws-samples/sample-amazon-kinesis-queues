// Writes OrderPlaced events to the base stream, the way an e-commerce checkout would.
// The partition key is customerId, so each customer's orders stay in order for the
// stream consumers (fraud, analytics) that need it. The queue doesn't care.
import { randomInt, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { KinesisClient, PutRecordsCommand, type PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import type { OrderPlaced, Simulation } from './orders';

/** Listed as discontinued in scenario 4's product catalog when the stack is deployed. */
const POISON_SKU = 'SKU-DISCONTINUED';

const USAGE = `Usage: npm run orders:produce -- [options]

  --count N         Orders to send (default: 100)
  --rate R          Orders per second; 0 sends them as fast as possible to build a backlog (default: 0)
  --customers N     Size of the customer pool, i.e. distinct partition keys (default: 50)
  --duplicates P    Share of orders sent twice, like a producer retry after a lost response, 0-1 (default: 0)
  --invalid N       Malformed events to mix in (default: 0)
  --poison N        Valid orders for a discontinued product (${POISON_SKU}) to mix in. The
                    warehouse rejects them every time, so they become dead-letter records (default: 0)
  --transient N     Orders whose first warehouse call times out. The queue redelivers them
                    and they're fulfilled on the second receive (default: 0)
  --ack-lost N      Scenario 2: orders the worker fulfills but fails to acknowledge on the
                    first receive, so the queue delivers them again (default: 0)
  --slow N          Scenario 2: orders that take longer than the visibility timeout to
                    fulfill; the worker extends visibility while it works (default: 0)
  --slow-no-heartbeat N
                    Scenario 2: the same slow orders, but the worker doesn't extend
                    visibility, so another worker receives them too (default: 0)
  --stream-arn ARN  Default: StreamArn from 0-quickstart/cdk-outputs.json
  --region REGION   Default: us-east-2
  --profile NAME    AWS profile; otherwise the standard credential chain`;

const BASE_OUTPUTS = path.resolve(__dirname, '..', '..', '0-quickstart', 'cdk-outputs.json');
const SKUS = [
  { sku: 'SKU-TSHIRT-M', price: 19.99 }, { sku: 'SKU-HOODIE-L', price: 49.0 }, { sku: 'SKU-SNEAKER-42', price: 89.5 },
  { sku: 'SKU-CAP', price: 14.95 }, { sku: 'SKU-BACKPACK', price: 64.0 }, { sku: 'SKU-SOCKS-3P', price: 9.99 },
];
const BATCH = 100; // PutRecords accepts up to 500; smaller batches keep the progress output readable.

function streamArnFromOutputs(): string {
  const outputs = JSON.parse(readFileSync(BASE_OUTPUTS, 'utf8')) as Record<string, { StreamArn?: string }>;
  const arns = Object.values(outputs).map((o) => o.StreamArn).filter(Boolean) as string[];
  if (arns.length !== 1) throw new Error(`Pass --stream-arn; found ${arns.length} StreamArn values in ${BASE_OUTPUTS}.`);
  return arns[0];
}

function makeOrder(customers: number): OrderPlaced {
  const items = Array.from({ length: randomInt(1, 4) }, () => {
    const product = SKUS[randomInt(SKUS.length)];
    return { sku: product.sku, qty: randomInt(1, 4), price: product.price };
  });
  return {
    eventType: 'OrderPlaced',
    orderId: `o-${randomUUID().slice(0, 13)}`,
    customerId: `c-${String(randomInt(1, customers + 1)).padStart(4, '0')}`,
    items,
    total: Math.round(items.reduce((sum, i) => sum + i.qty * i.price, 0) * 100) / 100,
    currency: 'EUR',
    createdAt: new Date().toISOString(),
  };
}

function toEntry(order: OrderPlaced): PutRecordsRequestEntry {
  return { PartitionKey: order.customerId, Data: Buffer.from(JSON.stringify(order)) };
}

async function put(client: KinesisClient, streamArn: string, entries: PutRecordsRequestEntry[]): Promise<void> {
  let pending = entries;
  for (let attempt = 1; pending.length > 0; attempt += 1) {
    const out = await client.send(new PutRecordsCommand({ StreamARN: streamArn, Records: pending }));
    if (!out.FailedRecordCount) return;
    // PutRecords is partially successful: retry only the entries that failed.
    pending = pending.filter((_, i) => out.Records?.[i]?.ErrorCode);
    if (attempt === 5) throw new Error(`${pending.length} records still failing after 5 attempts (${out.Records?.find((r) => r.ErrorCode)?.ErrorCode})`);
    await sleep(200 * 2 ** attempt);
  }
}

function fraction(text: string, name: string): number {
  const value = Number(text);
  if (!Number.isFinite(value) || value < 0 || value > 1) throw new Error(`--${name} must be between 0 and 1`);
  return value;
}

function wholeNumber(text: string, name: string, min: number): number {
  const value = Number(text);
  if (!Number.isInteger(value) || value < min) throw new Error(`--${name} must be a whole number of at least ${min}`);
  return value;
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: {
      count: { type: 'string', default: '100' },
      rate: { type: 'string', default: '0' },
      customers: { type: 'string', default: '50' },
      duplicates: { type: 'string', default: '0' },
      invalid: { type: 'string', default: '0' },
      poison: { type: 'string', default: '0' },
      transient: { type: 'string', default: '0' },
      'ack-lost': { type: 'string', default: '0' },
      slow: { type: 'string', default: '0' },
      'slow-no-heartbeat': { type: 'string', default: '0' },
      'stream-arn': { type: 'string' },
      region: { type: 'string', default: process.env.AWS_REGION ?? 'us-east-2' },
      profile: { type: 'string', default: process.env.AWS_PROFILE },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const count = wholeNumber(args.count, 'count', 1);
  const rate = Number(args.rate);
  if (!Number.isFinite(rate) || rate < 0) throw new Error('--rate must be 0 or more');
  const customers = wholeNumber(args.customers, 'customers', 1);
  const duplicateShare = fraction(args.duplicates, 'duplicates');
  const invalid = wholeNumber(args.invalid, 'invalid', 0);
  const poison = wholeNumber(args.poison, 'poison', 0);
  const simulated: Array<[Simulation, number]> = [
    ['transient-once', wholeNumber(args.transient, 'transient', 0)],
    ['ack-lost', wholeNumber(args['ack-lost'], 'ack-lost', 0)],
    ['slow', wholeNumber(args.slow, 'slow', 0)],
    ['slow-no-heartbeat', wholeNumber(args['slow-no-heartbeat'], 'slow-no-heartbeat', 0)],
  ];
  const streamArn = args['stream-arn'] ?? streamArnFromOutputs();

  const client = new KinesisClient({
    region: args.region,
    credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}),
  });

  // Build the whole sequence first so --rate only controls timing.
  const entries: PutRecordsRequestEntry[] = [];
  let duplicates = 0;
  for (let i = 0; i < count; i += 1) {
    const order = makeOrder(customers);
    entries.push(toEntry(order));
    if (Math.random() < duplicateShare) {
      entries.push(toEntry(order)); // same orderId and payload, a second stream record
      duplicates += 1;
    }
  }
  for (let i = 0; i < invalid; i += 1) {
    const at = randomInt(entries.length + 1);
    entries.splice(at, 0, { PartitionKey: 'c-invalid', Data: Buffer.from('{"eventType":"OrderPlaced","orderId":') });
  }
  const poisonIds: string[] = [];
  for (let i = 0; i < poison; i += 1) {
    const order = makeOrder(customers);
    order.items[0] = { sku: POISON_SKU, qty: 1, price: 29.0 };
    order.total = Math.round(order.items.reduce((sum, it) => sum + it.qty * it.price, 0) * 100) / 100;
    poisonIds.push(order.orderId);
    entries.splice(randomInt(entries.length + 1), 0, toEntry(order));
  }

  const simulatedIds = new Map<Simulation, string[]>();
  for (const [simulate, n] of simulated) {
    for (let i = 0; i < n; i += 1) {
      const order: OrderPlaced = { ...makeOrder(customers), simulate };
      simulatedIds.set(simulate, [...(simulatedIds.get(simulate) ?? []), order.orderId]);
      entries.splice(randomInt(entries.length + 1), 0, toEntry(order));
    }
  }

  const extras = [`+${duplicates} duplicate records`, `+${invalid} malformed`, `+${poison} poison`,
    ...simulated.filter(([, n]) => n > 0).map(([s, n]) => `+${n} ${s}`)];
  console.log(`Sending ${count} orders (${extras.join(', ')}) to ${streamArn}`);
  if (poisonIds.length > 0) console.log(`  poison orderIds: ${poisonIds.join(' ')}`);
  for (const [simulate, ids] of simulatedIds) console.log(`  ${simulate} orderIds: ${ids.join(' ')}`);
  const started = Date.now();
  let sent = 0;
  try {
    if (rate === 0) {
      for (let i = 0; i < entries.length; i += BATCH) {
        await put(client, streamArn, entries.slice(i, i + BATCH));
        sent += Math.min(BATCH, entries.length - i);
        process.stdout.write(`\r  ${sent}/${entries.length} records`);
      }
    } else {
      // Every tick, send the records that are due by the clock, so the rate holds even
      // when a PutRecords call takes longer than a tick.
      const tickMs = 250;
      while (sent < entries.length) {
        const tickStart = Date.now();
        const due = Math.min(entries.length, sent + BATCH, Math.floor(((tickStart - started) / 1000) * rate) + 1);
        if (due > sent) {
          await put(client, streamArn, entries.slice(sent, due));
          sent = due;
          process.stdout.write(`\r  ${sent}/${entries.length} records`);
        }
        await sleep(Math.max(0, tickMs - (Date.now() - tickStart)));
      }
    }
    const seconds = (Date.now() - started) / 1000;
    console.log(`\nDone in ${seconds.toFixed(1)} s (${(sent / seconds).toFixed(1)} records/s).`);
  } finally {
    client.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
  process.exitCode = 1;
});
