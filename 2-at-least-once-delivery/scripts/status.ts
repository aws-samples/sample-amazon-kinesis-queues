// Shows what the orders table absorbed: how many orders were fulfilled, and how many
// duplicate deliveries were recognized and didn't fulfill an order twice.
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { DynamoDBClient, ScanCommand, type AttributeValue } from '@aws-sdk/client-dynamodb';
import { DescribeServicesCommand, ECSClient } from '@aws-sdk/client-ecs';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

const USAGE = `Usage: npm run delivery:status -- [options]

  --stack-name NAME   Stack in 2-at-least-once-delivery/cdk-outputs.json (default: the only one)
  --interval SECONDS  Refresh interval (default: 10)
  --once              Print once and exit
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain

Lists every order that was delivered more than once, or that the producer marked
with a simulation (--ack-lost, --slow, --slow-no-heartbeat).`;

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');

interface StackOutputs {
  ClusterName: string;
  ServiceName: string;
  OrdersTableName: string;
}

interface OrderItem {
  orderId: string;
  fulfilledAt: string;
  fulfilledBy: string;
  receiveCount: number;
  processingMs: number;
  simulate?: string;
  duplicatesSkipped: number;
  duplicatesDiscarded: number;
  lastDuplicateBy?: string;
  lastDuplicateReceiveCount?: number;
}

function readOutputs(stackName: string | undefined): StackOutputs {
  let all: Record<string, StackOutputs>;
  try {
    all = JSON.parse(readFileSync(OUTPUTS, 'utf8'));
  } catch {
    throw new Error(`Deploy the stack with --outputs-file cdk-outputs.json first (looked for ${OUTPUTS}).`);
  }
  const names = stackName ? [stackName] : Object.keys(all);
  if (names.length !== 1 || !all[names[0]]) throw new Error(`Pass --stack-name; ${OUTPUTS} has ${Object.keys(all).join(', ')}.`);
  return all[names[0]];
}

function toItem(raw: Record<string, AttributeValue>): OrderItem {
  const n = (k: string) => Number(raw[k]?.N ?? 0);
  return {
    orderId: raw.orderId.S!,
    fulfilledAt: raw.fulfilledAt?.S ?? '',
    fulfilledBy: raw.fulfilledBy?.S ?? '',
    receiveCount: n('receiveCount'),
    processingMs: n('processingMs'),
    simulate: raw.simulate?.S,
    duplicatesSkipped: n('duplicatesSkipped'),
    duplicatesDiscarded: n('duplicatesDiscarded'),
    lastDuplicateBy: raw.lastDuplicateBy?.S,
    lastDuplicateReceiveCount: raw.lastDuplicateReceiveCount ? n('lastDuplicateReceiveCount') : undefined,
  };
}

/** A full scan is fine at demo sizes; don't do this in production. */
async function scanOrders(dynamo: DynamoDBClient, table: string): Promise<OrderItem[]> {
  const items: OrderItem[] = [];
  let ExclusiveStartKey: Record<string, AttributeValue> | undefined;
  do {
    const page = await dynamo.send(new ScanCommand({ TableName: table, ExclusiveStartKey }));
    items.push(...(page.Items ?? []).map(toItem));
    ExclusiveStartKey = page.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return items;
}

function print(orders: OrderItem[], running: string): void {
  const skipped = orders.reduce((s, o) => s + o.duplicatesSkipped, 0);
  const discarded = orders.reduce((s, o) => s + o.duplicatesDiscarded, 0);
  console.log(`\n${new Date().toISOString().slice(11, 19)}  workers ${running}  orders fulfilled ${orders.length}  `
    + `duplicate deliveries absorbed ${skipped + discarded} (${skipped} skipped before the warehouse call, `
    + `${discarded} discarded after it)`);
  const notable = orders
    .filter((o) => o.simulate || o.duplicatesSkipped || o.duplicatesDiscarded)
    .sort((a, b) => a.fulfilledAt.localeCompare(b.fulfilledAt))
    .slice(-20);
  if (notable.length === 0) return;
  console.log('  orderId           simulate            fulfilled by  on receive  took(s)  dup skipped  dup discarded');
  for (const o of notable) {
    console.log([
      `  ${o.orderId.padEnd(16)}`,
      (o.simulate ?? '-').padEnd(18),
      o.fulfilledBy.padStart(12),
      String(o.receiveCount).padStart(11),
      (o.processingMs / 1000).toFixed(1).padStart(8),
      String(o.duplicatesSkipped).padStart(12),
      String(o.duplicatesDiscarded).padStart(14),
    ].join(' '));
  }
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: {
      'stack-name': { type: 'string' },
      interval: { type: 'string', default: '10' },
      once: { type: 'boolean', default: false },
      region: { type: 'string', default: process.env.AWS_REGION ?? 'us-east-2' },
      profile: { type: 'string', default: process.env.AWS_PROFILE },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const intervalSeconds = Number(args.interval);
  if (!Number.isFinite(intervalSeconds) || intervalSeconds < 2) throw new Error('--interval must be at least 2 seconds');
  const outputs = readOutputs(args['stack-name']);
  const clientConfig = { region: args.region, credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}) };
  const dynamo = new DynamoDBClient(clientConfig);
  const ecs = new ECSClient(clientConfig);

  let stop = false;
  process.once('SIGINT', () => { stop = true; });
  try {
    while (!stop) {
      const [orders, svc] = await Promise.all([
        scanOrders(dynamo, outputs.OrdersTableName),
        ecs.send(new DescribeServicesCommand({ cluster: outputs.ClusterName, services: [outputs.ServiceName] })),
      ]);
      const s = svc.services?.[0];
      print(orders, `${s?.runningCount ?? '?'}/${s?.desiredCount ?? '?'}`);
      if (args.once) break;
      await sleep(intervalSeconds * 1000);
    }
  } finally {
    dynamo.destroy();
    ecs.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
  process.exitCode = 1;
});
