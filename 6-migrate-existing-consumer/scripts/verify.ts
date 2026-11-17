// Checks the migration's promise: every order in the stream was fulfilled once,
// whichever consumer got to it, and none was skipped at the cutover.
//
// It reads every OrderPlaced event from all shards of the stream since --since,
// then looks each order up in the orders table. Orders for a product the warehouse
// no longer stocks (the producer's --poison option) can't be fulfilled by design;
// they're reported separately.
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import {
  BatchGetItemCommand, DescribeTableCommand, DynamoDBClient, type AttributeValue, type BatchGetItemCommandOutput,
} from '@aws-sdk/client-dynamodb';
import {
  GetRecordsCommand, GetShardIteratorCommand, ListShardsCommand, ShardIteratorType, type KinesisClient, type Shard,
} from '@aws-sdk/client-kinesis';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { createClient, errorMessage } from '../../shared/src/common';
import { parseRecord } from '../../shared/src/orders';
import { count, readOutputs, scriptArgs } from './outputs';

const USAGE = `Usage: npm run migrate:verify -- [options]

  --since ISO         Check orders that reached the stream at or after this time
                      (default: when the orders table was created)
  --show N            List up to N orders that weren't fulfilled (default: 20)
  --relay             Check the relay migration (cdk-outputs-relay.json) instead of the KCL one
  --stack-name NAME   Stack in the outputs file (default: the only one)
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain

Exits with status 1 if an order that can be fulfilled wasn't.`;

const POISON_SKU = 'SKU-DISCONTINUED';

interface StreamOrder {
  orderId: string;
  poison: boolean;
  arrivedAt?: Date;
}

async function listShards(kinesis: KinesisClient, streamArn: string): Promise<Shard[]> {
  const shards: Shard[] = [];
  let NextToken: string | undefined;
  do {
    // StreamARN and NextToken can't be combined; the token already names the stream.
    const page = await kinesis.send(new ListShardsCommand(NextToken ? { NextToken } : { StreamARN: streamArn }));
    shards.push(...(page.Shards ?? []));
    NextToken = page.NextToken;
  } while (NextToken);
  return shards;
}

/** Reads one shard from `since` to its current end. */
async function readShard(kinesis: KinesisClient, streamArn: string, shardId: string, since: Date,
  orders: Map<string, StreamOrder>, totals: { events: number; malformed: number }): Promise<void> {
  const { ShardIterator } = await kinesis.send(new GetShardIteratorCommand({
    StreamARN: streamArn, ShardId: shardId, ShardIteratorType: ShardIteratorType.AT_TIMESTAMP, Timestamp: since,
  }));
  let iterator = ShardIterator;
  while (iterator) {
    const out = await kinesis.send(new GetRecordsCommand({ StreamARN: streamArn, ShardIterator: iterator, Limit: 10_000 }));
    for (const record of out.Records ?? []) {
      const parsed = parseRecord(record.Data);
      if (parsed.kind === 'invalid') totals.malformed += 1;
      if (parsed.kind !== 'order') continue;
      totals.events += 1;
      // A producer retry writes the same order twice; it's still one order.
      if (!orders.has(parsed.order.orderId)) {
        orders.set(parsed.order.orderId, {
          orderId: parsed.order.orderId,
          poison: parsed.order.items.some((i) => i.sku === POISON_SKU),
          arrivedAt: record.ApproximateArrivalTimestamp,
        });
      }
    }
    if ((out.Records?.length ?? 0) === 0 && out.MillisBehindLatest === 0) break; // caught up with an open shard
    iterator = out.NextShardIterator; // undefined once a closed shard is read to its end
    // A shard allows 5 GetRecords calls per second, shared with the KCL consumers.
    await sleep(400);
  }
}

interface TableEntry {
  consumer: string;
  /** Deliveries that found the order already in the table: the warehouse wasn't called. */
  skipped: number;
  /** Deliveries that lost the race to another one: the warehouse was called again. */
  discarded: number;
}

async function lookUp(dynamo: DynamoDBClient, table: string, orderIds: string[]): Promise<Map<string, TableEntry>> {
  const found = new Map<string, TableEntry>();
  for (let i = 0; i < orderIds.length; i += 100) {
    let keys: Record<string, AttributeValue>[] | undefined = orderIds.slice(i, i + 100).map((id) => ({ orderId: { S: id } }));
    for (let attempt = 0; keys && keys.length > 0; attempt += 1) {
      const out: BatchGetItemCommandOutput = await dynamo.send(new BatchGetItemCommand({
        RequestItems: {
          [table]: {
            Keys: keys,
            ConsistentRead: true,
            ProjectionExpression: '#id, #consumer, #skipped, #discarded',
            ExpressionAttributeNames: {
              '#id': 'orderId', '#consumer': 'consumer', '#skipped': 'duplicatesSkipped', '#discarded': 'duplicatesDiscarded',
            },
          },
        },
      }));
      for (const item of out.Responses?.[table] ?? []) {
        found.set(item.orderId.S!, {
          consumer: item.consumer?.S ?? '?',
          skipped: Number(item.duplicatesSkipped?.N ?? 0),
          discarded: Number(item.duplicatesDiscarded?.N ?? 0),
        });
      }
      keys = out.UnprocessedKeys?.[table]?.Keys;
      if (keys && keys.length > 0) await sleep(Math.min(2000, 100 * 2 ** attempt));
    }
  }
  return found;
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: { ...scriptArgs, since: { type: 'string' }, show: { type: 'string', default: '20' } },
  });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const show = Number(args.show);
  if (!Number.isInteger(show) || show < 0) throw new Error('--show must be a whole number');
  const outputs = readOutputs(args['stack-name'], args.relay ? 'relay' : 'kcl');
  const credentials = fromNodeProviderChain(args.profile ? { profile: args.profile } : {});
  const dynamo = new DynamoDBClient({ region: args.region, credentials });
  const kinesis = createClient({ region: args.region, profile: args.profile });
  try {
    let since: Date;
    if (args.since) {
      since = new Date(args.since);
      if (Number.isNaN(since.getTime())) throw new Error('--since must be an ISO date, for example 2026-11-10T09:00:00Z');
    } else {
      const { Table } = await dynamo.send(new DescribeTableCommand({ TableName: outputs.OrdersTableName }));
      since = Table!.CreationDateTime!;
    }

    const shards = await listShards(kinesis, outputs.StreamArn);
    const orders = new Map<string, StreamOrder>();
    const totals = { events: 0, malformed: 0 };
    // Shards are independent; read them in parallel.
    await Promise.all(shards.map((s) => readShard(kinesis, outputs.StreamArn, s.ShardId!, since, orders, totals)));
    const table = await lookUp(dynamo, outputs.OrdersTableName, [...orders.keys()]);

    const byConsumer = new Map<string, number>();
    let skipped = 0;
    let discarded = 0;
    const missing: StreamOrder[] = [];
    let poisonMissing = 0;
    for (const order of orders.values()) {
      const entry = table.get(order.orderId);
      if (entry) {
        byConsumer.set(entry.consumer, (byConsumer.get(entry.consumer) ?? 0) + 1);
        skipped += entry.skipped;
        discarded += entry.discarded;
      } else if (order.poison) {
        poisonMissing += 1;
      } else {
        missing.push(order);
      }
    }
    const fulfilled = orders.size - missing.length - poisonMissing;
    const consumers = [...byConsumer.entries()].sort().map(([c, n]) => `${c} ${count(n)}`).join(', ') || 'none';

    console.log(`Stream ${outputs.StreamArn.split('/').pop()} since ${since.toISOString()}, ${shards.length} shards:`);
    console.log(`  ${count(totals.events)} order events, ${count(orders.size)} distinct orders, ${count(totals.malformed)} malformed records`);
    console.log(`Orders table ${outputs.OrdersTableName}:`);
    console.log(`  fulfilled once: ${count(fulfilled)} of ${count(orders.size)} (by ${consumers})`);
    console.log(`  duplicate deliveries absorbed: ${count(skipped + discarded)} (${count(skipped)} skipped before the `
      + `warehouse call, ${count(discarded)} discarded after it)`);
    if (poisonMissing > 0) {
      console.log(`  not fulfilled by design: ${count(poisonMissing)} orders for ${POISON_SKU}, which the warehouse no longer stocks`);
    }
    console.log(`  missing: ${count(missing.length)}`);
    for (const order of missing.slice(0, show)) {
      console.log(`    ${order.orderId}  arrived ${order.arrivedAt?.toISOString() ?? '?'}`);
    }
    if (missing.length > 0) {
      console.log('\nResult: orders are missing. If consumers still have a backlog, wait for it to drain and run this again.');
      process.exitCode = 1;
    } else {
      console.log('\nResult: OK. Every order that can be fulfilled is in the orders table once, and none was skipped.');
    }
    if (discarded > 0) {
      // The table kept one fulfillment per order, but the warehouse was called again for these.
      console.log(`Note: ${count(discarded)} deliveries called the warehouse for an order that another delivery shipped at the same`);
      console.log('moment. Keep the overlap short, and pass orderId to the warehouse as its idempotency key.');
    }
  } finally {
    dynamo.destroy();
    kinesis.destroy();
  }
}

main().catch((error) => {
  console.error(`migrate:verify failed: ${errorMessage(error)}`);
  process.exitCode = 1;
});
