// Prints the fulfillment picture every few seconds: queue backlog, running tasks and
// how many orders are fulfilled, and how many dead-letter records the queue holds.
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { CloudWatchClient, GetMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { DescribeServicesCommand, ECSClient } from '@aws-sdk/client-ecs';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

const USAGE = `Usage: npm run orders:status -- [options]

  --stack-name NAME   Fulfillment stack in 4-failures-and-dead-letters/cdk-outputs.json (default: the only one)
  --interval SECONDS  Refresh interval (default: 10)
  --once              Print once and exit
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain`;

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');

interface StackOutputs {
  QueueArn: string;
  ClusterName: string;
  ServiceName: string;
  OrdersTableName: string;
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

/** Counts fulfilled orders. A full scan is fine at demo sizes; don't do this in production. */
async function countFulfilled(dynamo: DynamoDBClient, table: string): Promise<number> {
  let total = 0;
  let ExclusiveStartKey: Record<string, never> | undefined;
  do {
    const page = await dynamo.send(new ScanCommand({ TableName: table, Select: 'COUNT', ExclusiveStartKey }));
    total += page.Count ?? 0;
    ExclusiveStartKey = page.LastEvaluatedKey as typeof ExclusiveStartKey;
  } while (ExclusiveStartKey);
  return total;
}

async function latestQueueMetrics(cw: CloudWatchClient, queueArn: string) {
  const queueId = queueArn.split('/').pop()!;
  const dims = [{ Name: 'QueueId', Value: queueId }, { Name: 'QueueName', Value: 'order-fulfillment' }];
  const metric = (id: string, name: string) => ({
    Id: id,
    MetricStat: { Metric: { Namespace: 'AWS/Kinesis', MetricName: name, Dimensions: dims }, Period: 60, Stat: 'Maximum' },
  });
  const end = new Date();
  const out = await cw.send(new GetMetricDataCommand({
    StartTime: new Date(end.getTime() - 10 * 60_000),
    EndTime: end,
    ScanBy: 'TimestampDescending',
    MetricDataQueries: [
      metric('visible', 'VisibleQueueRecords'),
      metric('inflight', 'InFlightQueueRecords'),
      metric('dead', 'VisibleDeadLetterQueueRecords'),
    ],
  }));
  const latest = (id: string) => {
    const r = out.MetricDataResults?.find((m) => m.Id === id);
    return r?.Values?.length ? { value: r.Values[0], at: r.Timestamps?.[0] } : undefined;
  };
  return { visible: latest('visible'), inflight: latest('inflight'), dead: latest('dead') };
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
  const cw = new CloudWatchClient(clientConfig);
  const dynamo = new DynamoDBClient(clientConfig);
  const ecs = new ECSClient(clientConfig);

  let previous: { fulfilled: number; at: number } | undefined;
  let stop = false;
  process.once('SIGINT', () => { stop = true; });
  console.log('time      tasks(run/want)  fulfilled  rate/s  queue visible  in flight  dead-letter records   (queue metrics lag about 1-2 min)');
  try {
    while (!stop) {
      const [fulfilled, queue, svc] = await Promise.all([
        countFulfilled(dynamo, outputs.OrdersTableName),
        latestQueueMetrics(cw, outputs.QueueArn),
        ecs.send(new DescribeServicesCommand({ cluster: outputs.ClusterName, services: [outputs.ServiceName] })),
      ]);
      const now = Date.now();
      const rate = previous ? (fulfilled - previous.fulfilled) / ((now - previous.at) / 1000) : undefined;
      previous = { fulfilled, at: now };
      const s = svc.services?.[0];
      const fmt = (m?: { value: number }) => (m ? String(Math.round(m.value)) : '-');
      console.log([
        new Date(now).toISOString().slice(11, 19),
        `${s?.runningCount ?? '?'}/${s?.desiredCount ?? '?'}`.padStart(15),
        String(fulfilled).padStart(10),
        (rate === undefined ? '-' : rate.toFixed(1)).padStart(7),
        fmt(queue.visible).padStart(14),
        fmt(queue.inflight).padStart(10),
        fmt(queue.dead).padStart(13),
      ].join(' '));
      if (args.once) break;
      await sleep(intervalSeconds * 1000);
    }
  } finally {
    cw.destroy();
    dynamo.destroy();
    ecs.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
  process.exitCode = 1;
});
