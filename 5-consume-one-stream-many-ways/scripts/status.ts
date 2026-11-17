// One line per consumer of the stream: whether it's running, how much it has done,
// and, for the queues, how much is waiting.
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { CloudWatchClient, GetMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { CloudWatchLogsClient, FilterLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { DescribeServicesCommand, ECSClient } from '@aws-sdk/client-ecs';
import { FULFILLMENT_QUEUE, NOTIFICATIONS_QUEUE } from '../lib/config';
import { clientConfig, commonOptions, readOutputs } from './outputs';

const USAGE = `Usage: npm run fanout:status -- [options]

  --interval SECONDS  Refresh interval (default: 15)
  --once              Print once and exit
  --stack-name NAME   Stack in 5-consume-one-stream-many-ways/cdk-outputs.json (default: the only one)
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain`;

/** A full scan is fine at demo sizes; don't do this in production. */
async function count(dynamo: DynamoDBClient, table: string): Promise<number> {
  let total = 0;
  let ExclusiveStartKey: Record<string, never> | undefined;
  do {
    const page = await dynamo.send(new ScanCommand({ TableName: table, Select: 'COUNT', ExclusiveStartKey }));
    total += page.Count ?? 0;
    ExclusiveStartKey = page.LastEvaluatedKey as typeof ExclusiveStartKey;
  } while (ExclusiveStartKey);
  return total;
}

async function backlog(cw: CloudWatchClient, queueArn: string, queueName: string): Promise<number | undefined> {
  const end = new Date();
  const out = await cw.send(new GetMetricDataCommand({
    StartTime: new Date(end.getTime() - 10 * 60_000),
    EndTime: end,
    ScanBy: 'TimestampDescending',
    MetricDataQueries: [{
      Id: 'visible',
      MetricStat: {
        Metric: {
          Namespace: 'AWS/Kinesis',
          MetricName: 'VisibleQueueRecords',
          Dimensions: [{ Name: 'QueueId', Value: queueArn.split('/').pop()! }, { Name: 'QueueName', Value: queueName }],
        },
        Period: 60,
        Stat: 'Maximum',
      },
    }],
  }));
  return out.MetricDataResults?.[0]?.Values?.[0];
}

async function logCount(logs: CloudWatchLogsClient, group: string, pattern: string, prefix: string): Promise<number> {
  let total = 0;
  let nextToken: string | undefined;
  do {
    const page = await logs.send(new FilterLogEventsCommand({
      logGroupName: group, logStreamNamePrefix: prefix, filterPattern: pattern, startTime: Date.now() - 60 * 60_000, nextToken,
    }));
    total += page.events?.length ?? 0;
    nextToken = page.nextToken;
  } while (nextToken);
  return total;
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: { ...commonOptions, interval: { type: 'string', default: '15' }, once: { type: 'boolean', default: false } },
  });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const interval = Number(args.interval);
  if (!Number.isFinite(interval) || interval < 5) throw new Error('--interval must be at least 5 seconds');
  const config = clientConfig(args.region, args.profile);
  const cw = new CloudWatchClient(config);
  const logs = new CloudWatchLogsClient(config);
  const dynamo = new DynamoDBClient(config);
  const ecs = new ECSClient(config);

  let stop = false;
  process.once('SIGINT', () => { stop = true; });
  try {
    while (!stop) {
      // Read the outputs on every refresh: step 3 redeploys with the notifications queue.
      const o = readOutputs(args['stack-name']);
      const services = [o.FulfillmentServiceName, o.NotificationsServiceName, o.FraudSignalsServiceName].filter(Boolean) as string[];
      const [svc, fulfilled, notified, fulfillmentWaiting, notificationsWaiting, scored, signals] = await Promise.all([
        ecs.send(new DescribeServicesCommand({ cluster: o.ClusterName, services })),
        count(dynamo, o.FulfillmentTableName),
        o.NotificationsTableName ? count(dynamo, o.NotificationsTableName) : Promise.resolve(undefined),
        backlog(cw, o.FulfillmentQueueArn, FULFILLMENT_QUEUE),
        o.NotificationsQueueArn ? backlog(cw, o.NotificationsQueueArn, NOTIFICATIONS_QUEUE) : Promise.resolve(undefined),
        logCount(logs, o.LogGroupName, 'ORDER_SCORED', 'fraud-signals'),
        logCount(logs, o.LogGroupName, 'FRAUD_SIGNAL', 'fraud-signals'),
      ]);
      const tasks = (name?: string) => {
        const s = svc.services?.find((x) => x.serviceName === name);
        return s ? `${s.runningCount}/${s.desiredCount}` : '-';
      };
      const n = (v: number | undefined) => (v === undefined ? '-' : String(Math.round(v)));
      console.log(`\n${new Date().toISOString().slice(11, 19)}  (queue backlog metrics lag about 1-2 min)`);
      console.log('  consumer        kind    tasks  done                         waiting in its queue');
      console.log(`  fulfillment      queue   ${tasks(o.FulfillmentServiceName).padEnd(5)}  ${`${fulfilled} orders fulfilled`.padEnd(28)} ${n(fulfillmentWaiting)}`);
      console.log(o.NotificationsServiceName
        ? `  notifications   queue   ${tasks(o.NotificationsServiceName).padEnd(5)}  ${`${n(notified)} confirmations sent`.padEnd(28)} ${n(notificationsWaiting)}`
        : '  notifications   queue   -      not deployed yet');
      console.log(`  fraud signals   stream  ${tasks(o.FraudSignalsServiceName).padEnd(5)}  ${`${scored} orders scored, ${signals} signals`.padEnd(28)} - (reads the stream; last hour)`);
      if (args.once) break;
      await sleep(interval * 1000);
    }
  } finally {
    cw.destroy();
    logs.destroy();
    dynamo.destroy();
    ecs.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
  process.exitCode = 1;
});
