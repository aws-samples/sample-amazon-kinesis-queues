// One view of both consumers while you migrate: the tasks of each consumer and the
// shards each KCL task holds, live fulfillment rates, which consumer fulfilled the
// orders, the duplicates the orders table absorbed, the sideline queues, and the
// queue's own metrics.
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { CloudWatchClient, GetMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { CloudWatchLogsClient, FilterLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
import { DynamoDBClient, ScanCommand, type AttributeValue } from '@aws-sdk/client-dynamodb';
import { DescribeServicesCommand, ECSClient, type Service } from '@aws-sdk/client-ecs';
import { DescribeStreamSummaryCommand, KinesisClient } from '@aws-sdk/client-kinesis';
import { GetQueueAttributesCommand, QueueDoesNotExist, SQSClient } from '@aws-sdk/client-sqs';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { QUEUE_NAME, RELAY_QUEUE_NAME } from '../lib/config';
import { count, readOutputs, scriptArgs, type MigrationOutputs } from './outputs';

const USAGE = `Usage: npm run migrate:status -- [options]

  --interval SECONDS  Refresh interval (default: 10)
  --once              Print once and exit
  --relay             Show the relay migration (cdk-outputs-relay.json) instead of the KCL one
  --stack-name NAME   Stack in the outputs file (default: the only one)
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain

Read-only. Rates and shards come from the tasks' STATS log lines; queue metrics
arrive in CloudWatch about 1-2 minutes late.`;

interface TaskStats {
  consumer: 'kcl' | 'queue' | 'sqs';
  task: string;
  shards?: string[];
  fulfilledPerSecond: number;
  inFlight?: number;
}

/** The latest STATS line of every task that reported in the window, from both consumers. */
async function taskStats(logs: CloudWatchLogsClient, logGroupName: string, windowMs: number): Promise<TaskStats[]> {
  const latest = new Map<string, TaskStats>();
  let nextToken: string | undefined;
  do {
    const page = await logs.send(new FilterLogEventsCommand({
      logGroupName, startTime: Date.now() - windowMs, filterPattern: '"STATS"', nextToken,
    }));
    for (const e of page.events ?? []) {
      try {
        const line = JSON.parse(e.message ?? '') as Record<string, unknown>;
        if (line.event !== 'STATS' || typeof line.task !== 'string') continue;
        // Log streams are named <prefix>/<container>/<task>, with the prefix kcl, sqs or queue.
        const prefix = (e.logStreamName ?? '').split('/')[0];
        latest.set(line.task, {
          consumer: prefix === 'kcl' || prefix === 'sqs' ? prefix : 'queue',
          task: line.task,
          shards: Array.isArray(line.shards) ? line.shards as string[] : undefined,
          fulfilledPerSecond: typeof line.fulfilledPerSecond === 'number' ? line.fulfilledPerSecond : 0,
          inFlight: typeof line.inFlight === 'number' ? line.inFlight : undefined,
        });
      } catch {
        // not a JSON log line: KCL's own log output
      }
    }
    nextToken = page.nextToken;
  } while (nextToken);
  return [...latest.values()];
}

/** A full scan is fine at demo sizes; don't do this in production. */
async function ordersByConsumer(dynamo: DynamoDBClient, table: string) {
  const byConsumer: Record<string, number> = {};
  let skipped = 0;
  let discarded = 0;
  let total = 0;
  let ExclusiveStartKey: Record<string, AttributeValue> | undefined;
  do {
    const page = await dynamo.send(new ScanCommand({
      TableName: table,
      ExclusiveStartKey,
      ProjectionExpression: '#c, #s, #d',
      ExpressionAttributeNames: { '#c': 'consumer', '#s': 'duplicatesSkipped', '#d': 'duplicatesDiscarded' },
    }));
    for (const item of page.Items ?? []) {
      const consumer = item.consumer?.S ?? '?';
      byConsumer[consumer] = (byConsumer[consumer] ?? 0) + 1;
      skipped += Number(item.duplicatesSkipped?.N ?? 0);
      discarded += Number(item.duplicatesDiscarded?.N ?? 0);
      total += 1;
    }
    ExclusiveStartKey = page.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return { total, byConsumer, skipped, discarded };
}

async function sqsDepth(sqs: SQSClient, url: string) {
  try {
    const { Attributes: a } = await sqs.send(new GetQueueAttributesCommand({
      QueueUrl: url, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }));
    return Number(a?.ApproximateNumberOfMessages ?? 0) + Number(a?.ApproximateNumberOfMessagesNotVisible ?? 0);
  } catch (error) {
    // phase=after deletes the SQS queues before cdk deploy rewrites the outputs file.
    if (error instanceof QueueDoesNotExist) return undefined;
    throw error;
  }
}

async function queueMetrics(cw: CloudWatchClient, queueArn: string, queueName: string) {
  const dims = [{ Name: 'QueueId', Value: queueArn.split('/').pop()! }, { Name: 'QueueName', Value: queueName }];
  const query = (id: string, name: string) => ({
    Id: id,
    MetricStat: { Metric: { Namespace: 'AWS/Kinesis', MetricName: name, Dimensions: dims }, Period: 60, Stat: 'Maximum' },
  });
  const end = new Date();
  const out = await cw.send(new GetMetricDataCommand({
    StartTime: new Date(end.getTime() - 10 * 60_000), EndTime: end, ScanBy: 'TimestampDescending',
    MetricDataQueries: [
      query('visible', 'VisibleQueueRecords'), query('inflight', 'InFlightQueueRecords'),
      query('dead', 'VisibleDeadLetterQueueRecords'),
    ],
  }));
  const latest = (id: string) => out.MetricDataResults?.find((m) => m.Id === id)?.Values?.[0];
  return { visible: latest('visible'), inFlight: latest('inflight'), dead: latest('dead') };
}

function tasks(services: Service[], name: string | undefined): string {
  const s = services.find((x) => x.serviceName !== undefined && x.serviceName === name);
  return s ? `${s.runningCount ?? 0}/${s.desiredCount ?? 0}` : '-';
}

function line(label: string, text: string): string {
  return `  ${label.padEnd(9)} ${text}`;
}

async function render(o: MigrationOutputs, clients: {
  ecs: ECSClient; logs: CloudWatchLogsClient; dynamo: DynamoDBClient; sqs: SQSClient; cw: CloudWatchClient;
}, windowMs: number, shards: string): Promise<string> {
  const serviceNames = [o.KclServiceName, o.SqsServiceName, o.QueueServiceName].filter((n): n is string => Boolean(n));
  const [svc, stats, orders, sideline, sidelineDead, queue, relay, relayDead] = await Promise.all([
    serviceNames.length ? clients.ecs.send(new DescribeServicesCommand({ cluster: o.ClusterName, services: serviceNames })) : undefined,
    taskStats(clients.logs, o.LogGroupName, windowMs),
    ordersByConsumer(clients.dynamo, o.OrdersTableName),
    o.SidelineQueueUrl ? sqsDepth(clients.sqs, o.SidelineQueueUrl) : undefined,
    o.SidelineDeadLetterQueueUrl ? sqsDepth(clients.sqs, o.SidelineDeadLetterQueueUrl) : undefined,
    o.QueueArn ? queueMetrics(clients.cw, o.QueueArn, o.Migration === 'relay' ? RELAY_QUEUE_NAME : QUEUE_NAME) : undefined,
    o.RelayQueueUrl ? sqsDepth(clients.sqs, o.RelayQueueUrl) : undefined,
    o.RelayDeadLetterQueueUrl ? sqsDepth(clients.sqs, o.RelayDeadLetterQueueUrl) : undefined,
  ]);
  const services = svc?.services ?? [];
  const rate = (consumer: TaskStats['consumer']) => stats.filter((t) => t.consumer === consumer)
    .reduce((sum, t) => sum + t.fulfilledPerSecond, 0).toFixed(1);
  const fmt = (n?: number) => (n === undefined ? '-' : count(Math.round(n)));
  const out = [`${new Date().toISOString().slice(11, 19)}  phase ${o.Phase}  stream shards ${shards}`];

  if (o.KclServiceName) {
    const kcl = stats.filter((t) => t.consumer === 'kcl');
    const held = kcl.map((t) => `${t.task}=${t.shards?.length ?? 0}`).join(' ') || 'no STATS yet';
    const inFlight = kcl.reduce((sum, t) => sum + (t.inFlight ?? 0), 0);
    out.push(line('KCL', `tasks ${tasks(services, o.KclServiceName)}   shards per task: ${held}   `
      + `fulfilled/s ${rate('kcl')}   in flight ${inFlight}`));
  }
  if (o.SqsServiceName) {
    out.push(line('SQS', `tasks ${tasks(services, o.SqsServiceName)}   fulfilled/s ${rate('sqs')}`));
  }
  if (o.QueueServiceName) {
    out.push(line('queue', `tasks ${tasks(services, o.QueueServiceName)}   fulfilled/s ${rate('queue')}`));
  }
  const by = Object.entries(orders.byConsumer).sort().map(([c, n]) => `${c} ${count(n)}`).join(', ') || 'none yet';
  out.push(line('orders', `fulfilled ${count(orders.total)} (${by})   duplicates absorbed ${count(orders.skipped + orders.discarded)} `
    + `(${count(orders.skipped)} skipped before the warehouse call, ${count(orders.discarded)} discarded after it)`));
  if (sideline !== undefined) {
    out.push(line('sideline', `SQS queue ${sideline}   its DLQ ${sidelineDead ?? 0}`));
  }
  if (relay !== undefined) {
    out.push(line('relay', `pipe ${o.PipeName}   SQS queue ${relay}   its DLQ ${relayDead ?? 0}`));
  }
  if (queue) {
    out.push(line('queue', `waiting ${fmt(queue.visible)}, in flight ${fmt(queue.inFlight)}, dead-letter records ${fmt(queue.dead)}   `
      + '(CloudWatch, 1-2 min behind)'));
  }
  return out.join('\n');
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: { ...scriptArgs, interval: { type: 'string', default: '10' }, once: { type: 'boolean', default: false } },
  });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const intervalSeconds = Number(args.interval);
  if (!Number.isFinite(intervalSeconds) || intervalSeconds < 5) throw new Error('--interval must be at least 5 seconds');
  const outputs = readOutputs(args['stack-name'], args.relay ? 'relay' : 'kcl');
  const config = { region: args.region, credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}) };
  const clients = {
    ecs: new ECSClient(config), logs: new CloudWatchLogsClient(config), dynamo: new DynamoDBClient(config),
    sqs: new SQSClient(config), cw: new CloudWatchClient(config),
  };
  const kinesis = new KinesisClient(config);
  let shards = '?';
  try {
    const { StreamDescriptionSummary: s } = await kinesis.send(new DescribeStreamSummaryCommand({ StreamARN: outputs.StreamArn }));
    shards = String(s?.OpenShardCount ?? '?');
  } catch {
    // shard count stays unknown
  }

  let stop = false;
  process.once('SIGINT', () => { stop = true; });
  try {
    while (!stop) {
      // Read the outputs on every refresh: each phase is a new deploy with new outputs.
      const current = readOutputs(args['stack-name'], args.relay ? 'relay' : 'kcl');
      // Tasks log STATS every 10 s; a window of two intervals catches every running task once.
      console.log(`${await render(current, clients, Math.max(25, intervalSeconds * 2) * 1000, shards)}\n`);
      if (args.once) break;
      await sleep(intervalSeconds * 1000);
    }
  } finally {
    Object.values(clients).forEach((c) => c.destroy());
    kinesis.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
  process.exitCode = 1;
});
