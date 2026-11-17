// Prints the scale-out picture every few seconds: running workers next to the
// stream's shard count, the live fulfillment rate summed across workers, and the
// queue backlog. Run it while you scale to see the drain rate change.
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { CloudWatchClient, GetMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { CloudWatchLogsClient, FilterLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
import { DescribeServicesCommand, ECSClient } from '@aws-sdk/client-ecs';
import { DescribeStreamSummaryCommand, KinesisClient } from '@aws-sdk/client-kinesis';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { resolveStreamArn } from '../../shared/src/common';
import { QUEUE_NAME } from '../lib/config';

const USAGE = `Usage: npm run scale:status -- [options]

  --stack-name NAME   Stack in 3-scale-out/cdk-outputs.json (default: the only one)
  --interval SECONDS  Refresh interval (default: 10)
  --once              Print once and exit
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain`;

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');

interface Outputs {
  QueueArn: string;
  ClusterName: string;
  ServiceName: string;
  LogGroupName: string;
}

export function readOutputs(stackName: string | undefined): Outputs & { stackName: string } {
  let all: Record<string, Outputs>;
  try {
    all = JSON.parse(readFileSync(OUTPUTS, 'utf8'));
  } catch {
    throw new Error(`Deploy the stack with --outputs-file cdk-outputs.json first (looked for ${OUTPUTS}).`);
  }
  const names = stackName ? [stackName] : Object.keys(all);
  if (names.length !== 1 || !all[names[0]]) throw new Error(`Pass --stack-name; ${OUTPUTS} has ${Object.keys(all).join(', ')}.`);
  return { ...all[names[0]], stackName: names[0] };
}

/** Sums the latest STATS line of every worker that reported in the window. */
async function liveRate(logs: CloudWatchLogsClient, logGroupName: string, windowMs: number) {
  const latest = new Map<string, number>();
  let nextToken: string | undefined;
  do {
    const page = await logs.send(new FilterLogEventsCommand({
      logGroupName,
      startTime: Date.now() - windowMs,
      filterPattern: '"STATS"',
      nextToken,
    }));
    for (const e of page.events ?? []) {
      try {
        const line = JSON.parse(e.message ?? '') as { task?: string; fulfilledPerSecond?: number };
        if (line.task && typeof line.fulfilledPerSecond === 'number') latest.set(line.task, line.fulfilledPerSecond);
      } catch {
        // not a JSON log line
      }
    }
    nextToken = page.nextToken;
  } while (nextToken);
  return { reporting: latest.size, perSecond: [...latest.values()].reduce((a, b) => a + b, 0) };
}

async function backlog(cw: CloudWatchClient, queueArn: string) {
  const dims = [{ Name: 'QueueId', Value: queueArn.split('/').pop()! }, { Name: 'QueueName', Value: QUEUE_NAME }];
  const query = (id: string, name: string, stat: string) => ({
    Id: id,
    MetricStat: { Metric: { Namespace: 'AWS/Kinesis', MetricName: name, Dimensions: dims }, Period: 60, Stat: stat },
  });
  const end = new Date();
  const out = await cw.send(new GetMetricDataCommand({
    StartTime: new Date(end.getTime() - 10 * 60_000),
    EndTime: end,
    ScanBy: 'TimestampDescending',
    MetricDataQueries: [query('visible', 'VisibleQueueRecords', 'Maximum'), query('inflight', 'InFlightQueueRecords', 'Maximum')],
  }));
  const latest = (id: string) => out.MetricDataResults?.find((m) => m.Id === id)?.Values?.[0];
  return { visible: latest('visible'), inflight: latest('inflight') };
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
  if (!Number.isFinite(intervalSeconds) || intervalSeconds < 5) throw new Error('--interval must be at least 5 seconds');
  const outputs = readOutputs(args['stack-name']);
  const clientConfig = { region: args.region, credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}) };
  const ecs = new ECSClient(clientConfig);
  const cw = new CloudWatchClient(clientConfig);
  const logs = new CloudWatchLogsClient(clientConfig);
  const kinesis = new KinesisClient(clientConfig);

  // Shown next to the worker count: the point of the scenario is that they're unrelated.
  let shards: number | string = '?';
  try {
    const { StreamDescriptionSummary: s } = await kinesis.send(new DescribeStreamSummaryCommand({ StreamARN: resolveStreamArn(undefined, undefined) }));
    shards = s?.OpenShardCount ?? '?';
  } catch {
    // stream outputs not available locally; shard count stays unknown
  }

  let stop = false;
  process.once('SIGINT', () => { stop = true; });
  console.log(`Stream shards: ${shards}. Fulfilled/s is live from worker logs; queue backlog lags about 1-2 minutes.`);
  console.log('time      workers(run/want)  reporting  fulfilled/s  queue visible  in flight');
  try {
    while (!stop) {
      const [svc, rate, queue] = await Promise.all([
        ecs.send(new DescribeServicesCommand({ cluster: outputs.ClusterName, services: [outputs.ServiceName] })),
        liveRate(logs, outputs.LogGroupName, Math.max(20, intervalSeconds * 2) * 1000),
        backlog(cw, outputs.QueueArn),
      ]);
      const s = svc.services?.[0];
      const fmt = (n?: number) => (n === undefined ? '-' : String(Math.round(n)));
      console.log([
        new Date().toISOString().slice(11, 19),
        `${s?.runningCount ?? '?'}/${s?.desiredCount ?? '?'}`.padStart(17),
        String(rate.reporting).padStart(10),
        rate.perSecond.toFixed(1).padStart(12),
        fmt(queue.visible).padStart(14),
        fmt(queue.inflight).padStart(10),
      ].join(' '));
      if (args.once) break;
      await sleep(intervalSeconds * 1000);
    }
  } finally {
    ecs.destroy();
    cw.destroy();
    logs.destroy();
    kinesis.destroy();
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
    process.exitCode = 1;
  });
}
