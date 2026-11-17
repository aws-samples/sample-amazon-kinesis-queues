// Shows the first queue at a glance: its settings, whether the consumer is running,
// and what the queue did in the last 15 minutes.
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { parseArgs } from 'node:util';
import { CloudWatchClient, GetMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { DescribeServicesCommand, ECSClient } from '@aws-sdk/client-ecs';
import { DescribeQueueCommand } from '@aws-sdk/client-kinesis';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { commonArgs, createClient, errorMessage, handleHelp } from '../../shared/src/common';

const USAGE = `Usage: npm run first-queue:status -- [options]

  --stack-name NAME  Stack in 1-first-queue/cdk-outputs.json (default: the only one)
  --region REGION    Default: us-east-2
  --profile PROFILE  AWS profile; otherwise the standard credential chain

Metrics reach CloudWatch about 1-2 minutes late, so the last minute or two may read low.`;

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');

interface Outputs {
  QueueArn: string;
  QueueName: string;
  ConsumerClusterName: string;
  ConsumerServiceName: string;
}

function readOutputs(stackName: string | undefined): Outputs {
  let all: Record<string, Outputs>;
  try {
    all = JSON.parse(readFileSync(OUTPUTS, 'utf8'));
  } catch {
    throw new Error(`Deploy the stack first: (cd 1-first-queue && npx cdk deploy ... --outputs-file cdk-outputs.json), see the README (looked for ${OUTPUTS}).`);
  }
  const names = stackName ? [stackName] : Object.keys(all);
  if (names.length !== 1 || !all[names[0]]) throw new Error(`Pass --stack-name; ${OUTPUTS} has ${Object.keys(all).join(', ')}.`);
  return all[names[0]];
}

// The metric names the service publishes.
const METRICS = [
  { name: 'ReceivedQueueRecords', stat: 'Sum', label: 'records received' },
  { name: 'AcknowledgedQueueRecords', stat: 'Sum', label: 'records acknowledged' },
  { name: 'VisibleQueueRecords', stat: 'Maximum', label: 'waiting (peak)' },
  { name: 'InFlightQueueRecords', stat: 'Maximum', label: 'being processed (peak)' },
] as const;

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: { ...commonArgs, 'stack-name': { type: 'string' } },
  });
  handleHelp(args.help, USAGE);
  const outputs = readOutputs(args['stack-name']);
  const queueId = outputs.QueueArn.split('queue/')[1];

  const kinesis = createClient({ region: args.region, profile: args.profile });
  const clientConfig = { region: args.region, credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}) };
  const ecs = new ECSClient(clientConfig);
  const cloudwatch = new CloudWatchClient(clientConfig);
  try {
    const end = new Date();
    const [queue, service, data] = await Promise.all([
      kinesis.send(new DescribeQueueCommand({ QueueARN: outputs.QueueArn })),
      ecs.send(new DescribeServicesCommand({ cluster: outputs.ConsumerClusterName, services: [outputs.ConsumerServiceName] })),
      cloudwatch.send(new GetMetricDataCommand({
        StartTime: new Date(end.getTime() - 15 * 60_000),
        EndTime: end,
        MetricDataQueries: METRICS.map((m, i) => ({
          Id: `m${i}`,
          MetricStat: {
            Metric: {
              Namespace: 'AWS/Kinesis',
              MetricName: m.name,
              Dimensions: [{ Name: 'QueueId', Value: queueId }, { Name: 'QueueName', Value: outputs.QueueName }],
            },
            Period: 900,
            Stat: m.stat,
          },
        })),
      })),
    ]);

    const q = queue.QueueDescription;
    const config = q?.StreamConfigurationList?.[0];
    const s = service.services?.[0];
    console.log(`Queue ${outputs.QueueName}
  ARN                 ${outputs.QueueArn}
  status              ${q?.QueueStatus}
  stream              ${config?.StreamARN}
  starting position   ${config?.StartingPosition?.Type}
  consumer tasks      ${s?.runningCount ?? '?'} running of ${s?.desiredCount ?? '?'} wanted

Last 15 minutes (CloudWatch, 1-2 minutes behind)`);
    METRICS.forEach((m, i) => {
      // A 15-minute window can straddle two 15-minute periods, so combine whatever points came back.
      // No data means nothing happened, so it reads 0.
      const values = data.MetricDataResults?.find((r) => r.Id === `m${i}`)?.Values ?? [];
      const value = m.stat === 'Sum' ? values.reduce((a, b) => a + b, 0) : Math.max(0, ...values);
      console.log(`  ${m.label.padEnd(24)}${value}`);
    });
  } finally {
    kinesis.destroy();
    ecs.destroy();
    cloudwatch.destroy();
  }
}

main().catch((error) => {
  console.error(errorMessage(error));
  process.exitCode = 1;
});
