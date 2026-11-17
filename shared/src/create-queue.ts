// Creates a queue on the Quickstart stream (or --stream-arn) and waits until it is ACTIVE.
// The visibility timeout is left at the queue default (30 seconds).
import { setTimeout as sleep } from 'node:timers/promises';
import {
  CreateQueueCommand,
  DescribeQueueCommand,
  DescribeStreamSummaryCommand,
  KinesisClient,
  ListQueuesCommand,
  QueueStartingPositionType,
  type QueueDescription,
} from '@aws-sdk/client-kinesis';
import { parseArgs } from 'node:util';
import {
  DEFAULT_QUEUE_NAME, commonArgs, createClient, errorMessage, handleHelp, resolveStreamArn, saveQueueOutputs,
} from './common';

const USAGE = `Usage: npm run queue:create -- [options]

  --stream-arn ARN          Stream to attach the queue to (default: StreamArn in 0-quickstart/cdk-outputs.json)
  --stack-name NAME         Quickstart stack to read from that file when it holds several
  --queue-name NAME         Queue name (default: ${DEFAULT_QUEUE_NAME})
  --starting-position POS   TRIM_HORIZON (default), LATEST or AT_TIMESTAMP
  --timestamp ISO           Start time, required with AT_TIMESTAMP
  --region REGION           Default: us-east-2
  --profile PROFILE         AWS profile; otherwise the standard credential chain`;

const POLL_INTERVAL_MS = 5_000;
const MAX_WAIT_MS = 5 * 60_000;

async function checkStream(client: KinesisClient, streamArn: string): Promise<void> {
  const { StreamDescriptionSummary: s } = await client.send(new DescribeStreamSummaryCommand({ StreamARN: streamArn }));
  if (!s) throw new Error(`Stream not found: ${streamArn}`);
  const problems: string[] = [];
  if (s.StreamStatus !== 'ACTIVE') problems.push(`status is ${s.StreamStatus}, expected ACTIVE`);
  if (s.StreamModeDetails?.StreamMode !== 'ON_DEMAND') problems.push('queues need an On-demand stream');
  if (problems.length > 0) throw new Error(`Stream ${s.StreamName} can't be used:\n  - ${problems.join('\n  - ')}`);
  // An encrypted stream works too. Whoever receives from the queue then needs
  // kms:Decrypt on the stream's KMS key; creating the queue needs no KMS permissions.
  const encrypted = s.EncryptionType && s.EncryptionType !== 'NONE';
  console.log(`Stream ${s.StreamName}: ACTIVE, On-demand, ${encrypted ? `encrypted with ${s.KeyId}` : 'unencrypted'}`);
}

async function findExistingQueue(client: KinesisClient, streamArn: string, queueName: string) {
  let NextToken: string | undefined;
  do {
    // Match on name and stream, so a queue with the same name on another stream isn't reused.
    const page = await client.send(new ListQueuesCommand({ NextToken }));
    const match = page.QueueSummaries?.find(
      (q) => q.QueueName === queueName && q.Streams?.some((s) => s.StreamARN === streamArn),
    );
    if (match) return match;
    NextToken = page.NextToken;
  } while (NextToken);
  return undefined;
}

async function waitUntilActive(client: KinesisClient, queueArn: string): Promise<QueueDescription> {
  const deadline = Date.now() + MAX_WAIT_MS;
  for (;;) {
    const { QueueDescription: q } = await client.send(new DescribeQueueCommand({ QueueARN: queueArn }));
    if (q?.QueueStatus === 'ACTIVE') return q;
    if (Date.now() > deadline) throw new Error(`Queue still ${q?.QueueStatus} after ${MAX_WAIT_MS / 1000}s`);
    console.log(`  status ${q?.QueueStatus}, waiting...`);
    await sleep(POLL_INTERVAL_MS);
  }
}

function startingPosition(type: string, timestamp: string | undefined) {
  if (!Object.values(QueueStartingPositionType).includes(type as QueueStartingPositionType)) {
    throw new Error(`--starting-position must be one of ${Object.values(QueueStartingPositionType).join(', ')}`);
  }
  if (type !== 'AT_TIMESTAMP') return { Type: type as QueueStartingPositionType };
  const at = timestamp ? new Date(timestamp) : undefined;
  if (!at || Number.isNaN(at.getTime())) throw new Error('--timestamp must be an ISO date when using AT_TIMESTAMP');
  return { Type: QueueStartingPositionType.AT_TIMESTAMP, Timestamp: at };
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: {
      ...commonArgs,
      'stream-arn': { type: 'string' },
      'stack-name': { type: 'string' },
      'queue-name': { type: 'string', default: DEFAULT_QUEUE_NAME },
      'starting-position': { type: 'string', default: 'TRIM_HORIZON' },
      timestamp: { type: 'string' },
    },
  });
  handleHelp(args.help, USAGE);
  const queueName = args['queue-name'];
  const streamArn = resolveStreamArn(args['stream-arn'], args['stack-name']);
  const client = createClient({ region: args.region, profile: args.profile });

  try {
    await checkStream(client, streamArn);

    let queueArn: string;
    const existing = await findExistingQueue(client, streamArn, queueName);
    if (existing?.QueueARN) {
      queueArn = existing.QueueARN;
      console.log(`Queue ${queueName} already exists on this stream, reusing it.`);
    } else {
      const { QueueDescription: created } = await client.send(new CreateQueueCommand({
        QueueName: queueName,
        StreamConfigurationList: [{
          StreamARN: streamArn,
          StartingPosition: startingPosition(args['starting-position'], args.timestamp),
        }],
        // QueueAttributes omitted on purpose: keep the default visibility timeout.
        Tags: { demo: 'first-queue' },
      }));
      if (!created?.QueueARN) throw new Error('CreateQueue returned no QueueARN');
      queueArn = created.QueueARN;
      console.log(`Created queue ${queueName} (${created.QueueStatus})`);
    }

    const queue = await waitUntilActive(client, queueArn);
    saveQueueOutputs({ queueName, queueArn, streamArn, region: args.region });

    const start = queue.StreamConfigurationList?.[0]?.StartingPosition;
    console.log(`Queue ACTIVE
  ARN                 ${queueArn}
  starting position   ${start?.Type}${start?.Timestamp ? ` (${start.Timestamp.toISOString()})` : ''}
  visibility timeout  ${queue.QueueAttributes?.VisibilityTimeoutSeconds ?? 30}s (default)
Saved to shared/queue-outputs.json`);
  } catch (error) {
    console.error(`create-queue failed: ${errorMessage(error)}`);
    process.exitCode = 1;
  } finally {
    client.destroy();
  }
}

main().catch((error) => {
  console.error(errorMessage(error));
  process.exitCode = 1;
});
