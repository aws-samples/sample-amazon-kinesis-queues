// Shows or changes a queue's visibility timeout and dead-letter settings.
// Dead-letter settings live in QueueAttributes.DeadLetterConfiguration.
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import {
  DescribeQueueCommand, UpdateQueueCommand, type QueueAttributes, type QueueDescription,
} from '@aws-sdk/client-kinesis';
import { DEFAULT_QUEUE_NAME, commonArgs, createClient, errorMessage, handleHelp, resolveQueueArn } from './common';

const USAGE = `Usage: npm run queue:configure -- [options]

  --queue-name NAME             Queue, looked up in shared/queue-outputs.json (default: ${DEFAULT_QUEUE_NAME})
  --queue-arn ARN               Queue, instead of --queue-name
  --visibility-timeout SECONDS  0-43200
  --max-receive-count N         1-1000: receives without an acknowledgment before a record becomes a dead-letter record
  --dead-letter on|off          Turn dead-lettering on (BUILT_IN) or off (NONE)
  --region REGION               Default: us-east-2
  --profile PROFILE             AWS profile; otherwise the standard credential chain

With no change options, prints the current settings.`;

const POLL_INTERVAL_MS = 3_000;
const MAX_WAIT_MS = 5 * 60_000;

function describe(q: QueueDescription): string {
  const dl = q.QueueAttributes?.DeadLetterConfiguration;
  const deadLetter = !dl ? 'not reported'
    : dl.TargetType === 'NONE' ? 'off'
      : `on (${dl.TargetType}), max receive count ${dl.MaxReceiveCount}`;
  return `  ${q.QueueName} (${q.QueueStatus})
    visibility timeout  ${q.QueueAttributes?.VisibilityTimeoutSeconds ?? '?'} s
    dead-lettering      ${deadLetter}`;
}

function wholeNumber(text: string, name: string, min: number, max: number): number {
  const value = Number(text);
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`--${name} must be a whole number from ${min} to ${max}`);
  return value;
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: {
      ...commonArgs,
      'queue-arn': { type: 'string' },
      'queue-name': { type: 'string', default: DEFAULT_QUEUE_NAME },
      'visibility-timeout': { type: 'string' },
      'max-receive-count': { type: 'string' },
      'dead-letter': { type: 'string' },
    },
  });
  handleHelp(args.help, USAGE);
  const queueArn = resolveQueueArn(args['queue-arn'], args['queue-name']);
  const client = createClient({ region: args.region, profile: args.profile });
  const describeQueue = async () => {
    const { QueueDescription: q } = await client.send(new DescribeQueueCommand({ QueueARN: queueArn }));
    if (!q) throw new Error(`DescribeQueue returned no description for ${queueArn}`);
    return q;
  };

  const current = await describeQueue();
  console.log(`Current:\n${describe(current)}`);

  const changes: QueueAttributes = {};
  if (args['visibility-timeout'] !== undefined) {
    changes.VisibilityTimeoutSeconds = wholeNumber(args['visibility-timeout'], 'visibility-timeout', 0, 43_200);
  }
  const deadLetter = args['dead-letter'];
  if (deadLetter !== undefined && deadLetter !== 'on' && deadLetter !== 'off') throw new Error('--dead-letter must be on or off');
  if (deadLetter === 'off') {
    if (args['max-receive-count'] !== undefined) throw new Error('--max-receive-count cannot be combined with --dead-letter off');
    changes.DeadLetterConfiguration = { TargetType: 'NONE' };
  } else if (deadLetter === 'on' || args['max-receive-count'] !== undefined) {
    const existing = current.QueueAttributes?.DeadLetterConfiguration?.MaxReceiveCount;
    changes.DeadLetterConfiguration = {
      TargetType: 'BUILT_IN',
      MaxReceiveCount: args['max-receive-count'] !== undefined
        ? wholeNumber(args['max-receive-count'], 'max-receive-count', 1, 1000)
        : existing ?? 6,
    };
  }
  if (Object.keys(changes).length === 0) return;

  if (current.QueueStatus !== 'ACTIVE') throw new Error(`Queue is ${current.QueueStatus}; it must be ACTIVE to update.`);
  await client.send(new UpdateQueueCommand({ QueueARN: queueArn, QueueAttributes: changes }));
  const deadline = Date.now() + MAX_WAIT_MS;
  for (;;) {
    const q = await describeQueue();
    if (q.QueueStatus === 'ACTIVE') {
      console.log(`Updated:\n${describe(q)}`);
      return;
    }
    if (Date.now() > deadline) throw new Error(`Queue still ${q.QueueStatus} after ${MAX_WAIT_MS / 1000}s`);
    await sleep(POLL_INTERVAL_MS);
  }
}

main().catch((error) => {
  console.error(`configure-queue failed: ${errorMessage(error)}`);
  process.exitCode = 1;
});
