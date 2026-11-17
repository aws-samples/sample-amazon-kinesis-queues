// Deletes a demo queue. Run this before destroying the base stack:
// Kinesis blocks DeleteStream while a queue is attached to the stream.
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { DeleteQueueCommand, DescribeQueueCommand, ResourceNotFoundException } from '@aws-sdk/client-kinesis';
import {
  DEFAULT_QUEUE_NAME, commonArgs, createClient, errorMessage, handleHelp, removeQueueOutputs, resolveQueueArn,
} from './common';

const USAGE = `Usage: npm run queue:delete -- [options]

  --queue-name NAME  Queue to delete, looked up in shared/queue-outputs.json (default: ${DEFAULT_QUEUE_NAME})
  --queue-arn ARN    Queue to delete, instead of --queue-name
  --region REGION    Default: us-east-2
  --profile PROFILE  AWS profile; otherwise the standard credential chain`;

const POLL_INTERVAL_MS = 5_000;
const MAX_WAIT_MS = 5 * 60_000;

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: {
      ...commonArgs,
      'queue-arn': { type: 'string' },
      'queue-name': { type: 'string', default: DEFAULT_QUEUE_NAME },
    },
  });
  handleHelp(args.help, USAGE);
  const queueArn = resolveQueueArn(args['queue-arn'], args['queue-name']);
  const client = createClient({ region: args.region, profile: args.profile });
  try {
    try {
      await client.send(new DeleteQueueCommand({ QueueARN: queueArn }));
      console.log(`Deleting ${queueArn}`);
    } catch (error) {
      if (!(error instanceof ResourceNotFoundException)) throw error;
    }
    const deadline = Date.now() + MAX_WAIT_MS;
    for (;;) {
      try {
        const { QueueDescription: q } = await client.send(new DescribeQueueCommand({ QueueARN: queueArn }));
        if (Date.now() > deadline) throw new Error(`Queue still ${q?.QueueStatus} after ${MAX_WAIT_MS / 1000}s`);
        console.log(`  status ${q?.QueueStatus}, waiting...`);
        await sleep(POLL_INTERVAL_MS);
      } catch (error) {
        if (error instanceof ResourceNotFoundException) break;
        throw error;
      }
    }
    removeQueueOutputs(queueArn);
    console.log('Queue deleted. The stream and its records are unchanged.');
  } catch (error) {
    console.error(`delete-queue failed: ${errorMessage(error)}`);
    process.exitCode = 1;
  } finally {
    client.destroy();
  }
}

main().catch((error) => {
  console.error(errorMessage(error));
  process.exitCode = 1;
});
