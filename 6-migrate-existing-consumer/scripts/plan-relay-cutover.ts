// Picks the starting position for the Kinesis queue that replaces a relay into SQS, so
// the cutover skips no records.
//
// The pipe reads each shard in order and never skips a batch (one batch per shard at a
// time, retried until it succeeds). So for each shard, every record before the newest
// order the SQS consumer has finished went through the pipe: it's either fulfilled or
// still in the SQS queue, where the SQS consumer will finish it. Records after it may
// not have been copied yet. The SQS consumer stores each order's shard and arrival time
// in the orders table, so this script finds that newest order per shard. For a shard
// with no order from the SQS consumer yet, it takes the first record on the shard since
// the pipe was created. The queue starts at the oldest of those times, minus a margin.
//
// Records between that time and the pipe's position reach both consumers. The orders
// table makes the second delivery a no-op.
//
// The plan fails instead of guessing when a shard's position can't be read, or when no
// shard gives a time.
import { parseArgs } from 'node:util';
import { DynamoDBClient, ScanCommand, type AttributeValue } from '@aws-sdk/client-dynamodb';
import { ListShardsCommand, ShardIteratorType, type KinesisClient } from '@aws-sdk/client-kinesis';
import { GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { createClient, errorMessage } from '../../shared/src/common';
import { RELAY_QUEUE_NAME } from '../lib/config';
import { readOutputs, scriptArgs } from './outputs';
import { firstArrival } from './stream-positions';

const USAGE = `Usage: npm run migrate:relay:plan -- [options]

  --margin SECONDS    Start this long before the oldest position (default: 60)
  --stack-name NAME   Stack in 6-migrate-existing-consumer/cdk-outputs-relay.json (default: the only one)
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain

Reads the orders the SQS consumer finished and prints the queue:create command with an
AT_TIMESTAMP starting position that covers every record the pipe may not have copied.`;

interface ShardPosition {
  shardId: string;
  from?: Date;
  note: string;
  problem?: string;
}

/** Every shard in the stream's retention, open or closed. */
async function listShards(kinesis: KinesisClient, streamArn: string): Promise<string[]> {
  const ids: string[] = [];
  let NextToken: string | undefined;
  do {
    const page = await kinesis.send(new ListShardsCommand(NextToken ? { NextToken } : { StreamARN: streamArn }));
    for (const s of page.Shards ?? []) if (s.ShardId) ids.push(s.ShardId);
    NextToken = page.NextToken;
  } while (NextToken);
  return ids.sort();
}

/** Per shard, the arrival time of the newest order the SQS consumer fulfilled. A full scan is fine at demo sizes. */
async function newestRelayedOrders(dynamo: DynamoDBClient, table: string): Promise<Map<string, Date>> {
  const newest = new Map<string, Date>();
  let ExclusiveStartKey: Record<string, AttributeValue> | undefined;
  do {
    const page = await dynamo.send(new ScanCommand({
      TableName: table,
      ConsistentRead: true,
      ExclusiveStartKey,
      FilterExpression: '#c = :sqs AND attribute_exists(shardId)',
      ProjectionExpression: 'shardId, arrivedAt',
      ExpressionAttributeNames: { '#c': 'consumer' },
      ExpressionAttributeValues: { ':sqs': { S: 'sqs' } },
    }));
    for (const item of page.Items ?? []) {
      const shardId = item.shardId?.S;
      const at = item.arrivedAt?.S ? new Date(item.arrivedAt.S) : undefined;
      if (!shardId || !at || Number.isNaN(at.getTime())) continue;
      if (!newest.has(shardId) || newest.get(shardId)! < at) newest.set(shardId, at);
    }
    ExclusiveStartKey = page.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return newest;
}

async function sqsDepth(sqs: SQSClient, url: string): Promise<number> {
  const { Attributes: a } = await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: url, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
  }));
  return Number(a?.ApproximateNumberOfMessages ?? 0) + Number(a?.ApproximateNumberOfMessagesNotVisible ?? 0);
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({ options: { ...scriptArgs, margin: { type: 'string', default: '60' } } });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const marginSeconds = Number(args.margin);
  if (!Number.isInteger(marginSeconds) || marginSeconds < 0 || marginSeconds > 86_400) throw new Error('--margin must be 0-86400 seconds');
  const outputs = readOutputs(args['stack-name'], 'relay');
  if (!outputs.PipeCreationTime || !outputs.RelayQueueUrl) {
    throw new Error('The stack has no pipe: plan the cutover while the relay is still deployed.');
  }
  const pipeCreated = new Date(outputs.PipeCreationTime);
  if (Number.isNaN(pipeCreated.getTime())) throw new Error(`Can't read the pipe's creation time: ${outputs.PipeCreationTime}`);

  const credentials = fromNodeProviderChain(args.profile ? { profile: args.profile } : {});
  const dynamo = new DynamoDBClient({ region: args.region, credentials });
  const sqs = new SQSClient({ region: args.region, credentials });
  const kinesis = createClient({ region: args.region, profile: args.profile });
  try {
    const [shardIds, newest] = await Promise.all([
      listShards(kinesis, outputs.StreamArn), newestRelayedOrders(dynamo, outputs.OrdersTableName),
    ]);
    const positions: ShardPosition[] = await Promise.all(shardIds.map(async (shardId): Promise<ShardPosition> => {
      const relayed = newest.get(shardId);
      if (relayed) return { shardId, from: relayed, note: 'newest order the SQS consumer finished' };
      // Nothing from this shard reached the SQS consumer yet: everything on it since the
      // pipe was created may still be uncopied.
      try {
        const found = await firstArrival(kinesis, outputs.StreamArn, shardId,
          { type: ShardIteratorType.AT_TIMESTAMP, timestamp: pipeCreated });
        if (found.kind === 'record') return { shardId, from: found.at, note: 'no order relayed yet: first record since the pipe was created' };
        if (found.kind === 'caught-up') return { shardId, note: 'no records since the pipe was created' };
        return { shardId, note: 'no order relayed yet', problem: found.reason };
      } catch (error) {
        return { shardId, note: 'no order relayed yet', problem: errorMessage(error) };
      }
    }));

    console.log(`Pipe ${outputs.PipeName}, created ${pipeCreated.toISOString()}. ${shardIds.length} shards\n`);
    console.log('  shard                   position                      behind now');
    for (const p of positions) {
      const age = p.from ? `${Math.round((Date.now() - p.from.getTime()) / 1000)} s` : '-';
      const note = p.problem ? `${p.note}: ${p.problem}` : p.note;
      console.log(`  ${p.shardId.padEnd(22)}  ${(p.from?.toISOString() ?? '-').padEnd(28)}  ${age.padStart(10)}   ${note}`);
    }

    const problems = positions.filter((p) => p.problem);
    if (problems.length > 0) {
      throw new Error(`can't place ${problems.map((p) => p.shardId).join(', ')}. No queue command printed; run the plan again.`);
    }
    const times = positions.map((p) => p.from).filter((d): d is Date => d !== undefined);
    if (times.length === 0) {
      throw new Error('no shard has a position yet. Place some orders, wait for the SQS consumer to fulfill them, and run the plan again.');
    }
    const oldest = new Date(Math.min(...times.map((d) => d.getTime())));
    const start = new Date(oldest.getTime() - marginSeconds * 1000);
    console.log(`\nOldest position: ${oldest.toISOString()}. Start the queue ${marginSeconds} s before it, at ${start.toISOString()}:\n`);
    console.log(`  npm run queue:create -- --queue-name ${RELAY_QUEUE_NAME} --stream-arn ${outputs.StreamArn} \\`);
    console.log(`    --starting-position AT_TIMESTAMP --timestamp ${start.toISOString()}\n`);
    console.log('Records between that time and the pipe\'s position reach both consumers. The orders table makes');
    console.log('the second delivery a no-op, so no order ships twice and none is skipped.');

    const [waiting, dead] = await Promise.all([
      sqsDepth(sqs, outputs.RelayQueueUrl),
      outputs.RelayDeadLetterQueueUrl ? sqsDepth(sqs, outputs.RelayDeadLetterQueueUrl) : Promise.resolve(0),
    ]);
    console.log(`\nThe SQS queue holds ${waiting} messages and its dead-letter queue ${dead}. Some may be older than the`);
    console.log('start time, so the Kinesis queue won\'t deliver them. Keep the SQS consumer until both are empty.');
  } finally {
    dynamo.destroy();
    sqs.destroy();
    kinesis.destroy();
  }
}

main().catch((error) => {
  console.error(`migrate:relay:plan failed: ${errorMessage(error)}`);
  process.exitCode = 1;
});
