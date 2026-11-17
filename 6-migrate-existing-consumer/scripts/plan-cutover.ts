// Picks the starting position for the queue that replaces the KCL consumer, so the
// cutover skips no records.
//
// A queue starts at TRIM_HORIZON, LATEST or AT_TIMESTAMP, for the whole stream. KCL
// keeps one checkpoint per shard. This script lists the stream's open shards, reads
// every checkpoint from the lease table, looks up when the checkpointed record arrived
// in the stream, and takes the oldest of those times, minus a margin. Starting the queue there means every record
// KCL hasn't finished is delivered by the queue too. Records between that time and a
// shard's checkpoint go to both consumers, and the orders table makes the second
// delivery a no-op.
//
// LATEST would be wrong: records KCL hasn't reached yet when you stop it would be
// processed by neither consumer.
//
// The plan fails instead of guessing: when an open shard has no lease (for example a
// child shard right after the stream resharded), when a position can't be read, or
// when no shard gives a time at all.
import { parseArgs } from 'node:util';
import { DynamoDBClient, ScanCommand, type AttributeValue } from '@aws-sdk/client-dynamodb';
import { ShardIteratorType, type KinesisClient } from '@aws-sdk/client-kinesis';
import { GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { createClient, errorMessage } from '../../shared/src/common';
import { QUEUE_NAME } from '../lib/config';
import { readOutputs, scriptArgs } from './outputs';
import { firstArrival, listOpenShards, type FirstArrival } from './stream-positions';

const USAGE = `Usage: npm run migrate:plan -- [options]

  --margin SECONDS    Start this long before the oldest checkpoint (default: 60)
  --stack-name NAME   Stack in 6-migrate-existing-consumer/cdk-outputs.json (default: the only one)
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain

Reads the KCL lease table and prints the queue:create command with an AT_TIMESTAMP
starting position that covers every record the KCL consumer hasn't checkpointed.`;

interface Lease {
  shardId: string;
  /** Undefined: the lease exists, but KCL hasn't written a checkpoint attribute yet. */
  checkpoint?: string;
  owner?: string;
  ownerSwitches: number;
}

interface ShardPosition {
  shardId: string;
  lease?: Lease;
  /** When the first record KCL hasn't finished arrived. Undefined: nothing left to process. */
  from?: Date;
  note: string;
  /** Set when the position can't be established; the plan then fails. */
  problem?: string;
}

async function readLeases(dynamo: DynamoDBClient, table: string): Promise<Lease[]> {
  const leases: Lease[] = [];
  let ExclusiveStartKey: Record<string, AttributeValue> | undefined;
  do {
    const page = await dynamo.send(new ScanCommand({ TableName: table, ConsistentRead: true, ExclusiveStartKey }));
    for (const item of page.Items ?? []) {
      const key = item.leaseKey?.S ?? '';
      // KCL 3.5 can keep other metadata in the same table; only shard leases matter here.
      if (!key.startsWith('shardId-')) continue;
      leases.push({
        shardId: key,
        checkpoint: item.checkpoint?.S,
        owner: item.leaseOwner?.S,
        ownerSwitches: Number(item.ownerSwitchesSinceCheckpoint?.N ?? 0),
      });
    }
    ExclusiveStartKey = page.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return leases.sort((a, b) => a.shardId.localeCompare(b.shardId));
}

/** Turns what the iterator found into a shard position. */
function fromArrival(shardId: string, lease: Lease | undefined, found: FirstArrival, note: string): ShardPosition {
  if (found.kind === 'record') return { shardId, lease, from: found.at, note };
  if (found.kind === 'caught-up') return { shardId, lease, note: `${note}: nothing left to process` };
  return { shardId, lease, note, problem: found.reason };
}

async function position(kinesis: KinesisClient, streamArn: string, shardId: string, lease: Lease | undefined): Promise<ShardPosition> {
  if (!lease) {
    // An open shard nobody holds yet, for example a child shard right after a reshard.
    // Its records may be older than every checkpoint, so don't plan around it.
    return { shardId, note: 'no lease', problem: 'open shard with no lease. Wait until the KCL consumer holds every shard' };
  }
  const oldest = () => firstArrival(kinesis, streamArn, shardId, { type: ShardIteratorType.TRIM_HORIZON });
  switch (lease.checkpoint) {
    case undefined:
      return { shardId, lease, note: 'no checkpoint', problem: 'lease with no checkpoint yet. Wait for KCL to checkpoint every shard' };
    case 'SHARD_END':
      return { shardId, lease, note: 'closed and fully processed' };
    case 'TRIM_HORIZON':
    case 'LATEST':
    case 'AT_TIMESTAMP':
      // No checkpoint yet: the first batch may still be in progress. Start at the
      // shard's oldest record to be safe; the orders table absorbs the extra deliveries.
      return fromArrival(shardId, lease, await oldest(), `no checkpoint yet (${lease.checkpoint}): from the oldest record`);
    default:
      try {
        const found = await firstArrival(kinesis, streamArn, shardId,
          { type: ShardIteratorType.AT_SEQUENCE_NUMBER, sequenceNumber: lease.checkpoint });
        return fromArrival(shardId, lease, found, 'checkpointed record');
      } catch (error) {
        // The checkpointed record may be past the stream's retention.
        return fromArrival(shardId, lease, await oldest(), `checkpoint not readable (${errorMessage(error)}): from the oldest record`);
      }
  }
}

async function sidelineDepth(sqs: SQSClient, url: string | undefined): Promise<number | undefined> {
  if (!url) return undefined;
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
  const outputs = readOutputs(args['stack-name']);
  if (!outputs.KclLeaseTableName) throw new Error('The stack has no KCL lease table: plan the cutover while the KCL consumer is still deployed.');

  const credentials = fromNodeProviderChain(args.profile ? { profile: args.profile } : {});
  const dynamo = new DynamoDBClient({ region: args.region, credentials });
  const sqs = new SQSClient({ region: args.region, credentials });
  const kinesis = createClient({ region: args.region, profile: args.profile });
  try {
    const [leases, openShards] = await Promise.all([
      readLeases(dynamo, outputs.KclLeaseTableName), listOpenShards(kinesis, outputs.StreamArn),
    ]);
    if (leases.length === 0) throw new Error(`No shard leases in ${outputs.KclLeaseTableName} yet. Is the KCL consumer running?`);
    // Every leased shard, plus every open shard, so a shard without a lease isn't missed.
    const byShard = new Map(leases.map((l) => [l.shardId, l]));
    const shardIds = [...new Set([...byShard.keys(), ...openShards])].sort();
    const positions = await Promise.all(shardIds.map((id) => position(kinesis, outputs.StreamArn, id, byShard.get(id))));

    console.log(`KCL application ${outputs.KclLeaseTableName}: ${leases.length} shard leases, ${openShards.length} open shards\n`);
    console.log('  shard                   owner     first unfinished record       behind now');
    for (const p of positions) {
      const age = p.from ? `${Math.round((Date.now() - p.from.getTime()) / 1000)} s` : '-';
      const note = p.problem ? `${p.note}: ${p.problem}` : p.note;
      console.log(`  ${p.shardId.padEnd(22)}  ${(p.lease?.owner ?? '-').padEnd(8)}  ${(p.from?.toISOString() ?? '-').padEnd(28)}  ${age.padStart(10)}   ${note}`);
    }

    const problems = positions.filter((p) => p.problem);
    if (problems.length > 0) {
      throw new Error(`can't place ${problems.map((p) => p.shardId).join(', ')}. No queue command printed; fix that and run the plan again.`);
    }
    const times = positions.map((p) => p.from).filter((d): d is Date => d !== undefined);
    if (times.length === 0) {
      // Every shard is caught up or closed, which also happens when nothing was written
      // yet. Starting at "now" would be a guess, so stop instead.
      throw new Error('no shard has a record left to process, so there is no position to start from. Place some orders, wait for KCL to checkpoint, and run the plan again.');
    }
    const oldest = new Date(Math.min(...times.map((d) => d.getTime())));
    const start = new Date(oldest.getTime() - marginSeconds * 1000);
    console.log(`\nOldest checkpoint: ${oldest.toISOString()}. Start the queue ${marginSeconds} s before it, at ${start.toISOString()}:\n`);
    // The same stream the checkpoints belong to, even if it isn't the Quickstart's.
    console.log(`  npm run queue:create -- --queue-name ${QUEUE_NAME} --stream-arn ${outputs.StreamArn} \\`);
    console.log(`    --starting-position AT_TIMESTAMP --timestamp ${start.toISOString()}\n`);
    console.log('Records between that time and each checkpoint reach both consumers. The orders table makes the');
    console.log('second delivery a no-op, so no order ships twice and none is skipped.');

    const [waiting, dead] = await Promise.all([
      sidelineDepth(sqs, outputs.SidelineQueueUrl), sidelineDepth(sqs, outputs.SidelineDeadLetterQueueUrl),
    ]);
    if ((waiting ?? 0) + (dead ?? 0) > 0) {
      console.log(`\nThe sideline queue holds ${waiting ?? 0} orders and its dead-letter queue ${dead ?? 0}. The queue won't`);
      console.log('deliver those again if they arrived before the start time. Keep the sideline consumer until it has');
      console.log('worked through them, and resolve the dead-letter records before you decommission the KCL resources.');
    }
  } finally {
    dynamo.destroy();
    sqs.destroy();
    kinesis.destroy();
  }
}

main().catch((error) => {
  console.error(`migrate:plan failed: ${errorMessage(error)}`);
  process.exitCode = 1;
});
