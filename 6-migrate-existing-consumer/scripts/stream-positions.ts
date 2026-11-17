// Helpers both cutover plans use to read positions in the stream.
import {
  GetRecordsCommand, GetShardIteratorCommand, ListShardsCommand, ShardIteratorType, type KinesisClient,
} from '@aws-sdk/client-kinesis';

/** Pages followed before giving up on finding a record. Each page reads one record. */
const MAX_PAGES = 20;

/** Every shard that is still open (has no ending sequence number). */
export async function listOpenShards(kinesis: KinesisClient, streamArn: string): Promise<string[]> {
  const open: string[] = [];
  let NextToken: string | undefined;
  do {
    // StreamARN and NextToken can't be sent together.
    const page = await kinesis.send(new ListShardsCommand(NextToken ? { NextToken } : { StreamARN: streamArn }));
    for (const shard of page.Shards ?? []) {
      if (shard.ShardId && !shard.SequenceNumberRange?.EndingSequenceNumber) open.push(shard.ShardId);
    }
    NextToken = page.NextToken;
  } while (NextToken);
  return open.sort();
}

/**
 * What an iterator finds first:
 * - record: the arrival time of the first record it returns;
 * - caught-up: no record yet, and the iterator is at the tip of the shard;
 * - unknown: no record within MAX_PAGES pages, and not at the tip. Don't guess from this.
 */
export type FirstArrival =
  | { kind: 'record'; at: Date }
  | { kind: 'caught-up' }
  | { kind: 'unknown'; reason: string };

export interface IteratorStart {
  type: ShardIteratorType;
  sequenceNumber?: string;
  timestamp?: Date;
}

export async function firstArrival(kinesis: KinesisClient, streamArn: string, shardId: string,
  start: IteratorStart): Promise<FirstArrival> {
  const { ShardIterator } = await kinesis.send(new GetShardIteratorCommand({
    StreamARN: streamArn,
    ShardId: shardId,
    ShardIteratorType: start.type,
    StartingSequenceNumber: start.sequenceNumber,
    Timestamp: start.timestamp,
  }));
  let iterator = ShardIterator;
  // A fresh iterator can return empty pages before it reaches data; follow it.
  for (let page = 0; iterator && page < MAX_PAGES; page += 1) {
    const out = await kinesis.send(new GetRecordsCommand({ StreamARN: streamArn, ShardIterator: iterator, Limit: 1 }));
    const record = out.Records?.[0];
    if (record?.ApproximateArrivalTimestamp) return { kind: 'record', at: record.ApproximateArrivalTimestamp };
    if (out.MillisBehindLatest === 0) return { kind: 'caught-up' };
    iterator = out.NextShardIterator;
  }
  return iterator
    ? { kind: 'unknown', reason: `no record within ${MAX_PAGES} pages, and not at the tip of the shard` }
    : { kind: 'caught-up' }; // a closed shard that ends here
}
