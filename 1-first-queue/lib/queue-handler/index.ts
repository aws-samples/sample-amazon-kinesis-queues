// Lambda code behind the Custom::KinesisQueue resource (see ../kinesis-queue.ts).
// CloudFormation has no resource type for Kinesis queues yet, so this code calls
// the queue API for it. `onEvent` makes the change; `isComplete` is polled until
// the queue reaches the state CloudFormation is waiting for.
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  DescribeQueueCommand,
  DescribeStreamSummaryCommand,
  KinesisClient,
  TagResourceCommand,
  UntagResourceCommand,
  UpdateQueueCommand,
} from '@aws-sdk/client-kinesis';

const client = new KinesisClient({});

/** CloudFormation sends every property as a string, including numbers. */
interface QueueProperties {
  QueueName?: string;
  StreamArn?: string;
  StartingPosition?: string;
  VisibilityTimeoutSeconds?: string;
  Tags?: Record<string, string>;
}

interface Event {
  RequestType: 'Create' | 'Update' | 'Delete';
  PhysicalResourceId?: string;
  ResourceProperties: QueueProperties;
  OldResourceProperties?: QueueProperties;
}

const QUEUE_ARN = /^arn:[^:]+:kinesis:[^:]+:\d{12}:queue\/.+$/;

function errorName(error: unknown): string | undefined {
  return error instanceof Error ? error.name : undefined;
}

function queueData(queueArn: string, props: QueueProperties) {
  return { QueueArn: queueArn, QueueName: props.QueueName, QueueId: queueArn.split(':queue/')[1] };
}

function parse(props: QueueProperties) {
  const { QueueName, StreamArn, StartingPosition } = props;
  if (!QueueName || !StreamArn || !StartingPosition) throw new Error('QueueName, StreamArn and StartingPosition are required.');
  if (StartingPosition !== 'TRIM_HORIZON' && StartingPosition !== 'LATEST') {
    throw new Error(`StartingPosition must be TRIM_HORIZON or LATEST, not ${StartingPosition}.`);
  }
  const visibility = Number(props.VisibilityTimeoutSeconds);
  if (!Number.isInteger(visibility)) throw new Error('VisibilityTimeoutSeconds must be a whole number.');
  return { queueName: QueueName, streamArn: StreamArn, startingPosition: StartingPosition as 'TRIM_HORIZON' | 'LATEST', visibility, tags: props.Tags ?? {} };
}

/**
 * A queue works only on an On-demand stream, so fail the create with a clear message.
 * An encrypted stream is fine: CreateQueue needs no KMS permissions, and the consumer
 * gets kms:Decrypt on the stream's key (KinesisQueue.grantConsume).
 */
async function checkStream(streamArn: string) {
  const { StreamDescriptionSummary: s } = await client.send(new DescribeStreamSummaryCommand({ StreamARN: streamArn }));
  if (!s) throw new Error(`Stream not found: ${streamArn}`);
  if (s.StreamModeDetails?.StreamMode !== 'ON_DEMAND') {
    throw new Error(`Stream ${streamArn} is not On-demand. Queues need an On-demand stream.`);
  }
}

async function createQueue(props: QueueProperties) {
  const { queueName, streamArn, startingPosition, visibility, tags } = parse(props);
  await checkStream(streamArn);

  let created;
  try {
    created = await client.send(new CreateQueueCommand({
      QueueName: queueName,
      StreamConfigurationList: [{ StreamARN: streamArn, StartingPosition: { Type: startingPosition } }],
      QueueAttributes: { VisibilityTimeoutSeconds: visibility },
      // CreateQueue rejects an empty Tags map, so leave it out when there are no tags.
      ...(Object.keys(tags).length > 0 ? { Tags: tags } : {}),
    }));
  } catch (error) {
    // This stack never adopts an existing queue: deleting the stack would delete it.
    if (errorName(error) === 'ResourceInUseException') {
      throw new Error(`Could not create queue "${queueName}": queue names are unique per account and Region, and this one is taken. Choose another queueName.`);
    }
    throw error;
  }
  const { QueueDescription } = created;
  const queueArn = QueueDescription?.QueueARN;
  if (!queueArn) throw new Error('CreateQueue returned no queue ARN.');
  return { PhysicalResourceId: queueArn, Data: queueData(queueArn, props) };
}

async function updateTags(queueArn: string, oldTags: Record<string, string>, newTags: Record<string, string>) {
  const removed = Object.keys(oldTags).filter((key) => !(key in newTags));
  const changed = Object.fromEntries(Object.entries(newTags).filter(([key, value]) => oldTags[key] !== value));
  if (removed.length > 0) await client.send(new UntagResourceCommand({ ResourceARN: queueArn, TagKeys: removed }));
  if (Object.keys(changed).length > 0) await client.send(new TagResourceCommand({ ResourceARN: queueArn, Tags: changed }));
}

async function updateQueue(event: Event) {
  const queueArn = event.PhysicalResourceId!;
  const next = parse(event.ResourceProperties);

  // Compare with the real queue, not with the previous properties: when CloudFormation
  // rolls back a failed update it sends the old and new properties swapped, and this
  // way the rollback succeeds.
  const { QueueDescription: actual } = await client.send(new DescribeQueueCommand({ QueueARN: queueArn }));
  const stream = actual?.StreamConfigurationList?.[0];
  if (actual?.QueueName !== next.queueName || stream?.StreamARN !== next.streamArn || stream?.StartingPosition?.Type !== next.startingPosition) {
    throw new Error(
      "A queue's name, stream and starting position can't change in place. Give the KinesisQueue a new construct id "
      + 'and a new queueName, so CloudFormation creates a new queue and deletes the old one.',
    );
  }

  if (actual?.QueueAttributes?.VisibilityTimeoutSeconds !== next.visibility) {
    await client.send(new UpdateQueueCommand({ QueueARN: queueArn, QueueAttributes: { VisibilityTimeoutSeconds: next.visibility } }));
  }
  await updateTags(queueArn, parse(event.OldResourceProperties ?? event.ResourceProperties).tags, next.tags);
  return { PhysicalResourceId: queueArn, Data: queueData(queueArn, event.ResourceProperties) };
}

async function deleteQueue(event: Event) {
  const queueArn = event.PhysicalResourceId ?? '';
  // A failed create leaves a placeholder ID, not a queue ARN: nothing to delete.
  if (QUEUE_ARN.test(queueArn)) {
    try {
      await client.send(new DeleteQueueCommand({ QueueARN: queueArn }));
    } catch (error) {
      // At most 5 queues per account can be creating or deleting at once. isComplete retries.
      const name = errorName(error);
      if (name !== 'ResourceNotFoundException' && name !== 'LimitExceededException') throw error;
    }
  }
  return { PhysicalResourceId: event.PhysicalResourceId };
}

export async function onEvent(event: Event) {
  console.log(`${event.RequestType} ${event.PhysicalResourceId ?? ''}`);
  switch (event.RequestType) {
    case 'Create': return createQueue(event.ResourceProperties);
    case 'Update': return updateQueue(event);
    case 'Delete': return deleteQueue(event);
  }
}

/** Polled by the provider framework until it returns IsComplete: true. */
export async function isComplete(event: Event) {
  const queueArn = event.PhysicalResourceId ?? '';
  if (!QUEUE_ARN.test(queueArn)) return { IsComplete: true };
  try {
    const { QueueDescription } = await client.send(new DescribeQueueCommand({ QueueARN: queueArn }));
    const status = QueueDescription?.QueueStatus;
    console.log(`${queueArn} is ${status}`);
    if (event.RequestType !== 'Delete') {
      // A create goes CREATING -> ACTIVE, an update UPDATING -> ACTIVE. Right after
      // UpdateQueue the status may still read ACTIVE for a moment (eventual consistency). Harmless.
      return { IsComplete: status === 'ACTIVE' };
    }
    if (status === 'ACTIVE') {
      // The delete never started (onEvent hit the limit on queues changing at once). Try again.
      try {
        await client.send(new DeleteQueueCommand({ QueueARN: queueArn }));
      } catch (error) {
        const name = errorName(error);
        if (name !== 'LimitExceededException' && name !== 'ResourceInUseException' && name !== 'ResourceNotFoundException') throw error;
      }
    }
    return { IsComplete: false };
  } catch (error) {
    // For a delete, "not found" is the goal.
    if (event.RequestType === 'Delete' && errorName(error) === 'ResourceNotFoundException') return { IsComplete: true };
    throw error;
  }
}
