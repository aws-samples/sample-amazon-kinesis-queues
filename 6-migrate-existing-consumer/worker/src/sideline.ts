// The sideline consumer: part of the KCL setup, removed by the migration.
//
// The KCL consumer parks orders it gives up on in an SQS queue. They can't go back
// onto the stream, because every consumer of the stream would process them again,
// so this separate consumer, a Lambda function, works through the queue. Orders that
// fail here too end up in a second dead-letter queue, behind this one.
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { readFulfillmentConfig } from './config';
import { Fulfillment } from './fulfillment';
import { errorMessage, log, setConsumerLabel } from './log';
import { parseRecord } from '../../../shared/src/orders';

/** The subset of the SQS event that this function uses. */
interface SqsEvent {
  Records: Array<{ messageId: string; body: string; attributes?: { ApproximateReceiveCount?: string } }>;
}

interface SqsBatchResponse {
  batchItemFailures: Array<{ itemIdentifier: string }>;
}

/** What the KCL consumer writes for each order it sidelines: the record plus an audit trail. */
interface SidelinedRecord {
  data: string;
  partitionKey?: string;
  shardId?: string;
  sequenceNumber?: string;
  attempts?: number;
  reason?: string;
  sidelinedBy?: string;
  sidelinedAt?: string;
}

setConsumerLabel('sideline');
const config = readFulfillmentConfig();
const fulfillment = new Fulfillment(new DynamoDBClient({ region: config.region }), config, 'sideline');

export async function handler(event: SqsEvent): Promise<SqsBatchResponse> {
  const failures: SqsBatchResponse['batchItemFailures'] = [];
  await Promise.all(event.Records.map(async (message) => {
    const receiveCount = Number(message.attributes?.ApproximateReceiveCount ?? 1);
    let envelope: SidelinedRecord;
    try {
      envelope = JSON.parse(message.body) as SidelinedRecord;
    } catch {
      log('ERROR', 'UNREADABLE_SIDELINE_MESSAGE', { messageId: message.messageId });
      failures.push({ itemIdentifier: message.messageId });
      return;
    }
    const parsed = parseRecord(Buffer.from(envelope.data ?? '', 'base64'));
    if (parsed.kind !== 'order') {
      // A malformed event can't be fulfilled here either. Fail it until SQS moves it to
      // the sideline queue's own dead-letter queue, for someone to look at.
      log('ERROR', 'NOT_AN_ORDER', { messageId: message.messageId, shardId: envelope.shardId, sequenceNumber: envelope.sequenceNumber, receiveCount });
      failures.push({ itemIdentifier: message.messageId });
      return;
    }
    try {
      const result = await fulfillment.fulfill(parsed.order, receiveCount);
      log('INFO', result.status, {
        orderId: parsed.order.orderId,
        receiveCount,
        sidelinedBy: envelope.sidelinedBy,
        sidelinedReason: envelope.reason,
        delayMs: envelope.sidelinedAt ? Date.now() - Date.parse(envelope.sidelinedAt) : undefined,
      });
    } catch (error) {
      log('WARN', 'FULFILLMENT_FAILED', { orderId: parsed.order.orderId, receiveCount, error: errorMessage(error) });
      failures.push({ itemIdentifier: message.messageId });
    }
  }));
  return { batchItemFailures: failures };
}
