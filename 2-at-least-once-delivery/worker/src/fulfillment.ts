import { randomInt, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  ConditionalCheckFailedException, DynamoDBClient, GetItemCommand, PutItemCommand, UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';
import type { WorkerConfig } from './config';
import { TASK_ID } from './log';
import type { OrderPlaced } from '../../../shared/src/orders';

export type FulfillmentOutcome =
  | { status: 'FULFILLED'; shipmentId: string; processingMs: number }
  /** Already fulfilled when this delivery arrived. The warehouse wasn't called. */
  | { status: 'DUPLICATE_SKIPPED' }
  /**
   * Another delivery of the same order finished first while this one was still
   * working. The warehouse was called twice; the table kept one fulfillment.
   */
  | { status: 'DUPLICATE_DISCARDED'; processingMs: number };

/**
 * Fulfills each order once, however many times the queue delivers it.
 *
 * The orders table is the source of truth, keyed on orderId:
 * 1. A consistent GetItem skips the warehouse call for orders that are already done.
 *    That covers most redeliveries: an acknowledgment that didn't arrive, or a
 *    duplicate record from a producer retry.
 * 2. A conditional put (attribute_not_exists) records the fulfillment. When two
 *    deliveries of the same order are processed at the same time, only the first
 *    put succeeds, so the second can't fulfill the order again.
 *
 * In a real system the warehouse would also take orderId as its idempotency key,
 * because the call runs before the conditional put. That's the step this table
 * can't protect.
 *
 * Duplicate deliveries are counted on the order's item, so the demo's status script
 * can show how many the table absorbed.
 */
export class Fulfillment {
  constructor(private readonly dynamo: DynamoDBClient, private readonly config: WorkerConfig) {}

  async fulfill(order: OrderPlaced, receiveCount: number): Promise<FulfillmentOutcome> {
    const existing = await this.dynamo.send(new GetItemCommand({
      TableName: this.config.ordersTable,
      Key: { orderId: { S: order.orderId } },
      ProjectionExpression: 'orderId',
      ConsistentRead: true,
    }));
    if (existing.Item) {
      await this.countDuplicate(order.orderId, 'duplicatesSkipped', receiveCount);
      return { status: 'DUPLICATE_SKIPPED' };
    }

    const started = Date.now();
    const shipmentId = await this.bookWarehouse(order);
    const processingMs = Date.now() - started;

    try {
      await this.dynamo.send(new PutItemCommand({
        TableName: this.config.ordersTable,
        Item: {
          orderId: { S: order.orderId },
          customerId: { S: order.customerId },
          shipmentId: { S: shipmentId },
          total: { N: String(order.total) },
          currency: { S: order.currency ?? 'EUR' },
          fulfilledAt: { S: new Date().toISOString() },
          fulfilledBy: { S: TASK_ID },
          receiveCount: { N: String(receiveCount) },
          processingMs: { N: String(processingMs) },
          ...(order.simulate ? { simulate: { S: order.simulate } } : {}),
        },
        ConditionExpression: 'attribute_not_exists(orderId)',
      }));
    } catch (error) {
      if (!(error instanceof ConditionalCheckFailedException)) throw error;
      await this.countDuplicate(order.orderId, 'duplicatesDiscarded', receiveCount);
      return { status: 'DUPLICATE_DISCARDED', processingMs };
    }
    return { status: 'FULFILLED', shipmentId, processingMs };
  }

  /** Demo bookkeeping only. Best effort: a failure here mustn't fail the record. */
  private async countDuplicate(orderId: string, counter: 'duplicatesSkipped' | 'duplicatesDiscarded', receiveCount: number) {
    try {
      await this.dynamo.send(new UpdateItemCommand({
        TableName: this.config.ordersTable,
        Key: { orderId: { S: orderId } },
        UpdateExpression: 'ADD #counter :one SET lastDuplicateAt = :now, lastDuplicateReceiveCount = :rc, lastDuplicateBy = :task',
        ConditionExpression: 'attribute_exists(orderId)',
        ExpressionAttributeNames: { '#counter': counter },
        ExpressionAttributeValues: {
          ':one': { N: '1' }, ':now': { S: new Date().toISOString() }, ':rc': { N: String(receiveCount) }, ':task': { S: TASK_ID },
        },
      }));
    } catch {
      // ignore
    }
  }

  /**
   * Stand-in for reserving stock and booking a shipment with an external warehouse
   * API. Orders marked `slow` or `slow-no-heartbeat` take longer than the visibility
   * timeout, on every delivery, like a large order or a slow downstream system.
   */
  private async bookWarehouse(order: OrderPlaced): Promise<string> {
    const { processingMsMin: min, processingMsMax: max, slowProcessingMs } = this.config;
    const slow = order.simulate === 'slow' || order.simulate === 'slow-no-heartbeat';
    await sleep(slow ? slowProcessingMs : max > min ? randomInt(min, max + 1) : min);
    return `shp-${randomUUID().slice(0, 12)}`;
  }
}
