import { randomInt, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  ConditionalCheckFailedException, DynamoDBClient, GetItemCommand, PutItemCommand, UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';
import type { FulfillmentConfig } from './config';
import { TASK_ID } from './log';
import type { OrderPlaced } from '../../../shared/src/orders';

/** The warehouse can't take this order right now, or ever. */
export class WarehouseError extends Error {
  override name = 'WarehouseError';
}

export type FulfillmentOutcome =
  | { status: 'FULFILLED'; shipmentId: string; processingMs: number }
  /** Already in the orders table when this delivery arrived. The warehouse wasn't called. */
  | { status: 'DUPLICATE_SKIPPED' }
  /** Another delivery, possibly by the other consumer, finished first while this one worked. */
  | { status: 'DUPLICATE_DISCARDED'; processingMs: number };

/** Where the order sits in the stream. The relay cutover plan reads it from the orders table. */
export interface StreamOrigin {
  shardId: string;
  sequenceNumber: string;
  arrivedAt: Date;
}

/**
 * Fulfills each order at most once: the business logic the migration keeps. The KCL
 * consumer (kcl-consumer/.../OrderFulfillment.java) has the same rules and uses the same
 * orders table, which is what lets both run side by side during the cutover.
 *
 * - A consistent GetItem skips orders that are already done.
 * - A conditional put on orderId records the fulfillment. When both consumers work on the
 *   same order at the same time, only the first put succeeds.
 *
 * The warehouse call runs before the conditional put, so in that race it runs twice. A
 * real warehouse API should take orderId as its idempotency key for that reason.
 */
export class Fulfillment {
  constructor(
    private readonly dynamo: DynamoDBClient,
    private readonly config: FulfillmentConfig,
    /** Recorded on each order, so the status script can tell who fulfilled it. */
    private readonly consumer: 'queue' | 'sideline' | 'sqs',
  ) {}

  async fulfill(order: OrderPlaced, receiveCount: number, origin?: StreamOrigin): Promise<FulfillmentOutcome> {
    const existing = await this.dynamo.send(new GetItemCommand({
      TableName: this.config.ordersTable,
      Key: { orderId: { S: order.orderId } },
      ProjectionExpression: 'orderId',
      ConsistentRead: true,
    }));
    if (existing.Item) {
      await this.countDuplicate(order.orderId, 'duplicatesSkipped');
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
          orderCreatedAt: { S: order.createdAt ?? '' },
          fulfilledAt: { S: new Date().toISOString() },
          fulfilledBy: { S: TASK_ID },
          consumer: { S: this.consumer },
          attempt: { N: String(receiveCount) },
          processingMs: { N: String(processingMs) },
          ...(origin ? {
            shardId: { S: origin.shardId },
            sequenceNumber: { S: origin.sequenceNumber },
            arrivedAt: { S: origin.arrivedAt.toISOString() },
          } : {}),
        },
        ConditionExpression: 'attribute_not_exists(orderId)',
      }));
    } catch (error) {
      if (!(error instanceof ConditionalCheckFailedException)) throw error;
      await this.countDuplicate(order.orderId, 'duplicatesDiscarded');
      return { status: 'DUPLICATE_DISCARDED', processingMs };
    }
    return { status: 'FULFILLED', shipmentId, processingMs };
  }

  /**
   * Stand-in for reserving stock and booking a shipment with an external warehouse API.
   * An order for a product the warehouse no longer stocks fails on every attempt.
   */
  private async bookWarehouse(order: OrderPlaced): Promise<string> {
    const { processingMsMin: min, processingMsMax: max } = this.config;
    await sleep(max > min ? randomInt(min, max + 1) : min);
    const discontinued = order.items.find((i) => this.config.discontinuedSkus.has(i.sku));
    if (discontinued) throw new WarehouseError(`warehouse rejected ${order.orderId}: ${discontinued.sku} is no longer stocked`);
    return `shp-${randomUUID().slice(0, 12)}`;
  }

  /** Demo bookkeeping, so the status script can show how many duplicates the table absorbed. */
  private async countDuplicate(orderId: string, counter: 'duplicatesSkipped' | 'duplicatesDiscarded'): Promise<void> {
    try {
      await this.dynamo.send(new UpdateItemCommand({
        TableName: this.config.ordersTable,
        Key: { orderId: { S: orderId } },
        UpdateExpression: 'ADD #counter :one SET lastDuplicateAt = :now, lastDuplicateBy = :by',
        ConditionExpression: 'attribute_exists(orderId)',
        ExpressionAttributeNames: { '#counter': counter },
        ExpressionAttributeValues: {
          ':one': { N: '1' }, ':now': { S: new Date().toISOString() }, ':by': { S: `${this.consumer}:${TASK_ID}` },
        },
      }));
    } catch {
      // Best effort: a failed counter mustn't fail the record.
    }
  }
}
