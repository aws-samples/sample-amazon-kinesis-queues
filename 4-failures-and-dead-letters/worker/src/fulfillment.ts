import { randomInt, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  ConditionalCheckFailedException, DynamoDBClient, GetItemCommand, PutItemCommand,
} from '@aws-sdk/client-dynamodb';
import { Catalog } from './catalog';
import type { WorkerConfig } from './config';
import { TASK_ID } from './log';
import type { OrderPlaced } from '../../../shared/src/orders';

export class TransientError extends Error {
  override name = 'TransientError';
}

/** The warehouse API is down. Every order fails until it's back; nothing is wrong with the order. */
export class WarehouseUnavailableError extends TransientError {
  override name = 'WarehouseUnavailableError';
}

/** The warehouse will never accept this order as it stands. Retrying doesn't help. */
export class PermanentError extends Error {
  override name = 'PermanentError';
}

export type FulfillmentOutcome =
  | { status: 'FULFILLED'; shipmentId: string; processingMs: number }
  /** Already fulfilled, by an earlier delivery of this order or by another task. */
  | { status: 'DUPLICATE' };

/**
 * Fulfills one order at most once, even when the queue delivers it more than once.
 *
 * The orders table is the source of truth. Each fulfilled order is written with a
 * conditional put on orderId, so only the first writer wins. The cheap GetItem
 * first skips the warehouse call for orders that are already done; the conditional
 * put closes the race when two tasks hold the same order at the same time.
 *
 * In a real system the warehouse call would also take orderId as its idempotency
 * key, because it runs before the conditional put.
 */
export class Fulfillment {
  private readonly catalog: Catalog;

  constructor(private readonly dynamo: DynamoDBClient, private readonly config: WorkerConfig) {
    this.catalog = new Catalog(config.region, config.catalogParameter, config.warehouseParameter);
  }

  async fulfill(order: OrderPlaced, receiveCount: number): Promise<FulfillmentOutcome> {
    const existing = await this.dynamo.send(new GetItemCommand({
      TableName: this.config.ordersTable,
      Key: { orderId: { S: order.orderId } },
      ProjectionExpression: 'orderId',
      ConsistentRead: true,
    }));
    if (existing.Item) return { status: 'DUPLICATE' };

    const started = Date.now();
    const shipmentId = await this.bookWarehouse(order, receiveCount);
    const processingMs = Date.now() - started;

    try {
      await this.dynamo.send(new PutItemCommand({
        TableName: this.config.ordersTable,
        Item: {
          orderId: { S: order.orderId },
          customerId: { S: order.customerId },
          shipmentId: { S: shipmentId },
          itemCount: { N: String(order.items.reduce((n, i) => n + i.qty, 0)) },
          total: { N: String(order.total) },
          currency: { S: order.currency ?? 'EUR' },
          orderCreatedAt: { S: order.createdAt ?? '' },
          fulfilledAt: { S: new Date().toISOString() },
          fulfilledBy: { S: TASK_ID },
          receiveCount: { N: String(receiveCount) },
        },
        ConditionExpression: 'attribute_not_exists(orderId)',
      }));
    } catch (error) {
      if (error instanceof ConditionalCheckFailedException) return { status: 'DUPLICATE' };
      throw error;
    }
    return { status: 'FULFILLED', shipmentId, processingMs };
  }

  /**
   * Stand-in for reserving stock and booking a shipment with an external warehouse API.
   * It fails in the three ways the scenario demonstrates:
   * - transient: a timeout on the first delivery of orders marked `transient-once`,
   *   which succeeds when the queue redelivers the order;
   * - outage: while the warehouse status is "down", every order fails. Each one is
   *   redelivered until it becomes a dead-letter record, and is restored once it's back up;
   * - permanent: a product the catalog lists as discontinued, which fails on every
   *   delivery until an operator fixes the catalog.
   */
  private async bookWarehouse(order: OrderPlaced, receiveCount: number): Promise<string> {
    const { processingMsMin: min, processingMsMax: max } = this.config;
    await sleep(max > min ? randomInt(min, max + 1) : min);
    if (!(await this.catalog.warehouseUp())) {
      throw new WarehouseUnavailableError(`warehouse API unavailable for ${order.orderId}`);
    }
    const discontinuedSkus = await this.catalog.discontinuedSkus();
    const discontinued = order.items.find((i) => discontinuedSkus.has(i.sku));
    if (discontinued) {
      throw new PermanentError(`warehouse rejected ${order.orderId}: ${discontinued.sku} is no longer stocked`);
    }
    if (order.simulate === 'transient-once' && receiveCount <= 1) {
      throw new TransientError(`warehouse API timed out for ${order.orderId}`);
    }
    return `shp-${randomUUID().slice(0, 12)}`;
  }
}
