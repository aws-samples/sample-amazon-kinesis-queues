/** The OrderPlaced event the e-commerce service writes to the stream. Shared with the producer. */
export interface OrderItem {
  sku: string;
  qty: number;
  price: number;
}

export interface OrderPlaced {
  eventType: 'OrderPlaced';
  orderId: string;
  customerId: string;
  items: OrderItem[];
  total: number;
  currency: string;
  createdAt: string;
  /** Demo only: makes a scenario's worker take one of its failure or delivery paths. */
  simulate?: Simulation;
}

/**
 * - `transient-once` (scenario 4): the warehouse times out on the first delivery and
 *   succeeds on the next.
 * - `ack-lost` (scenario 2): the worker fulfills the order but fails before it
 *   acknowledges, so the queue delivers the order again.
 * - `slow` (scenario 2): fulfillment takes longer than the visibility timeout; the
 *   worker extends visibility while it works.
 * - `slow-no-heartbeat` (scenario 2): the same, without extending visibility.
 */
export type Simulation = 'transient-once' | 'ack-lost' | 'slow' | 'slow-no-heartbeat';

export type ParsedRecord =
  | { kind: 'order'; order: OrderPlaced }
  /** Valid JSON with a different eventType: not this consumer's business. */
  | { kind: 'other'; eventType: string }
  /** Can never succeed, however often it is retried. */
  | { kind: 'invalid'; reason: string };

const decoder = new TextDecoder('utf-8', { fatal: true });

export function parseRecord(data: Uint8Array | undefined): ParsedRecord {
  let value: unknown;
  try {
    value = JSON.parse(decoder.decode(data ?? new Uint8Array()));
  } catch {
    return { kind: 'invalid', reason: 'not valid JSON' };
  }
  if (typeof value !== 'object' || value === null) return { kind: 'invalid', reason: 'not a JSON object' };
  const event = value as Record<string, unknown>;
  if (event.eventType !== 'OrderPlaced') return { kind: 'other', eventType: String(event.eventType ?? 'unknown') };

  if (typeof event.orderId !== 'string' || event.orderId === '') return { kind: 'invalid', reason: 'missing orderId' };
  if (typeof event.customerId !== 'string' || event.customerId === '') return { kind: 'invalid', reason: 'missing customerId' };
  if (!Array.isArray(event.items) || event.items.length === 0) return { kind: 'invalid', reason: 'no items' };
  for (const item of event.items as unknown[]) {
    const i = item as Partial<OrderItem>;
    if (typeof i?.sku !== 'string' || !Number.isInteger(i.qty) || (i.qty ?? 0) <= 0 || typeof i.price !== 'number') {
      return { kind: 'invalid', reason: 'malformed item' };
    }
  }
  if (typeof event.total !== 'number') return { kind: 'invalid', reason: 'missing total' };
  return { kind: 'order', order: event as unknown as OrderPlaced };
}
