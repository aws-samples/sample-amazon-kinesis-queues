import {
  UpdateQueueRecordsVisibilityCommand, type KinesisClient, type QueueRecord,
} from '@aws-sdk/client-kinesis';
import type { WorkerConfig } from './config';
import { errorMessage, log } from './log';
import { matchFailed } from '../../../shared/src/batch';

interface Held {
  record: QueueRecord;
  orderId?: string;
  receivedAt: number;
}

/**
 * Keeps the records of one batch hidden from other workers while this worker still
 * holds them.
 *
 * Every `heartbeatSeconds` it sets the visibility timeout of every held record back
 * to `visibilityTimeoutSeconds` with one UpdateQueueRecordsVisibility call (up to 10
 * entries). The new timeout counts from the call, not from the receive, so a record
 * stays hidden for as long as the heartbeat keeps running.
 *
 * Records stay held until they're acknowledged, not just until they're processed:
 * a finished record that waits for the batch's acknowledgment can expire too.
 */
export class Heartbeat {
  private readonly held = new Map<string, Held>();
  private readonly timer: NodeJS.Timeout;
  private sending = false;

  constructor(
    private readonly kinesis: KinesisClient,
    private readonly config: WorkerConfig,
    private readonly onExtended: (records: number) => void,
  ) {
    this.timer = setInterval(() => void this.beat(), config.heartbeatSeconds * 1000);
  }

  /** Starts extending a record. `key` must be unique within the batch. */
  hold(key: string, record: QueueRecord, orderId?: string): void {
    this.held.set(key, { record, orderId, receivedAt: Date.now() });
  }

  /** Stops extending a record. Unless it's acknowledged, it reappears when its timeout expires. */
  release(key: string): void {
    this.held.delete(key);
  }

  stop(): void {
    clearInterval(this.timer);
    this.held.clear();
  }

  private async beat(): Promise<void> {
    if (this.sending || this.held.size === 0) return;
    const now = Date.now();
    const entries: Array<[string, Held]> = [];
    for (const [key, h] of this.held) {
      if (now - h.receivedAt >= this.config.maxHoldSeconds * 1000) {
        // A handler this slow is probably stuck. Let the record go, so the queue
        // delivers it again and, after MaxReceiveCount receives, dead-letters it.
        this.held.delete(key);
        log('WARN', 'HOLD_LIMIT_REACHED', { orderId: h.orderId, heldSeconds: Math.round((now - h.receivedAt) / 1000) });
        continue;
      }
      entries.push([key, h]);
    }
    if (entries.length === 0) return;

    this.sending = true;
    const items = entries.map(([key, h]) => ({ key, h, ReceiptHandle: h.record.ReceiptHandle }));
    try {
      const { Failed } = await this.kinesis.send(new UpdateQueueRecordsVisibilityCommand({
        QueueARN: this.config.queueArn,
        Entries: items.map(({ ReceiptHandle }) => ({
          ReceiptHandle, VisibilityTimeoutSeconds: this.config.visibilityTimeoutSeconds,
        })),
      }));
      // HTTP 200 doesn't mean every entry succeeded. A failed entry usually means the
      // record already expired and may be with another worker now: stop extending it.
      // The orders table makes finishing it here safe.
      const failures = matchFailed(items, Failed);
      const failedKeys = new Set<string>();
      for (const { item, failure: f } of failures) {
        if (item) {
          failedKeys.add(item.key);
          this.held.delete(item.key);
        }
        log('WARN', 'VISIBILITY_EXTEND_FAILED', { orderId: item?.h.orderId, code: f.ErrorCode, message: f.ErrorMessage });
      }
      const extended = items.length - failures.length;
      this.onExtended(extended);
      log('INFO', 'VISIBILITY_EXTENDED', {
        records: extended,
        orderIds: items.filter((i) => !failedKeys.has(i.key)).map((i) => i.h.orderId).filter(Boolean),
        visibilityTimeoutSeconds: this.config.visibilityTimeoutSeconds,
      });
    } catch (error) {
      // The next beat tries again. If they keep failing, the records expire and are
      // delivered again, which the orders table absorbs.
      log('WARN', 'VISIBILITY_EXTEND_CALL_FAILED', { records: entries.length, error: errorMessage(error) });
    } finally {
      this.sending = false;
    }
  }
}
