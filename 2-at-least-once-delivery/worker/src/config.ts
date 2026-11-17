export interface WorkerConfig {
  region: string;
  queueArn: string;
  ordersTable: string;
  /** Poll loops per task. Each loop receives up to 10 orders and processes them in parallel. */
  concurrency: number;
  /**
   * Sent with every receive, so the worker knows how long it holds each record
   * without reading the queue's setting.
   */
  visibilityTimeoutSeconds: number;
  /** How often the heartbeat extends the visibility of records still being processed. */
  heartbeatSeconds: number;
  /**
   * The heartbeat stops extending a record after this long, so a stuck handler
   * can't hold a record forever. The queue then delivers it again.
   */
  maxHoldSeconds: number;
  /** Simulated warehouse call duration, picked uniformly between min and max. */
  processingMsMin: number;
  processingMsMax: number;
  /** How long orders marked `slow` or `slow-no-heartbeat` take to fulfill. */
  slowProcessingMs: number;
  statsIntervalMs: number;
}

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`Environment variable ${name} is required.`);
  return value;
}

function number(name: string, fallback: number, min: number, max: number): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new Error(`${name} must be a number from ${min} to ${max}, got "${raw}".`);
  }
  return value;
}

export function readWorkerConfig(): WorkerConfig {
  const visibilityTimeoutSeconds = Math.trunc(number('VISIBILITY_TIMEOUT_SECONDS', 10, 1, 43_200));
  const config: WorkerConfig = {
    region: required('AWS_REGION'),
    queueArn: required('QUEUE_ARN'),
    ordersTable: required('ORDERS_TABLE'),
    concurrency: Math.trunc(number('CONCURRENCY', 4, 1, 32)),
    visibilityTimeoutSeconds,
    // Extending at a third of the timeout leaves two chances before a record expires.
    heartbeatSeconds: number('HEARTBEAT_SECONDS', Math.max(1, Math.floor(visibilityTimeoutSeconds / 3)), 1, 43_200),
    maxHoldSeconds: number('MAX_HOLD_SECONDS', 300, 1, 43_200),
    processingMsMin: number('PROCESSING_MS_MIN', 300, 0, 60_000),
    processingMsMax: number('PROCESSING_MS_MAX', 800, 0, 60_000),
    slowProcessingMs: number('SLOW_PROCESSING_SECONDS', 25, 1, 3600) * 1000,
    statsIntervalMs: number('STATS_INTERVAL_SECONDS', 30, 5, 3600) * 1000,
  };
  if (config.processingMsMax < config.processingMsMin) {
    throw new Error('PROCESSING_MS_MAX must be greater than or equal to PROCESSING_MS_MIN.');
  }
  if (config.heartbeatSeconds >= visibilityTimeoutSeconds) {
    throw new Error('HEARTBEAT_SECONDS must be less than VISIBILITY_TIMEOUT_SECONDS, or records expire between heartbeats.');
  }
  return config;
}
