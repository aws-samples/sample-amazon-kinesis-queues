export interface FulfillmentConfig {
  region: string;
  ordersTable: string;
  /** Simulated warehouse call duration, picked uniformly between min and max. */
  processingMsMin: number;
  processingMsMax: number;
  /** Products the warehouse no longer stocks: orders for them fail on every attempt. */
  discontinuedSkus: Set<string>;
}

export interface WorkerConfig extends FulfillmentConfig {
  queueArn: string;
  /** Poll loops per task. Each loop receives up to 10 orders and fulfills them in parallel. */
  concurrency: number;
  /** Sent with every receive. A failed order is delivered again after this long. */
  visibilityTimeoutSeconds: number;
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
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be a whole number from ${min} to ${max}, got "${raw}".`);
  }
  return value;
}

export function readFulfillmentConfig(): FulfillmentConfig {
  const config: FulfillmentConfig = {
    region: required('AWS_REGION'),
    ordersTable: required('ORDERS_TABLE'),
    processingMsMin: number('PROCESSING_MS_MIN', 300, 0, 60_000),
    processingMsMax: number('PROCESSING_MS_MAX', 800, 0, 60_000),
    discontinuedSkus: new Set((process.env.DISCONTINUED_SKUS ?? 'SKU-DISCONTINUED').split(',').map((s) => s.trim()).filter(Boolean)),
  };
  if (config.processingMsMax < config.processingMsMin) throw new Error('PROCESSING_MS_MAX must be >= PROCESSING_MS_MIN.');
  return config;
}

/** The relay's SQS consumer, before the migration. */
export interface SqsWorkerConfig extends FulfillmentConfig {
  queueUrl: string;
  concurrency: number;
  visibilityTimeoutSeconds: number;
  statsIntervalMs: number;
}

export function readSqsWorkerConfig(): SqsWorkerConfig {
  return {
    ...readFulfillmentConfig(),
    queueUrl: required('SQS_QUEUE_URL'),
    concurrency: number('CONCURRENCY', 4, 1, 32),
    visibilityTimeoutSeconds: number('VISIBILITY_TIMEOUT_SECONDS', 10, 1, 43_200),
    statsIntervalMs: number('STATS_INTERVAL_SECONDS', 10, 5, 3600) * 1000,
  };
}

export function readWorkerConfig(): WorkerConfig {
  return {
    ...readFulfillmentConfig(),
    queueArn: required('QUEUE_ARN'),
    concurrency: number('CONCURRENCY', 4, 1, 32),
    visibilityTimeoutSeconds: number('VISIBILITY_TIMEOUT_SECONDS', 10, 1, 43_200),
    statsIntervalMs: number('STATS_INTERVAL_SECONDS', 10, 5, 3600) * 1000,
  };
}
