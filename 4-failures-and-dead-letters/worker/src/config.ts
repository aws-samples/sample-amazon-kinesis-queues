export interface WorkerConfig {
  region: string;
  queueArn: string;
  ordersTable: string;
  /** Poll loops per task. Each loop receives up to 10 orders and processes them in parallel. */
  concurrency: number;
  /** Simulated warehouse call duration, picked uniformly between min and max. */
  processingMsMin: number;
  processingMsMax: number;
  /**
   * SSM parameter with the catalog's discontinued SKUs. Orders containing them can't
   * be fulfilled, so the queue redelivers them until they become dead-letter records.
   */
  catalogParameter: string;
  /** SSM parameter with the simulated warehouse's status, "up" or "down". */
  warehouseParameter: string;
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
  const config: WorkerConfig = {
    region: required('AWS_REGION'),
    queueArn: required('QUEUE_ARN'),
    ordersTable: required('ORDERS_TABLE'),
    concurrency: Math.trunc(number('CONCURRENCY', 4, 1, 32)),
    processingMsMin: number('PROCESSING_MS_MIN', 300, 0, 60_000),
    processingMsMax: number('PROCESSING_MS_MAX', 800, 0, 60_000),
    catalogParameter: required('CATALOG_PARAMETER'),
    warehouseParameter: required('WAREHOUSE_PARAMETER'),
    statsIntervalMs: number('STATS_INTERVAL_SECONDS', 30, 5, 3600) * 1000,
  };
  if (config.processingMsMax < config.processingMsMin) {
    throw new Error('PROCESSING_MS_MAX must be greater than or equal to PROCESSING_MS_MIN.');
  }
  return config;
}
