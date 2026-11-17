import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { Construct } from 'constructs';

export const QUEUE_NAME = 'order-at-least-once';
/** Written by `npm run queue:create -- --queue-name order-at-least-once`. */
const QUEUE_OUTPUTS = path.resolve(__dirname, '..', '..', 'shared', 'queue-outputs.json');

export interface AtLeastOnceConfig {
  accountId: string;
  region: string;
  stackName: string;
  queueArn: string;
  /** Fixed number of worker tasks. Two, so a redelivered order can land on the other worker. */
  workers: number;
  concurrency: number;
  visibilityTimeoutSeconds: number;
  /** Undefined: a third of the visibility timeout. */
  heartbeatSeconds?: number;
  maxHoldSeconds: number;
  slowProcessingSeconds: number;
}

export function readAtLeastOnceConfig(scope: Construct): AtLeastOnceConfig {
  function context(name: string): string | undefined {
    const value: unknown = scope.node.tryGetContext(name);
    if (value === undefined) return undefined;
    if (typeof value !== 'string' && typeof value !== 'number') throw new Error(`Context "${name}" must be a string.`);
    const text = String(value).trim();
    if (text === '') throw new Error(`Context "${name}" must not be empty.`);
    return text;
  }
  function required(name: string): string {
    const value = context(name);
    if (value === undefined) throw new Error(`Missing context "${name}". Supply -c ${name}=VALUE with your CDK command.`);
    return value;
  }
  function number(name: string, fallback: number, min: number, max: number): number {
    const text = context(name);
    if (text === undefined) return fallback;
    const value = Number(text);
    if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name} must be a whole number from ${min} to ${max}.`);
    return value;
  }

  const accountId = required('accountId');
  const region = required('region');
  const stackName = required('stackName');
  if (!/^\d{12}$/.test(accountId)) throw new Error('accountId must be a 12-digit AWS account ID.');
  if (!/^[a-z]{2}(?:-[a-z0-9]+)+-\d+$/.test(region)) throw new Error('region must be an AWS Region code, for example us-east-2.');
  if (!/^[A-Za-z][A-Za-z0-9-]{0,127}$/.test(stackName)) {
    throw new Error('stackName must start with a letter and contain only letters, numbers or hyphens.');
  }

  const queueArn = context('queueArn') ?? queueArnFromOutputs();
  if (!/^arn:aws:kinesis:[a-z0-9-]+:\d{12}:queue\/[A-Za-z0-9]+$/.test(queueArn)) {
    throw new Error(`queueArn doesn't look like a Kinesis queue ARN: ${queueArn}`);
  }
  if (!queueArn.startsWith(`arn:aws:kinesis:${region}:${accountId}:`)) {
    throw new Error('queueArn must be in the same account and Region as the stack.');
  }

  const visibilityTimeoutSeconds = number('visibilityTimeoutSeconds', 10, 1, 43_200);
  const heartbeatText = context('heartbeatSeconds');
  const heartbeatSeconds = heartbeatText === undefined ? undefined : number('heartbeatSeconds', 0, 1, visibilityTimeoutSeconds - 1);

  return {
    accountId,
    region,
    stackName,
    queueArn,
    workers: number('workers', 2, 1, 10),
    concurrency: number('concurrency', 4, 1, 32),
    visibilityTimeoutSeconds,
    heartbeatSeconds,
    maxHoldSeconds: number('maxHoldSeconds', 300, 1, 43_200),
    slowProcessingSeconds: number('slowProcessingSeconds', 25, 1, 3600),
  };
}

function queueArnFromOutputs(): string {
  let outputs: Record<string, { queueArn?: string }>;
  try {
    outputs = JSON.parse(readFileSync(QUEUE_OUTPUTS, 'utf8'));
  } catch {
    outputs = {};
  }
  const arn = outputs[QUEUE_NAME]?.queueArn;
  if (!arn) {
    throw new Error(`Pass -c queueArn=ARN, or create the queue first: npm run queue:create -- --queue-name ${QUEUE_NAME} --starting-position LATEST`);
  }
  return arn;
}
