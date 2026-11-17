import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { Construct } from 'constructs';

export const QUEUE_NAME = 'order-fulfillment';
/** Written by `npm run queue:create -- --queue-name order-fulfillment`. */
const QUEUE_OUTPUTS = path.resolve(__dirname, '..', '..', 'shared', 'queue-outputs.json');

export interface FulfillmentConfig {
  accountId: string;
  region: string;
  stackName: string;
  queueArn: string;
  concurrency: number;
  processingMsMin: number;
  processingMsMax: number;
  /** Optional address subscribed to the dead-letter alarm topic (confirm the email AWS sends). */
  alarmEmail?: string;
}

/**
 * The product catalog's starting list of discontinued SKUs. It matches the SKU the
 * producer's --poison option puts in orders. The list lives in an SSM parameter that
 * the workers read at run time, so "fixing the cause" is a data change
 * (npm run orders:catalog), not a redeploy.
 */
export const INITIAL_DISCONTINUED_SKUS = 'SKU-DISCONTINUED';

export function readFulfillmentConfig(scope: Construct): FulfillmentConfig {
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
  function number(name: string, fallback: number, min: number, max: number, integer = true): number {
    const text = context(name);
    if (text === undefined) return fallback;
    const value = Number(text);
    if (!Number.isFinite(value) || value < min || value > max || (integer && !Number.isInteger(value))) {
      throw new Error(`${name} must be ${integer ? 'a whole number' : 'a number'} from ${min} to ${max}.`);
    }
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

  const processingMsMin = number('processingMsMin', 300, 0, 60_000);
  const processingMsMax = number('processingMsMax', 800, 0, 60_000);
  if (processingMsMax < processingMsMin) throw new Error('processingMsMax must be >= processingMsMin.');

  return {
    accountId,
    region,
    stackName,
    queueArn,
    concurrency: number('concurrency', 4, 1, 32),
    processingMsMin,
    processingMsMax,
    alarmEmail: alarmEmail(context('alarmEmail')),
  };
}

function alarmEmail(text: string | undefined): string | undefined {
  if (text === undefined) return undefined;
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text)) throw new Error(`alarmEmail is not an email address: "${text}"`);
  return text;
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
