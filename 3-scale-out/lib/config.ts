import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { Construct } from 'constructs';

export const QUEUE_NAME = 'order-scale-out';
/** Written by `npm run queue:create -- --queue-name order-scale-out`. */
const QUEUE_OUTPUTS = path.resolve(__dirname, '..', '..', 'shared', 'queue-outputs.json');

export interface ScaleOutConfig {
  accountId: string;
  region: string;
  stackName: string;
  queueArn: string;
  /** Fewest worker tasks the backlog alarms can scale down to. 0 stops all workers when the queue is idle. */
  minTasks: number;
  /** Most worker tasks the backlog alarms can scale up to. */
  maxTasks: number;
  concurrency: number;
  processingMsMin: number;
  processingMsMax: number;
}

export function readScaleOutConfig(scope: Construct): ScaleOutConfig {
  function context(name: string): string | undefined {
    const value: unknown = scope.node.tryGetContext(name);
    if (value === undefined) return undefined;
    if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean') {
      throw new Error(`Context "${name}" must be a string.`);
    }
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

  const minTasks = number('minTasks', 1, 0, 20);
  const maxTasks = number('maxTasks', 10, 1, 20);
  if (maxTasks < minTasks) throw new Error('maxTasks must be >= minTasks.');

  const processingMsMin = number('processingMsMin', 300, 0, 60_000);
  const processingMsMax = number('processingMsMax', 800, 0, 60_000);
  if (processingMsMax < processingMsMin) throw new Error('processingMsMax must be >= processingMsMin.');

  return {
    accountId,
    region,
    stackName,
    queueArn,
    minTasks,
    maxTasks,
    concurrency: number('concurrency', 4, 1, 32),
    processingMsMin,
    processingMsMax,
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
