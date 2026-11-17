import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { Construct } from 'constructs';

/** The queue that replaces the KCL consumer. */
export const QUEUE_NAME = 'order-migration';
/** The queue that replaces the relay and its SQS queue. */
export const RELAY_QUEUE_NAME = 'order-relay';
const REPO_ROOT = path.resolve(__dirname, '..', '..');
/** Written by `npm run queue:create -- --queue-name order-migration`. */
const QUEUE_OUTPUTS = path.join(REPO_ROOT, 'shared', 'queue-outputs.json');
/** Written by `cdk deploy --outputs-file cdk-outputs.json` in 0-quickstart/. */
const QUICKSTART_OUTPUTS = path.join(REPO_ROOT, '0-quickstart', 'cdk-outputs.json');

/**
 * - before: the consumer as teams run it today. For `kcl`, the KCL consumer with its
 *   sideline queue. For `relay`, an EventBridge pipe that copies the stream into an SQS
 *   queue, and a consumer of that SQS queue.
 * - cutover: the old consumer and the queue consumer side by side on the same orders table.
 * - after: the queue consumer only. Everything the old consumer needed is removed.
 */
export type Phase = 'before' | 'cutover' | 'after';

/** Which existing consumer is migrated: a KCL application, or a relay into SQS. */
export type Migration = 'kcl' | 'relay';

export interface MigrationConfig {
  accountId: string;
  region: string;
  stackName: string;
  migration: Migration;
  phase: Phase;
  /** An existing VPC with public subnets, instead of a new one. */
  vpcId?: string;
  streamArn: string;
  /** The queue that replaces the old consumer. Required for cutover and after. */
  queueArn?: string;
  /** Relay only: whether the pipe copies records into SQS. Stop it before you remove it. */
  pipeState: 'RUNNING' | 'STOPPED';
  /** Relay only: tasks consuming the SQS queue. */
  sqsTasks: number;
  /** KCL tasks. Set 0 during the cutover to stop KCL but keep its checkpoints. */
  kclTasks: number;
  kclInitialPosition: 'LATEST' | 'TRIM_HORIZON';
  queueTasks: number;
  /** Poll loops per queue task. */
  concurrency: number;
  visibilityTimeoutSeconds: number;
  processingMsMin: number;
  processingMsMax: number;
}

export function readMigrationConfig(scope: Construct): MigrationConfig {
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

  const migration = context('migration') ?? 'kcl';
  if (migration !== 'kcl' && migration !== 'relay') throw new Error('migration must be kcl or relay.');
  const phase = context('phase') ?? 'before';
  if (phase !== 'before' && phase !== 'cutover' && phase !== 'after') throw new Error('phase must be before, cutover or after.');

  const streamArn = context('streamArn') ?? streamArnFromQuickstart();
  if (!/^arn:aws:kinesis:[a-z0-9-]+:\d{12}:stream\/[A-Za-z0-9_.-]+$/.test(streamArn)) {
    throw new Error(`streamArn doesn't look like a Kinesis stream ARN: ${streamArn}`);
  }
  if (!streamArn.startsWith(`arn:aws:kinesis:${region}:${accountId}:`)) {
    throw new Error('streamArn must be in the same account and Region as the stack.');
  }

  let queueArn: string | undefined;
  if (phase !== 'before') {
    queueArn = context('queueArn') ?? queueArnFromOutputs(migration === 'relay' ? RELAY_QUEUE_NAME : QUEUE_NAME);
    if (!/^arn:aws:kinesis:[a-z0-9-]+:\d{12}:queue\/[A-Za-z0-9]+$/.test(queueArn)) {
      throw new Error(`queueArn doesn't look like a Kinesis queue ARN: ${queueArn}`);
    }
    if (!queueArn.startsWith(`arn:aws:kinesis:${region}:${accountId}:`)) {
      throw new Error('queueArn must be in the same account and Region as the stack.');
    }
  }

  const pipeState = (context('pipeState') ?? 'running').toUpperCase();
  if (pipeState !== 'RUNNING' && pipeState !== 'STOPPED') throw new Error('pipeState must be running or stopped.');
  const vpcId = context('vpcId');
  if (vpcId !== undefined && !/^vpc-[0-9a-f]{8,17}$/.test(vpcId)) {
    throw new Error(`vpcId must look like vpc-0123456789abcdef0, got "${vpcId}".`);
  }

  const kclInitialPosition = (context('kclInitialPosition') ?? 'LATEST').toUpperCase();
  if (kclInitialPosition !== 'LATEST' && kclInitialPosition !== 'TRIM_HORIZON') {
    throw new Error('kclInitialPosition must be LATEST or TRIM_HORIZON.');
  }
  const processingMsMin = number('processingMsMin', 300, 0, 60_000);
  const processingMsMax = number('processingMsMax', 800, 0, 60_000);
  if (processingMsMax < processingMsMin) throw new Error('processingMsMax must be >= processingMsMin.');

  return {
    accountId,
    region,
    stackName,
    migration,
    phase,
    vpcId,
    streamArn,
    queueArn,
    pipeState,
    sqsTasks: number('sqsTasks', 2, 0, 10),
    kclTasks: number('kclTasks', 2, 0, 10),
    kclInitialPosition,
    queueTasks: number('queueTasks', 2, 0, 10),
    concurrency: number('concurrency', 4, 1, 32),
    visibilityTimeoutSeconds: number('visibilityTimeoutSeconds', 10, 1, 43_200),
    processingMsMin,
    processingMsMax,
  };
}

function streamArnFromQuickstart(): string {
  let outputs: Record<string, { StreamArn?: string }>;
  try {
    outputs = JSON.parse(readFileSync(QUICKSTART_OUTPUTS, 'utf8'));
  } catch {
    throw new Error(`Pass -c streamArn=ARN, or deploy the Quickstart with --outputs-file cdk-outputs.json (looked for ${QUICKSTART_OUTPUTS}).`);
  }
  const arns = Object.values(outputs).map((o) => o.StreamArn).filter(Boolean) as string[];
  if (arns.length !== 1) throw new Error(`Pass -c streamArn=ARN; found ${arns.length} StreamArn values in ${QUICKSTART_OUTPUTS}.`);
  return arns[0];
}

function queueArnFromOutputs(queueName: string): string {
  let outputs: Record<string, { queueArn?: string }>;
  try {
    outputs = JSON.parse(readFileSync(QUEUE_OUTPUTS, 'utf8'));
  } catch {
    outputs = {};
  }
  const arn = outputs[queueName]?.queueArn;
  if (!arn) {
    const plan = queueName === RELAY_QUEUE_NAME ? 'migrate:relay:plan' : 'migrate:plan';
    throw new Error(`Pass -c queueArn=ARN, or create ${queueName} first. Run "npm run ${plan}" for the command with the right starting position.`);
  }
  return arn;
}
