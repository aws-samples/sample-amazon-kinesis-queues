import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { Construct } from 'constructs';

export const FULFILLMENT_QUEUE = 'fanout-fulfillment';
export const NOTIFICATIONS_QUEUE = 'fanout-notifications';
/** Written by `npm run queue:create`. */
const QUEUE_OUTPUTS = path.resolve(__dirname, '..', '..', 'shared', 'queue-outputs.json');
/** Written by the Quickstart deploy. */
const QUICKSTART_OUTPUTS = path.resolve(__dirname, '..', '..', '0-quickstart', 'cdk-outputs.json');

export interface OneStreamConfig {
  accountId: string;
  region: string;
  stackName: string;
  streamArn: string;
  fulfillmentQueueArn: string;
  /**
   * Undefined until the notifications queue exists. The walkthrough creates it later,
   * then deploys again to add its worker.
   */
  notificationsQueueArn?: string;
  velocityWindowSeconds: number;
  velocityThreshold: number;
  /**
   * Optional: run in an existing VPC instead of creating one, for accounts at their
   * VPC quota. It needs public subnets, because the tasks have no NAT gateway.
   */
  vpcId?: string;
}

export function readOneStreamConfig(scope: Construct): OneStreamConfig {
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
  const prefix = `arn:aws:kinesis:${region}:${accountId}:`;
  const check = (arn: string, kind: 'stream' | 'queue', name: string) => {
    const pattern = kind === 'stream' ? /^arn:aws:kinesis:[a-z0-9-]+:\d{12}:stream\/[A-Za-z0-9_.-]+$/ : /^arn:aws:kinesis:[a-z0-9-]+:\d{12}:queue\/[A-Za-z0-9]+$/;
    if (!pattern.test(arn)) throw new Error(`${name} doesn't look like a Kinesis ${kind} ARN: ${arn}`);
    if (!arn.startsWith(prefix)) throw new Error(`${name} must be in the same account and Region as the stack.`);
    return arn;
  };

  const streamArn = check(context('streamArn') ?? streamArnFromQuickstart(), 'stream', 'streamArn');
  const fulfillmentArn = context('fulfillmentQueueArn') ?? queueArnFromOutputs(FULFILLMENT_QUEUE);
  if (!fulfillmentArn) {
    throw new Error(`Pass -c fulfillmentQueueArn=ARN, or create the queue first: npm run queue:create -- --queue-name ${FULFILLMENT_QUEUE} --starting-position LATEST`);
  }
  const notificationsArn = context('notificationsQueueArn') ?? queueArnFromOutputs(NOTIFICATIONS_QUEUE);

  return {
    accountId,
    region,
    stackName,
    streamArn,
    fulfillmentQueueArn: check(fulfillmentArn, 'queue', 'fulfillmentQueueArn'),
    notificationsQueueArn: notificationsArn ? check(notificationsArn, 'queue', 'notificationsQueueArn') : undefined,
    velocityWindowSeconds: number('velocityWindowSeconds', 60, 1, 86_400),
    velocityThreshold: number('velocityThreshold', 5, 2, 10_000),
    vpcId: vpcId(context('vpcId')),
  };
}

function vpcId(text: string | undefined): string | undefined {
  if (text === undefined) return undefined;
  if (!/^vpc-[0-9a-f]{8,17}$/.test(text)) throw new Error(`vpcId must look like vpc-0123456789abcdef0, got "${text}".`);
  return text;
}

function readJson(file: string): Record<string, Record<string, string | undefined>> {
  try {
    return JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    return {};
  }
}

function queueArnFromOutputs(name: string): string | undefined {
  return readJson(QUEUE_OUTPUTS)[name]?.queueArn;
}

function streamArnFromQuickstart(): string {
  const arns = Object.values(readJson(QUICKSTART_OUTPUTS)).map((o) => o.StreamArn).filter(Boolean) as string[];
  if (arns.length !== 1) throw new Error(`Pass -c streamArn=ARN; found ${arns.length} StreamArn values in ${QUICKSTART_OUTPUTS}.`);
  return arns[0];
}
