import { readFileSync, writeFileSync } from 'node:fs';
import * as path from 'node:path';
import { KinesisClient } from '@aws-sdk/client-kinesis';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

/** Default Region for the scenarios. Override with --region or AWS_REGION. */
export const DEFAULT_REGION = 'us-east-2';
export const DEFAULT_QUEUE_NAME = 'first-queue';

/** The shared/ folder at the repository root. */
const DEMO_DIR = path.resolve(__dirname, '..');
const REPO_ROOT = path.resolve(DEMO_DIR, '..');
/** Written by create-queue, read by consume and delete-queue. Git-ignored. */
export const QUEUE_OUTPUT_FILE = path.join(DEMO_DIR, 'queue-outputs.json');
/** Written by `cdk deploy --outputs-file cdk-outputs.json` in 0-quickstart/. */
const BASE_OUTPUT_FILE = path.join(REPO_ROOT, '0-quickstart', 'cdk-outputs.json');

export interface CommonOptions {
  region: string;
  profile?: string;
}

export const commonArgs = {
  region: { type: 'string', default: process.env.AWS_REGION ?? DEFAULT_REGION },
  profile: { type: 'string', default: process.env.AWS_PROFILE },
  help: { type: 'boolean', short: 'h', default: false },
} as const;

/** Prints usage and exits when --help was passed. */
export function handleHelp(help: boolean | undefined, usage: string): void {
  if (help) {
    console.log(usage);
    process.exit(0);
  }
}

/** A Kinesis client for the scripts, with the profile from --profile or the standard credential chain. */
export function createClient({ region, profile }: CommonOptions): KinesisClient {
  return new KinesisClient({
    region,
    credentials: fromNodeProviderChain(profile ? { profile } : {}),
  });
}

/** Stream ARN from --stream-arn, or from the Quickstart's 0-quickstart/cdk-outputs.json. */
export function resolveStreamArn(explicit: string | undefined, stackName: string | undefined): string {
  if (explicit) return explicit;
  let outputs: Record<string, Record<string, string>>;
  try {
    outputs = JSON.parse(readFileSync(BASE_OUTPUT_FILE, 'utf8'));
  } catch {
    throw new Error(`Pass --stream-arn, or deploy the Quickstart (0-quickstart) with --outputs-file cdk-outputs.json (looked for ${BASE_OUTPUT_FILE}).`);
  }
  const stacks = stackName ? [stackName] : Object.keys(outputs);
  if (stacks.length !== 1 || !outputs[stacks[0]]?.StreamArn) {
    throw new Error(`Could not pick a StreamArn from ${BASE_OUTPUT_FILE}; pass --stack-name or --stream-arn.`);
  }
  return outputs[stacks[0]].StreamArn;
}

export interface QueueOutputs {
  queueName: string;
  queueArn: string;
  streamArn: string;
  region: string;
}

/** queue-outputs.json maps queue name to its outputs, so several queues can coexist. */
export function readQueueOutputs(): Record<string, QueueOutputs> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(QUEUE_OUTPUT_FILE, 'utf8'));
  } catch {
    return {};
  }
  // Older single-queue format: { queueName, queueArn, ... }.
  const legacy = parsed as Partial<QueueOutputs>;
  if (typeof legacy.queueArn === 'string' && typeof legacy.queueName === 'string') {
    return { [legacy.queueName]: legacy as QueueOutputs };
  }
  return parsed as Record<string, QueueOutputs>;
}

export function saveQueueOutputs(outputs: QueueOutputs): void {
  const all = { ...readQueueOutputs(), [outputs.queueName]: outputs };
  writeFileSync(QUEUE_OUTPUT_FILE, `${JSON.stringify(all, null, 2)}\n`);
}

export function removeQueueOutputs(queueArn: string): void {
  const remaining = Object.fromEntries(Object.entries(readQueueOutputs()).filter(([, q]) => q.queueArn !== queueArn));
  writeFileSync(QUEUE_OUTPUT_FILE, `${JSON.stringify(remaining, null, 2)}\n`);
}

/** Queue ARN from --queue-arn, or by name from queue-outputs.json written by create-queue. */
export function resolveQueueArn(explicit: string | undefined, queueName: string = DEFAULT_QUEUE_NAME): string {
  if (explicit) return explicit;
  const arn = readQueueOutputs()[queueName]?.queueArn;
  if (!arn) {
    throw new Error(`Pass --queue-arn, or run "npm run queue:create -- --queue-name ${queueName}" first (looked in ${QUEUE_OUTPUT_FILE}).`);
  }
  return arn;
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
