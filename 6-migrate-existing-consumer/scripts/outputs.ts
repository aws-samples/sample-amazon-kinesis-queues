import { readFileSync } from 'node:fs';
import * as path from 'node:path';

/** Each migration writes its own outputs file, so deploying one doesn't overwrite the other. */
const OUTPUT_FILES = {
  kcl: path.resolve(__dirname, '..', 'cdk-outputs.json'),
  relay: path.resolve(__dirname, '..', 'cdk-outputs-relay.json'),
} as const;

/** Stack outputs. Which ones exist depends on the phase that was last deployed. */
export interface MigrationOutputs {
  stackName: string;
  Phase: 'before' | 'cutover' | 'after';
  Migration?: 'relay';
  StreamArn: string;
  OrdersTableName: string;
  ClusterName: string;
  LogGroupName: string;
  KclLeaseTableName?: string;
  KclServiceName?: string;
  SidelineQueueUrl?: string;
  SidelineDeadLetterQueueUrl?: string;
  QueueArn?: string;
  QueueServiceName?: string;
  PipeName?: string;
  PipeCreationTime?: string;
  RelayQueueUrl?: string;
  RelayDeadLetterQueueUrl?: string;
  SqsServiceName?: string;
}

export function readOutputs(stackName: string | undefined, migration: 'kcl' | 'relay' = 'kcl'): MigrationOutputs {
  const file = OUTPUT_FILES[migration];
  let all: Record<string, Omit<MigrationOutputs, 'stackName'>>;
  try {
    all = JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    throw new Error(`Deploy the stack with --outputs-file ${path.basename(file)} first (looked for ${file}).`);
  }
  const names = stackName ? [stackName] : Object.keys(all);
  if (names.length !== 1 || !all[names[0]]) throw new Error(`Pass --stack-name; ${file} has ${Object.keys(all).join(', ') || 'no stacks'}.`);
  return { ...all[names[0]], stackName: names[0] };
}

export const scriptArgs = {
  relay: { type: 'boolean', default: false },
  'stack-name': { type: 'string' },
  region: { type: 'string', default: process.env.AWS_REGION ?? 'us-east-2' },
  profile: { type: 'string', default: process.env.AWS_PROFILE },
  help: { type: 'boolean', short: 'h', default: false },
} as const;

export function count(n: number): string {
  return n.toLocaleString('en-US');
}
