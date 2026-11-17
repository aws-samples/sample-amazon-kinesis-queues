import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');

export interface StackOutputs {
  ClusterName: string;
  LogGroupName: string;
  FulfillmentQueueArn: string;
  FulfillmentServiceName: string;
  FulfillmentTableName: string;
  FraudSignalsServiceName: string;
  NotificationsQueueArn?: string;
  NotificationsServiceName?: string;
  NotificationsTableName?: string;
}

export function readOutputs(stackName: string | undefined): StackOutputs {
  let all: Record<string, StackOutputs>;
  try {
    all = JSON.parse(readFileSync(OUTPUTS, 'utf8'));
  } catch {
    throw new Error(`Deploy the stack with --outputs-file cdk-outputs.json first (looked for ${OUTPUTS}).`);
  }
  const names = stackName ? [stackName] : Object.keys(all);
  if (names.length !== 1 || !all[names[0]]) throw new Error(`Pass --stack-name; ${OUTPUTS} has ${Object.keys(all).join(', ')}.`);
  return all[names[0]];
}

export const commonOptions = {
  'stack-name': { type: 'string' },
  region: { type: 'string', default: process.env.AWS_REGION ?? 'us-east-2' },
  profile: { type: 'string', default: process.env.AWS_PROFILE },
  help: { type: 'boolean', short: 'h', default: false },
} as const;

export function clientConfig(region: string, profile: string | undefined) {
  return { region, credentials: fromNodeProviderChain(profile ? { profile } : {}) };
}
