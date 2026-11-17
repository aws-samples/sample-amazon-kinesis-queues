import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { Construct } from 'constructs';

/** Written by `cdk deploy --outputs-file cdk-outputs.json` in 0-quickstart/. */
const QUICKSTART_OUTPUTS = path.resolve(__dirname, '..', '..', '0-quickstart', 'cdk-outputs.json');

export interface FirstQueueConfig {
  account: string;
  region: string;
  /** Also the queue name, so one Quickstart gets one clearly named queue. */
  stackName: string;
  streamArn: string;
}

/**
 * Everything comes from the Quickstart's outputs file. Optional overrides:
 * -c streamArn=ARN (another stream; the Quickstart name then comes from
 * -c baseStackName, default kinesis-queues-demo) and -c stackName=NAME.
 */
export function readFirstQueueConfig(scope: Construct): FirstQueueConfig {
  const context = (name: string): string | undefined => scope.node.tryGetContext(name)?.toString().trim() || undefined;

  let streamArn = context('streamArn');
  let quickstartName = context('baseStackName') ?? 'kinesis-queues-demo';
  if (!streamArn) {
    let outputs: Record<string, { StreamArn?: string }>;
    try {
      outputs = JSON.parse(readFileSync(QUICKSTART_OUTPUTS, 'utf8'));
    } catch {
      throw new Error(`Deploy the Quickstart first, from 0-quickstart/ with --outputs-file cdk-outputs.json (looked for ${QUICKSTART_OUTPUTS}), or pass -c streamArn=ARN.`);
    }
    const names = Object.keys(outputs);
    if (names.length !== 1 || !outputs[names[0]].StreamArn) {
      throw new Error(`Expected one stack with a StreamArn in ${QUICKSTART_OUTPUTS}; pass -c streamArn=ARN instead.`);
    }
    [quickstartName, streamArn] = [names[0], outputs[names[0]].StreamArn!];
  }

  // arn:aws:kinesis:REGION:ACCOUNT:stream/NAME
  const [, , , region, account, resource] = streamArn.split(':');
  if (!/^[a-z0-9-]+$/.test(region ?? '') || !/^\d{12}$/.test(account ?? '') || !resource?.startsWith('stream/')) {
    throw new Error(`Not a Kinesis stream ARN: ${streamArn}`);
  }

  const stackName = context('stackName') ?? `${quickstartName}-first-queue`;
  // Valid as both a CloudFormation stack name and a queue name.
  if (!/^[A-Za-z][A-Za-z0-9-]{0,127}$/.test(stackName)) {
    throw new Error(`The stack name doubles as the queue name: use a letter, then letters, numbers or hyphens (max 128): ${stackName}`);
  }
  return { account, region, stackName, streamArn };
}
