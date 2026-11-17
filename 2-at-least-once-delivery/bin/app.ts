#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { AtLeastOnceStack } from '../lib/at-least-once-stack';
import { readAtLeastOnceConfig } from '../lib/config';

const app = new App();
const config = readAtLeastOnceConfig(app);

new AtLeastOnceStack(app, config.stackName, {
  env: { account: config.accountId, region: config.region },
  stackName: config.stackName,
  description: 'At-least-once delivery with a Kinesis Data Streams queue: idempotent fulfillment and visibility heartbeats',
  config,
});
