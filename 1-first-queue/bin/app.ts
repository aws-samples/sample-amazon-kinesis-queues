#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { readFirstQueueConfig } from '../lib/config';
import { FirstQueueStack } from '../lib/first-queue-stack';

const app = new App();
const config = readFirstQueueConfig(app);

new FirstQueueStack(app, config.stackName, {
  env: { account: config.account, region: config.region },
  stackName: config.stackName,
  description: 'Your first Kinesis queue and a Fargate consumer for it (order confirmation scenario)',
  config,
});
