#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { readOneStreamConfig } from '../lib/config';
import { OneStreamStack } from '../lib/one-stream-stack';

const app = new App();
const config = readOneStreamConfig(app);

new OneStreamStack(app, config.stackName, {
  env: { account: config.accountId, region: config.region },
  stackName: config.stackName,
  description: 'One Kinesis data stream, three consumers: two queues and a KCL stream consumer',
  config,
});
