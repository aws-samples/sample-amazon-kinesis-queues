#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { readScaleOutConfig } from '../lib/config';
import { ScaleOutStack } from '../lib/scale-out-stack';

const app = new App();
const config = readScaleOutConfig(app);

new ScaleOutStack(app, config.stackName, {
  env: { account: config.accountId, region: config.region },
  stackName: config.stackName,
  description: 'Scale out queue consumers independently of shards (order fulfillment scenario)',
  config,
});
