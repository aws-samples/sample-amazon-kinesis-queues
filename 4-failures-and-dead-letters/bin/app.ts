#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { readFulfillmentConfig } from '../lib/config';
import { FailuresDeadLettersStack } from '../lib/failures-dead-letters-stack';

const app = new App();
const config = readFulfillmentConfig(app);

new FailuresDeadLettersStack(app, config.stackName, {
  env: { account: config.accountId, region: config.region },
  stackName: config.stackName,
  description: 'Failures and dead-letter records with a Kinesis Data Streams queue (order fulfillment scenario)',
  config,
});
