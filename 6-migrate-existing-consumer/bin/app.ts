#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { readMigrationConfig } from '../lib/config';
import { MigrationStack } from '../lib/migration-stack';
import { RelayStack } from '../lib/relay-stack';

const app = new App();
const config = readMigrationConfig(app);
const common = { env: { account: config.accountId, region: config.region }, stackName: config.stackName, config };

if (config.migration === 'relay') {
  new RelayStack(app, config.stackName, {
    ...common,
    description: `Migrate an order fulfillment consumer from a relay into SQS to a Kinesis Data Streams queue (phase: ${config.phase})`,
  });
} else {
  new MigrationStack(app, config.stackName, {
    ...common,
    description: `Migrate an order fulfillment consumer from KCL to a Kinesis Data Streams queue (phase: ${config.phase})`,
  });
}
