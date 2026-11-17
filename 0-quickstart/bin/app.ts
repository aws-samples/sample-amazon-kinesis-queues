#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { readDeploymentConfig } from '../lib/config';
import { KinesisDataStreamStack } from '../lib/kinesis-data-stream-stack';

const app = new App();
const config = readDeploymentConfig(app);

new KinesisDataStreamStack(app, config.stackName, {
  env: { account: config.accountId, region: config.region },
  stackName: config.stackName,
  description: 'Kinesis stream and one KCL consumer on ECS Fargate',
  streamName: config.streamName,
  retentionHours: config.retentionHours,
});
