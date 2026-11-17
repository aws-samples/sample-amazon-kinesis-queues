import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import {
  AwsLogDriver, ContainerImage, CpuArchitecture, FargateTaskDefinition, OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { CfnPipe } from 'aws-cdk-lib/aws-pipes';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { streamDecryptStatement } from '../../shared/cdk/stream-decrypt';
import { WORKER_DIR, bundleWorker } from './bundle-worker';
import type { MigrationConfig } from './config';
import { consumerNetwork, type ConsumerNetwork } from './network';

export interface RelayStackProps extends StackProps {
  config: MigrationConfig;
}

/**
 * Order fulfillment through a relay into SQS, migrated to a Kinesis queue:
 *
 * - before: an EventBridge pipe copies every record from the stream into an SQS queue,
 *   and a consumer on Fargate reads that SQS queue. -c pipeState=stopped stops the copy.
 * - cutover: the same, plus the queue consumer reading a Kinesis queue on the stream.
 *   Both write to one orders table, which keeps any order from shipping twice.
 * - after: only the queue consumer. Deploying this phase removes the pipe, the SQS
 *   queue, its dead-letter queue and the SQS consumer.
 *
 * The stream and the Kinesis queue are referenced by ARN, not owned here.
 */
export class RelayStack extends Stack {
  constructor(scope: Construct, id: string, props: RelayStackProps) {
    const { config, ...stackProps } = props;
    super(scope, id, stackProps);

    const orders = new Table(this, 'FulfilledOrders', {
      partitionKey: { name: 'orderId', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const network = consumerNetwork(this, config.vpcId);
    bundleWorker();
    const image = ContainerImage.fromAsset(WORKER_DIR, { platform: Platform.LINUX_ARM64 });
    const fulfillmentEnv = {
      AWS_REGION: this.region,
      ORDERS_TABLE: orders.tableName,
      PROCESSING_MS_MIN: String(config.processingMsMin),
      PROCESSING_MS_MAX: String(config.processingMsMax),
      CONCURRENCY: String(config.concurrency),
      VISIBILITY_TIMEOUT_SECONDS: String(config.visibilityTimeoutSeconds),
    };

    if (config.phase !== 'after') this.addRelay(config, network, orders, image, fulfillmentEnv);
    if (config.phase !== 'before') this.addQueueConsumer(config, network, orders, image, fulfillmentEnv);

    new CfnOutput(this, 'Migration', { value: 'relay' });
    new CfnOutput(this, 'Phase', { value: config.phase });
    new CfnOutput(this, 'StreamArn', { value: config.streamArn });
    new CfnOutput(this, 'OrdersTableName', { value: orders.tableName });
    new CfnOutput(this, 'ClusterName', { value: network.cluster.clusterName });
    new CfnOutput(this, 'LogGroupName', { value: network.logGroup.logGroupName });
  }

  /** The setup before the migration: the pipe, the SQS queue, and its consumer. */
  private addRelay(config: MigrationConfig, network: ConsumerNetwork, orders: Table, image: ContainerImage,
    env: Record<string, string>): void {
    const deadLetters = new Queue(this, 'RelayDeadLetters', { retentionPeriod: Duration.days(14), enforceSSL: true });
    const relayQueue = new Queue(this, 'RelayQueue', {
      visibilityTimeout: Duration.seconds(config.visibilityTimeoutSeconds),
      retentionPeriod: Duration.days(4),
      enforceSSL: true,
      deadLetterQueue: { queue: deadLetters, maxReceiveCount: 5 },
    });

    // The pipe's role: read the stream, write to the SQS queue.
    const pipeRole = new Role(this, 'PipeRole', {
      assumedBy: new ServicePrincipal('pipes.amazonaws.com', {
        conditions: { StringEquals: { 'aws:SourceAccount': this.account } },
      }),
    });
    pipeRole.addToPolicy(new PolicyStatement({
      actions: [
        'kinesis:DescribeStream', 'kinesis:DescribeStreamSummary', 'kinesis:ListShards',
        'kinesis:GetShardIterator', 'kinesis:GetRecords',
      ],
      resources: [config.streamArn],
    }));
    pipeRole.addToPolicy(streamDecryptStatement(this, config.streamArn));
    relayQueue.grantSendMessages(pipeRole);

    const pipe = new CfnPipe(this, 'Relay', {
      name: `${this.stackName}-relay`,
      description: 'Copies every record from the stream into the SQS queue. Removed by the migration.',
      roleArn: pipeRole.roleArn,
      desiredState: config.pipeState,
      source: config.streamArn,
      sourceParameters: {
        kinesisStreamParameters: {
          // Like the KCL consumer in the other migration, the relay starts at the tip.
          startingPosition: 'LATEST',
          batchSize: 10,
          maximumBatchingWindowInSeconds: 1,
          // The defaults, written out because the cutover plan relies on them: a shard's
          // batches go in order, and a failed batch is retried, never skipped.
          parallelizationFactor: 1,
          maximumRetryAttempts: -1,
        },
      },
      target: relayQueue.queueArn,
    });
    pipe.node.addDependency(pipeRole);

    const task = new FargateTaskDefinition(this, 'SqsTask', {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    relayQueue.grantConsumeMessages(task.taskRole);
    orders.grant(task.taskRole, 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem');
    task.addContainer('SqsConsumer', {
      image,
      command: ['node', '--enable-source-maps', 'sqs-main.js'],
      environment: { ...env, SQS_QUEUE_URL: relayQueue.queueUrl },
      logging: new AwsLogDriver({ logGroup: network.logGroup, streamPrefix: 'sqs' }),
      stopTimeout: Duration.seconds(30),
    });
    const service = network.service('SqsService', task, config.sqsTasks);

    new CfnOutput(this, 'PipeName', { value: pipe.name! });
    new CfnOutput(this, 'PipeCreationTime', { value: pipe.attrCreationTime });
    new CfnOutput(this, 'RelayQueueUrl', { value: relayQueue.queueUrl });
    new CfnOutput(this, 'RelayDeadLetterQueueUrl', { value: deadLetters.queueUrl });
    new CfnOutput(this, 'SqsServiceName', { value: service.serviceName });
  }

  /** The consumer after the migration: the same code as in the KCL migration. */
  private addQueueConsumer(config: MigrationConfig, network: ConsumerNetwork, orders: Table, image: ContainerImage,
    env: Record<string, string>): void {
    const queueArn = config.queueArn!;
    const task = new FargateTaskDefinition(this, 'QueueTask', {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['kinesis:ReceiveQueueRecords', 'kinesis:AcknowledgeQueueRecords'],
      resources: [queueArn],
    }));
    task.addToTaskRolePolicy(streamDecryptStatement(this, config.streamArn));
    orders.grant(task.taskRole, 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem');
    task.addContainer('QueueConsumer', {
      image,
      environment: { ...env, QUEUE_ARN: queueArn },
      logging: new AwsLogDriver({ logGroup: network.logGroup, streamPrefix: 'queue' }),
      stopTimeout: Duration.seconds(30),
    });
    const service = network.service('QueueService', task, config.queueTasks);

    new CfnOutput(this, 'QueueArn', { value: queueArn });
    new CfnOutput(this, 'QueueServiceName', { value: service.serviceName });
  }
}
