import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { SecurityGroup, SubnetType } from 'aws-cdk-lib/aws-ec2';
import {
  AwsLogDriver, Cluster, ContainerImage, CpuArchitecture, FargateService, FargateTaskDefinition, OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { scenarioVpc } from '../../shared/cdk/vpc';
import { anyStreamInAccount, streamDecryptStatement } from '../../shared/cdk/stream-decrypt';
import { WORKER_DIR, bundleWorker } from './bundle-worker';
import type { AtLeastOnceConfig } from './config';

export interface AtLeastOnceStackProps extends StackProps {
  config: AtLeastOnceConfig;
}

/**
 * Fulfillment workers on Fargate that process a Kinesis queue safely under
 * at-least-once delivery. The queue sits on the Quickstart stream and is referenced
 * by ARN, so this stack deploys and is destroyed independently.
 */
export class AtLeastOnceStack extends Stack {
  constructor(scope: Construct, id: string, props: AtLeastOnceStackProps) {
    const { config, ...stackProps } = props;
    super(scope, id, stackProps);

    // One item per fulfilled order, keyed on orderId. The conditional put on orderId
    // is what makes fulfillment idempotent. A dedicated table: don't reuse a KCL lease
    // table, which KCL owns and reads.
    const orders = new Table(this, 'FulfilledOrders', {
      partitionKey: { name: 'orderId', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // A new VPC, or an existing one with -c vpcId=vpc-... (see shared/cdk/vpc.ts).
    const vpc = scenarioVpc(this, 'workers');
    const securityGroup = new SecurityGroup(this, 'WorkerSecurityGroup', {
      vpc,
      description: 'Outbound only: the workers call AWS APIs and accept no connections.',
      allowAllOutbound: true,
    });
    const cluster = new Cluster(this, 'Cluster', { vpc });
    const logGroup = new LogGroup(this, 'WorkerLogs', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const task = new FargateTaskDefinition(this, 'WorkerTask', {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Receive, acknowledge and extend visibility on this one queue. The workers can't
    // create, change or delete the queue, or touch its dead-letter records.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['kinesis:ReceiveQueueRecords', 'kinesis:AcknowledgeQueueRecords', 'kinesis:UpdateQueueRecordsVisibility'],
      resources: [config.queueArn],
    }));
    // Decrypt records from an encrypted stream. Only needed for a customer managed key.
    task.addToTaskRolePolicy(streamDecryptStatement(this, anyStreamInAccount(this)));
    // UpdateItem only counts duplicate deliveries on an order's item, for the demo.
    orders.grant(task.taskRole, 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem');

    bundleWorker();
    task.addContainer('Worker', {
      image: ContainerImage.fromAsset(WORKER_DIR, { platform: Platform.LINUX_ARM64 }),
      environment: {
        AWS_REGION: this.region,
        QUEUE_ARN: config.queueArn,
        ORDERS_TABLE: orders.tableName,
        CONCURRENCY: String(config.concurrency),
        VISIBILITY_TIMEOUT_SECONDS: String(config.visibilityTimeoutSeconds),
        ...(config.heartbeatSeconds ? { HEARTBEAT_SECONDS: String(config.heartbeatSeconds) } : {}),
        MAX_HOLD_SECONDS: String(config.maxHoldSeconds),
        SLOW_PROCESSING_SECONDS: String(config.slowProcessingSeconds),
      },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'worker' }),
      // Time to finish in-flight orders after SIGTERM (deployments). Longer than a
      // slow order takes, so a deployment doesn't cause a redelivery.
      stopTimeout: Duration.seconds(Math.min(120, config.slowProcessingSeconds + 30)),
    });

    const service = new FargateService(this, 'WorkerService', {
      cluster,
      taskDefinition: task,
      // A fixed fleet: two workers, so a redelivered order can go to either one.
      // Scaling on queue metrics is scenario 3.
      desiredCount: config.workers,
      minHealthyPercent: 100,
      maxHealthyPercent: 200,
      assignPublicIp: true,
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      securityGroups: [securityGroup],
      circuitBreaker: { rollback: true },
    });

    new CfnOutput(this, 'QueueArn', { value: config.queueArn });
    new CfnOutput(this, 'ClusterName', { value: cluster.clusterName });
    new CfnOutput(this, 'ServiceName', { value: service.serviceName });
    new CfnOutput(this, 'LogGroupName', { value: logGroup.logGroupName });
    new CfnOutput(this, 'OrdersTableName', { value: orders.tableName });
    new CfnOutput(this, 'WorkerTaskRoleArn', { value: task.taskRole.roleArn });
  }
}
