import { Arn, ArnFormat, CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { AttributeType, BillingMode, ProjectionType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { type IVpc, SecurityGroup, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import {
  AwsLogDriver, Cluster, ContainerImage, CpuArchitecture, FargateService, FargateTaskDefinition, OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { streamDecryptStatement } from '../../shared/cdk/stream-decrypt';
import { FRAUD_SIGNALS_DIR, WORKERS_DIR, bundleWorkers } from './bundle-workers';
import type { OneStreamConfig } from './config';

export interface OneStreamStackProps extends StackProps {
  config: OneStreamConfig;
}

/**
 * Three consumers of the Quickstart stream, each independent of the others:
 * - fulfillment: a queue consumer, one item per order in its own table;
 * - notifications: a second queue consumer on a second queue, added later;
 * - fraud signals: a KCL stream consumer that reads every record in order per shard.
 *
 * The stream and queues are referenced by ARN, not owned here.
 */
export class OneStreamStack extends Stack {
  private readonly cluster: Cluster;
  private readonly logGroup: LogGroup;
  private readonly securityGroup: SecurityGroup;

  constructor(scope: Construct, id: string, props: OneStreamStackProps) {
    const { config, ...stackProps } = props;
    super(scope, id, stackProps);

    // An existing VPC (-c vpcId=...) helps accounts at their VPC quota; for example
    // the Quickstart's. CDK looks it up at synth time and caches it in cdk.context.json.
    const vpc: IVpc = config.vpcId
      ? Vpc.fromLookup(this, 'Vpc', { vpcId: config.vpcId })
      : new Vpc(this, 'Vpc', {
        maxAzs: 2,
        natGateways: 0,
        subnetConfiguration: [{ name: 'consumers', subnetType: SubnetType.PUBLIC }],
      });
    if (config.vpcId && vpc.publicSubnets.length === 0) {
      throw new Error(`VPC ${config.vpcId} has no public subnets. The tasks need them, because there's no NAT gateway.`);
    }
    this.securityGroup = new SecurityGroup(this, 'ConsumerSecurityGroup', {
      vpc,
      description: 'Outbound only: the consumers call AWS APIs and accept no connections.',
      allowAllOutbound: true,
    });
    this.cluster = new Cluster(this, 'Cluster', { vpc });
    this.logGroup = new LogGroup(this, 'ConsumerLogs', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    bundleWorkers();
    const workerImage = ContainerImage.fromAsset(WORKERS_DIR, { platform: Platform.LINUX_ARM64 });

    const fulfillment = this.addQueueWorker('Fulfillment', 'fulfillment', config.fulfillmentQueueArn, config.streamArn, workerImage);
    const notifications = config.notificationsQueueArn
      ? this.addQueueWorker('Notifications', 'notifications', config.notificationsQueueArn, config.streamArn, workerImage)
      : undefined;
    const fraud = this.addFraudSignals(config);

    new CfnOutput(this, 'ClusterName', { value: this.cluster.clusterName });
    new CfnOutput(this, 'LogGroupName', { value: this.logGroup.logGroupName });
    new CfnOutput(this, 'StreamArn', { value: config.streamArn });
    new CfnOutput(this, 'FulfillmentQueueArn', { value: config.fulfillmentQueueArn });
    new CfnOutput(this, 'FulfillmentServiceName', { value: fulfillment.service.serviceName });
    new CfnOutput(this, 'FulfillmentTableName', { value: fulfillment.table.tableName });
    new CfnOutput(this, 'FraudSignalsServiceName', { value: fraud.serviceName });
    if (notifications) {
      new CfnOutput(this, 'NotificationsQueueArn', { value: config.notificationsQueueArn! });
      new CfnOutput(this, 'NotificationsServiceName', { value: notifications.service.serviceName });
      new CfnOutput(this, 'NotificationsTableName', { value: notifications.table.tableName });
    }
  }

  private service(id: string, task: FargateTaskDefinition): FargateService {
    return new FargateService(this, `${id}Service`, {
      cluster: this.cluster,
      taskDefinition: task,
      desiredCount: 1,
      minHealthyPercent: 100,
      maxHealthyPercent: 200,
      assignPublicIp: true,
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      securityGroups: [this.securityGroup],
      circuitBreaker: { rollback: true },
    });
  }

  /** A queue worker with its own table and a role scoped to its one queue. */
  private addQueueWorker(id: string, role: 'fulfillment' | 'notifications', queueArn: string, streamArn: string, image: ContainerImage) {
    // One item per order that this consumer has handled, keyed on orderId.
    const table = new Table(this, `${id}Table`, {
      partitionKey: { name: 'orderId', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const task = new FargateTaskDefinition(this, `${id}Task`, {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Each worker can only receive and acknowledge on its own queue.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['kinesis:ReceiveQueueRecords', 'kinesis:AcknowledgeQueueRecords'],
      resources: [queueArn],
    }));
    // Decrypt records from an encrypted stream. Only needed for a customer managed key.
    task.addToTaskRolePolicy(streamDecryptStatement(this, streamArn));
    table.grant(task.taskRole, 'dynamodb:PutItem');
    task.addContainer(id, {
      image,
      environment: { ROLE: role, AWS_REGION: this.region, QUEUE_ARN: queueArn, TABLE_NAME: table.tableName },
      logging: new AwsLogDriver({ logGroup: this.logGroup, streamPrefix: role }),
      stopTimeout: Duration.seconds(30),
    });
    return { table, service: this.service(id, task) };
  }

  /** The KCL stream consumer, with its lease table. Same setup as the Quickstart consumer. */
  private addFraudSignals(config: OneStreamConfig): FargateService {
    const streamName = Arn.split(config.streamArn, ArnFormat.SLASH_RESOURCE_NAME).resourceName!;
    // KCL's lease table and index, created up front so they're ready before the task starts.
    const leases = new Table(this, 'FraudSignalsLeases', {
      partitionKey: { name: 'leaseKey', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    leases.addGlobalSecondaryIndex({
      indexName: 'LeaseOwnerToLeaseKeyIndex',
      partitionKey: { name: 'leaseOwner', type: AttributeType.STRING },
      sortKey: { name: 'leaseKey', type: AttributeType.STRING },
      projectionType: ProjectionType.KEYS_ONLY,
    });

    const task = new FargateTaskDefinition(this, 'FraudSignalsTask', {
      cpu: 256,
      memoryLimitMiB: 1024,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Polling reads (GetRecords), so it doesn't register an enhanced fan-out consumer.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: [
        'kinesis:DescribeStream', 'kinesis:DescribeStreamSummary',
        'kinesis:ListShards', 'kinesis:GetShardIterator', 'kinesis:GetRecords',
      ],
      resources: [config.streamArn],
    }));
    // Decrypt records from an encrypted stream. Only needed for a customer managed key.
    task.addToTaskRolePolicy(streamDecryptStatement(this, config.streamArn));
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: [
        'dynamodb:DescribeTable', 'dynamodb:Scan', 'dynamodb:GetItem',
        'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem',
      ],
      resources: [leases.tableArn],
    }));
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['dynamodb:Query'],
      resources: [`${leases.tableArn}/index/LeaseOwnerToLeaseKeyIndex`],
    }));
    // KCL checks for these legacy tables even for a new single-table application.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['dynamodb:DescribeTable'],
      resources: [`${leases.tableArn}-CoordinatorState`, `${leases.tableArn}-WorkerMetricStats`],
    }));
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['cloudwatch:PutMetricData'],
      resources: ['*'],
      conditions: { StringEquals: { 'cloudwatch:namespace': leases.tableName } },
    }));
    task.addContainer('FraudSignals', {
      image: ContainerImage.fromAsset(FRAUD_SIGNALS_DIR, { platform: Platform.LINUX_ARM64 }),
      environment: {
        STREAM_NAME: streamName,
        AWS_REGION: this.region,
        APPLICATION_NAME: leases.tableName,
        VELOCITY_WINDOW_SECONDS: String(config.velocityWindowSeconds),
        VELOCITY_THRESHOLD: String(config.velocityThreshold),
        JAVA_TOOL_OPTIONS: '-XX:MaxRAMPercentage=60.0 -XX:MaxDirectMemorySize=128m -XX:+ExitOnOutOfMemoryError',
      },
      logging: new AwsLogDriver({ logGroup: this.logGroup, streamPrefix: 'fraud-signals' }),
      stopTimeout: Duration.seconds(120),
    });
    const service = this.service('FraudSignals', task);
    service.node.addDependency(leases);
    return service;
  }
}
