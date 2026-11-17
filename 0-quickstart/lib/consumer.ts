import * as path from 'node:path';
import { CfnOutput, Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import { AttributeType, BillingMode, ProjectionType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { SecurityGroup, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import {
  AwsLogDriver, Cluster, ContainerImage, CpuArchitecture, FargateService,
  FargateTaskDefinition, OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { IStream } from 'aws-cdk-lib/aws-kinesis';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

export class KinesisConsumer extends Construct {
  constructor(scope: Construct, id: string, stream: IStream) {
    super(scope, id);
    const stack = Stack.of(this);

    // Create KCL's table and index together so they are ready before the task starts.
    const checkpoints = new Table(this, 'Checkpoints', {
      partitionKey: { name: 'leaseKey', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    checkpoints.addGlobalSecondaryIndex({
      indexName: 'LeaseOwnerToLeaseKeyIndex',
      partitionKey: { name: 'leaseOwner', type: AttributeType.STRING },
      sortKey: { name: 'leaseKey', type: AttributeType.STRING },
      projectionType: ProjectionType.KEYS_ONLY,
    });

    const vpc = new Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [{ name: 'consumer', subnetType: SubnetType.PUBLIC }],
    });
    const securityGroup = new SecurityGroup(this, 'SecurityGroup', {
      vpc,
      description: 'Outbound access for the Kinesis consumer; no inbound connections.',
      allowAllOutbound: true,
    });
    const cluster = new Cluster(this, 'Cluster', { vpc });
    const logGroup = new LogGroup(this, 'Logs', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const task = new FargateTaskDefinition(this, 'Task', {
      cpu: 256,
      memoryLimitMiB: 1024,
      runtimePlatform: {
        cpuArchitecture: CpuArchitecture.X86_64,
        operatingSystemFamily: OperatingSystemFamily.LINUX,
      },
    });
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: [
        'kinesis:DescribeStream', 'kinesis:DescribeStreamSummary',
        'kinesis:ListShards', 'kinesis:GetShardIterator', 'kinesis:GetRecords',
      ],
      resources: [stream.streamArn],
    }));
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: [
        'dynamodb:DescribeTable', 'dynamodb:Scan', 'dynamodb:GetItem',
        'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem',
      ],
      resources: [checkpoints.tableArn],
    }));
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['dynamodb:Query'],
      resources: [`${checkpoints.tableArn}/index/LeaseOwnerToLeaseKeyIndex`],
    }));
    // KCL checks for these legacy tables even when using a fresh single-table application.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['dynamodb:DescribeTable'],
      resources: [
        `${checkpoints.tableArn}-CoordinatorState`,
        `${checkpoints.tableArn}-WorkerMetricStats`,
      ],
    }));
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['cloudwatch:PutMetricData'],
      resources: ['*'],
      conditions: { StringEquals: { 'cloudwatch:namespace': checkpoints.tableName } },
    }));
    task.addContainer('Consumer', {
      image: ContainerImage.fromAsset(path.join(__dirname, '..', 'consumer'), {
        platform: Platform.LINUX_AMD64,
      }),
      environment: {
        STREAM_NAME: stream.streamName,
        AWS_REGION: stack.region,
        APPLICATION_NAME: checkpoints.tableName,
        JAVA_TOOL_OPTIONS: '-XX:MaxRAMPercentage=60.0 -XX:MaxDirectMemorySize=128m -XX:+ExitOnOutOfMemoryError',
      },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'consumer' }),
      stopTimeout: Duration.seconds(120),
    });
    const service = new FargateService(this, 'Service', {
      cluster,
      taskDefinition: task,
      desiredCount: 1,
      minHealthyPercent: 100,
      maxHealthyPercent: 200,
      assignPublicIp: true,
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      securityGroups: [securityGroup],
      circuitBreaker: { rollback: true },
    });
    service.node.addDependency(checkpoints);

    new CfnOutput(stack, 'ConsumerApplicationName', { value: checkpoints.tableName });
    new CfnOutput(stack, 'ConsumerClusterName', { value: cluster.clusterName });
    new CfnOutput(stack, 'ConsumerServiceName', { value: service.serviceName });
    new CfnOutput(stack, 'ConsumerLogGroupName', { value: logGroup.logGroupName });
  }
}
