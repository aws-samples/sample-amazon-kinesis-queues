import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { SecurityGroup, SubnetType } from 'aws-cdk-lib/aws-ec2';
import {
  AwsLogDriver, Cluster, ContainerImage, CpuArchitecture, FargateService, FargateTaskDefinition, OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { scenarioVpc } from '../../shared/cdk/vpc';
import { WORKER_DIR, bundleWorker } from './bundle-worker';
import { type FirstQueueConfig } from './config';
import { KinesisQueue } from './kinesis-queue';

export interface FirstQueueStackProps extends StackProps {
  config: FirstQueueConfig;
}

/**
 * A queue on the Quickstart stream, and Fargate tasks that run the same
 * receive -> process -> acknowledge loop as `npm run queue:consume`.
 */
export class FirstQueueStack extends Stack {
  constructor(scope: Construct, id: string, props: FirstQueueStackProps) {
    const { config, ...stackProps } = props;
    super(scope, id, stackProps);

    // The stream belongs to the Quickstart stack; this stack only reads from it.
    const queue = new KinesisQueue(this, 'Queue', { stream: config.streamArn, queueName: config.stackName });

    // A new VPC, or an existing one with -c vpcId=vpc-... (see shared/cdk/vpc.ts).
    const vpc = scenarioVpc(this, 'consumers');
    const securityGroup = new SecurityGroup(this, 'ConsumerSecurityGroup', {
      vpc,
      description: 'Outbound only: the consumers call AWS APIs and accept no connections.',
      allowAllOutbound: true,
    });
    const cluster = new Cluster(this, 'Cluster', { vpc });
    const logGroup = new LogGroup(this, 'ConsumerLogs', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const task = new FargateTaskDefinition(this, 'ConsumerTask', {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Consumers can only receive and acknowledge on this one queue.
    queue.grantConsume(task.taskRole);

    bundleWorker();
    task.addContainer('Consumer', {
      image: ContainerImage.fromAsset(WORKER_DIR, { platform: Platform.LINUX_ARM64 }),
      environment: {
        AWS_REGION: this.region,
        QUEUE_ARN: queue.queueArn,
      },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'consumer' }),
      // Time to finish processing and acknowledge the current batch after SIGTERM;
      // the long poll is aborted at once.
      stopTimeout: Duration.seconds(30),
    });

    const service = new FargateService(this, 'ConsumerService', {
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

    new CfnOutput(this, 'QueueArn', { value: queue.queueArn });
    new CfnOutput(this, 'QueueName', { value: queue.queueName });
    new CfnOutput(this, 'ConsumerClusterName', { value: cluster.clusterName });
    new CfnOutput(this, 'ConsumerServiceName', { value: service.serviceName });
    new CfnOutput(this, 'ConsumerLogGroupName', { value: logGroup.logGroupName });
  }
}
