import { Arn, ArnFormat, CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { AdjustmentType } from 'aws-cdk-lib/aws-applicationautoscaling';
import { Dashboard, GraphWidget, Metric, TextWidget } from 'aws-cdk-lib/aws-cloudwatch';
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
import { QUEUE_NAME, type ScaleOutConfig } from './config';

export interface ScaleOutStackProps extends StackProps {
  config: ScaleOutConfig;
}

/**
 * Identical, stateless fulfillment workers on Fargate, consuming one queue on the
 * Quickstart stream. The worker count follows the queue backlog through CloudWatch
 * alarms, independent of the stream's shard count.
 */
export class ScaleOutStack extends Stack {
  constructor(scope: Construct, id: string, props: ScaleOutStackProps) {
    const { config, ...stackProps } = props;
    super(scope, id, stackProps);

    const queueId = Arn.split(config.queueArn, ArnFormat.SLASH_RESOURCE_NAME).resourceName!;
    // Queue metrics are published per queue with QueueId and QueueName dimensions.
    const queueMetric = (metricName: string, statistic: string) => new Metric({
      namespace: 'AWS/Kinesis',
      metricName,
      dimensionsMap: { QueueId: queueId, QueueName: QUEUE_NAME },
      statistic,
      period: Duration.minutes(1),
    });
    const backlog = queueMetric('VisibleQueueRecords', 'Maximum');

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
    // Workers can only receive and acknowledge on this one queue.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['kinesis:ReceiveQueueRecords', 'kinesis:AcknowledgeQueueRecords'],
      resources: [config.queueArn],
    }));
    // Decrypt records from an encrypted stream. Only needed for a customer managed key.
    task.addToTaskRolePolicy(streamDecryptStatement(this, anyStreamInAccount(this)));

    bundleWorker();
    task.addContainer('Worker', {
      image: ContainerImage.fromAsset(WORKER_DIR, { platform: Platform.LINUX_ARM64 }),
      environment: {
        AWS_REGION: this.region,
        QUEUE_ARN: config.queueArn,
        CONCURRENCY: String(config.concurrency),
        PROCESSING_MS_MIN: String(config.processingMsMin),
        PROCESSING_MS_MAX: String(config.processingMsMax),
        STATS_INTERVAL_SECONDS: '10',
      },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'worker' }),
      // Time to finish in-flight orders after SIGTERM (scale-in, deployments).
      stopTimeout: Duration.seconds(30),
    });

    const service = new FargateService(this, 'WorkerService', {
      cluster,
      taskDefinition: task,
      // No desiredCount: the task count is owned by the backlog alarms below, and a
      // deploy never resets it. Tasks are interchangeable and aren't tied to shards.
      minHealthyPercent: 100,
      maxHealthyPercent: 200,
      assignPublicIp: true,
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      securityGroups: [securityGroup],
      circuitBreaker: { rollback: true },
    });

    // The only way the worker count changes: CloudWatch alarms on the queue backlog
    // drive ECS Service Auto Scaling step policies.
    const scaling = service.autoScaleTaskCount({ minCapacity: config.minTasks, maxCapacity: config.maxTasks });
    scaling.scaleOnMetric('QueueBacklog', {
      metric: backlog,
      adjustmentType: AdjustmentType.CHANGE_IN_CAPACITY,
      scalingSteps: [
        { upper: 20, change: -1 }, // backlog nearly drained: remove a task
        { lower: 200, change: +1 }, // backlog building: add a task
        { lower: 1000, change: +3 }, // large backlog: add three
      ],
      cooldown: Duration.minutes(1),
      evaluationPeriods: 1,
    });

    const dashboard = new Dashboard(this, 'Dashboard', { defaultInterval: Duration.hours(1) });
    dashboard.addWidgets(new TextWidget({
      markdown: `### Scale out on queue \`${queueId}\`\nAdd workers and watch the backlog drain faster. The stream's shard count doesn't limit them.`,
      width: 24,
      height: 2,
    }));
    dashboard.addWidgets(
      new GraphWidget({
        title: 'Backlog: orders waiting and in flight',
        left: [backlog, queueMetric('InFlightQueueRecords', 'Maximum')],
        width: 12,
      }),
      new GraphWidget({
        title: 'Orders acknowledged per minute',
        left: [queueMetric('AcknowledgedQueueRecords', 'Sum')],
        width: 12,
      }),
    );
    dashboard.addWidgets(new GraphWidget({
      title: 'Worker CPU (average across tasks)',
      left: [new Metric({
        namespace: 'AWS/ECS',
        metricName: 'CPUUtilization',
        dimensionsMap: { ClusterName: cluster.clusterName, ServiceName: service.serviceName },
        statistic: 'Average',
        period: Duration.minutes(1),
      })],
      width: 24,
    }));

    new CfnOutput(this, 'QueueArn', { value: config.queueArn });
    new CfnOutput(this, 'ClusterName', { value: cluster.clusterName });
    new CfnOutput(this, 'ServiceName', { value: service.serviceName });
    new CfnOutput(this, 'LogGroupName', { value: logGroup.logGroupName });
    new CfnOutput(this, 'AutoScaling', { value: `${config.minTasks}-${config.maxTasks} tasks on queue backlog` });
    new CfnOutput(this, 'DashboardUrl', {
      value: `https://${this.region}.console.aws.amazon.com/cloudwatch/home?region=${this.region}#dashboards/dashboard/${dashboard.dashboardName}`,
    });
  }
}
