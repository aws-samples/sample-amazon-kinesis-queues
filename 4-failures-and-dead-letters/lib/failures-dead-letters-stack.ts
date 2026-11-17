import { Arn, ArnFormat, CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import {
  Alarm, AlarmStatusWidget, ComparisonOperator, Dashboard, GraphWidget, MathExpression, Metric, TextWidget, TreatMissingData,
} from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { SecurityGroup, SubnetType } from 'aws-cdk-lib/aws-ec2';
import {
  AwsLogDriver, Cluster, ContainerImage, CpuArchitecture, FargateService, FargateTaskDefinition, OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { Construct } from 'constructs';
import { scenarioVpc } from '../../shared/cdk/vpc';
import { anyStreamInAccount, streamDecryptStatement } from '../../shared/cdk/stream-decrypt';
import { WORKER_DIR, bundleWorker } from './bundle-worker';
import { INITIAL_DISCONTINUED_SKUS, QUEUE_NAME, type FulfillmentConfig } from './config';

export interface FailuresDeadLettersStackProps extends StackProps {
  config: FulfillmentConfig;
}

/**
 * Order fulfillment workers on Fargate, consuming a Kinesis queue that sits on the
 * base demo's stream. The stream and queue are referenced by ARN, not owned here,
 * so this stack deploys and is destroyed independently of the base stack.
 */
export class FailuresDeadLettersStack extends Stack {
  constructor(scope: Construct, id: string, props: FailuresDeadLettersStackProps) {
    const { config, ...stackProps } = props;
    super(scope, id, stackProps);

    const queueId = Arn.split(config.queueArn, ArnFormat.SLASH_RESOURCE_NAME).resourceName!;

    // One item per fulfilled order. The conditional put on orderId is what makes
    // fulfillment idempotent under at-least-once delivery.
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
      cpu: 512,
      memoryLimitMiB: 1024,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Workers can only receive and acknowledge on this one queue. They can't create,
    // change or delete it. Add kinesis:UpdateQueueRecordsVisibility if a worker starts
    // extending visibility for long-running records.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['kinesis:ReceiveQueueRecords', 'kinesis:AcknowledgeQueueRecords'],
      resources: [config.queueArn],
    }));
    // Decrypt records from an encrypted stream. Only needed for a customer managed key.
    task.addToTaskRolePolicy(streamDecryptStatement(this, anyStreamInAccount(this)));
    orders.grant(task.taskRole, 'dynamodb:GetItem', 'dynamodb:PutItem');

    // The product catalog's discontinued SKUs. Workers read it at run time, so an
    // operator fixes the cause of failing orders by changing data, not by redeploying.
    const catalog = new StringParameter(this, 'DiscontinuedSkus', {
      parameterName: `/${this.stackName}/catalog/discontinued-skus`,
      description: 'Comma-separated SKUs the simulated warehouse rejects, or "none"',
      stringValue: INITIAL_DISCONTINUED_SKUS,
    });
    catalog.grantRead(task.taskRole);

    // The simulated warehouse API's status, "up" or "down". Setting it to "down" makes
    // every order fail, like an outage of a downstream service.
    const warehouse = new StringParameter(this, 'WarehouseStatus', {
      parameterName: `/${this.stackName}/warehouse/status`,
      description: 'Status of the simulated warehouse API: "up" or "down"',
      stringValue: 'up',
    });
    warehouse.grantRead(task.taskRole);

    bundleWorker();
    const image = ContainerImage.fromAsset(WORKER_DIR, { platform: Platform.LINUX_ARM64 });
    task.addContainer('Worker', {
      image,
      environment: {
        AWS_REGION: this.region,
        QUEUE_ARN: config.queueArn,
        ORDERS_TABLE: orders.tableName,
        CATALOG_PARAMETER: catalog.parameterName,
        WAREHOUSE_PARAMETER: warehouse.parameterName,
        CONCURRENCY: String(config.concurrency),
        PROCESSING_MS_MIN: String(config.processingMsMin),
        PROCESSING_MS_MAX: String(config.processingMsMax),
      },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'worker' }),
      // Time to finish in-flight orders after SIGTERM (scale-in, deployments).
      stopTimeout: Duration.seconds(60),
    });

    const service = new FargateService(this, 'WorkerService', {
      cluster,
      taskDefinition: task,
      // One worker is enough to show every failure path. Scaling is scenario 3.
      desiredCount: 1,
      minHealthyPercent: 100,
      maxHealthyPercent: 200,
      assignPublicIp: true,
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      securityGroups: [securityGroup],
      circuitBreaker: { rollback: true },
    });

    // Dead-letter task: the shared dead-letter tool (npm run queue:dead-letters) under a
    // separate operator role, started on demand with RunTask after the cause of the
    // failures is fixed. It lists, restores or discards dead-letter records; restored records go
    // back to the workers. Same image, different entry point. It isn't a service, so
    // nothing runs (or costs) between runs.
    const deadLetterTask = new FargateTaskDefinition(this, 'DeadLetterTask', {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Only the dead-letter operations. It can't receive or acknowledge regular records,
    // so it can't compete with the workers, and the workers can't touch dead-letter records.
    // It doesn't process orders, so it has no access to the orders table or catalog.
    deadLetterTask.addToTaskRolePolicy(new PolicyStatement({
      actions: [
        'kinesis:ReceiveDeadLetterQueueRecords',
        'kinesis:AcknowledgeDeadLetterQueueRecords',
        'kinesis:RestoreDeadLetterQueueRecords',
      ],
      resources: [config.queueArn],
    }));
    // Dead-letter records are stream records too, so the same decrypt statement applies.
    deadLetterTask.addToTaskRolePolicy(streamDecryptStatement(this, anyStreamInAccount(this)));
    deadLetterTask.addContainer('DeadLetterConsumer', {
      image,
      // The default lists the dead-letter records. npm run orders:dead-letter-task overrides
      // the command to restore or discard a selection.
      command: ['node', '--enable-source-maps', 'dead-letters.js', 'list', '--queue-arn', config.queueArn],
      environment: { AWS_REGION: this.region },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'dead-letters' }),
    });

    this.addDashboard(queueId, service, config.alarmEmail);

    new CfnOutput(this, 'DeadLetterTaskDefinitionArn', { value: deadLetterTask.taskDefinitionArn });
    new CfnOutput(this, 'DeadLetterTaskRoleArn', { value: deadLetterTask.taskRole.roleArn });
    new CfnOutput(this, 'WorkerTaskRoleArn', { value: task.taskRole.roleArn });
    new CfnOutput(this, 'SubnetIds', { value: vpc.publicSubnets.map((s) => s.subnetId).join(',') });
    new CfnOutput(this, 'SecurityGroupId', { value: securityGroup.securityGroupId });

    new CfnOutput(this, 'QueueArn', { value: config.queueArn });
    new CfnOutput(this, 'ClusterName', { value: cluster.clusterName });
    new CfnOutput(this, 'ServiceName', { value: service.serviceName });
    new CfnOutput(this, 'LogGroupName', { value: logGroup.logGroupName });
    new CfnOutput(this, 'OrdersTableName', { value: orders.tableName });
    new CfnOutput(this, 'CatalogParameterName', { value: catalog.parameterName });
    new CfnOutput(this, 'WarehouseParameterName', { value: warehouse.parameterName });
  }

  private addDashboard(queueId: string, service: FargateService, alarmEmail: string | undefined): void {
    // Queue metrics are published per queue with QueueId and QueueName dimensions.
    const queueMetric = (metricName: string, statistic: string) => new Metric({
      namespace: 'AWS/Kinesis',
      metricName,
      dimensionsMap: { QueueId: queueId, QueueName: QUEUE_NAME },
      statistic,
      period: Duration.minutes(1),
    });
    const cpu = new Metric({
      namespace: 'AWS/ECS',
      metricName: 'CPUUtilization',
      dimensionsMap: { ClusterName: service.cluster.clusterName, ServiceName: service.serviceName },
      statistic: 'Average',
      period: Duration.minutes(1),
    });

    const dashboard = new Dashboard(this, 'Dashboard', { defaultInterval: Duration.hours(1) });
    dashboard.addWidgets(new TextWidget({
      markdown: `### Failures and dead-letter records on queue \`${queueId}\`\nFailed orders are redelivered on their own; `
        + 'orders that keep failing become dead-letter records and set off the alarm.',
      width: 24,
      height: 2,
    }));
    dashboard.addWidgets(
      new GraphWidget({
        title: 'Backlog: orders waiting and in flight',
        left: [queueMetric('VisibleQueueRecords', 'Maximum'), queueMetric('InFlightQueueRecords', 'Maximum')],
        width: 12,
      }),
      new GraphWidget({
        title: 'Throughput per minute',
        left: [queueMetric('ReceivedQueueRecords', 'Sum'), queueMetric('AcknowledgedQueueRecords', 'Sum')],
        width: 12,
      }),
    );
    dashboard.addWidgets(
      new GraphWidget({
        title: 'Worker CPU (average across tasks)',
        left: [cpu],
        width: 12,
      }),
      new GraphWidget({
        title: 'Empty receives (idle workers)',
        left: [queueMetric('EmptyReceives', 'Sum')],
        width: 12,
      }),
    );
    // Alert as soon as any order lands in the dead-letter state: each one is an order
    // that won't ship until someone fixes the cause and resolves it. Count dead-letter records
    // that are visible and those an operator is reading (in flight), so the alarm
    // doesn't drop to OK while the operator tool holds them.
    const visibleDeadLetters = queueMetric('VisibleDeadLetterQueueRecords', 'Maximum');
    const inFlightDeadLetters = queueMetric('InFlightDeadLetterQueueRecords', 'Maximum');
    const deadLetters = new MathExpression({
      expression: 'FILL(visible, 0) + FILL(inflight, 0)',
      usingMetrics: { visible: visibleDeadLetters, inflight: inFlightDeadLetters },
      label: 'Dead-letter records (visible + in flight)',
      period: Duration.minutes(1),
    });
    const topic = new Topic(this, 'DeadLetterAlarmTopic', { displayName: 'Order fulfillment dead-letter records' });
    if (alarmEmail) topic.addSubscription(new EmailSubscription(alarmEmail));
    const alarm = new Alarm(this, 'DeadLetterAlarm', {
      alarmDescription: `Orders in the dead-letter state of queue ${QUEUE_NAME} (${queueId}). `
        + 'List them with: npm run queue:dead-letters -- --queue-name order-fulfillment',
      metric: deadLetters,
      threshold: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
      // No datapoint means no dead-letter records reported, not a problem.
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });
    alarm.addAlarmAction(new SnsAction(topic));
    alarm.addOkAction(new SnsAction(topic));

    dashboard.addWidgets(
      new AlarmStatusWidget({ title: 'Dead-letter alarm', alarms: [alarm], width: 6, height: 6 }),
      new GraphWidget({
        title: 'Dead-letter records (records that reached the max receive count)',
        left: [visibleDeadLetters, inFlightDeadLetters],
        leftAnnotations: [{ value: 1, label: 'alarm threshold' }],
        width: 18,
      }),
    );
    new CfnOutput(this, 'DeadLetterAlarmName', { value: alarm.alarmName });
    new CfnOutput(this, 'DeadLetterAlarmTopicArn', { value: topic.topicArn });
    new CfnOutput(this, 'DashboardUrl', {
      value: `https://${this.region}.console.aws.amazon.com/cloudwatch/home?region=${this.region}#dashboards/dashboard/${dashboard.dashboardName}`,
    });
  }
}
