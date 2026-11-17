import { Arn, ArnFormat, CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { Dashboard, GraphWidget, MathExpression, Metric, TextWidget } from 'aws-cdk-lib/aws-cloudwatch';
import { AttributeType, BillingMode, ProjectionType, Table } from 'aws-cdk-lib/aws-dynamodb';
import {
  AwsLogDriver, ContainerImage, CpuArchitecture, FargateService, FargateTaskDefinition, OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Architecture, Code, Function as LambdaFunction, Runtime } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { streamDecryptStatement } from '../../shared/cdk/stream-decrypt';
import { KCL_CONSUMER_DIR, SIDELINE_DIR, WORKER_DIR, bundleWorker } from './bundle-worker';
import { QUEUE_NAME, type MigrationConfig } from './config';
import { consumerNetwork, type ConsumerNetwork } from './network';

export interface MigrationStackProps extends StackProps {
  config: MigrationConfig;
}

/** Products the warehouse no longer stocks. The order producer's --poison option orders this one. */
const DISCONTINUED_SKUS = 'SKU-DISCONTINUED';
/** Namespace of the custom metrics the KCL consumer publishes (see kcl-consumer/.../Stats.java). */
const KCL_METRICS_NAMESPACE = 'KinesisQueuesDemo/KclFulfillment';

interface Shared {
  config: MigrationConfig;
  streamName: string;
  orders: Table;
  network: ConsumerNetwork;
  logGroup: LogGroup;
  fulfillmentEnv: Record<string, string>;
}

/**
 * Order fulfillment on the Quickstart stream, in three phases:
 *
 * - before: a KCL consumer on Fargate, with its lease table, the SQS queue it sidelines
 *   failed orders to, the Lambda function that works through that queue, and a
 *   dashboard built from the custom metrics it publishes.
 * - cutover: the same, plus queue consumers on Fargate reading a queue on the stream.
 *   Both consumers write to one orders table, which keeps any order from shipping twice.
 * - after: only the queue consumers. Deploying this phase removes every KCL resource.
 *
 * The stream and the queue are referenced by ARN, not owned here.
 */
export class MigrationStack extends Stack {
  constructor(scope: Construct, id: string, props: MigrationStackProps) {
    const { config, ...stackProps } = props;
    super(scope, id, stackProps);

    const streamName = Arn.split(config.streamArn, ArnFormat.SLASH_RESOURCE_NAME).resourceName!;

    // The business data: one item per fulfilled order, with a conditional put on orderId.
    // Both consumers use it, so it's what makes running them side by side safe.
    const orders = new Table(this, 'FulfilledOrders', {
      partitionKey: { name: 'orderId', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const network = consumerNetwork(this, config.vpcId);

    bundleWorker();
    const shared: Shared = {
      config,
      streamName,
      orders,
      network,
      logGroup: network.logGroup,
      // The same fulfillment rules for every consumer.
      fulfillmentEnv: {
        ORDERS_TABLE: orders.tableName,
        PROCESSING_MS_MIN: String(config.processingMsMin),
        PROCESSING_MS_MAX: String(config.processingMsMax),
        DISCONTINUED_SKUS,
      },
    };
    if (config.phase !== 'after') this.addKclConsumer(shared);
    if (config.phase !== 'before') this.addQueueConsumer(shared);

    new CfnOutput(this, 'Phase', { value: config.phase });
    new CfnOutput(this, 'StreamArn', { value: config.streamArn });
    new CfnOutput(this, 'OrdersTableName', { value: orders.tableName });
    new CfnOutput(this, 'ClusterName', { value: network.cluster.clusterName });
    new CfnOutput(this, 'LogGroupName', { value: network.logGroup.logGroupName });
  }

  /** The consumer before the migration, and everything it needs around it. */
  private addKclConsumer(shared: Shared): void {
    const { config, orders, logGroup } = shared;

    // KCL's lease table and index, created before the tasks start (as in the Quickstart).
    // It holds one checkpoint per shard and which task owns each shard.
    const leases = new Table(this, 'KclLeases', {
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

    // The team's own dead-letter queue for the stream consumer: orders a KCL task gave
    // up on. Behind it, a second dead-letter queue for orders that fail there too.
    const sidelineDeadLetters = new Queue(this, 'SidelineDeadLetters', {
      retentionPeriod: Duration.days(14),
      enforceSSL: true,
    });
    const sideline = new Queue(this, 'Sideline', {
      // At least six times the function timeout, as Lambda recommends for SQS sources.
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.days(4),
      enforceSSL: true,
      deadLetterQueue: { queue: sidelineDeadLetters, maxReceiveCount: 3 },
    });

    // The separate consumer the sideline queue needs. Putting the orders back on the
    // stream instead would send them to every consumer of the stream.
    const sidelineLogs = new LogGroup(this, 'SidelineConsumerLogs', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const sidelineConsumer = new LambdaFunction(this, 'SidelineConsumer', {
      description: 'Fulfills orders the KCL consumer sidelined. Removed by the migration.',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      handler: 'sideline.handler',
      code: Code.fromAsset(SIDELINE_DIR),
      timeout: Duration.seconds(10),
      memorySize: 256,
      // Lambda sets AWS_REGION itself.
      environment: { ...shared.fulfillmentEnv, NODE_OPTIONS: '--enable-source-maps' },
      logGroup: sidelineLogs,
    });
    sidelineConsumer.addEventSource(new SqsEventSource(sideline, { batchSize: 10, reportBatchItemFailures: true, maxConcurrency: 2 }));
    orders.grant(sidelineConsumer, 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem');

    const task = new FargateTaskDefinition(this, 'KclTask', {
      cpu: 256,
      memoryLimitMiB: 1024,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Read the stream directly: list its shards and poll each one the task leases.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: [
        'kinesis:DescribeStream', 'kinesis:DescribeStreamSummary',
        'kinesis:ListShards', 'kinesis:GetShardIterator', 'kinesis:GetRecords',
      ],
      resources: [config.streamArn],
    }));
    // Decrypt records from an encrypted stream. Only needed for a customer managed key.
    task.addToTaskRolePolicy(streamDecryptStatement(this, config.streamArn));
    // Leases and checkpoints. The same permissions as the Quickstart consumer.
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
    // KCL checks for these legacy tables even when using a fresh single-table application.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['dynamodb:DescribeTable'],
      resources: [`${leases.tableArn}-CoordinatorState`, `${leases.tableArn}-WorkerMetricStats`],
    }));
    // KCL's own metrics, published to a namespace named after the application.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['cloudwatch:PutMetricData'],
      resources: ['*'],
      conditions: { StringEquals: { 'cloudwatch:namespace': leases.tableName } },
    }));
    orders.grant(task.taskRole, 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem');
    sideline.grantSendMessages(task.taskRole);

    task.addContainer('KclConsumer', {
      image: ContainerImage.fromAsset(KCL_CONSUMER_DIR, { platform: Platform.LINUX_ARM64 }),
      environment: {
        ...shared.fulfillmentEnv,
        AWS_REGION: this.region,
        STREAM_NAME: shared.streamName,
        // The lease table's name is the KCL application name, as in the Quickstart.
        APPLICATION_NAME: leases.tableName,
        SIDELINE_QUEUE_URL: sideline.queueUrl,
        MAX_ATTEMPTS: '3',
        MAX_RECORDS: '25',
        INITIAL_POSITION: config.kclInitialPosition,
        JAVA_TOOL_OPTIONS: '-XX:MaxRAMPercentage=60.0 -XX:MaxDirectMemorySize=128m -XX:+ExitOnOutOfMemoryError',
      },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'kcl' }),
      // Time to finish the current batch and checkpoint after SIGTERM.
      stopTimeout: Duration.seconds(120),
    });
    const kclService = shared.network.service('KclService', task, config.kclTasks);
    kclService.node.addDependency(leases);

    const dashboard = this.addKclDashboard(shared, leases.tableName, sideline, sidelineDeadLetters, kclService);

    new CfnOutput(this, 'KclApplicationName', { value: leases.tableName });
    new CfnOutput(this, 'KclLeaseTableName', { value: leases.tableName });
    new CfnOutput(this, 'KclServiceName', { value: kclService.serviceName });
    new CfnOutput(this, 'SidelineQueueUrl', { value: sideline.queueUrl });
    new CfnOutput(this, 'SidelineDeadLetterQueueUrl', { value: sidelineDeadLetters.queueUrl });
    new CfnOutput(this, 'SidelineFunctionName', { value: sidelineConsumer.functionName });
    new CfnOutput(this, 'SidelineLogGroupName', { value: sidelineLogs.logGroupName });
    new CfnOutput(this, 'KclDashboardUrl', { value: this.dashboardUrl(dashboard) });
  }

  /** The consumer after the migration: receive, fulfill, acknowledge. */
  private addQueueConsumer(shared: Shared): void {
    const { config, orders, logGroup } = shared;
    const queueArn = config.queueArn!;

    const task = new FargateTaskDefinition(this, 'QueueTask', {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: { cpuArchitecture: CpuArchitecture.ARM64, operatingSystemFamily: OperatingSystemFamily.LINUX },
    });
    // Two actions on one queue. No stream, lease table, metrics or sideline permissions.
    task.addToTaskRolePolicy(new PolicyStatement({
      actions: ['kinesis:ReceiveQueueRecords', 'kinesis:AcknowledgeQueueRecords'],
      resources: [queueArn],
    }));
    // Decrypt records from an encrypted stream. Only needed for a customer managed key.
    task.addToTaskRolePolicy(streamDecryptStatement(this, config.streamArn));
    orders.grant(task.taskRole, 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem');
    task.addContainer('QueueConsumer', {
      image: ContainerImage.fromAsset(WORKER_DIR, { platform: Platform.LINUX_ARM64 }),
      environment: {
        ...shared.fulfillmentEnv,
        AWS_REGION: this.region,
        QUEUE_ARN: queueArn,
        CONCURRENCY: String(config.concurrency),
        VISIBILITY_TIMEOUT_SECONDS: String(config.visibilityTimeoutSeconds),
      },
      logging: new AwsLogDriver({ logGroup, streamPrefix: 'queue' }),
      // Time to finish in-flight orders after SIGTERM (scale-in, deployments).
      stopTimeout: Duration.seconds(30),
    });
    const queueService = shared.network.service('QueueService', task, config.queueTasks);

    const dashboard = this.addQueueDashboard(shared, queueArn, queueService);

    new CfnOutput(this, 'QueueArn', { value: queueArn });
    new CfnOutput(this, 'QueueServiceName', { value: queueService.serviceName });
    new CfnOutput(this, 'QueueDashboardUrl', { value: this.dashboardUrl(dashboard) });
  }

  /** What the team watches on KCL: metrics it publishes itself, plus stream-level iterator age. */
  private addKclDashboard(shared: Shared, applicationName: string, sideline: Queue, sidelineDeadLetters: Queue,
    service: FargateService): Dashboard {
    const custom = (metricName: string, label: string) => new Metric({
      namespace: KCL_METRICS_NAMESPACE,
      metricName,
      dimensionsMap: { Application: applicationName },
      statistic: 'Sum',
      period: Duration.minutes(1),
      label,
    });
    const dashboard = new Dashboard(this, 'KclDashboard', { defaultInterval: Duration.hours(1) });
    dashboard.addWidgets(new TextWidget({
      markdown: '### Before: order fulfillment on KCL\n'
        + 'Every graph except iterator age and the SQS queues is a **custom metric** that the consumer measures and publishes '
        + 'itself, and pays for. None of them says how many orders are waiting: the stream only reports how far behind '
        + 'a reader is, in milliseconds.',
      width: 24,
      height: 3,
    }));
    dashboard.addWidgets(
      new GraphWidget({
        title: 'Orders fulfilled and sidelined per minute (custom)',
        left: [custom('OrdersFulfilled', 'Fulfilled'), custom('OrdersSidelined', 'Sidelined to SQS')],
        width: 12,
      }),
      new GraphWidget({
        title: 'Records in flight: fetched, not yet checkpointed (custom, summed over tasks)',
        left: [new MathExpression({
          expression: `SUM(SEARCH('{${KCL_METRICS_NAMESPACE},Application,Task} MetricName="RecordsInFlight" Application="${applicationName}"', 'Average', 60))`,
          label: 'In flight',
          period: Duration.minutes(1),
        })],
        width: 12,
      }),
    );
    dashboard.addWidgets(
      new GraphWidget({
        title: 'How far behind: KCL MillisBehindLatest per shard (custom) and stream iterator age',
        left: [
          new MathExpression({
            expression: `SEARCH('{${applicationName},Operation,ShardId} MetricName="MillisBehindLatest"', 'Maximum', 60)`,
            label: 'KCL',
            period: Duration.minutes(1),
          }),
          new Metric({
            namespace: 'AWS/Kinesis',
            metricName: 'GetRecords.IteratorAgeMilliseconds',
            dimensionsMap: { StreamName: shared.streamName },
            statistic: 'Maximum',
            period: Duration.minutes(1),
            label: 'Stream (oldest of all its consumers)',
          }),
        ],
        width: 12,
      }),
      new GraphWidget({
        title: 'Retries in place (custom): each one holds up its shard',
        left: [custom('RetryAttempts', 'Retry attempts')],
        width: 12,
      }),
    );
    dashboard.addWidgets(
      new GraphWidget({
        title: 'Sidelined orders waiting in SQS, and in the sideline dead-letter queue',
        left: [
          sideline.metricApproximateNumberOfMessagesVisible({ period: Duration.minutes(1), label: 'Sideline queue' }),
          sidelineDeadLetters.metricApproximateNumberOfMessagesVisible({ period: Duration.minutes(1), label: 'Sideline DLQ' }),
        ],
        width: 12,
      }),
      new GraphWidget({
        title: 'KCL task CPU (average across tasks)',
        left: [this.cpu(shared, service)],
        width: 12,
      }),
    );
    return dashboard;
  }

  /** What the team watches with a queue: metrics the queue publishes on its own. */
  private addQueueDashboard(shared: Shared, queueArn: string, service: FargateService): Dashboard {
    const queueId = Arn.split(queueArn, ArnFormat.SLASH_RESOURCE_NAME).resourceName!;
    // Queue metrics are published per queue with QueueId and QueueName dimensions.
    const queueMetric = (metricName: string, statistic: string, label: string) => new Metric({
      namespace: 'AWS/Kinesis',
      metricName,
      dimensionsMap: { QueueId: queueId, QueueName: QUEUE_NAME },
      statistic,
      period: Duration.minutes(1),
      label,
    });
    const dashboard = new Dashboard(this, 'QueueDashboard', { defaultInterval: Duration.hours(1) });
    dashboard.addWidgets(new TextWidget({
      markdown: `### After: order fulfillment on queue \`${queueId}\`\n`
        + 'Every graph is a metric the queue publishes on its own. The consumer publishes nothing.',
      width: 24,
      height: 2,
    }));
    dashboard.addWidgets(
      new GraphWidget({
        title: 'Backlog: orders waiting and in flight',
        left: [
          queueMetric('VisibleQueueRecords', 'Maximum', 'Waiting'),
          queueMetric('InFlightQueueRecords', 'Maximum', 'In flight'),
        ],
        width: 12,
      }),
      new GraphWidget({
        title: 'Orders received and acknowledged per minute',
        left: [
          queueMetric('ReceivedQueueRecords', 'Sum', 'Received'),
          queueMetric('AcknowledgedQueueRecords', 'Sum', 'Acknowledged'),
        ],
        width: 12,
      }),
    );
    dashboard.addWidgets(
      new GraphWidget({
        title: 'Dead-letter records: orders that failed MaxReceiveCount times',
        left: [queueMetric('VisibleDeadLetterQueueRecords', 'Maximum', 'Dead-letter records')],
        width: 12,
      }),
      new GraphWidget({
        title: 'Queue task CPU (average across tasks)',
        left: [this.cpu(shared, service)],
        width: 12,
      }),
    );
    return dashboard;
  }

  private cpu(shared: Shared, service: FargateService): Metric {
    return new Metric({
      namespace: 'AWS/ECS',
      metricName: 'CPUUtilization',
      dimensionsMap: { ClusterName: shared.network.cluster.clusterName, ServiceName: service.serviceName },
      statistic: 'Average',
      period: Duration.minutes(1),
    });
  }

  private dashboardUrl(dashboard: Dashboard): string {
    return `https://${this.region}.console.aws.amazon.com/cloudwatch/home?region=${this.region}#dashboards/dashboard/${dashboard.dashboardName}`;
  }
}
