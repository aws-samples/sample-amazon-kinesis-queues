import * as path from 'node:path';
import { CustomResource, Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import { IGrantable, Grant, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { IStream } from 'aws-cdk-lib/aws-kinesis';
import { Architecture, Code, Function as LambdaFunction, Runtime } from 'aws-cdk-lib/aws-lambda';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Provider } from 'aws-cdk-lib/custom-resources';
import { buildSync } from 'esbuild';
import { Construct } from 'constructs';

// Actions on the queue. CreateQueue is granted on queue/*, because the queue's resource
// ID doesn't exist until the queue is created.
const QUEUE_ACTIONS = [
  'kinesis:CreateQueue', 'kinesis:DescribeQueue', 'kinesis:UpdateQueue', 'kinesis:DeleteQueue',
  'kinesis:TagResource', 'kinesis:UntagResource', 'kinesis:ListTagsForResource',
];
// Actions on the stream. CreateQueue also checks AssociateStreamsWithQueue against the
// stream ARN, so the handler can only attach queues to the streams it is granted.
// DescribeStreamSummary lets the handler refuse a provisioned stream before CreateQueue.
const STREAM_ACTIONS = ['kinesis:AssociateStreamsWithQueue', 'kinesis:DescribeStreamSummary'];

const HANDLER_DIR = path.resolve(__dirname, 'queue-handler');

export interface KinesisQueueProps {
  /** The stream the queue reads from: a stream construct or its ARN. The stream must be On-demand, and can be encrypted. */
  stream: IStream | string;
  /** Unique per account and Region. 1-128 letters, numbers, underscores, periods or hyphens. */
  queueName: string;
  /** Where the queue starts reading on the stream. Default: TRIM_HORIZON, the oldest record still retained. */
  startingPosition?: 'TRIM_HORIZON' | 'LATEST';
  /** How long a received record stays hidden from other consumers. Default: 30 seconds (0 to 12 hours). */
  visibilityTimeout?: Duration;
  tags?: Record<string, string>;
}

/**
 * A Kinesis Data Streams queue.
 *
 * CloudFormation has no resource type for Kinesis queues yet, so this construct fills
 * the gap with a custom resource: a Lambda function that calls CreateQueue, UpdateQueue
 * and DeleteQueue. Replace it with the native CloudFormation resource when that is
 * generally available.
 *
 * Behavior to know about:
 * - Create fails if the queue name is taken. Queue names are unique per account and
 *   Region, and the construct never adopts a queue it didn't create, because deleting
 *   the stack deletes it.
 * - A queue's name, stream and starting position can't change in place; such an update
 *   fails. To change one, give the construct a new id and a new `queueName`:
 *   CloudFormation creates the new queue, then deletes the old one. A new queue with
 *   TRIM_HORIZON reads and redelivers every record the stream still retains.
 * - Changing `visibilityTimeout` and `tags` updates the queue in place.
 * - Dead-letter handling stays at the queue default: a record received 6 times without an
 *   acknowledgment becomes a dead-letter record (scenario 4 shows how to handle them).
 * - Destroying the stack deletes the queue.
 * - All KinesisQueue constructs in a stack share one handler Lambda and provider.
 */
export class KinesisQueue extends Construct {
  readonly queueArn: string;
  readonly queueName: string;
  /** The ID in the queue ARN (the part after `queue/`). Queue metrics use it as the QueueId dimension. */
  readonly queueId: string;
  private readonly streamArn: string;

  constructor(scope: Construct, id: string, props: KinesisQueueProps) {
    super(scope, id);
    const streamArn = typeof props.stream === 'string' ? props.stream : props.stream.streamArn;
    this.streamArn = streamArn;
    const provider = QueueProvider.of(Stack.of(this));
    // Each queue adds its own stream to the shared handler role, so the role never covers other streams.
    provider.onEventHandler.addToRolePolicy(new PolicyStatement({ actions: STREAM_ACTIONS, resources: [streamArn] }));

    const queue = new CustomResource(this, 'Queue', {
      serviceToken: provider.serviceToken,
      resourceType: 'Custom::KinesisQueue',
      properties: {
        QueueName: props.queueName,
        StreamArn: streamArn,
        StartingPosition: props.startingPosition ?? 'TRIM_HORIZON',
        VisibilityTimeoutSeconds: (props.visibilityTimeout ?? Duration.seconds(30)).toSeconds(),
        Tags: props.tags,
      },
    });

    this.queueArn = queue.getAttString('QueueArn');
    this.queueName = queue.getAttString('QueueName');
    this.queueId = queue.getAttString('QueueId');
  }

  /**
   * Lets the grantee receive, acknowledge and change the visibility of records on this queue.
   *
   * On an encrypted stream, ReceiveQueueRecords also needs kms:Decrypt on the stream's KMS
   * key. The construct doesn't know which key that is, so the grant covers any key in the
   * account, but only for requests that Kinesis Data Streams makes for this stream. The
   * AWS managed key (aws/kinesis) already allows this in its key policy; a customer
   * managed key needs this grant.
   */
  grantConsume(grantee: IGrantable): Grant {
    const stack = Stack.of(this);
    Grant.addToPrincipal({
      grantee,
      actions: ['kms:Decrypt'],
      resourceArns: [`arn:${stack.partition}:kms:${stack.region}:${stack.account}:key/*`],
      conditions: {
        StringEquals: {
          'kms:ViaService': `kinesis.${stack.region}.${stack.urlSuffix}`,
          'kms:EncryptionContext:aws:kinesis:arn': this.streamArn,
        },
      },
    });
    return Grant.addToPrincipal({
      grantee,
      actions: ['kinesis:ReceiveQueueRecords', 'kinesis:AcknowledgeQueueRecords', 'kinesis:UpdateQueueRecordsVisibility'],
      resourceArns: [this.queueArn],
    });
  }
}

/** The handler Lambdas and provider, created once per stack and shared by every KinesisQueue in it. */
class QueueProvider extends Construct {
  static of(stack: Stack): QueueProvider {
    return (stack.node.tryFindChild('KinesisQueueProvider') as QueueProvider | undefined) ?? new QueueProvider(stack, 'KinesisQueueProvider');
  }

  readonly serviceToken: string;
  readonly onEventHandler: LambdaFunction;

  private constructor(scope: Construct, id: string) {
    super(scope, id);
    const stack = Stack.of(this);
    bundleHandler();
    const code = Code.fromAsset(path.join(HANDLER_DIR, 'dist'));
    const logGroup = (logId: string) => new LogGroup(this, logId, { retention: RetentionDays.ONE_WEEK, removalPolicy: RemovalPolicy.DESTROY });
    const handler = (functionId: string, entryPoint: string, description: string) => new LambdaFunction(this, functionId, {
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      code,
      handler: `index.${entryPoint}`,
      timeout: Duration.minutes(1),
      description,
      logGroup: logGroup(`${functionId}Logs`),
    });
    const onEventHandler = handler('OnEvent', 'onEvent', 'Creates, updates and deletes a Kinesis queue for CloudFormation');
    const isCompleteHandler = handler('IsComplete', 'isComplete', 'Waits for a Kinesis queue to become ACTIVE or to be deleted');

    // A queue that doesn't exist yet can only be named with the queue/* pattern. The
    // stream permissions are added per queue, in the KinesisQueue constructor.
    const queues = `arn:${stack.partition}:kinesis:${stack.region}:${stack.account}:queue/*`;
    onEventHandler.addToRolePolicy(new PolicyStatement({ actions: QUEUE_ACTIONS, resources: [queues] }));
    this.onEventHandler = onEventHandler;
    // The wait function only checks the status and retries a delete that never started.
    isCompleteHandler.addToRolePolicy(new PolicyStatement({
      actions: ['kinesis:DescribeQueue', 'kinesis:DeleteQueue'],
      resources: [queues],
    }));

    this.serviceToken = new Provider(this, 'Provider', {
      onEventHandler,
      isCompleteHandler,
      logGroup: logGroup('ProviderLogs'),
      // A queue takes a little while to become ACTIVE or to delete.
      queryInterval: Duration.seconds(5),
      totalTimeout: Duration.minutes(15),
    }).serviceToken;
  }
}

let bundled = false;

/** Bundles the handler once per process, however many queues the app defines. */
function bundleHandler(): void {
  if (bundled) return;
  // The Lambda runtime's built-in AWS SDK may not include the queue operations yet, so
  // the handler bundles the repo's own @aws-sdk/client-kinesis.
  buildSync({
    entryPoints: [path.join(HANDLER_DIR, 'index.ts')],
    outdir: path.join(HANDLER_DIR, 'dist'),
    bundle: true,
    platform: 'node',
    target: 'node22',
    format: 'cjs',
    sourcemap: true,
    sourcesContent: false,
    legalComments: 'none',
    logLevel: 'warning',
  });
  bundled = true;
}
