import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { Stream, StreamEncryption, StreamMode } from 'aws-cdk-lib/aws-kinesis';
import { Construct } from 'constructs';
import { KinesisConsumer } from './consumer';

export interface KinesisDataStreamStackProps extends StackProps {
  streamName?: string;
  retentionHours: number;
}

export class KinesisDataStreamStack extends Stack {
  constructor(scope: Construct, id: string, props: KinesisDataStreamStackProps) {
    const { streamName, retentionHours, ...stackProps } = props;
    super(scope, id, stackProps);

    const stream = new Stream(this, 'Stream', {
      streamName,
      streamMode: StreamMode.ON_DEMAND,
      // Encrypted at rest with the AWS managed key (aws/kinesis). Queues and KCL
      // consumers in the same account read it without a KMS grant.
      encryption: StreamEncryption.MANAGED,
      retentionPeriod: Duration.hours(retentionHours),
      // This is a disposable lab: destroying the stack deletes the stream and its records.
      removalPolicy: RemovalPolicy.DESTROY,
    });

    new CfnOutput(this, 'StreamName', { value: stream.streamName });
    new CfnOutput(this, 'StreamArn', { value: stream.streamArn });
    new KinesisConsumer(this, 'Consumer', stream);
  }
}
