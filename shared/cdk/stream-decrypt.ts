import { Stack } from 'aws-cdk-lib';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import type { Construct } from 'constructs';

/**
 * kms:Decrypt for reading records from an encrypted stream, either from a queue on it
 * or directly with GetRecords.
 *
 * The AWS managed key (aws/kinesis), which the Quickstart uses, already allows this in
 * its key policy, so the statement changes nothing there. A stream encrypted with a
 * customer managed key needs it: without it, ReceiveQueueRecords and GetRecords fail
 * with KMSAccessDeniedException.
 *
 * The statement only applies to calls that Kinesis makes on the caller's behalf
 * (kms:ViaService), for records of `streamArn` (the encryption context). Pass a stream
 * ARN with a `*` to allow any stream that matches it, for example every stream in the
 * account when the stack only knows the queue.
 */
export function streamDecryptStatement(scope: Construct, streamArn: string): PolicyStatement {
  const stack = Stack.of(scope);
  const viaService = { 'kms:ViaService': `kinesis.${stack.region}.${stack.urlSuffix}` };
  const context = { 'kms:EncryptionContext:aws:kinesis:arn': streamArn };
  return new PolicyStatement({
    actions: ['kms:Decrypt'],
    resources: [`arn:${stack.partition}:kms:${stack.region}:${stack.account}:key/*`],
    conditions: streamArn.includes('*')
      ? { StringEquals: viaService, StringLike: context }
      : { StringEquals: { ...viaService, ...context } },
  });
}

/** Every stream in the stack's account and Region, for stacks that know only the queue. */
export function anyStreamInAccount(scope: Construct): string {
  const stack = Stack.of(scope);
  return `arn:${stack.partition}:kinesis:${stack.region}:${stack.account}:stream/*`;
}
