# Kinesis Data Streams queues: sample scenarios

These scenarios go with the launch blog post *Reduce the complexity and cost of
real-time processing with queues in Amazon Kinesis Data Streams*. They all use the
same example: an e-commerce checkout writes order events to one Kinesis data stream.

- **Stream consumers** read that stream in order, for fraud detection and analytics.
- **Queue consumers** process each order on its own, for fulfillment and notifications.

Start with the [Quickstart](0-quickstart/README.md), which deploys the stream and a
KCL stream consumer. Each scenario then lives in its own numbered folder with a
README. The scenarios build on the Quickstart but don't depend on each other, so you
can run them in any order.

Run `npm ci` once at the repository root. All scenarios share its `package.json`.

- **Versions:** queues need the AWS SDK for JavaScript v3 3.1171.0 or later, which
  `npm ci` installs, and AWS CLI 2.41.0 or later for the `aws kinesis` queue commands.
  Check your CLI with `aws --version`.
- **Language:** the CDK stacks, queue consumers and scripts are TypeScript. The KCL
  stream consumers (in the Quickstart and scenarios 5 and 6) are Java.
- **Region and names:** the commands use `us-east-2` and the Quickstart stack name
  `kinesis-queues-demo`. For another Region, export `AWS_REGION` or pass `--region`
  to the scripts and `-c region=...` to CDK. For another Quickstart stack name, pass
  it to each scenario as its README describes.
- **VPCs:** each scenario stack creates a VPC unless you pass `-c vpcId=vpc-...` with
  an existing VPC that has public subnets, such as the Quickstart's. An account has
  5 VPCs per Region by default, and the Quickstart and six scenarios need seven if
  you run them all at once with their own VPCs.

## When a queue is the right consumer

A queue fits when **each record is a unit of work on its own, and finishing it
means it's done.** The consumer handles a record without looking at the records
around it, acknowledges it, and the queue never delivers it again. Order isn't
part of the job, so any free worker can take the next record.

A stream consumer fits when **the meaning comes from records together**: in
sequence, per key, or in aggregate. Its consumers read the same records in order,
and can go back and read them again.

| Job | Better as | Why |
| --- | --- | --- |
| Fulfill an order, send its confirmation, charge a payment | Queue | Each order is done once its work is done. Orders don't wait on each other |
| Resize an uploaded image, index a document, call a partner API per event | Queue | Independent jobs that can take different amounts of time, and fail and retry on their own |
| Fraud signals per customer, running totals, sessionization | Stream consumer | Needs every record for a key, in order |
| Analytics, replicating to a data lake, rebuilding a view by replaying | Stream consumer | Needs every record, and the ability to read them again |

Most streams need both. The order events in these scenarios feed fulfillment and
confirmations, which are queue jobs, and fraud detection and analytics, which are
stream jobs.

**"Done" means done for that queue, not deleted.** Acknowledging a record removes
it from that queue only. The record stays in the stream for its retention period,
where stream consumers and every other queue on the stream still read it. Each
queue receives every record and tracks its own progress. That's how one stream can
serve fulfillment, notifications and analytics without copying data (scenario 5).

## Why queues

Teams that process each stream record independently, on KCL or by copying records
to Amazon SQS, run into the same problems. The scenarios show how Kinesis queues
remove each one:

| Problem today | With a Kinesis queue | Scenario |
| --- | --- | --- |
| Per-record state built on sequential checkpoints | The queue tracks every record; you receive and acknowledge | 1, 2 |
| A failed record blocks the shard, or is sidelined to a separate DLQ | Retried on its own, then dead-lettered in the same queue | 4 |
| Parallelism capped at one worker per shard | Add workers; shard count doesn't matter | 3 |
| Queue metrics derived from stream-level data | Per-queue metrics for backlog, in-flight records and dead-letter records | 3, 4 |
| A second copy of the data in SQS, plus a relay | One stream feeds stream consumers and Kinesis queues | 5, 6 |
| KCL lease tables, version upgrades, Java only | Any AWS SDK, no extra infrastructure | 1, 6 |

## Scenarios

| # | Scenario | Folder | Status |
| --- | --- | --- | --- |
| 0 | Quickstart: the stream and a KCL stream consumer | [`0-quickstart/`](0-quickstart/README.md) | Working |
| 1 | Build your first queue consumer | [`1-first-queue/`](1-first-queue/README.md) | Working |
| 2 | Process records safely with at-least-once delivery | [`2-at-least-once-delivery/`](2-at-least-once-delivery/README.md) | Working |
| 3 | Scale out independently of shards | [`3-scale-out/`](3-scale-out/README.md) | Working |
| 4 | Handle failures and dead-letter records | [`4-failures-and-dead-letters/`](4-failures-and-dead-letters/README.md) | Working |
| 5 | Consume one stream in more than one way | [`5-consume-one-stream-many-ways/`](5-consume-one-stream-many-ways/README.md) | Working |
| 6 | Migrate an existing consumer, from KCL or from a relay into SQS | [`6-migrate-existing-consumer/`](6-migrate-existing-consumer/README.md) | Working |
| – | Shared queue tools (client setup, queue settings, dead-letter records) | [`shared/`](shared/README.md) | Used by every scenario |

## How the pieces connect

```text
0-quickstart/cdk-outputs.json (StreamArn) ──► npm run queue:create ──► shared/queue-outputs.json (queue ARNs) ──► scenarios
```

- **`npm run queue:create`** reads `StreamArn` from `0-quickstart/cdk-outputs.json`,
  creates the queue, and saves each queue ARN by name.
- **Scenarios with infrastructure have their own CDK stack.** Each one reads its queue
  ARN from that file, or from `-c queueArn=ARN`.
- **Scenario 1 is the exception.** Its CDK stack creates its own queue and reads the stream
  ARN from the Quickstart outputs. It doesn't use `queue:create` or `queue-outputs.json`.
- **Deploy order:** the Quickstart, then the queues, then the scenario stacks.
- **Destroy order:** the reverse. Kinesis won't delete a stream while a queue is
  attached to it.

## Clean up

Destroy each scenario you deployed, then the Quickstart. Stop any local producer or
consumer with **Ctrl+C** first. From the repository root:

```bash
export AWS_PROFILE=YOUR_PROFILE AWS_REGION=us-east-2
ARGS=(--profile "$AWS_PROFILE" -c accountId=YOUR_ACCOUNT_ID -c region="$AWS_REGION")
```

1. **Each scenario: destroy its stack, then delete its queues.** Destroy the stack
   first, so no worker polls a queue that's gone, and because the stack reads its queue
   ARN from `shared/queue-outputs.json`, which `queue:delete` updates. Skip the
   scenarios you didn't deploy, and use your own stack names if you changed them.

   | Scenario | Destroy the stack | Then delete the queues |
   | --- | --- | --- |
   | 1 | `(cd 1-first-queue && npx cdk destroy kinesis-queues-demo-first-queue --profile "$AWS_PROFILE")` | None: the stack deletes its queue |
   | 2 | `(cd 2-at-least-once-delivery && npx cdk destroy at-least-once "${ARGS[@]}" -c stackName=at-least-once)` | `npm run queue:delete -- --queue-name order-at-least-once` |
   | 3 | `(cd 3-scale-out && npx cdk destroy scale-out "${ARGS[@]}" -c stackName=scale-out)` | `npm run queue:delete -- --queue-name order-scale-out` |
   | 4 | `(cd 4-failures-and-dead-letters && npx cdk destroy failures-dead-letters "${ARGS[@]}" -c stackName=failures-dead-letters)` | `npm run queue:delete -- --queue-name order-fulfillment` |
   | 5 | `(cd 5-consume-one-stream-many-ways && npx cdk destroy one-stream-many-ways "${ARGS[@]}" -c stackName=one-stream-many-ways)` | `npm run queue:delete -- --queue-name fanout-notifications`, then `--queue-name fanout-fulfillment` |
   | 6 | `(cd 6-migrate-existing-consumer && npx cdk destroy migrate-consumer "${ARGS[@]}" -c stackName=migrate-consumer)` | `npm run queue:delete -- --queue-name order-migration` |
   | 6, relay | `(cd 6-migrate-existing-consumer && npx cdk destroy migrate-relay "${ARGS[@]}" -c stackName=migrate-relay -c migration=relay)` | `npm run queue:delete -- --queue-name order-relay` |

   If you created other queues on the stream, for example with `npm run queue:create`
   in the first-queue scripts, delete them the same way.
2. **Check that no queue is left on the stream.** Kinesis won't delete a stream while a
   queue is attached to it. List the queues with `aws kinesis list-queues --region
   "$AWS_REGION"` or on the Kinesis console.
3. **Destroy the Quickstart last.** This deletes the stream and its records, the KCL
   consumer and its lease table:

   ```bash
   (cd 0-quickstart && npx cdk destroy kinesis-queues-demo "${ARGS[@]}" -c stackName=kinesis-queues-demo)
   ```

CDK's bootstrap stack (`CDKToolkit`), with its S3 bucket and ECR repository of
published assets, stays in the account. Delete it only if nothing else in the account
uses CDK. The `cdk-outputs.json` and `shared/queue-outputs.json` files stay in your
local copy and are ignored by git.


## Disclaimer

This is sample code, for demonstration and learning. It is not production-ready.
Before you use any of it in production, review it against your own security,
reliability and compliance requirements, and test it with your own workloads. The
scenarios create AWS resources that are charged while they run; see
[Clean up](#clean-up).

## Code of Conduct

See [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
