# Scenario 1: Build your first queue consumer

**Scenario: order confirmations.** The checkout writes an `OrderPlaced` event to the
stream for every order. A queue consumer sends the customer a confirmation for each
order. The confirmation is simulated with a log line.

You deploy a Kinesis queue on the Quickstart stream, and a consumer that runs on AWS
Fargate. The consumer is a plain receive → process → acknowledge loop that uses the
AWS SDK for JavaScript v3. It needs no KCL, lease table or shard logic.

![Architecture: the order producer writes to the Kinesis data stream. The Quickstart KCL consumer reads the stream. The queue in this stack reads the same stream, and a Fargate queue consumer receives and acknowledges its records. A CloudFormation custom resource creates and deletes the queue. CloudWatch shows the queue metrics.](images/architecture.png)

The stream feeds two consumers at the same time. The Quickstart KCL consumer reads
records in order, per shard. The queue gives each record to one of its consumers.
The stream delivers every record to both the KCL consumer and the queue.

## Why a queue fits this job

Each order is a unit of work on its own. Sending its confirmation doesn't depend on
any other order. Once the confirmation is sent and the record is acknowledged, the
order is done: the queue normally doesn't deliver it again. Delivery is
at-least-once (scenario 2). The record itself stays in the stream for its retention
period, so the Quickstart's KCL consumer and any other queue still read it.

What that saves you, compared with a KCL consumer:

| Job | On KCL you build | With a queue |
| --- | --- | --- |
| **Read records** | A Java KCL application, a lease table and a checkpoint strategy | `ReceiveQueueRecords` and `AcknowledgeQueueRecords` from any AWS SDK |
| **Share work between consumers** | One worker per shard at a time, and leases that move between workers | Start another consumer. Consumers share the queue without coordinating |
| **Mark an order as done** | A checkpoint that covers everything before it | Acknowledge that record |
| **Get a record back after a crash** | Replay from the last checkpoint | The record you didn't acknowledge is delivered again |

## What gets deployed

A CDK stack in this folder. Its name is the Quickstart stack name plus
`-first-queue`, which is `kinesis-queues-demo-first-queue` by default. It reads the
stream ARN from `0-quickstart/cdk-outputs.json`, and takes the account and Region
from that ARN.

- **Queue:** a Kinesis queue on the Quickstart stream, named like the stack. It
  starts at `TRIM_HORIZON` and has a 30-second visibility timeout. Dead-letter
  handling stays at the default: a record that is received 6 times without an
  acknowledgment becomes a dead-letter record.
- **Queue custom resource:** the `KinesisQueue` construct in `lib/kinesis-queue.ts`.
  CloudFormation has no resource type for queues yet, so a Lambda-backed custom
  resource creates, updates and deletes the queue.
- **Consumer:** a Fargate service with one task on ARM64, 0.25 vCPU and 0.5 GiB. It
  runs `src/consume.ts` in a small Node.js image. The task has a public IP and no NAT
  gateway. It accepts no inbound connections.
- **Consumer role:** `ReceiveQueueRecords`, `AcknowledgeQueueRecords` and
  `UpdateQueueRecordsVisibility` on this queue only. On an encrypted stream, receiving
  also needs `kms:Decrypt` on the stream's KMS key. The role gets it for any key in the
  account, limited to requests that Kinesis Data Streams makes for this stream. The
  Quickstart's AWS managed key (`aws/kinesis`) already allows this in its key policy,
  so the grant matters when the stream uses a customer managed key.
- **Custom resource role:** `CreateQueue`, `DescribeQueue`, `UpdateQueue`,
  `DeleteQueue` and tagging on queues. On the stream, `AssociateStreamsWithQueue`
  (`CreateQueue` checks it when it attaches the queue to the stream) and
  `DescribeStreamSummary`, to refuse a provisioned stream before `CreateQueue`.
  `CreateQueue` needs no KMS permissions.
- **Log group:** the consumer's logs, kept for one week.

## 1. Prerequisites

- **Quickstart:** deployed from `0-quickstart/`. See `../0-quickstart/README.md`.
  Its stream is encrypted with the AWS managed key (`aws/kinesis`), and queues work on
  encrypted streams.
- **Software:** Node.js 22, Docker, `jq`, and the AWS CLI with credentials for your account.

The queue scenarios need the AWS SDK for JavaScript v3 3.1171.0 or later, which
`npm ci` installs, and AWS CLI 2.41.0 or later for the `aws kinesis` queue commands.

Install the dependencies once, from the repository root. Run every command in this
README from the repository root:

```bash
npm ci
```

## 2. Choose your settings

Use Bash or Zsh. Replace `YOUR_PROFILE` with your own profile, and
`YOUR_QUICKSTART_STACK` with the `STACK_NAME` you used for the Quickstart:

```bash
export AWS_PROFILE="YOUR_PROFILE"
export AWS_REGION="us-east-2"
export STACK_NAME="YOUR_QUICKSTART_STACK"
export FIRST_QUEUE_STACK="$STACK_NAME-first-queue"   # this stack, and its queue
```

Check that the profile identifies your intended account:

```bash
aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text
```

The result should be the account where you deployed the Quickstart. Each new
terminal needs these `export` lines again.

## 3. Deploy

This stack needs no `-c` flags. It reads the stream ARN from
`0-quickstart/cdk-outputs.json`, and takes the account and Region from that ARN. Its
name is the Quickstart stack name plus `-first-queue`. The stack name is also the
queue name.

Docker must be running: CDK builds the consumer's container image. Deploy the stack
and write its outputs to a file:

```bash
(cd 1-first-queue && npx cdk deploy "$FIRST_QUEUE_STACK" \
  --profile "$AWS_PROFILE" \
  --outputs-file cdk-outputs.json)
```

To use another On-demand stream in the same account instead, pass its ARN. See
[Settings](#settings) for the other values:

```bash
(cd 1-first-queue && npx cdk deploy "$FIRST_QUEUE_STACK" \
  --profile "$AWS_PROFILE" \
  -c streamArn=YOUR_STREAM_ARN \
  -c stackName="$FIRST_QUEUE_STACK" \
  --outputs-file cdk-outputs.json)
```

### What happens during deployment?

1. The CDK app validates your settings.
2. It reads the stream ARN from the Quickstart outputs.
3. CDK builds the consumer image and uploads it to the bootstrap ECR repository.
4. CloudFormation creates the custom resource Lambda. The Lambda creates the queue
   and waits until it is `ACTIVE`.
5. ECS starts one consumer task. It long-polls the queue.
6. CDK writes the outputs to `1-first-queue/cdk-outputs.json`.

The deploy takes a few minutes. Read the values you need into shell variables. The
next sections reuse them:

```bash
OUT=1-first-queue/cdk-outputs.json
QUEUE_ARN=$(jq -r ".\"$FIRST_QUEUE_STACK\".QueueArn" "$OUT")
QUEUE_NAME=$(jq -r ".\"$FIRST_QUEUE_STACK\".QueueName" "$OUT")
LOG_GROUP=$(jq -r ".\"$FIRST_QUEUE_STACK\".ConsumerLogGroupName" "$OUT")
```

## 4. Place orders

**The consumer starts automatically during `cdk deploy`.** You don't need to run a
consumer on your laptop.

Open a second terminal in the repository root. Set the `export` lines from
section 2 and the variables from section 3. Send 20 orders, two per second. The
producer writes `OrderPlaced` events the way a checkout would:

```bash
npm run orders:produce -- --count 20 --rate 2
```

## 5. Watch the queue consumer

In the first terminal, read the consumer's logs:

```bash
aws logs tail "$LOG_GROUP" \
  --since 5m \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

The consumer logs one line per order:

```text
CONFIRMATION_SENT consumer=537a0cea orderId=o-5906ddcb-39d6 customerId=c-0032 total=188.99 EUR receiveCount=1 latencyMs=51
```

Other event types on the stream are logged as `SKIPPED` and acknowledged. This consumer only handles orders.
A malformed order event isn't acknowledged, so the queue delivers it again after the
visibility timeout. After 6 receives without an acknowledgment, the queue stops
delivering it and keeps it as a dead-letter record. Scenario 4 shows how to handle
dead-letter records.

## 6. Demo: check it from the CLI and the console

Check the queue from the command line first, then from the console.

### With the CLI

These commands need AWS CLI 2.41.0 or later. Check with `aws --version`.

Show the queue (status, stream, starting position, visibility timeout):

```bash
aws kinesis describe-queue \
  --queue-arn "$QUEUE_ARN" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"

aws kinesis list-queues \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

Read the Quickstart KCL consumer's logs. It logs the same records, because the stream
feeds both consumers:

```bash
KCL_LOG_GROUP=$(jq -r ".\"$STACK_NAME\".ConsumerLogGroupName" 0-quickstart/cdk-outputs.json)

aws logs tail "$KCL_LOG_GROUP" \
  --since 5m \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

```text
RECORD shard=shardId-000000000003 sequence=… data={"eventType":"OrderPlaced",…}
```

Count the acknowledged records. Replace the times with a window that covers your run:

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name AcknowledgedQueueRecords \
  --dimensions Name=QueueId,Value="${QUEUE_ARN##*queue/}" Name=QueueName,Value="$QUEUE_NAME" \
  --start-time 2026-11-10T14:00:00Z \
  --end-time 2026-11-10T14:30:00Z \
  --period 1800 \
  --statistics Sum \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

Receive one record yourself. Do this **while `orders:produce` is sending**, in the
second terminal. The Fargate consumer drains the queue in milliseconds, so afterwards
you would get nothing:

```bash
aws kinesis receive-queue-records \
  --queue-arn "$QUEUE_ARN" \
  --limit 1 \
  --wait-time-seconds 5 \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

> **Warning:** a record that you receive this way is hidden from the consumer for the
> visibility timeout (30 seconds). Don't acknowledge it. About 30 seconds later the
> Fargate consumer logs that order with `receiveCount=2`. This is redelivery.

If your AWS CLI is older than 2.41.0, one command shows the queue, the consumer
tasks and the last 15 minutes of metrics instead. It uses the SDK and is read-only:

```bash
npm run first-queue:status
```

```text
Queue kinesis-queues-demo-first-queue
  ARN                 arn:aws:kinesis:us-east-2:111122223333:queue/…
  status              ACTIVE
  stream              arn:aws:kinesis:us-east-2:111122223333:stream/…
  starting position   TRIM_HORIZON
  consumer tasks      1 running of 1 wanted

Last 15 minutes (CloudWatch, 1-2 minutes behind)
  records received        20
  records acknowledged    20
  waiting (peak)          0
  being processed (peak)  0
```

After the 20 orders, `ReceivedQueueRecords` is 20, `AcknowledgedQueueRecords` is 20,
`VisibleQueueRecords` is 0 and `InFlightQueueRecords` is 0. Every received order was
acknowledged, and none are waiting.

### In the console

1. Open the Kinesis console and choose the US East (Ohio) Region.
2. In the navigation pane, choose **Queues**. Choose your queue, named like the stack.
3. In **Details**, check the status (`Active`), starting position, visibility
   timeout and data stream.
   **Details** also shows that dead-letter handling is on, with a maximum receive
   count of 6.
4. Choose the **Monitoring** tab to see the queue's CloudWatch metrics.
5. To receive records without a consumer, choose **Receive records**, then
   **Receive records** again on the page that opens. Do this while `orders:produce`
   is sending, and choose **Stop** when you have a few. The same warning applies: the
   records you receive are hidden from the consumer for the visibility timeout. Don't
   choose **Acknowledge**.

To see the consumer's logs:

1. Open the Amazon ECS console and choose **Clusters**.
2. Choose the cluster from `ConsumerClusterName` in `1-first-queue/cdk-outputs.json`,
   then **Services**.
3. Choose the service from `ConsumerServiceName`, then the **Logs** tab.

To see the metrics in CloudWatch, choose **Metrics**, then the **AWS/Kinesis**
namespace, then the **QueueId, QueueName** dimensions.

The stream's page in the Kinesis console shows "Record distribution: User set".
Kinesis routes records by your partition key. That keeps the order per customer for
the KCL consumer. Queues don't depend on it.

## 7. Things to try

- **Two consumers.** In a new terminal in the repository root, set the two `export`
  lines and the variables from sections 2 and 3. Run a consumer on your machine:
  `npm run queue:consume -- --queue-arn "$QUEUE_ARN"`. Then send orders from one
  customer: `npm run orders:produce -- --count 20 --rate 2 --customers 1`. Every record
  has the same partition key and lands on one shard, yet the two consumers share the
  orders. Each order gets one confirmation.
- **Redelivery.** While `orders:produce` runs, receive one record yourself and don't
  acknowledge it, with **Receive records** in the console or
  `aws kinesis receive-queue-records` (see section 6). About 30 seconds later the
  Fargate consumer logs that order with `receiveCount=2`. Scenario 2 shows how to keep
  a redelivery from sending a second confirmation.
- **Bounded run.** `npm run queue:consume -- --queue-arn "$QUEUE_ARN" --count 5` stops
  after it has acknowledged 5 records itself. It shares the queue with the Fargate
  consumer, so keep `orders:produce` sending until it stops.

`latencyMs` includes any backlog. If the consumer starts after the producer, it
measures how long records waited, not delivery time.

## 8. Clean up

Stop any local consumer or producer with **Ctrl+C**. The Fargate service and
CloudWatch usage incur AWS charges while deployed. Destroy this stack:

```bash
(cd 1-first-queue && npx cdk destroy "$FIRST_QUEUE_STACK" --profile "$AWS_PROFILE")
```

This deletes the queue and the consumer. The Quickstart stays for the other
scenarios. When you're done with all of them, destroy it as its README describes.
Kinesis blocks `DeleteStream` while any queue is attached to the stream, so destroy
this stack first.

In rare cases a failed deploy leaves a queue behind. Find it with
`aws kinesis list-queues --region "$AWS_REGION"` or in the console. Delete it with
`aws kinesis delete-queue --queue-arn ARN --region "$AWS_REGION"` or in the console,
before you destroy the Quickstart.

## How the consumer handles each record

| Record | Consumer action | Outcome |
| --- | --- | --- |
| `OrderPlaced` | Log `CONFIRMATION_SENT` | Acknowledged |
| Other event type | Log `SKIPPED` | Acknowledged |
| Malformed order | Log `PROCESS_FAILED`, don't acknowledge | Delivered again after the visibility timeout. After 6 receives it becomes a dead-letter record (scenario 4) |

The loop receives up to 10 records and waits up to 20 seconds. It acknowledges each
batch in one call, with one `ReceiptHandle` per record, and logs every entry in the
`Failed` list as `ACK_FAILED`. Each failed entry carries the `ReceiptHandle` it failed
for, so the consumer matches it to its record. A record whose acknowledgment failed
is delivered again after its visibility timeout. On SIGTERM it finishes the current
batch and exits.

## Settings

| CDK context | Default | Meaning |
| --- | --- | --- |
| `streamArn` | from `0-quickstart/cdk-outputs.json` | Stream to read. It must be On-demand, and can be encrypted |
| `vpcId` | none: a new VPC | An existing VPC with public subnets to run the tasks in, for example the Quickstart's. Use it if your account is at its VPC quota |
| `baseStackName` | `kinesis-queues-demo` | Quickstart stack name, used when you pass `streamArn` |
| `stackName` | `<Quickstart stack>-first-queue` | Stack name. It is also the queue name |

Pass them with `-c`, for example `-c streamArn=ARN`, to the deploy command in section 3.
The queue name is unique per account and Region. The `KinesisQueue` construct also sets
the starting position and visibility timeout (see `lib/kinesis-queue.ts`).

## Project files

- `bin/app.ts`: the CDK app.
- `lib/config.ts`: reads the Quickstart outputs and the context overrides.
- `lib/first-queue-stack.ts`: the queue, the Fargate consumer, its role, and logs.
- `lib/kinesis-queue.ts`: the `KinesisQueue` construct (a custom resource).
- `lib/queue-handler/index.ts`: the Lambda code behind the custom resource.
- `worker/Dockerfile`: the consumer image.
- `src/consume.ts`: the receive → send confirmation → acknowledge loop
  (`npm run queue:consume`).
- `lib/bundle-worker.ts`: bundles `src/consume.ts` into the consumer image.
- `scripts/status.ts`: `npm run first-queue:status`, a read-only view of the queue,
  consumer tasks and metrics.

Client setup and argument handling come from `../shared/src/common.ts`. Order parsing
comes from `../shared/src/orders.ts`, and the order producer from
`../shared/src/produce-orders.ts`.
