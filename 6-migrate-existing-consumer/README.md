# Scenario 6: Migrate an existing consumer to a Kinesis queue

**Scenario: move order fulfillment off KCL.** The checkout writes an `OrderPlaced` event to
the stream for every order. Fraud detection and analytics read that stream in order, with
KCL, and they stay as they are. The fulfillment team built its consumer on KCL too, before
queues existed. Fulfillment doesn't need order, so the team added what it does need:
retries, a sideline queue for orders it gives up on, a function that works through that
queue, and its own metrics.

This scenario moves that consumer to a Kinesis queue on the same stream while orders keep
arriving. It covers both parts of a migration: the code change, and the cutover from the
old consumer to the new one without skipping an order or shipping one twice. The producer
and the other stream consumers don't change.

It shows two migrations: from a KCL application, and, in
[Migrate a relay into SQS](#migrate-a-relay-into-sqs), from a relay that copies the stream
into an SQS queue.

```text
                       ┌──► fraud, analytics: stream consumers, in order ── unchanged
checkout ──► stream ───┤
                       ├──► before: fulfillment on KCL ── lease table, one checkpoint per shard
                       │       └── gives up on an order ──► SQS sideline queue ──► sideline function
                       │
                       └──► after: Kinesis queue "order-migration" ──► fulfillment workers
```

This scenario is about the move. How a queue handles at-least-once delivery, scaling and
failed records is shown in scenarios 2, 3 and 4.

## Which consumers to migrate

Move a consumer to a queue when its job suits one (see
[When a queue is the right consumer](../README.md#when-a-queue-is-the-right-consumer)):

- **Each record is a job on its own.** Handling it doesn't need the records before or after it.
- **Done means done.** Once the work is finished, this consumer never needs the record again.
- **A repeat is harmless,** or you can make it harmless with a business key such as `orderId`.

Fulfillment passes all three: each order ships on its own, once. Fraud detection doesn't:
it needs every order of a customer, in sequence. So only the fulfillment consumer moves.
The fraud consumer keeps reading the stream, and the migration doesn't touch it. In this
scenario the Quickstart's KCL consumer stands in for it and keeps running throughout.

## What changes in the code

The fulfillment rules stay: the same checks, the same warehouse call, and the same orders
table with a conditional put on `orderId`. Compare `kcl-consumer/.../OrderFulfillment.java`
with `worker/src/fulfillment.ts`. What goes is everything KCL made the team build around
those rules:

| KCL consumer (`kcl-consumer/`) | Queue consumer (`worker/src/main.ts`) |
| --- | --- |
| A KCL `Scheduler` with a lease table, and a record processor per shard (`KclFulfillmentApp`, `FulfillmentProcessor`) | Poll loops: `ReceiveQueueRecords`, fulfill, `AcknowledgeQueueRecords` |
| `processRecords` gets a batch from one shard and handles it in order | A receive returns up to 10 orders from any shard, fulfilled in parallel |
| Progress is one checkpoint per shard: "everything up to here is done" | Each order that's done is acknowledged |
| A failed order is retried in place after 1 s and 2 s, while the rest of the shard waits | A failed order isn't acknowledged. It comes back after the visibility timeout, to any worker |
| After 3 failures the task sidelines the order to SQS with an audit trail (`Sideline`), and a Lambda function works through that queue | After `MaxReceiveCount` receives, counted across all workers, the order becomes a dead-letter record in the same Kinesis queue |
| After a lease moves, the new owner replays everything after the checkpoint | Only orders that weren't acknowledged come back |
| Custom metrics for fulfilled and sidelined orders, retries and records in flight (`Stats`) | None. The queue publishes waiting, in flight, received, acknowledged and dead-letter records |
| On shutdown: finish the batch and checkpoint | On shutdown: stop polling, then finish and acknowledge the orders held |
| Java, with KCL and its [migrations between major versions](https://docs.aws.amazon.com/streams/latest/dev/kcl-migration-from-2-3.html) | Any language with an AWS SDK |

The queue consumer here is TypeScript, like the other scenarios. A Java team would write
the same loop with the AWS SDK for Java and keep its fulfillment code as it is.

The setup around the consumer gets smaller too:

| | Before | After |
| --- | --- | --- |
| **IAM for the consumer,** besides the orders table | 16 actions on the stream, the lease table, CloudWatch metrics and the sideline queue | 2 actions on the queue |
| **DynamoDB** | The [lease table](https://docs.aws.amazon.com/streams/latest/dev/kcl-dynamoDB.html) and the orders table | The orders table |
| **Failed orders** | An SQS sideline queue, its dead-letter queue, and a Lambda function | The Kinesis queue's own dead-letter records |
| **Monitoring** | Custom metrics, and a dashboard built on them | The queue's metrics |

## How the cutover works

A queue on the stream delivers every record from its starting position on, whatever the
KCL consumer does. Two things make the switch safe:

- **No order is skipped.** KCL keeps one checkpoint per shard, but a queue starts at one
  position for the whole stream. `npm run migrate:plan` reads every checkpoint from the
  lease table, finds when each checkpointed record arrived, and prints the `queue:create`
  command with an `AT_TIMESTAMP` start a minute before the oldest one. Every order KCL
  hasn't finished arrived after that time. Starting at `LATEST` instead would skip the
  orders KCL hasn't reached when you stop it.
- **No order ships twice.** Both consumers record each order in the same orders table,
  with a conditional put on `orderId`. While both run, whichever reaches an order first
  ships it, and the other finds it in the table and skips it. If both work on the same
  order at the same moment, both call the warehouse, and only the first put succeeds.
  Keep the overlap short, and in a real system pass `orderId` to the warehouse as its
  idempotency key.

Then you stop the KCL consumer, check that no order was skipped, and only then remove the
KCL setup. Until that last step you can roll back by starting the KCL tasks again: they
resume from their checkpoints.

## What gets deployed

One CDK stack in this folder, deployed again for each phase with `-c phase=...`. It
refers to the Quickstart stream by ARN, and in the later phases to the queue too.

| Phase | What runs |
| --- | --- |
| `before` | The KCL consumer (Java, KCL 3.5.4) on Fargate ARM64, its lease table, the sideline SQS queue and its dead-letter queue, the sideline Lambda function, and a dashboard of the consumer's custom metrics |
| `cutover` | Everything above, plus the queue consumer (TypeScript) on Fargate ARM64 and a dashboard of the queue's metrics |
| `after` | The queue consumer only. Deploying this phase removes every KCL resource: the tasks, the lease table, both sideline queues, the sideline function and the KCL dashboard |

Both consumers use one **orders table** in DynamoDB.

| Role | Permissions |
| --- | --- |
| KCL task | Read the stream; read and write the lease table; `cloudwatch:PutMetricData` in KCL's own namespace; send to the sideline queue; the orders table |
| Sideline function | Receive and delete on the sideline queue; the orders table |
| Queue task | `kinesis:ReceiveQueueRecords` and `kinesis:AcknowledgeQueueRecords` on the queue; the orders table |

Both tasks that read records also have `kms:Decrypt`, limited to requests that Kinesis
Data Streams makes for this stream. The Quickstart's AWS managed key (`aws/kinesis`)
already allows this in its key policy, so the grant matters when the stream uses a
customer managed key.

The KCL consumer publishes its custom metrics in CloudWatch embedded metric format in its
logs, under the namespace `KinesisQueuesDemo/KclFulfillment`. KCL publishes its own
metrics, such as `MillisBehindLatest`, at `SUMMARY` level. Both are billed as custom
metrics. The queue consumer publishes none: the queue's metrics are built in.

## Run it

### Prerequisites

- **Quickstart:** deployed from `0-quickstart/`, in a Region where queues are
  available. See `../0-quickstart/README.md`.
- **Software:** Node.js 22, Docker, and the AWS CLI with credentials for your account. You
  don't need Java or Maven: Docker builds the KCL consumer.
- **VPC:** the stack creates its own VPC unless you pass `-c vpcId=vpc-...` with an
  existing VPC that has public subnets, for example the Quickstart's. Use that if your
  account is at its VPC quota (5 per Region by default).

From the repository root:

```bash
export AWS_PROFILE=YOUR_PROFILE AWS_REGION=us-east-2
STACK=migrate-consumer
MIG_ARGS=(--profile "$AWS_PROFILE" -c accountId=YOUR_ACCOUNT_ID -c region="$AWS_REGION" -c stackName="$STACK")
deploy() { (cd 6-migrate-existing-consumer && npx cdk deploy "$STACK" "${MIG_ARGS[@]}" --outputs-file cdk-outputs.json "$@"); }
```

Every `deploy` sets the phase and the settings for that step. A setting you leave out goes
back to its default.

Keep the status view running in a second terminal. It shows the tasks of both consumers,
the shards each KCL task holds, the live fulfillment rate, which consumer fulfilled the
orders, the duplicates the orders table absorbed, the sideline queues, and the queue's
metrics:

```bash
npm run migrate:status
```

To follow the consumers' logs:

```bash
LOGS=$(jq -r '."migrate-consumer".LogGroupName' 6-migrate-existing-consumer/cdk-outputs.json)
aws logs tail "$LOGS" --follow --format short | grep -v '"STATS"'
```

### 1. Run the KCL consumer

```bash
deploy -c phase=before
```

Two KCL tasks start and share the stream's shards through the lease table. KCL starts at
the tip of the stream (`LATEST`), so wait until `migrate:status` shows every shard held.
Then note the time, for the check in step 4, and start placing orders in a third
terminal. They keep arriving for about 15 minutes, through the whole migration:

```bash
START=$(date -u +%FT%TZ)
npm run orders:produce -- --count 3600 --rate 4
```

`migrate:status` shows the orders fulfilled by `kcl`. The KCL dashboard (`KclDashboardUrl`
in `6-migrate-existing-consumer/cdk-outputs.json`) shows the metrics the consumer
publishes about itself. None of them says how many orders are waiting.

### 2. Create the queue

```bash
npm run migrate:plan
```

It lists the stream's open shards, reads each checkpoint, and prints the command that
creates the queue on the same stream:

```text
KCL application migrate-consumer-KclLeases…: 4 shard leases, 4 open shards

  shard                   owner     first unfinished record       behind now
  shardId-000000000000    e15a2f0f  2026-11-16T17:34:36.919Z             5 s   checkpointed record
  shardId-000000000001    f084ef35  2026-11-16T17:34:39.261Z             2 s   checkpointed record
  shardId-000000000002    e15a2f0f  2026-11-16T17:34:37.263Z             4 s   checkpointed record
  shardId-000000000003    f084ef35  2026-11-16T17:34:38.934Z             3 s   checkpointed record

Oldest checkpoint: 2026-11-16T17:34:36.919Z. Start the queue 60 s before it, at 2026-11-16T17:33:36.919Z:

  npm run queue:create -- --queue-name order-migration --stream-arn arn:aws:kinesis:…:stream/… \
    --starting-position AT_TIMESTAMP --timestamp 2026-11-16T17:33:36.919Z
```

Run that command. The plan prints no command, and exits with an error, if it can't place
every shard: an open shard with no lease (for example a child shard right after the
stream resharded), a lease with no checkpoint yet, or a position it can't read. Wait,
and run it again.

From now on the queue holds every order since that time until a consumer acknowledges it.

### 3. Run both consumers

```bash
deploy -c phase=cutover
```

The queue workers start with a backlog: every order since the start time. KCL has already
shipped most of them, so the workers find them in the orders table and skip the warehouse
call. After that, every new order reaches both consumers. `migrate:status` shows orders
fulfilled by `kcl` and by `queue`, and the duplicates the orders table absorbed: skipped
before the warehouse call, or discarded after it when both consumers worked on the same
order at once:

```text
17:41:13  phase cutover  stream shards 4
  KCL       tasks 2/2   shards per task: e15a2f0f=2 f084ef35=2   fulfilled/s 0.4   in flight 0
  queue     tasks 2/2   fulfilled/s 2.2
  sideline  SQS queue 0   its DLQ 0
  queue     waiting 0, in flight 1, dead-letter records 0   (CloudWatch, 1-2 min behind)
```

### 4. Stop the KCL consumer and check

```bash
deploy -c phase=cutover -c kclTasks=0
```

The KCL tasks finish their current batch, checkpoint and stop. The queue workers carry on
alone. The lease table stays, so you can still roll back: deploy `-c phase=cutover`
again, and KCL resumes from its checkpoints.

When the producer has finished and `migrate:status` shows no orders waiting in the queue,
check that every order since step 1 was fulfilled once:

```bash
npm run migrate:verify -- --since "$START"
```

It reads every order event from all shards of the stream since `$START` and looks each
one up in the orders table. It exits with an error if an order wasn't fulfilled. From a
run with 1,810 orders over about 11 minutes, with both consumers side by side for 3 of
them:

```text
  1,810 order events, 1,810 distinct orders, 0 malformed records
Orders table migrate-consumer-FulfilledOrders…:
  fulfilled once: 1,810 of 1,810 (by kcl 850, queue 960)
  duplicate deliveries absorbed: 1,353 (999 skipped before the warehouse call, 354 discarded after it)
  missing: 0

Result: OK. Every order that can be fulfilled is in the orders table once, and none was skipped.
```

The 354 discarded duplicates are orders both consumers worked on at the same moment, so
the warehouse was called twice for them. That's why the overlap should be short, and why a
real warehouse API should take `orderId` as its idempotency key.

### 5. Remove the KCL setup

Check the sideline queues first: `migrate:status` shows how many orders the sideline
queue and its dead-letter queue hold. Orders sidelined before the queue's start time
aren't in the queue, so let the sideline function finish them, or fix the cause and
redrive the dead-letter records, before you remove both. In this walkthrough no order fails, so
both are empty.

```bash
deploy -c phase=after
```

CloudFormation removes the KCL tasks, the lease table, both sideline queues, the sideline
function and the KCL dashboard. The queue workers, the orders table and the queue
dashboard (`QueueDashboardUrl`) stay. In the test run the stack went from 40 resources
during the cutover to 23, 11 of them its own VPC. With `-c vpcId`, it's 29 to 12.

### 6. Clean up

```bash
(cd 6-migrate-existing-consumer && npx cdk destroy "$STACK" "${MIG_ARGS[@]}")
npm run queue:delete -- --queue-name order-migration
```

If you ran the relay migration too:

```bash
(cd 6-migrate-existing-consumer && npx cdk destroy "$RELAY" "${REL_ARGS[@]}")
npm run queue:delete -- --queue-name order-relay
```

Destroy the stack before you delete the queue, so no worker is polling a deleted queue.
Kinesis won't delete the Quickstart stream while a queue is attached to it.

## Migrate a relay into SQS

Some teams copy the stream into an SQS queue with a relay, such as an EventBridge pipe or
a Lambda function, and consume the SQS queue. When that SQS queue carries only records
from the stream, a Kinesis queue on the stream replaces both the relay and the SQS queue:
there is no second copy of the data to pay for and keep in sync. If other applications
also send messages to that SQS queue, keep it.

```text
before:  checkout ──► stream ──► EventBridge pipe ──► SQS queue ──► SQS consumer
                                                          └──► dead-letter queue (redrive policy)
after:   checkout ──► stream ──► Kinesis queue "order-relay" ──► queue consumer
```

### What changes in the code

The relay's SQS consumer, `worker/src/sqs-main.ts`, has the same structure as the queue
consumer in `worker/src/main.ts`, and both use the same fulfillment code. Only the calls
change:

| SQS consumer | Queue consumer |
| --- | --- |
| `ReceiveMessage` | `ReceiveQueueRecords` |
| `DeleteMessage`, `DeleteMessageBatch` | `AcknowledgeQueueRecords` |
| `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch` | `UpdateQueueRecordsVisibility` |
| A dead-letter queue with a redrive policy | The queue's built-in dead-letter records |
| The message body is the pipe's copy of the record: base64 `data`, plus `partitionKey`, `sequenceNumber` and `eventID` | The record itself: `Data`, `PartitionKey`, `ApproximateArrivalTimestamp` |

What goes is upstream of the consumer: the pipe, its IAM role, the SQS queue and its
dead-letter queue.

### How the cutover works

As in the KCL migration, both consumers run side by side on one orders table, so no order
ships twice. The starting position works differently, because a pipe has no checkpoints
you can read:

- **The pipe reads each shard in order and never skips a batch.** The stack sets this
  explicitly: one batch per shard at a time (`parallelizationFactor: 1`), retried until it
  succeeds (`maximumRetryAttempts: -1`). So on each shard, every record before the newest
  order the SQS consumer finished has already gone through the pipe.
- **The SQS consumer records where each order was in the stream:** its shard and arrival
  time, from the pipe's copy of the record.
- `npm run migrate:relay:plan` finds the newest finished order on each shard. For a shard
  with none yet, it takes the first record since the pipe was created. It prints the
  `queue:create` command with an `AT_TIMESTAMP` start a minute before the oldest of those
  times, and fails without a command if a shard can't be placed.

Orders still waiting in the SQS queue went through the pipe, so the Kinesis queue may not
deliver them. Keep the SQS consumer until the SQS queue and its dead-letter queue are
empty, and only then remove it.

### Run it

The relay migration is a separate stack, `migrate-relay`, deployed with
`-c migration=relay`. It has its own orders table and writes its outputs to
`cdk-outputs-relay.json`, so it can run next to the KCL migration. From the repository
root:

```bash
RELAY=migrate-relay
REL_ARGS=(--profile "$AWS_PROFILE" -c accountId=YOUR_ACCOUNT_ID -c region="$AWS_REGION" -c stackName="$RELAY" -c migration=relay)
relay() { (cd 6-migrate-existing-consumer && npx cdk deploy "$RELAY" "${REL_ARGS[@]}" --outputs-file cdk-outputs-relay.json "$@"); }
npm run migrate:status -- --relay          # second terminal
```

1. **Run the relay.** The pipe starts at the tip of the stream (`LATEST`). Then place
   orders, as in the KCL migration:

   ```bash
   relay -c phase=before
   START=$(date -u +%FT%TZ)
   npm run orders:produce -- --count 1500 --rate 4
   ```

2. **Create the queue.** While the relay keeps running:

   ```bash
   npm run migrate:relay:plan
   ```

   ```text
     shard                   position                      behind now
     shardId-000000000000    2026-11-16T18:02:55.240Z             6 s   newest order the SQS consumer finished
     shardId-000000000001    2026-11-16T18:02:58.698Z             2 s   newest order the SQS consumer finished
     shardId-000000000002    2026-11-16T18:02:57.629Z             3 s   newest order the SQS consumer finished
     shardId-000000000003    2026-11-16T18:02:59.392Z             1 s   newest order the SQS consumer finished

   Oldest position: 2026-11-16T18:02:55.240Z. Start the queue 60 s before it, at 2026-11-16T18:01:55.240Z:

     npm run queue:create -- --queue-name order-relay --stream-arn arn:aws:kinesis:…:stream/… \
       --starting-position AT_TIMESTAMP --timestamp 2026-11-16T18:01:55.240Z
   ```

   Run the command it prints.

3. **Run both consumers,** then stop the relay. The SQS consumer keeps running and
   finishes what's left in the SQS queue:

   ```bash
   relay -c phase=cutover
   relay -c phase=cutover -c pipeState=stopped
   ```

   ```text
     SQS       tasks 2/2   fulfilled/s 0.5
     queue     tasks 2/2   fulfilled/s 2.4
     relay     pipe migrate-relay-relay   SQS queue 0   its DLQ 0
   ```

4. **Check.** When the producer has finished and the SQS queue and its dead-letter queue
   are empty:

   ```bash
   npm run migrate:verify -- --relay --since "$START"
   ```

   From a run with 1,500 orders over about 6 minutes, with both consumers side by side
   for most of them:

   ```text
     1,500 order events, 1,500 distinct orders, 0 malformed records
     fulfilled once: 1,500 of 1,500 (by queue 738, sqs 762)
     duplicate deliveries absorbed: 1,500 (1,109 skipped before the warehouse call, 391 discarded after it)
     missing: 0
   ```

5. **Remove the relay.**

   ```bash
   relay -c phase=after
   ```

   CloudFormation removes the pipe and its role, the SQS queue, its dead-letter queue and
   the SQS consumer. The queue consumer and the orders table stay.

## How each consumer handles a record

| Record | KCL consumer | Queue consumer |
| --- | --- | --- |
| New order | Fulfill, then a conditional put on `orderId`. The checkpoint after the batch covers it | Fulfill, conditional put, acknowledge |
| Already in the orders table: a replay, a producer retry, or the other consumer was first | Skip the warehouse call | Skip the warehouse call, acknowledge |
| Shipped by the other consumer while this one was working on it | Discard this result | Discard this result, acknowledge |
| Warehouse call fails | Retry in place after 1 s, then 2 s. The shard waits | Don't acknowledge. Delivered again after the visibility timeout, to any worker |
| Fails 3 times | Sideline to SQS with an audit trail. The sideline function tries again | Keeps being redelivered |
| Fails `MaxReceiveCount` times | | Dead-letter record in the same queue |
| Malformed event | Sideline to SQS straight away | Don't acknowledge. Dead-letter record after `MaxReceiveCount` receives |
| Other event type | Skip | Acknowledge |

## Settings

| CDK context | Default | Meaning |
| --- | --- | --- |
| `migration` | `kcl` | `kcl`, or `relay` for the relay migration's stack |
| `phase` | `before` | `before`, `cutover` or `after` |
| `vpcId` | none: a new VPC | An existing VPC with public subnets to run the tasks in |
| `streamArn` | from `0-quickstart/cdk-outputs.json` | Stream both consumers read |
| `queueArn` | from `queue:create` | Queue for `cutover` and `after` (`order-migration`) |
| `kclTasks` | 2 | KCL tasks. `0` stops KCL but keeps its lease table and checkpoints |
| `kclInitialPosition` | `LATEST` | Where a new KCL application starts: `LATEST` or `TRIM_HORIZON` |
| `queueTasks` | 2 | Queue workers |
| `concurrency` | 4 | Poll loops per queue worker |
| `visibilityTimeoutSeconds` | 10 | Sent with every receive. A failed order is delivered again after this long |
| `processingMsMin` / `processingMsMax` | 300 / 800 | Simulated warehouse call duration, for both consumers |
| `pipeState` | `running` | Relay only: `running` or `stopped`. Stop the pipe before you remove it |
| `sqsTasks` | 2 | Relay only: tasks consuming the SQS queue |

## Files

- `kcl-consumer/`: the KCL consumer before the migration (Java 17, KCL 3.5.4).
  - `FulfillmentProcessor.java`: the record processor, with retries in place, sidelining and checkpoints.
  - `OrderFulfillment.java`: the fulfillment rules and the orders table.
  - `Sideline.java`: sends an order and its audit trail to the sideline queue.
  - `Stats.java`: the custom metrics.
  - `KclFulfillmentApp.java`: starts and stops the KCL scheduler.
- `worker/src/main.ts`: the queue consumer after the migration: receive, fulfill, acknowledge.
- `worker/src/fulfillment.ts`: the same fulfillment rules and orders table, in TypeScript.
- `worker/src/sideline.ts`: the sideline Lambda function. Part of the KCL setup, removed in the `after` phase.
- `lib/migration-stack.ts`: the KCL migration's stack for all three phases, and both dashboards.
- `lib/relay-stack.ts`: the relay migration's stack: the pipe, the SQS queue and its dead-letter queue, and both consumers.
- `worker/src/sqs-main.ts`: the relay's SQS consumer, before the migration.
- `scripts/plan-relay-cutover.ts`: `npm run migrate:relay:plan`.
- `scripts/plan-cutover.ts`: `npm run migrate:plan`.
- `scripts/stream-positions.ts`: lists open shards and finds the first record after a position, for both plans.
- `lib/network.ts`: the VPC, security group, cluster and log group.
- `scripts/status.ts`: `npm run migrate:status`. Read-only.
- `scripts/verify.ts`: `npm run migrate:verify`. Read-only.
