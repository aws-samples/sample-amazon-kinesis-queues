# Scenario 5: Consume one stream in more than one way

**Scenario: one checkout, three jobs.** The checkout writes each `OrderPlaced`
event to the stream once. Three consumers read the same events, independently:

| Consumer | Kind | Job | Why this kind |
| --- | --- | --- | --- |
| Fulfillment | Queue `fanout-fulfillment` | Book the shipment for each order | Each order is a job on its own. Once shipped and acknowledged, it's done |
| Notifications | Queue `fanout-notifications` | Send the customer an order confirmation | Same: one confirmation per order, in any order |
| Fraud signals | KCL stream consumer | Flag a customer who places 5 orders within 60 seconds | Needs every order of a customer, in sequence. That's a stream job |

```text
                         ┌──► queue "fanout-fulfillment"    ──► fulfillment worker     ships each order
checkout ──► stream ─────┼──► queue "fanout-notifications" ──► notifications worker  confirms each order
             (written    │      starts AT_TIMESTAMP, catches up on orders already in the stream
              once)      └──► KCL consumer (polling)        ──► fraud signals         per customer, in order
```

## What this shows

- **No second copy of the data.** Each record is written to the stream once. Every
  Kinesis queue on the stream receives every record, and the stream consumer reads
  them too. There's no relay to SQS, and no second service to pay for and operate.
- **Each consumer keeps its own progress.** Acknowledging an order in the
  fulfillment queue removes it from that queue only. The notifications queue and the
  fraud-signals consumer still see it.
- **A new consumer can start in the past.** The notifications queue is created
  after the orders were placed, with an `AT_TIMESTAMP` starting position. It works
  through the orders already in the stream, then keeps up with new ones. Nothing is
  written to the stream again.
- **Consumers don't affect each other.** Stopping the fulfillment worker builds up a
  backlog in its queue only. Notifications and fraud signals carry on.

## What a queue changes

| Today | With Kinesis queues |
| --- | --- |
| Jobs that need order read the stream. Jobs that handle each record on its own get a copy of the records in an SQS queue, filled by a relay such as Lambda or EventBridge Pipes | Both kinds of job read the same stream. Stream consumers read it directly, and the others read from a Kinesis queue on it. Nothing is copied |
| Each job that needs its own SQS queue means another copy of the data, and a relay or fan-out step to fill it | Add another Kinesis queue to the stream. Each receives every record |
| A new consumer that needs past records means replaying onto the stream, which reaches every consumer | Create a Kinesis queue with `AT_TIMESTAMP` or `TRIM_HORIZON`. Only that queue sees the past records |
| A slow consumer can fall behind on its copy, and the relay is one more thing to watch | Each Kinesis queue has its own backlog metric, `VisibleQueueRecords` |

## What gets deployed

A separate CDK stack in this folder, `one-stream-many-ways`. It refers to the stream
and queues by ARN.

- **Fulfillment worker and notifications worker:** one Fargate task each, on ARM64,
  0.25 vCPU and 0.5 GiB. They run the same Node.js code (`workers/src/main.ts`)
  with a different `ROLE`. Each has its own DynamoDB table, one item per order,
  with a conditional write so a redelivery doesn't do the work twice.
- **Fraud-signals consumer:** one Fargate task on ARM64, 0.25 vCPU and 1 GiB,
  running Java 17 and KCL 3 (`fraud-signals/`). It uses polling (`GetRecords`), so it
  doesn't register an enhanced fan-out consumer. Its lease table is in DynamoDB.
- **IAM:** each queue worker can only receive and acknowledge on its own queue. The
  fraud-signals role can read the stream and its own lease table. On an encrypted
  stream, all three roles also have `kms:Decrypt`, limited to requests that Kinesis Data
  Streams makes for this stream. The Quickstart's AWS managed key (`aws/kinesis`)
  already allows this in its key policy, so the grant matters when the stream uses a
  customer managed key.
- **Network:** by default, a new VPC with public subnets and no NAT gateway. To run in
  an existing VPC, for example the Quickstart's, pass `-c vpcId=vpc-...`. It needs
  public subnets. The tasks accept no inbound connections.

The notifications worker is only deployed once its queue exists, so the walkthrough
deploys twice.

## Run it

### Prerequisites

- **Quickstart:** deployed from `0-quickstart/`, in a Region where queues are
  available. See `../0-quickstart/README.md`.
- **Software:** Node.js 22, Docker, and the AWS CLI with credentials for your account.

From the repository root:

```bash
export AWS_PROFILE=YOUR_PROFILE AWS_REGION=us-east-2
STACK=one-stream-many-ways
FAN_ARGS=(--profile "$AWS_PROFILE" -c accountId=YOUR_ACCOUNT_ID -c region="$AWS_REGION" -c stackName="$STACK")
# Optional, if your account is at its VPC quota: reuse the Quickstart's VPC.
# FAN_ARGS+=(-c vpcId=vpc-...)
```

### 1. Create the fulfillment queue and deploy

```bash
npm run queue:create -- --queue-name fanout-fulfillment --starting-position LATEST
(cd 5-consume-one-stream-many-ways && npx cdk deploy "$STACK" "${FAN_ARGS[@]}" --outputs-file cdk-outputs.json)
npm run fanout:status          # second terminal: one line per consumer
```

This deploys the fulfillment worker and the fraud-signals consumer.

### 2. Place orders

Note the time first. The notifications queue will start from it.

```bash
START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
npm run orders:produce -- --count 20 --rate 4
npm run orders:produce -- --count 6 --rate 2 --customers 1    # one customer, many orders
```

Fulfillment ships all 26 orders. The fraud-signals consumer scores all 26 and raises
one signal, for the customer who placed 6 orders in a few seconds:

```text
FRAUD_SIGNAL shard=shardId-000000000000 customerId=c-0001 orders=5 withinSeconds=60 lastOrderId=o-48a245bc-9992
```

That signal works because the checkout uses `customerId` as the partition key. All of
a customer's orders land on one shard, in order, and KCL gives each shard to one
processor. That's the ordering a queue doesn't provide, and doesn't need to for
fulfillment.

### 3. Add a consumer that starts in the past

```bash
npm run queue:create -- --queue-name fanout-notifications --starting-position AT_TIMESTAMP --timestamp "$START"
(cd 5-consume-one-stream-many-ways && npx cdk deploy "$STACK" "${FAN_ARGS[@]}" --outputs-file cdk-outputs.json)
```

The notifications queue didn't exist when the orders were placed. Its worker still
sends a confirmation for each of the 26 orders, because the queue starts reading the
stream at `$START`:

```text
  consumer        kind    tasks  done                         waiting in its queue
  fulfillment      queue   1/1    26 orders fulfilled          0
  notifications   queue   1/1    26 confirmations sent        0
  fraud signals   stream  1/1    26 orders scored, 1 signals  - (reads the stream; last hour)
```

### 4. Follow one order through every consumer

Pick any `orderId` from a worker log line (the producer doesn't print them):

```bash
npm run fanout:trace -- --order-id o-140e0e57-7d25
```

```text
Order o-140e0e57-7d25, as each consumer saw it:

  09:56:57.578  fulfillment     FULFILLED role=fulfillment orderId=o-140e0e57-7d25 customerId=c-0002 receiveCount=1 ageMs=71 ...
  09:56:58.226  fraud-signals  ORDER_SCORED shard=shardId-000000000000 orderId=o-140e0e57-7d25 customerId=c-0002 ordersInWindow=1
  09:59:15.564  notifications  CONFIRMATION_SENT role=notifications orderId=o-140e0e57-7d25 customerId=c-0002 receiveCount=1 ageMs=137846 ...

3 consumer(s): fulfillment, fraud-signals, notifications. The record was written to the stream once.
```

The notifications worker handled the order more than two minutes after it was
placed (`ageMs`), because its queue was created later and caught up.

### 5. Stop one consumer; the others carry on

```bash
CLUSTER=$(jq -r ".\"$STACK\".ClusterName" 5-consume-one-stream-many-ways/cdk-outputs.json)
FULFILLMENT=$(jq -r ".\"$STACK\".FulfillmentServiceName" 5-consume-one-stream-many-ways/cdk-outputs.json)
aws ecs update-service --cluster "$CLUSTER" --service "$FULFILLMENT" --desired-count 0
aws ecs wait services-stable --cluster "$CLUSTER" --services "$FULFILLMENT"   # until the task has stopped
npm run orders:produce -- --count 30 --rate 3
```

After a couple of minutes:

```text
  consumer        kind    tasks  done                         waiting in its queue
  fulfillment      queue   0/0    26 orders fulfilled          30
  notifications   queue   1/1    56 confirmations sent        0
  fraud signals   stream  1/1    56 orders scored, 1 signals  - (reads the stream; last hour)
```

Thirty orders wait in the fulfillment queue, and only there. Start the worker again
with `--desired-count 1`, and it works through them.

### 6. Clean up

```bash
(cd 5-consume-one-stream-many-ways && npx cdk destroy "$STACK" "${FAN_ARGS[@]}")
npm run queue:delete -- --queue-name fanout-notifications
npm run queue:delete -- --queue-name fanout-fulfillment
```

Kinesis won't delete a stream while a queue is attached to it.

## Limits to plan for

- **Queues per stream:** queues count toward the stream's limit on registered
  consumers, together with enhanced fan-out consumers: 20 per stream in On-demand
  Standard mode and 50 in On-demand Advantage mode. The fraud-signals consumer uses polling, so it doesn't count. A KCL consumer
  that uses enhanced fan-out does.
- **Read throughput:** polling consumers share each shard's read throughput. On an
  On-demand stream, Kinesis scales shards with the write traffic.
- **How far back a new queue can start:** a queue created with `AT_TIMESTAMP` or
  `TRIM_HORIZON` can only reach records still within the stream's retention period.

## Files

- `workers/src/main.ts`: the queue worker, run as fulfillment or notifications.
- `fraud-signals/src/main/java/com/example/fraud/FraudSignalsProcessor.java`: the
  per-customer order-velocity signal.
- `fraud-signals/src/main/java/com/example/fraud/ConsumerApp.java`: KCL setup and
  shutdown, adapted from the Quickstart consumer.
- `scripts/status.ts`: `npm run fanout:status`.
- `scripts/trace.ts`: `npm run fanout:trace`.
- `lib/one-stream-stack.ts`: the three consumers, their tables and roles.
