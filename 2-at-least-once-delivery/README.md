# Scenario 2: Process records safely with at-least-once delivery

**Scenario: order fulfillment.** An e-commerce checkout writes an `OrderPlaced` event
to a Kinesis data stream for every order. Two fulfillment workers consume the orders
through a queue on that stream. Fulfillment suits a queue: each order is a job on
its own, and once it has shipped and its record is acknowledged, it's done.

Each order must ship exactly once, but the queue delivers every record **at least
once**. This scenario shows the two patterns that make that safe:

1. **An idempotent handler.** Each fulfilled order is recorded in DynamoDB with a
   conditional write on `orderId`. A second delivery of the same order can't fulfill
   it again.
2. **A heartbeat for long-running records.** While a worker is still processing a
   record, it extends the record's visibility timeout with
   `UpdateQueueRecordsVisibility`, so no other worker receives it.

```text
checkout ──► stream ──► queue "order-at-least-once" ──► worker A ─┐
                         │      ▲                     ──► worker B ─┤ receive → fulfill → acknowledge
                         │      │                                   │
                         │      └─ heartbeat: UpdateQueueRecordsVisibility while still working
                         │                                          ▼
                         └─ not acknowledged in time: delivered again ──► orders table (conditional put on orderId)
                                                                          one fulfillment per order
```

## When a record is delivered more than once

A worker receives a record, which hides it from other workers for the **visibility
timeout**. If the worker doesn't acknowledge the record before the timeout expires,
the queue makes it visible again and delivers it again, with `ReceiveCount` one
higher. That happens when:

| Cause | What the next worker sees |
| --- | --- |
| The worker crashed, or lost its connection, after the work but before the acknowledgment | A record whose work is already done |
| Processing took longer than the visibility timeout | A record another worker is still working on |
| The producer retried a write and the stream holds the record twice | Two records with the same order |
| An operator restored or re-processed dead-letter records (scenario 4) | A record that may or may not have been processed |

The worker can't tell these apart from the record. It needs to make the work itself
safe to repeat, and to hold on to records it's still working on.

## Why a queue makes this easier

| Job | On KCL you build | With a queue |
| --- | --- | --- |
| **Know what's been processed** | Per-record state on top of a sequential checkpoint that was designed for batches. A checkpoint can't record that record 7 is done while record 5 still fails | The queue tracks every record. You acknowledge what's done |
| **Re-process after a crash** | Everything after the last checkpoint is replayed, including records that were already done | Only records that weren't acknowledged come back |
| **Take longer on one record** | The whole shard waits, and a lease that's taken over mid-batch means another worker replays the batch | Extend that record's visibility. Other records keep flowing to other workers |
| **Make the work idempotent** | Needed, and the replay window after a failover is large | Still needed, but redeliveries are per record and rare |

## What gets deployed

A separate CDK stack in this folder, `at-least-once`. It refers to the queue by ARN,
and the queue sits on the Quickstart stream.

- **Two fulfillment workers:** a Fargate service with two tasks on ARM64, 0.25 vCPU
  and 0.5 GiB each. Two, so a redelivered order can land on the other worker. The
  fleet is fixed; scaling on queue metrics is scenario 3.
- **Orders table:** DynamoDB, one item per fulfilled order, keyed on `orderId`. It's
  dedicated to this consumer. Don't reuse a KCL lease table for this: KCL owns that
  table's schema and reads its items.
- **Worker role:** `ReceiveQueueRecords`, `AcknowledgeQueueRecords` and
  `UpdateQueueRecordsVisibility` on this queue, and `GetItem`, `PutItem` and
  `UpdateItem` on the orders table. `UpdateItem` only counts duplicate deliveries for
  the demo.
  On an encrypted stream it also has `kms:Decrypt`, limited to requests that Kinesis
  Data Streams makes for a stream in this account. The stack only knows the queue, so it
  can't name the stream. The Quickstart's AWS managed key (`aws/kinesis`) already allows
  this in its key policy, so the grant matters when the stream uses a customer managed key.

## Run it

### Prerequisites

- **Quickstart:** deployed from `0-quickstart/`, in a Region where queues are
  available. See `../0-quickstart/README.md`.
- **Software:** Node.js 22, Docker, and the AWS CLI with credentials for your account.

From the repository root:

```bash
export AWS_PROFILE=YOUR_PROFILE AWS_REGION=us-east-2
STACK=at-least-once
ALO_ARGS=(--profile "$AWS_PROFILE" -c accountId=YOUR_ACCOUNT_ID -c region="$AWS_REGION" -c stackName="$STACK")
```

### 1. Create the queue and deploy the workers

```bash
npm run queue:create -- --queue-name order-at-least-once --starting-position LATEST
(cd 2-at-least-once-delivery && npx cdk deploy "$STACK" "${ALO_ARGS[@]}" --outputs-file cdk-outputs.json)
LOGS=$(jq -r '."at-least-once".LogGroupName' 2-at-least-once-delivery/cdk-outputs.json)
aws logs tail "$LOGS" --follow --format short | grep -v '"STATS"'   # second terminal
```

The workers receive with a 10-second visibility timeout
(`VisibilityTimeoutSeconds` on `ReceiveQueueRecords`). It's short so redeliveries
are quick to watch. The heartbeat extends a record every 3 seconds, a third of the
timeout, which leaves two chances before a record expires.

### 2. A lost acknowledgment doesn't ship an order twice

```bash
npm run orders:produce -- --count 20 --ack-lost 2
```

The worker fulfills the two `ack-lost` orders, then behaves as if it crashed: it
doesn't acknowledge them. Ten seconds later the queue delivers them again, often to
the other worker. The orders table already has them, so the worker skips the
warehouse call and acknowledges them:

```text
d96f1e80 FULFILLED           orderId=o-a6b19890-f433  receiveCount=1  simulate=ack-lost
d96f1e80 ACK_LOST_SIMULATED  orderId=o-a6b19890-f433  receiveCount=1  redeliveryInSeconds=10
1d6c8e3a DUPLICATE_SKIPPED   orderId=o-a6b19890-f433  receiveCount=2  simulate=ack-lost
```

### 3. A producer retry doesn't either

```bash
npm run orders:produce -- --count 20 --duplicates 0.2
```

About one order in five is written to the stream twice, as if the producer retried
after a lost response. Both copies are separate queue records, and they often
arrive in the same batch, so the worker processes them side by side. Both pass the
`GetItem` check; only the first conditional put succeeds, and the other copy is
discarded:

```text
1d6c8e3a FULFILLED            orderId=o-5c952cd0-9a60  receiveCount=1
1d6c8e3a DUPLICATE_DISCARDED  orderId=o-5c952cd0-9a60  receiveCount=1
```

The warehouse was called twice for that order. The table makes sure the order is
recorded as fulfilled once. In a real system, pass `orderId` to the warehouse as
its idempotency key too, because that call happens before the conditional put.

### 4. A slow order: with and without the heartbeat

Orders marked slow take 25 seconds, longer than the 10-second visibility timeout.

```bash
npm run orders:produce -- --count 1 --slow 1
```

With the heartbeat, the worker extends the record every 3 seconds. Nobody else
receives it, and it's fulfilled on its first receive:

```text
1d6c8e3a VISIBILITY_EXTENDED  records=1  orderIds=[o-89aca720-d150]  visibilityTimeoutSeconds=10
   ... 7 more, every 3 s ...
1d6c8e3a FULFILLED            orderId=o-89aca720-d150  receiveCount=1  simulate=slow  processingMs=25000
```

Now the same order without the heartbeat:

```bash
npm run orders:produce -- --count 1 --slow-no-heartbeat 1
```

Every 10 seconds the record becomes visible again while a worker is still working
on it, so in 25 seconds the queue delivers it three times. Each delivery starts the
same work. The first one finishes first, and the conditional puts of the other two
fail:

```text
c184d0ac FULFILLED            orderId=o-31c22b11-2cdf  receiveCount=1  processingMs=25002
53b727db DUPLICATE_DISCARDED  orderId=o-31c22b11-2cdf  receiveCount=2  processingMs=25001
c184d0ac DUPLICATE_DISCARDED  orderId=o-31c22b11-2cdf  receiveCount=3  processingMs=25000
```

The order still shipped once, but 25 seconds of work were done three times and the
workers were tied up for nothing. With many slow records, that wasted work can snowball.

### 5. See what the table absorbed

```bash
npm run delivery:status -- --once
```

```text
08:44:55  workers 2/2  orders fulfilled 46  duplicate deliveries absorbed 8 (2 skipped before the warehouse call, 6 discarded after it)
  orderId           simulate            fulfilled by  on receive  took(s)  dup skipped  dup discarded
  o-1b6857f2-e80c  ack-lost               53b727db           1      0.4            1              0
  o-0423e932-6ddf  ack-lost               c184d0ac           1      0.7            1              0
  o-793a29d4-ee1e  -                      53b727db           1      0.4            0              1
  o-14d8ed3a-51f2  -                      53b727db           1      0.4            0              1
  o-48fc0a26-7b17  -                      53b727db           1      0.6            0              1
  o-6e32343a-14b4  -                      53b727db           1      0.7            0              1
  o-0680b6e7-c8f9  slow                   c184d0ac           1     25.0            0              0
  o-31c22b11-2cdf  slow-no-heartbeat      c184d0ac           1     25.0            0              2
```

Leave off `--once` to refresh every 10 seconds.

### 6. Clean up

```bash
(cd 2-at-least-once-delivery && npx cdk destroy "$STACK" "${ALO_ARGS[@]}")
npm run queue:delete -- --queue-name order-at-least-once
```

Kinesis won't delete a stream while a queue is attached to it.

## How the worker handles each record

| Record | Worker action | Outcome |
| --- | --- | --- |
| New order | Fulfill, then a conditional put on `orderId` | Acknowledged |
| Already in the table | Skip the warehouse call | Acknowledged |
| Fulfilled by another delivery while this one worked | Conditional put fails; discard this result | Acknowledged |
| Still being processed | Heartbeat extends its visibility | Hidden from other workers |
| Fulfillment threw an error | Stop extending it | Delivered again after the visibility timeout |
| Malformed | Don't acknowledge | Dead-letter record after `MaxReceiveCount` receives (scenario 4) |
| Other event type | Ignore | Acknowledged |

### The heartbeat

- **One call per batch.** Every 3 seconds, one `UpdateQueueRecordsVisibility` call
  resets the timeout of every record in the batch that the worker still holds, up
  to 10 entries.
- **The new timeout counts from the call.** Setting it to 10 seconds hides the
  record for 10 more seconds from now, however long the worker has already held
  it. Setting it to 0 makes the record visible again at once.
- **Records are held until they're acknowledged,** not just until they're
  processed. The worker acknowledges a batch when all of it is done, so finished
  records wait in the batch and need extending too.
- **Check the `Failed` list.** An entry fails when the record already expired, or
  was acknowledged. The worker stops extending it and finishes anyway; the orders
  table catches a duplicate.
- **A hold limit.** After 300 seconds the heartbeat lets a record go, so a stuck
  handler can't hold it forever. The queue delivers it again and, after
  `MaxReceiveCount` receives, dead-letters it.

### Choosing the idempotency key

The worker uses `orderId`, a business key. It stays the same across every way an
order can be delivered twice, including the producer retry in step 3, which writes
two separate stream records. A key based on the stream record would treat those as
two orders.

## Settings

| CDK context | Default | Meaning |
| --- | --- | --- |
| `queueArn` | from `queue:create` | Queue to consume |
| `vpcId` | none: a new VPC | An existing VPC with public subnets to run the tasks in, for example the Quickstart's. Use it if your account is at its VPC quota |
| `workers` | 2 | Worker tasks in the service |
| `concurrency` | 4 | Poll loops per task. The work waits on I/O, so one task keeps several batches in progress |
| `visibilityTimeoutSeconds` | 10 | Sent with every receive and every heartbeat |
| `heartbeatSeconds` | a third of the timeout | How often the heartbeat extends held records |
| `maxHoldSeconds` | 300 | How long the heartbeat holds one record at most |
| `slowProcessingSeconds` | 25 | How long `slow` and `slow-no-heartbeat` orders take |

In production, set the visibility timeout above your usual processing time and use
the heartbeat for the outliers. A timeout that's too short causes redeliveries; one
that's too long delays the retry after a worker crashes.

## Files

- `worker/src/main.ts`: the receive → fulfill → acknowledge loop.
- `worker/src/fulfillment.ts`: idempotent fulfillment and the simulated warehouse.
- `worker/src/heartbeat.ts`: extends visibility for the records a worker holds.
- `scripts/status.ts`: `npm run delivery:status`.
- `lib/at-least-once-stack.ts`: the workers, orders table and role.
- `../shared/src/produce-orders.ts`: the order producer (`npm run orders:produce`),
  with `--ack-lost`, `--slow` and `--slow-no-heartbeat` for this scenario.
