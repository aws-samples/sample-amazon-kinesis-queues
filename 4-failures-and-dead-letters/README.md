# Scenario 4: Handle failures and dead-letter records

**Scenario: order fulfillment.** An e-commerce checkout writes an `OrderPlaced`
event to a Kinesis data stream for every order. Fulfillment workers consume the
orders through a queue on that stream. Each order is a job on its own, so one that
fails can be retried, or set aside, without holding up the others. Once an order is
fulfilled and acknowledged, it's done.

Fulfillment is where things go wrong:

- **A warehouse call times out.** Retrying the same order a moment later works.
- **The warehouse API is down.** Every order fails until it's back, often for
  longer than the retries last.
- **An order references a discontinued product.** It fails on every attempt
  until someone fixes the catalog.
- **An event arrives malformed.** No retry or fix can process it.

This scenario sends records down each of those paths and shows how the queue
handles delivery: redelivering failed orders on their own, dead-lettering the ones
that keep failing, and restoring them once the cause is fixed, so the same workers
process them. The workers keep running unchanged throughout.

```text
checkout ──► stream ──► queue "order-fulfillment" ──► fulfillment worker
                              │   ▲                     receive → fulfill → acknowledge
                              │   └── not acknowledged: redelivered after the visibility timeout
                              │
                              └── received MaxReceiveCount times ──► dead-letter records ──► alarm
                                                                          │
        operator fixes the cause ──► dead-letter task (own IAM role) restores them ──► workers
```

## How a queue delivers records

1. **Receive.** `ReceiveQueueRecords` returns up to 10 records. Each one is hidden
   from other consumers for the *visibility timeout*.
2. **Process.** The consumer does the work for each record.
3. **Acknowledge.** `AcknowledgeQueueRecords` marks the records done.

A record that isn't acknowledged becomes visible again when its visibility timeout
expires, and its `ReceiveCount` goes up by one. After `MaxReceiveCount` receives, 6
by default, the queue moves it to its built-in **dead-letter** state. Regular
receives never return dead-letter records, so they stop taking worker capacity.

Delivery is at least once, so the worker makes fulfillment idempotent: each order is
written to a DynamoDB table with a conditional put on `orderId`.

## Why a queue makes failures easier

What handling failed orders takes, on KCL compared with a queue:

| Job | On KCL you build | With a Kinesis queue |
| --- | --- | --- |
| **Survive a failed order** | Retry in place, which stalls every order behind it on the shard, or skip it and track the failure yourself | Don't acknowledge it. It's retried on its own while other orders keep flowing |
| **Decide what's poison** | Your consumer decides alone, because ordering stops other instances from retrying first. One bad instance can sideline many healthy records | The queue counts receives across all workers and dead-letters a record after *N* failed attempts |
| **Park poison records** | Copy them to SQS as a DLQ, with its own consumer fleet, because replaying onto the stream reaches every consumer | Built in. Dead-letter records stay in the same Kinesis queue, with no second queue or ARN, and regular workers stop seeing them |
| **Recover after a fix** | Your own replay tooling, plus deduplication | Restore the dead-letter records. The existing workers process them again, with a fresh receive count. Discard only what can't be processed |
| **Separate who handles failures** | Custom permissions on your sideline queue and tooling | Dead-letter operations have their own IAM actions, so the workers' role can't touch them |
| **Alert on failures** | Emit custom metrics | `VisibleDeadLetterQueueRecords` and related metrics, per queue |

## What gets deployed

A separate CDK stack in this folder, named `failures-dead-letters` in the commands below. It refers to the
queue by ARN, and the queue sits on the Quickstart stream.

- **Fulfillment worker:** one Fargate task on ARM64, 0.5 vCPU and 1 GiB. One worker
  is enough to show every failure path; scaling is scenario 3.
- **Product catalog:** an SSM parameter listing discontinued SKUs, starting with
  `SKU-DISCONTINUED`. The worker reads it at run time, so fixing the catalog is a
  data change: no redeploy, no restart.
- **Warehouse status:** an SSM parameter, `up` or `down`. Setting it to `down`
  simulates an outage of the warehouse API. The worker reads it at run time too.
- **Orders table:** DynamoDB, one item per fulfilled order. It makes fulfillment
  idempotent.
- **Dead-letter task:** a separate Fargate task definition, 0.25 vCPU and 0.5 GiB,
  started on demand with `RunTask`. It runs the shared dead-letter tool
  (`npm run queue:dead-letters`, see `../shared/README.md`) under the operator role,
  to list, restore or discard dead-letter records. It doesn't process orders itself:
  restored records go back to the workers. It isn't a service, so it costs nothing
  between runs.
- **Two IAM roles, separate duties:**

  | Role | Queue permissions | Can't |
  | --- | --- | --- |
  | Worker task role | `ReceiveQueueRecords`, `AcknowledgeQueueRecords` | Touch dead-letter records |
  | Dead-letter task role (operator) | `ReceiveDeadLetterQueueRecords`, `AcknowledgeDeadLetterQueueRecords`, `RestoreDeadLetterQueueRecords` | Receive or acknowledge regular records |

  Both roles also have `kms:Decrypt`, limited to requests that Kinesis Data Streams
  makes for a stream in this account, because dead-letter records are stream records
  too. The Quickstart's AWS managed key (`aws/kinesis`) already allows this in its key
  policy, so the grant matters when the stream uses a customer managed key.

  Only the worker role can read the catalog and the warehouse status, and write the
  orders table. Dead-letter records are processed by the workers after a restore, so the
  operator role needs no access to either.
- **Dead-letter alarm:** fires when the queue holds at least one dead-letter record,
  counting dead-letter records that are visible and those an operator is reading. It
  notifies an SNS topic; add `-c alarmEmail=you@example.com` to get emails.
- **CloudWatch dashboard:** backlog, throughput, dead-letter records and the alarm state.

## Run it

### Prerequisites

- **Quickstart:** deployed from `0-quickstart/`, in a Region where queues are
  available. See `../0-quickstart/README.md`.
- **Software:** Node.js 22, Docker, and the AWS CLI with credentials for your account.

From the repository root:

```bash
export AWS_PROFILE=YOUR_PROFILE AWS_REGION=us-east-2
STACK=failures-dead-letters
FUL_ARGS=(--profile "$AWS_PROFILE" -c accountId=YOUR_ACCOUNT_ID -c region="$AWS_REGION" -c stackName="$STACK")
```

### 1. Create the queue and deploy the worker

```bash
npm run queue:create -- --queue-name order-fulfillment --starting-position LATEST
npm run queue:configure -- --queue-name order-fulfillment --visibility-timeout 2   # retry every ~2 s for the demo
(cd 4-failures-and-dead-letters && npx cdk deploy "$STACK" "${FUL_ARGS[@]}" --outputs-file cdk-outputs.json)
npm run orders:status          # second terminal: fulfilled, backlog, dead-letter records
```

A 2-second visibility timeout makes redeliveries quick enough to watch. The default
is 30 seconds. Keep it above the worker's batch time, about 0.9 s here.

### 2. A transient failure succeeds on redelivery

```bash
npm run orders:produce -- --count 10 --transient 2
```

The two marked orders time out on their first warehouse call. The worker doesn't
acknowledge them, the queue redelivers them about 2 seconds later, and they're
fulfilled. The other orders aren't held up. In the worker log:

```text
TRANSIENT_FAILURE  orderId=o-…  receiveCount=1
FULFILLED          orderId=o-…  receiveCount=2
```

### 3. Poison orders become dead-letter records

```bash
npm run orders:produce -- --count 20 --poison 3 --invalid 2
```

- **`--poison`** sends valid orders for `SKU-DISCONTINUED`, which the catalog lists
  as discontinued. They fail on every delivery.
- **`--invalid`** sends truncated JSON, which can never be processed.

The worker never decides to give up. It just doesn't acknowledge, and the queue
counts the receives. After 6 receives, about 12 seconds here, each failing record
becomes a dead-letter record, while the 20 good orders are fulfilled on their first receive.

```bash
npm run queue:dead-letters -- --queue-name order-fulfillment     # lists the 5 dead-letter records
```

Within 2–3 minutes the dead-letter alarm goes to `ALARM`.

### 4. Fix the cause and restore the dead-letter records

The product is back in stock. Fix the catalog; the running worker picks the change
up within about 10 seconds:

```bash
npm run orders:catalog -- restock SKU-DISCONTINUED
```

Then restore the dead-letter records for that product, so the workers process them again:

```bash
npm run orders:dead-letter-task -- restore --sku SKU-DISCONTINUED --dry-run
npm run orders:dead-letter-task -- restore --sku SKU-DISCONTINUED
```

The dead-letter task runs under its own role. It reads the dead-letter records with
`ReceiveDeadLetterQueueRecords` and restores the 3 poison orders with
`RestoreDeadLetterQueueRecords`. They're visible in the queue again with a fresh
receive count, and the worker fulfills them like any other order:

```text
RESTORED   receiveCount=9 … orderId=o-… skus=SKU-DISCONTINUED,…     (task log)
FULFILLED  orderId=o-…  receiveCount=1                                (worker log)
```

The 2 malformed events can't be processed by any fix, so they stay dead-letter records.
Discard them:

```bash
npm run orders:dead-letter-task -- discard --malformed --dry-run
npm run orders:dead-letter-task -- discard --malformed
```

Discarding acknowledges the dead-letter records, which removes them from this queue only.
The records stay in the stream until its retention ends, so stream consumers and
other queues still read them.

The task runs the same tool as `npm run queue:dead-letters`, which you can also run
with your own credentials. The task lets an operator act without any queue
permissions: they need `ecs:RunTask` and `iam:PassRole` on the task roles.

Once no dead-letter records are left, the alarm returns to `OK`. To run step 3 again, mark
the product discontinued once more:

```bash
npm run orders:catalog -- discontinue SKU-DISCONTINUED
```

### 5. A downstream outage

The most common cause of dead-letter records is a downstream service that's down. Take the
warehouse API down, then send orders:

```bash
npm run orders:warehouse -- down
npm run orders:produce -- --count 10
```

Every order fails with `WAREHOUSE_UNAVAILABLE`. Nothing is wrong with the orders, but
the outage lasts longer than the retries: after 6 receives, about 12 seconds here,
all 10 are dead-letter records, and the alarm goes to `ALARM`. Regular receives no longer
return them, so the workers stop spending capacity on them. When the warehouse is
back, restore all of them:

```bash
npm run orders:warehouse -- up
npm run orders:dead-letter-task -- restore --all
```

The worker fulfills all 10 with a fresh receive count. With a 30-second visibility
timeout, the default, the queue would retry for about 3 minutes before
dead-lettering. Set `MaxReceiveCount` and the visibility timeout so that short
outages are covered by retries, and longer ones end up as dead-letter records to restore.

### 6. Clean up

```bash
(cd 4-failures-and-dead-letters && npx cdk destroy "$STACK" "${FUL_ARGS[@]}")
npm run queue:delete -- --queue-name order-fulfillment
```

Kinesis won't delete a stream while a queue is attached to it.

## How the worker handles each record

| Record | Worker action | Outcome |
| --- | --- | --- |
| New `OrderPlaced` | Fulfill, then a conditional put on `orderId` | Acknowledged |
| Already fulfilled (a redelivery or a duplicate) | Skip the warehouse call | Acknowledged |
| Warehouse timeout | Don't acknowledge | Redelivered after the visibility timeout |
| Warehouse down | Don't acknowledge | Redelivered; a dead-letter record after `MaxReceiveCount` receives if the outage lasts. Restore it when the warehouse is back |
| Discontinued SKU, or malformed | Don't acknowledge | Dead-letter record after `MaxReceiveCount` receives |
| Other event type | Ignore | Acknowledged |

When ECS stops the task during a deploy, the task stops polling, finishes and
acknowledges the orders it holds, then exits.

## Settings

| CDK context | Default | Meaning |
| --- | --- | --- |
| `queueArn` | from `queue:create` | Queue to consume |
| `vpcId` | none: a new VPC | An existing VPC with public subnets to run the tasks in, for example the Quickstart's. Use it if your account is at its VPC quota |
| `concurrency` | 4 | Poll loops in the worker. The work waits on I/O, so one task keeps several batches in progress |
| `processingMsMin` / `processingMsMax` | 300 / 800 | Simulated warehouse call duration |
| `alarmEmail` | none | Email address subscribed to the dead-letter alarm |

Queue settings live on the queue itself: `npm run queue:configure -- --queue-name
order-fulfillment` shows or changes the visibility timeout and `MaxReceiveCount`.

## Files

- `worker/src/main.ts`: the receive → fulfill → acknowledge loop.
- `worker/src/fulfillment.ts`: idempotent fulfillment and the simulated warehouse.
- `worker/src/catalog.ts`: reads the discontinued SKUs and the warehouse status from SSM at run time.
- `../shared/src/dead-letters.ts`: the dead-letter tool the task runs.
- `scripts/catalog.ts`: `npm run orders:catalog`, to show, discontinue or restock a SKU.
- `scripts/warehouse.ts`: `npm run orders:warehouse`, to take the warehouse API down or bring it back.
- `scripts/run-dead-letter-task.ts`: `npm run orders:dead-letter-task`, to run the dead-letter tool as a task under the operator role.
- `scripts/status.ts`: `npm run orders:status`.
- `lib/failures-dead-letters-stack.ts`: the worker, catalog, warehouse status, table,
  dead-letter task, roles, alarm and dashboard.
