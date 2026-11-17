# Scenario 3: Scale out independently of shards

**Scenario: order fulfillment.** A burst of `OrderPlaced` events lands on the
stream: a flash sale, or a backlog after an outage. Fulfillment needs to catch up.
With a queue you add identical workers, and each one takes the next available
orders. You don't reshard the stream, rebalance leases or touch partition keys.

This works because fulfillment suits a queue: each order is a job on its own, so it
doesn't matter which worker takes it or in what order orders finish. Once an order
is fulfilled and acknowledged, it's done.

## What a queue changes

| On KCL | With a queue |
| --- | --- |
| Each shard is read by one worker of an application at a time, so throughput stops growing once you have one worker per shard | Workers aren't assigned to shards. Add as many as the backlog needs |
| Going faster means resharding the stream, which affects every consumer | The stream doesn't change. Only the queue consumers scale |
| A hot partition key keeps one shard, and one worker, busy while others sit idle | Partition keys don't decide who gets an order |
| Backlog is estimated from iterator age, which measures time, not work left | `VisibleQueueRecords` is the number of orders waiting |

## What gets deployed

A separate CDK stack in this folder, named `scale-out` in the commands below. It refers to one queue on the
Quickstart stream by ARN.

- **Fargate service:** fulfillment workers on ARM64, 0.25 vCPU and 0.5 GiB per task.
  Every task is identical and stateless. Each task runs 4 poll loops by default.
  Each loop receives up to 10 orders,
  fulfills them in parallel with a simulated 300–800 ms warehouse call, and
  acknowledges them. Each task logs its throughput every 10 seconds.
  The work waits on I/O, so one loop leaves the task mostly idle. In testing, one
  task fulfilled about 13 orders per second with 1 loop and about 52 with 4, with
  CPU under 20%. Raise the loops per task first, and let the alarms add tasks only
  when that isn't enough.
- **Least-privilege task role:** receive and acknowledge on this one queue only.
  On an encrypted stream it also has `kms:Decrypt`, limited to requests that Kinesis
  Data Streams makes for a stream in this account. The stack only knows the queue, so it
  can't name the stream. The Quickstart's AWS managed key (`aws/kinesis`) already allows
  this in its key policy, so the grant matters when the stream uses a customer managed key.
- **Backlog-driven scaling:** two CloudWatch alarms on the queue's
  `VisibleQueueRecords` metric drive ECS Service Auto Scaling step policies. They're
  the only thing that changes the number of workers. The stack sets no fixed task
  count, and a deploy never resets it.
- **CloudWatch dashboard:** backlog, acknowledgments per minute and worker CPU.

## Run it

### Prerequisites

- **Quickstart:** deployed from `0-quickstart/`. See `../0-quickstart/README.md`.
- **Software:** Node.js 22, Docker, and the AWS CLI with credentials for your account.

From the repository root:

```bash
export AWS_PROFILE=YOUR_PROFILE AWS_REGION=us-east-2
STACK=scale-out
SCALE_ARGS=(--profile "$AWS_PROFILE" -c accountId=YOUR_ACCOUNT_ID -c region="$AWS_REGION" -c stackName="$STACK")
```

### 1. Create the queue and deploy the workers

```bash
npm run queue:create -- --queue-name order-scale-out --starting-position LATEST
(cd 3-scale-out && npx cdk deploy "$STACK" "${SCALE_ARGS[@]}" -c maxTasks=8 --outputs-file cdk-outputs.json)
```

The service starts with `minTasks` workers (default 1) and scales between
`minTasks` and `maxTasks` on its own. In a second terminal, keep the status view
running:

```bash
npm run scale:status
```

It shows running workers next to the stream's shard count. It also shows the live
fulfillment rate, summed from every worker's log, and the queue backlog.

### 2. Send a burst and watch the alarms add workers

```bash
npm run orders:produce -- --count 20000
```

The two backlog alarms decide the worker count:

| Backlog (`VisibleQueueRecords`) | Action |
| --- | --- |
| 1,000 or more | Add 3 tasks |
| 200–999 | Add 1 task |
| 20 or fewer | Remove 1 task, never below `minTasks` |

Each action is followed by a 1-minute cooldown.

What to expect, from a test run with 20,000 orders and the default 4 poll loops
per task. The producer takes about 100 seconds to send them:

| Time after the burst starts | Workers | Fulfilled/s |
| --- | --- | --- |
| 0–2.5 min | 1 | about 52 |
| about 2.5 min | 1 → 4 (backlog alarm) | about 205 |
| about 4 min | 4 | backlog drained |
| about 4.5 min | 4 → 7, from a backlog reading taken before the drain | idle |
| then every 2 min | −1 per alarm cycle, back to `minTasks` | idle |

Nothing else changes: not the stream, not the queue, not the code. New workers take
orders within about a minute of the alarm, and the rate rises in proportion. The
4-shard stream doesn't cap it.

Queue metrics arrive about a minute late, so the first scale-out comes about 2
minutes after the burst starts, and the alarms can still add workers after the
backlog is gone. A short burst can drain before extra workers arrive: in the same
test, one task fulfilled a 5,000-order burst in about 95 seconds, before the new
workers started. To see a longer scale-out, send more orders or slow the simulated
work (`-c processingMsMin=1000 -c processingMsMax=2000`).

To make the point about partition keys, send the whole burst with a single customer,
so every record lands on one shard. The workers still share it evenly:

```bash
npm run orders:produce -- --count 5000 --customers 1
```

When ECS removes a task, the task finishes and acknowledges the orders it holds
before it exits.

### 3. Stop or clean up

```bash
# Pause: let the alarms scale the workers to zero once the queue is idle
(cd 3-scale-out && npx cdk deploy "$STACK" "${SCALE_ARGS[@]}" -c maxTasks=8 -c minTasks=0 --outputs-file cdk-outputs.json)

# Remove: the stack, then the queue
(cd 3-scale-out && npx cdk destroy "$STACK" "${SCALE_ARGS[@]}")
npm run queue:delete -- --queue-name order-scale-out
```

With `minTasks=0`, the low-backlog alarm keeps removing tasks until none are left.
The next burst of 200 or more orders starts workers again.

## Settings

| CDK context | Default | Meaning |
| --- | --- | --- |
| `queueArn` | from `queue:create` | Queue to consume (`order-scale-out`) |
| `vpcId` | none: a new VPC | An existing VPC with public subnets to run the tasks in, for example the Quickstart's. Use it if your account is at its VPC quota |
| `minTasks` | 1 | Fewest workers the alarms scale down to. `0` stops all workers when the queue is idle |
| `maxTasks` | 10 | Most workers the alarms scale up to, up to 20 |
| `concurrency` | 4 | Poll loops per task. Each loop handles up to 10 orders at a time. Raise it to use each task more before adding tasks |
| `processingMsMin` / `processingMsMax` | 300 / 800 | Simulated warehouse call duration |

The worker count isn't a setting. Only the backlog alarms change it.

## Files

- `worker/src/main.ts`: the receive → fulfill → acknowledge loop, throughput logging,
  and graceful shutdown.
- `lib/scale-out-stack.ts`: the Fargate service, task role, backlog alarms and
  scaling policies, and dashboard.
- `scripts/status.ts`: the status view (`npm run scale:status`). It's read-only.
- `../shared/src/produce-orders.ts`: the order producer (`npm run orders:produce`),
  shared with other scenarios.

This scenario doesn't deduplicate orders or handle failing records. Scenario 2
covers at-least-once delivery, and scenario 4 covers failures and dead-letter records.
