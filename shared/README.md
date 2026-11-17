# Shared queue tools

Code and command-line tools that every demo uses. These aren't a demo themselves.

| File | What it is |
| --- | --- |
| `src/common.ts` | Kinesis client setup, argument defaults, and reading stream and queue ARNs from the output files |
| `src/create-queue.ts` | `npm run queue:create`: create a queue on the Quickstart stream (or `--stream-arn`) and wait until it's `ACTIVE`. Scenarios 2 to 6 use it |
| `src/delete-queue.ts` | `npm run queue:delete`: delete a queue and wait until it's gone |
| `src/configure-queue.ts` | `npm run queue:configure`: show or change a queue's visibility timeout and dead-letter settings |
| `src/dead-letters.ts` | `npm run queue:dead-letters`: list, restore or discard a queue's dead-letter records |
| `queue-outputs.json` | Written by `queue:create`. Maps each queue name to its ARN. Git-ignored |

```bash
npm run queue:configure -- --queue-name NAME                       # show settings
npm run queue:configure -- --queue-name NAME --visibility-timeout 2 --max-receive-count 5
npm run queue:configure -- --queue-name NAME --dead-letter off
npm run queue:dead-letters -- --queue-name NAME                    # list
npm run queue:dead-letters -- discard --malformed --queue-name NAME --dry-run
```

`restore` and `discard` need a selection: `--order-id`, `--sku`, `--malformed` or
`--all`. Use `--dry-run` to preview. Reading dead-letter records increases their
`ReceiveCount`.

## Dead-letter settings

A queue's dead-letter behavior is set in `QueueAttributes.DeadLetterConfiguration`:

```json
{ "TargetType": "BUILT_IN", "MaxReceiveCount": 6 }
```

- **`TargetType`:** `BUILT_IN` moves a record to the queue's dead-letter state after
  `MaxReceiveCount` receives without an acknowledgment (6 by default). `NONE` turns dead-lettering
  off; omit `MaxReceiveCount` in that case.
- **Dead-letter operations:** `ReceiveDeadLetterQueueRecords`,
  `RestoreDeadLetterQueueRecords` and `AcknowledgeDeadLetterQueueRecords`. Each one has
  an IAM action of the same name, so dead-letter handling can go to an operator role.
- **Metrics:** each queue publishes `VisibleDeadLetterQueueRecords` and related
  metrics in `AWS/Kinesis`, with the `QueueId` and `QueueName` dimensions.
