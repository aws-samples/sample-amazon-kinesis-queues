// Runs the shared dead-letter tool (npm run queue:dead-letters) as a one-off Fargate
// task (RunTask) under the stack's operator role, waits for it to finish, and prints
// its output. The operator role has only the dead-letter operations on the queue, so
// whoever starts the task needs no queue permissions of their own.
//
// The arguments are the tool's own: list, restore or discard, then a selection.
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { CloudWatchLogsClient, GetLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
import { DescribeTasksCommand, ECSClient, RunTaskCommand } from '@aws-sdk/client-ecs';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

const USAGE = `Usage: npm run orders:dead-letter-task -- [list|restore|discard] [selection] [options]

Selection (restore and discard need one):
  --order-id ID      Dead-letter records whose payload has this orderId (repeatable)
  --sku SKU          Dead-letter records for orders containing this SKU (repeatable)
  --malformed        Dead-letter records whose payload isn't valid JSON
  --all              Every dead-letter record

Options:
  --dry-run          Show what restore or discard would do, change nothing
  --max N            Look at no more than N dead-letter records (default: 100)
  --stack-name NAME  Fulfillment stack in 4-failures-and-dead-letters/cdk-outputs.json (default: the only one)
  --region REGION    Default: us-east-2
  --profile PROFILE  AWS profile used to start the task (needs ecs:RunTask and iam:PassRole on the task roles)`;

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');
const CONTAINER = 'DeadLetterConsumer';

interface Outputs {
  ClusterName: string;
  DeadLetterTaskDefinitionArn: string;
  SubnetIds: string;
  SecurityGroupId: string;
  LogGroupName: string;
  QueueArn: string;
}

function readOutputs(stackName: string | undefined): Outputs {
  const all = JSON.parse(readFileSync(OUTPUTS, 'utf8')) as Record<string, Outputs>;
  const names = stackName ? [stackName] : Object.keys(all);
  if (names.length !== 1 || !all[names[0]]) throw new Error(`Pass --stack-name; ${OUTPUTS} has ${Object.keys(all).join(', ')}.`);
  const outputs = all[names[0]];
  if (!outputs.DeadLetterTaskDefinitionArn) throw new Error('Redeploy the stack: it has no dead-letter task yet.');
  return outputs;
}

/** Turns the task's JSON log lines into one readable line each. */
function format(message: string): string {
  try {
    const { time, event, level: _level, task: _task, ...fields } = JSON.parse(message) as Record<string, unknown>;
    const detail = Object.entries(fields).map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`).join(' ');
    return `${String(time).slice(11, 19)} ${String(event).padEnd(30)} ${detail}`;
  } catch {
    return message;
  }
}

async function main(): Promise<void> {
  const { values: args, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      'order-id': { type: 'string', multiple: true, default: [] },
      sku: { type: 'string', multiple: true, default: [] },
      malformed: { type: 'boolean', default: false },
      all: { type: 'boolean', default: false },
      'dry-run': { type: 'boolean', default: false },
      max: { type: 'string', default: '100' },
      'stack-name': { type: 'string' },
      region: { type: 'string', default: process.env.AWS_REGION ?? 'us-east-2' },
      profile: { type: 'string', default: process.env.AWS_PROFILE },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const max = Number(args.max);
  if (!Number.isInteger(max) || max < 1) throw new Error('--max must be a positive whole number');
  const command = positionals[0] ?? 'list';
  if (!['list', 'restore', 'discard'].includes(command) || positionals.length > 1) {
    throw new Error(`Unknown command "${positionals.join(' ')}". Use list, restore or discard.`);
  }
  // Validated here, so the task gets only known flags and well-formed values.
  const ids = args['order-id'].filter((id) => /^[A-Za-z0-9_-]{1,64}$/.test(id));
  const skus = args.sku.filter((sku) => /^[A-Za-z0-9_-]{1,64}$/.test(sku));
  if (ids.length !== args['order-id'].length || skus.length !== args.sku.length) {
    throw new Error('--order-id and --sku take letters, numbers, hyphens and underscores only.');
  }
  const outputs = readOutputs(args['stack-name']);
  const toolArgs = [
    command, '--queue-arn', outputs.QueueArn, '--max', String(max),
    ...ids.flatMap((id) => ['--order-id', id]),
    ...skus.flatMap((sku) => ['--sku', sku]),
    ...(args.malformed ? ['--malformed'] : []),
    ...(args.all ? ['--all'] : []),
    ...(args['dry-run'] ? ['--dry-run'] : []),
  ];
  const clientConfig = { region: args.region, credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}) };
  const ecs = new ECSClient(clientConfig);
  const logs = new CloudWatchLogsClient(clientConfig);

  try {
    const run = await ecs.send(new RunTaskCommand({
      cluster: outputs.ClusterName,
      taskDefinition: outputs.DeadLetterTaskDefinitionArn,
      launchType: 'FARGATE',
      count: 1,
      startedBy: 'dead-letter-task',
      networkConfiguration: {
        awsvpcConfiguration: {
          subnets: outputs.SubnetIds.split(','),
          securityGroups: [outputs.SecurityGroupId],
          assignPublicIp: 'ENABLED',
        },
      },
      overrides: {
        containerOverrides: [{ name: CONTAINER, command: ['node', '--enable-source-maps', 'dead-letters.js', ...toolArgs] }],
      },
    }));
    if (run.failures?.length) throw new Error(`RunTask failed: ${JSON.stringify(run.failures)}`);
    const taskArn = run.tasks?.[0]?.taskArn;
    if (!taskArn) throw new Error('RunTask returned no task.');
    const taskId = taskArn.split('/').pop()!;
    console.log(`Started dead-letter task ${taskId}: ${toolArgs.filter((a) => a !== outputs.QueueArn && a !== '--queue-arn').join(' ')}`);
    console.log('Waiting for it to finish...');

    let lastStatus = '';
    let exitCode: number | undefined;
    let stoppedReason: string | undefined;
    const deadline = Date.now() + 15 * 60_000;
    for (;;) {
      const { tasks } = await ecs.send(new DescribeTasksCommand({ cluster: outputs.ClusterName, tasks: [taskArn] }));
      const t = tasks?.[0];
      if (t?.lastStatus && t.lastStatus !== lastStatus) {
        lastStatus = t.lastStatus;
        console.log(`  ${new Date().toISOString().slice(11, 19)} ${lastStatus}`);
      }
      if (t?.lastStatus === 'STOPPED') {
        exitCode = t.containers?.find((c) => c.name === CONTAINER)?.exitCode;
        stoppedReason = t.stoppedReason;
        break;
      }
      if (Date.now() > deadline) throw new Error('Task still running after 15 minutes; check it in the ECS console.');
      await sleep(5_000);
    }

    // Log delivery trails the task by a few seconds.
    await sleep(5_000);
    const stream = `dead-letters/${CONTAINER}/${taskId}`;
    const events = await logs.send(new GetLogEventsCommand({ logGroupName: outputs.LogGroupName, logStreamName: stream, startFromHead: true }))
      .then((r) => r.events ?? [])
      .catch(() => []);
    console.log(events.length ? `\nTask log (${stream}):` : `\nNo log lines found in ${stream}.`);
    for (const e of events) console.log(`  ${format(e.message ?? '')}`);

    console.log(`\nTask exited with code ${exitCode ?? '?'}${stoppedReason ? ` (${stoppedReason})` : ''}.`);
    if (exitCode !== 0) process.exitCode = 1;
  } finally {
    ecs.destroy();
    logs.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
  process.exitCode = 1;
});
