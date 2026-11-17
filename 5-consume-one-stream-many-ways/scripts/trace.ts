// Follows one order through every consumer of the stream: fulfillment, notifications
// and fraud signals each log it independently.
import { parseArgs } from 'node:util';
import { CloudWatchLogsClient, FilterLogEventsCommand, type FilteredLogEvent } from '@aws-sdk/client-cloudwatch-logs';
import { clientConfig, commonOptions, readOutputs } from './outputs';

const USAGE = `Usage: npm run fanout:trace -- --order-id ID [options]

  --order-id ID       Order to follow (the producer prints orderIds it marks; any orderId from a log works)
  --minutes N         How far back to search (default: 60)
  --stack-name NAME   Stack in 5-consume-one-stream-many-ways/cdk-outputs.json (default: the only one)
  --region REGION     Default: us-east-2
  --profile NAME      AWS profile; otherwise the standard credential chain`;

/** Turns a JSON log line into a short line; the KCL consumer already logs key=value text. */
function summarise(message: string): string {
  try {
    const { time, task, level, ...rest } = JSON.parse(message) as Record<string, unknown>;
    void time; void task; void level;
    const { event, ...fields } = rest;
    return `${String(event)} ${Object.entries(fields).map(([k, v]) => `${k}=${String(v)}`).join(' ')}`;
  } catch {
    return message.trim();
  }
}

async function main(): Promise<void> {
  const { values: args } = parseArgs({
    options: { ...commonOptions, 'order-id': { type: 'string' }, minutes: { type: 'string', default: '60' } },
  });
  if (args.help) {
    console.log(USAGE);
    return;
  }
  const orderId = args['order-id'];
  if (!orderId || !/^[A-Za-z0-9-]{1,64}$/.test(orderId)) throw new Error('--order-id is required: letters, numbers and hyphens only');
  const minutes = Number(args.minutes);
  if (!Number.isInteger(minutes) || minutes < 1) throw new Error('--minutes must be a whole number of at least 1');
  const outputs = readOutputs(args['stack-name']);
  const logs = new CloudWatchLogsClient(clientConfig(args.region, args.profile));

  const events: FilteredLogEvent[] = [];
  try {
    let nextToken: string | undefined;
    do {
      const page = await logs.send(new FilterLogEventsCommand({
        logGroupName: outputs.LogGroupName,
        startTime: Date.now() - minutes * 60_000,
        // A quoted term matches the exact text, in JSON and plain-text lines alike.
        filterPattern: `"${orderId}"`,
        nextToken,
      }));
      events.push(...(page.events ?? []));
      nextToken = page.nextToken;
    } while (nextToken && events.length < 200);
  } finally {
    logs.destroy();
  }

  if (events.length === 0) {
    console.log(`No consumer has logged ${orderId} in the last ${minutes} minutes.`);
    return;
  }
  const consumers = new Set<string>();
  console.log(`Order ${orderId}, as each consumer saw it:\n`);
  for (const e of events.sort((a, b) => (a.timestamp ?? 0) - (b.timestamp ?? 0))) {
    const consumer = (e.logStreamName ?? '?').split('/')[0];
    consumers.add(consumer);
    console.log(`  ${new Date(e.timestamp ?? 0).toISOString().slice(11, 23)}  ${consumer.padEnd(14)} ${summarise(e.message ?? '')}`);
  }
  console.log(`\n${consumers.size} consumer(s): ${[...consumers].join(', ')}. The record was written to the stream once.`);
}

main().catch((error) => {
  console.error(error instanceof Error ? `${error.name}: ${error.message}` : error);
  process.exitCode = 1;
});
