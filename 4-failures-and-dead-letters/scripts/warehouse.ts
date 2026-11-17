// Shows or changes the simulated warehouse API's status. "down" simulates an outage
// of a downstream service: every order fails until it's back "up". The running
// workers read the status at run time (cached for 10 s), so nothing is redeployed.
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { parseArgs } from 'node:util';
import { GetParameterCommand, PutParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

const USAGE = `Usage: npm run orders:warehouse -- [show | down | up] [options]

  show               Print the warehouse status (default)
  down               Simulate an outage: every order fails and, after repeated receives, becomes a dead-letter record
  up                 End the outage: orders are fulfilled again
  --stack-name NAME  Stack in 4-failures-and-dead-letters/cdk-outputs.json (default: the only one)
  --region REGION    Default: us-east-2
  --profile NAME     AWS profile; needs ssm:GetParameter and ssm:PutParameter on the warehouse parameter`;

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');

function parameterName(stackName: string | undefined): string {
  let all: Record<string, { WarehouseParameterName?: string }>;
  try {
    all = JSON.parse(readFileSync(OUTPUTS, 'utf8'));
  } catch {
    throw new Error(`Deploy the stack with --outputs-file cdk-outputs.json first (looked for ${OUTPUTS}).`);
  }
  const names = stackName ? [stackName] : Object.keys(all);
  const name = names.length === 1 ? all[names[0]]?.WarehouseParameterName : undefined;
  if (!name) throw new Error('No WarehouseParameterName in the stack outputs; pass --stack-name or redeploy the stack.');
  return name;
}

async function main(): Promise<void> {
  const { values: args, positionals } = parseArgs({
    allowPositionals: true,
    options: {
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
  const [command = 'show'] = positionals;
  if (!['show', 'down', 'up'].includes(command) || positionals.length > 1) throw new Error(USAGE);

  const Name = parameterName(args['stack-name']);
  const ssm = new SSMClient({ region: args.region, credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}) });
  try {
    const current = (await ssm.send(new GetParameterCommand({ Name }))).Parameter?.Value?.trim() ?? 'up';
    if (command === 'show') {
      console.log(`Warehouse: ${current}`);
      return;
    }
    await ssm.send(new PutParameterCommand({ Name, Value: command, Overwrite: true }));
    console.log(`Warehouse: ${current} -> ${command}`);
    console.log('Running workers pick this up within about 10 seconds. No redeploy needed.');
  } finally {
    ssm.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
