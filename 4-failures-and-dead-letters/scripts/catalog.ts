// Shows or changes the product catalog's discontinued SKUs. This is how an operator
// "fixes the cause" of poison orders: the running workers read the list at run time
// (cached for 10 s), so nothing is redeployed or restarted.
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { parseArgs } from 'node:util';
import { GetParameterCommand, PutParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';

const USAGE = `Usage: npm run orders:catalog -- [show | discontinue SKU | restock SKU] [options]

  show               Print the discontinued SKUs (default)
  discontinue SKU    Add SKU: orders containing it fail and, after repeated receives, become dead-letter records
  restock SKU        Remove SKU: orders containing it are fulfilled again
  --stack-name NAME  Stack in 4-failures-and-dead-letters/cdk-outputs.json (default: the only one)
  --region REGION    Default: us-east-2
  --profile NAME     AWS profile; needs ssm:GetParameter and ssm:PutParameter on the catalog parameter`;

const OUTPUTS = path.resolve(__dirname, '..', 'cdk-outputs.json');

function parameterName(stackName: string | undefined): string {
  let all: Record<string, { CatalogParameterName?: string }>;
  try {
    all = JSON.parse(readFileSync(OUTPUTS, 'utf8'));
  } catch {
    throw new Error(`Deploy the stack with --outputs-file cdk-outputs.json first (looked for ${OUTPUTS}).`);
  }
  const names = stackName ? [stackName] : Object.keys(all);
  const name = names.length === 1 ? all[names[0]]?.CatalogParameterName : undefined;
  if (!name) throw new Error('No CatalogParameterName in the stack outputs; pass --stack-name or redeploy the stack.');
  return name;
}

function parse(value: string | undefined): string[] {
  const text = value?.trim() ?? '';
  return text.toLowerCase() === 'none' ? [] : text.split(',').map((s) => s.trim()).filter(Boolean);
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
  const [command = 'show', sku] = positionals;
  if (!['show', 'discontinue', 'restock'].includes(command) || positionals.length > 2) throw new Error(USAGE);
  if (command !== 'show' && !/^[A-Za-z0-9_-]{1,64}$/.test(sku ?? '')) throw new Error(`${command} needs a SKU, for example SKU-DISCONTINUED.`);

  const Name = parameterName(args['stack-name']);
  const ssm = new SSMClient({ region: args.region, credentials: fromNodeProviderChain(args.profile ? { profile: args.profile } : {}) });
  try {
    const current = parse((await ssm.send(new GetParameterCommand({ Name }))).Parameter?.Value);
    if (command === 'show') {
      console.log(`Discontinued SKUs: ${current.length ? current.join(', ') : 'none'}`);
      return;
    }
    const next = command === 'discontinue' ? [...new Set([...current, sku!])] : current.filter((s) => s !== sku);
    await ssm.send(new PutParameterCommand({ Name, Value: next.length ? next.join(',') : 'none', Overwrite: true }));
    console.log(`Discontinued SKUs: ${current.join(', ') || 'none'} -> ${next.join(', ') || 'none'}`);
    console.log('Running workers pick this up within about 10 seconds. No redeploy needed.');
  } finally {
    ssm.destroy();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
