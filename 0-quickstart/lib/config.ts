import { Construct } from 'constructs';

export interface DeploymentConfig {
  accountId: string;
  region: string;
  stackName: string;
  streamName?: string;
  retentionHours: number;
}

export function readDeploymentConfig(scope: Construct): DeploymentConfig {
  function context(name: string, required = false): string | undefined {
    const value: unknown = scope.node.tryGetContext(name);
    if (value === undefined) {
      if (required) {
        throw new Error(`Missing context "${name}". Supply -c ${name}=VALUE with your CDK command.`);
      }
      return undefined;
    }
    if (typeof value !== 'string' || value.trim() === '') {
      throw new Error(`Context "${name}" must be a non-empty string.`);
    }
    return value.trim();
  }

  const accountId = context('accountId', true)!;
  const region = context('region', true)!;
  const stackName = context('stackName', true)!;
  const streamName = context('streamName');
  const retentionText = context('retentionHours') ?? '24';

  if (!/^\d{12}$/.test(accountId)) {
    throw new Error('accountId must be a 12-digit AWS account ID.');
  }
  if (!/^[a-z]{2}(?:-[a-z0-9]+)+-\d+$/.test(region)) {
    throw new Error('region must be an AWS Region code, for example eu-west-1.');
  }
  if (!/^[A-Za-z][A-Za-z0-9-]{0,127}$/.test(stackName)) {
    throw new Error('stackName must start with a letter and contain only letters, numbers or hyphens (128 characters maximum).');
  }
  if (streamName !== undefined && !/^[A-Za-z0-9_.-]{1,128}$/.test(streamName)) {
    throw new Error('streamName must contain 1–128 letters, numbers, underscores, periods or hyphens.');
  }
  if (!/^\d+$/.test(retentionText)) {
    throw new Error('retentionHours must be a whole number between 24 and 8760.');
  }
  const retentionHours = Number(retentionText);
  if (retentionHours < 24 || retentionHours > 8760) {
    throw new Error('retentionHours must be a whole number between 24 and 8760.');
  }

  return { accountId, region, stackName, streamName, retentionHours };
}
