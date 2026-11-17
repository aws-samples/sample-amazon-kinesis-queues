import { type IVpc, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import type { Construct } from 'constructs';

/**
 * The VPC a scenario's tasks run in.
 *
 * By default, a new VPC with public subnets in two Availability Zones and no NAT
 * gateway. With -c vpcId=vpc-..., an existing VPC instead, for example the
 * Quickstart's: an account has 5 VPCs per Region by default, and each scenario
 * would otherwise use one. CDK looks the VPC up at synth time and caches it in
 * cdk.context.json. The tasks get a public IP to reach AWS APIs, so the VPC needs
 * public subnets.
 */
export function scenarioVpc(scope: Construct, subnetName: string): IVpc {
  const value: unknown = scope.node.tryGetContext('vpcId');
  if (value === undefined) {
    return new Vpc(scope, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [{ name: subnetName, subnetType: SubnetType.PUBLIC }],
    });
  }
  const vpcId = String(value).trim();
  if (!/^vpc-[0-9a-f]{8,17}$/.test(vpcId)) throw new Error(`vpcId must look like vpc-0123456789abcdef0, got "${vpcId}".`);
  const vpc = Vpc.fromLookup(scope, 'Vpc', { vpcId });
  if (vpc.publicSubnets.length === 0) {
    throw new Error(`VPC ${vpcId} has no public subnets. The tasks need them, because there's no NAT gateway.`);
  }
  return vpc;
}
