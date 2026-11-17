import { RemovalPolicy, type Stack } from 'aws-cdk-lib';
import { type IVpc, SecurityGroup, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import { Cluster, FargateService, type FargateTaskDefinition } from 'aws-cdk-lib/aws-ecs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';

/** Where the consumers run. Both migrations use the same layout. */
export interface ConsumerNetwork {
  vpc: IVpc;
  securityGroup: SecurityGroup;
  cluster: Cluster;
  logGroup: LogGroup;
  /** A Fargate service in public subnets with a public IP, so it reaches AWS APIs without a NAT gateway. */
  service(id: string, task: FargateTaskDefinition, desiredCount: number): FargateService;
}

export function consumerNetwork(stack: Stack, vpcId: string | undefined): ConsumerNetwork {
  // An existing VPC (-c vpcId=...) helps accounts at their VPC quota; for example the
  // Quickstart's. CDK looks it up at synth time and caches it in cdk.context.json.
  const vpc: IVpc = vpcId
    ? Vpc.fromLookup(stack, 'Vpc', { vpcId })
    : new Vpc(stack, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [{ name: 'consumers', subnetType: SubnetType.PUBLIC }],
    });
  if (vpcId && vpc.publicSubnets.length === 0) {
    throw new Error(`VPC ${vpcId} has no public subnets. The tasks need them, because there's no NAT gateway.`);
  }
  const securityGroup = new SecurityGroup(stack, 'ConsumerSecurityGroup', {
    vpc,
    description: 'Outbound only: the consumers call AWS APIs and accept no connections.',
    allowAllOutbound: true,
  });
  const cluster = new Cluster(stack, 'Cluster', { vpc });
  const logGroup = new LogGroup(stack, 'ConsumerLogs', {
    retention: RetentionDays.ONE_WEEK,
    removalPolicy: RemovalPolicy.DESTROY,
  });
  return {
    vpc,
    securityGroup,
    cluster,
    logGroup,
    service: (id, task, desiredCount) => new FargateService(stack, id, {
      cluster,
      taskDefinition: task,
      desiredCount,
      minHealthyPercent: 100,
      maxHealthyPercent: 200,
      assignPublicIp: true,
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      securityGroups: [securityGroup],
      circuitBreaker: { rollback: true },
    }),
  };
}
