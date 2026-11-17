# Quickstart: a Kinesis data stream with a KCL consumer

A small TypeScript AWS CDK app that creates **one Amazon Kinesis data stream** and
**one Java consumer running as an ECS service on AWS Fargate**, using the
**Kinesis Client Library (KCL) 3.5.4**. Order events come from the same producer
the other scenarios use, `npm run orders:produce`. Every queue scenario builds on
this stream.

You choose the account, Region and stack name when you run CDK. No account or
Region is embedded in the application.

The stream uses **on-demand capacity**, **AWS-managed KMS encryption**
(`alias/aws/kinesis`) and **24-hour retention** by default. CloudFormation
generates a unique stream name unless you provide one. The stack outputs the
stream name and ARN for use by producers and consumers.

## 1. Prerequisites

- Node.js 22 or later and npm. If you use nvm, run `nvm install` and `nvm use`.
- AWS CLI v2 and an authenticated AWS profile for your deployment account.
  For an IAM Identity Center profile, run `aws sso login --profile YOUR_PROFILE`.
- Docker with its engine running. CDK builds the consumer image with Java and
  Maven inside Docker; you do not need Java or Maven installed locally.
- Permission to bootstrap/deploy the CDK stack, including its Kinesis, ECS,
  networking, DynamoDB, logs and IAM resources. Sending records also requires
  `kinesis:PutRecord` permission on the deployed stream.

Install dependencies once from the repository root, then run every command in this
README from the `0-quickstart` folder:

```bash
npm ci
npm run build
cd 0-quickstart
```

The CDK CLI is installed locally with the project; no global CDK install is needed.
The deploy writes `0-quickstart/cdk-outputs.json`, which the queue scenarios read
to find the stream.

## 2. Choose your deployment settings

Use Bash or Zsh. Replace `YOUR_PROFILE` and `YOUR_ACCOUNT_ID` with your own values.
The scenarios use `us-east-2` and the stack name `kinesis-queues-demo`. If you use
another Region, set it here and pass `--region` to the scripts in the other
scenarios, or keep `AWS_REGION` exported:

```bash
export AWS_PROFILE="YOUR_PROFILE"
export AWS_ACCOUNT_ID="YOUR_ACCOUNT_ID"
export AWS_REGION="us-east-2"
export STACK_NAME="kinesis-queues-demo"

# Reuse these arguments for every command targeting this stack.
CDK_ARGS=(
  --profile "$AWS_PROFILE"
  -c accountId="$AWS_ACCOUNT_ID"
  -c region="$AWS_REGION"
  -c stackName="$STACK_NAME"
  -c retentionHours=24
)
```

To run more than one copy in the same account and Region, give each a different
`STACK_NAME`. Reusing a stack name updates the existing stack. The other scenarios
default to `kinesis-queues-demo`; with another name, pass it to them as their
READMEs describe.

Check that the profile identifies your intended account:

```bash
aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text
```

The result should match `AWS_ACCOUNT_ID`. Account IDs and Region settings select
the deployment destination; `--profile` supplies credentials. They do not grant
access to another account.

Choose a Region where queues in Kinesis Data Streams are available.

| CDK context | Required | Meaning |
| --- | --- | --- |
| `accountId` | Yes | Target 12-digit AWS account ID. |
| `region` | Yes | Target AWS Region, for example `us-east-2`. |
| `stackName` | Yes | Stack name, for example `kinesis-queues-demo`. Starts with a letter and uses letters, numbers and hyphens. Maximum 128 characters. |
| `streamName` | No | Optional stream name, unique in the account and Region. Otherwise CloudFormation generates one. |
| `retentionHours` | No | Whole number from 24 to 8760; defaults to 24. Longer retention changes cost. |

These are **CDK context arguments** (`-c`), read when `cdk deploy` synthesizes the
app. They are not CloudFormation `--parameters` and do not cause an interactive
prompt. Missing required arguments produce a clear error.

## 3. Bootstrap and deploy

Bootstrap is normally needed once per account and Region. It creates the CDK
deployment roles and supporting resources. Skip it if the account and Region are
already bootstrapped.

```bash
npx cdk bootstrap "aws://$AWS_ACCOUNT_ID/$AWS_REGION" "${CDK_ARGS[@]}"

# Generate and inspect the proposed infrastructure.
npx cdk synth "$STACK_NAME" "${CDK_ARGS[@]}"
npx cdk diff "$STACK_NAME" "${CDK_ARGS[@]}"

# Deploy and write the stream name and ARN to a local JSON file.
npx cdk deploy "$STACK_NAME" "${CDK_ARGS[@]}" --outputs-file cdk-outputs.json
```

To supply your account and Region directly when running `cdk deploy`, use this
command after replacing the placeholders:

```bash
npx cdk deploy kinesis-queues-demo \
  --profile YOUR_PROFILE \
  -c accountId=YOUR_ACCOUNT_ID \
  -c region=us-east-2 \
  -c stackName=kinesis-queues-demo \
  -c retentionHours=48 \
  --outputs-file cdk-outputs.json
```

Add `-c streamName=YOUR_UNIQUE_STREAM_NAME` if you need a predictable stream name.
Use the same settings in subsequent commands. Changing the name of an existing
stream requires replacement, so avoid changing it after you have stored records.

### What happens during deployment?

1. The CDK app validates your supplied settings.
2. CDK generates a CloudFormation template for the specified account and Region.
3. CDK builds the consumer Docker image and uploads it to the bootstrap ECR repository.
4. CloudFormation creates the stream, KCL checkpoint table and index, networking,
   task roles, logs and ECS service.
5. ECS starts one consumer task on Fargate. It immediately begins reading retained
   records once KCL has acquired the shard leases.
6. CDK prints the stream, consumer and log names and writes them to `cdk-outputs.json`.

## 4. Place orders

Run the order producer from the repository root. It writes `OrderPlaced` events to
the stream in `0-quickstart/cdk-outputs.json`, with the customer ID as the partition
key, so each customer's orders stay in order within a shard:

```bash
cd ..
npm run orders:produce -- --count 20 --rate 2
```

Each record is one order:

```json
{
  "eventType": "OrderPlaced",
  "orderId": "o-8c1220ea-3c8e",
  "customerId": "c-0003",
  "items": [{ "sku": "SKU-CAP", "qty": 1, "price": 14.95 }, { "sku": "SKU-CAP", "qty": 2, "price": 14.95 }],
  "total": 44.85,
  "currency": "EUR",
  "createdAt": "2026-11-17T09:39:30.784Z"
}
```

Run `npm run orders:produce -- --help` for the other options. The queue scenarios
use them to send duplicates, malformed events and orders that fail.

## 5. Watch the Fargate consumer

**The consumer starts automatically during `cdk deploy`.** ECS maintains one
running Fargate task. You do not need to run a consumer on your laptop.

Open another terminal in the `0-quickstart` folder and set the same `AWS_PROFILE`,
`AWS_REGION` and `STACK_NAME` values from section 2. Read the log-group output
and follow the consumer's logs:

```bash
LOG_GROUP=$(node -p 'require("./cdk-outputs.json")[process.env.STACK_NAME].ConsumerLogGroupName')

aws logs tail "$LOG_GROUP" \
  --follow \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

While the producer sends orders, you will see lines such as:

```text
RECORD shard=shardId-000000000002 sequence=4968014258197712661122925965207998025598751997636588… data={"eventType":"OrderPlaced","orderId":"o-8c1220ea-3c8e","customerId":"c-0003",…}
```

To check the ECS service:

```bash
CLUSTER_NAME=$(node -p 'require("./cdk-outputs.json")[process.env.STACK_NAME].ConsumerClusterName')
SERVICE_NAME=$(node -p 'require("./cdk-outputs.json")[process.env.STACK_NAME].ConsumerServiceName')

aws ecs describe-services \
  --cluster "$CLUSTER_NAME" \
  --services "$SERVICE_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --query 'services[0].{Desired:desiredCount,Running:runningCount,Pending:pendingCount}'
```

Pressing Ctrl+C in the log terminal stops following the logs. The consumer keeps
running in Fargate until the service is stopped or the stack is destroyed.

### What happens to each record?

1. The order producer writes order events to the stream.
2. KCL in the Fargate task discovers the stream's shards and reads them using polling.
3. The consumer prints each record to its CloudWatch log stream.
4. After a batch is printed successfully, KCL checkpoints its position in DynamoDB.
5. If ECS replaces or restarts the task, the replacement resumes from that checkpoint.

One task can read multiple shards. The consumer reads the **Kinesis Data Stream**
directly. No Kinesis Queue or enhanced fan-out subscription is created.

### Why DynamoDB is included

**KCL uses DynamoDB for its own progress and coordination data.** It records the
last successfully processed position in each shard and which worker owns that
shard. The event records remain in Kinesis.

CDK creates the table and its index together, before starting the ECS service.
It also supplies the table name as the KCL application name. You do not need to
create or configure this table manually, and `cdk destroy` removes it.

### Deployment details

- **One Fargate task:** 0.25 vCPU and 1 GiB memory, running Java 17 and KCL 3.5.4.
- **Task IAM role:** permits reading this stream, using its KCL metadata and
  publishing CloudWatch metrics. There are no static AWS credentials in the image.
- **Networking:** public subnets and an assigned public IP provide outbound access
  to AWS APIs. No inbound ports, load balancer or NAT gateway are configured.
- **Logs:** CloudWatch Logs with one-week retention.
- **Initial position:** a new application starts at the oldest retained records
  (`TRIM_HORIZON`); saved checkpoints take precedence on subsequent task starts.
- **Shutdown:** ECS gives the task up to 120 seconds to shut down and checkpoint.

KCL 3.5.4 is pinned in `consumer/pom.xml` so builds use the same version.

Delivery is **at least once**: a retry, task replacement or lease change can cause
records to be processed again. Use the `orderId` for deduplication if you add
business processing, as scenario 2 shows. This demo checkpoints after successful printing;
when adding real processing, checkpoint only after that processing succeeds.

An unrecovered processing or checkpoint error exits the consumer with a nonzero
status. ECS replaces the failed task, which resumes using the saved checkpoint.

## 6. Clean up

Stop the order producer with **Ctrl+C** if it's still running. The Fargate service, stream, public IP,
DynamoDB operations and CloudWatch usage incur AWS charges while deployed.

**Destroying the stack deletes the stream and its
records, the consumer's checkpoint table and its log group.** Run this in your
deployment terminal, where `CDK_ARGS` is still defined, or repeat section 2 first:

```bash
npx cdk destroy "$STACK_NAME" "${CDK_ARGS[@]}"
```

CDK's bootstrap resources and published image assets are separate from this
stack and remain after this command.

## Project files

- `bin/app.ts`: reads deployment settings and creates the stack.
- `lib/config.ts`: validates the deployment settings.
- `lib/kinesis-data-stream-stack.ts`: defines the stream and adds the consumer.
- `lib/consumer.ts`: defines the Fargate service, checkpoint table, IAM roles, networking and logs.
- `../shared/src/produce-orders.ts`: `npm run orders:produce`, the order producer every scenario uses.
- `consumer/pom.xml`: pins KCL and builds the Java application.
- `consumer/Dockerfile`: builds the application and packages it for Fargate.
- `consumer/entrypoint.sh`: starts the consumer using the settings supplied by CDK.
- `consumer/src/main/java/com/example/kinesis/ConsumerApp.java`: starts the KCL scheduler with polling and handles shutdown.
- `consumer/src/main/java/com/example/kinesis/PrintingRecordProcessor.java`: prints records and checkpoints successful batches.
- `consumer/src/main/java/com/example/kinesis/ConsumerOptions.java`: validates runtime settings.

## AWS references

- [CDK context arguments](https://docs.aws.amazon.com/cdk/v2/guide/context.html)
- [CDK environments](https://docs.aws.amazon.com/cdk/v2/guide/environments.html)
- [CDK bootstrapping](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html)
- [CDK Kinesis stream construct](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_kinesis.Stream.html)
- [Writing records to Kinesis](https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html)
- [Developing Java consumers with KCL](https://docs.aws.amazon.com/streams/latest/dev/develop-kcl-consumers-java.html)
- [KCL 3.5.4 release notes](https://github.com/awslabs/amazon-kinesis-client/releases/tag/v3.5.4)
- [KCL IAM permissions](https://docs.aws.amazon.com/streams/latest/dev/kcl-iam-permissions.html)
- [KCL DynamoDB metadata tables](https://docs.aws.amazon.com/streams/latest/dev/kcl-dynamoDB.html)

- [ECS task IAM roles](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html)
- [Fargate networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/networking-outbound.html)
