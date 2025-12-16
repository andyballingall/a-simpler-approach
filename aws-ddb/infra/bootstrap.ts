import { DynamoDBClient, CreateTableCommand, DescribeTableCommand } from "@aws-sdk/client-dynamodb";
import { EventBridgeClient, CreateEventBusCommand, PutRuleCommand, PutTargetsCommand, DescribeEventBusCommand } from "@aws-sdk/client-eventbridge";
import { PipesClient, CreatePipeCommand } from "@aws-sdk/client-pipes";
import { IAMClient, CreateRoleCommand, GetRoleCommand } from "@aws-sdk/client-iam";
import { SQSClient, CreateQueueCommand, GetQueueUrlCommand, GetQueueAttributesCommand } from "@aws-sdk/client-sqs";

const ENDPOINT = process.env.AWS_ENDPOINT || "http://localhost:4566";
const REGION = "us-east-1";
const config = { region: REGION, endpoint: ENDPOINT, credentials: { accessKeyId: "test", secretAccessKey: "test" } };

async function main() {
    console.log("Bootstrapping LocalStack...");

    const ddb = new DynamoDBClient(config);
    const eb = new EventBridgeClient(config);
    const pipes = new PipesClient(config);
    const iam = new IAMClient(config);
    const sqs = new SQSClient(config);

    // 1. Create Table
    try {
        await ddb.send(new CreateTableCommand({
            TableName: "entity-x",
            KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
            AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
            ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
            StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" }
        }));
        console.log("Table created");
    } catch (e: any) {
        if (e.name !== "ResourceInUseException") throw e;
        console.log("Table exists");
    }

    const tableDesc = await ddb.send(new DescribeTableCommand({ TableName: "entity-x" }));
    const streamArn = tableDesc.Table?.LatestStreamArn;
    if (!streamArn) throw new Error("No Stream ARN");
    console.log("Stream ARN:", streamArn);

    // 2. Create Event Bus
    try {
        await eb.send(new CreateEventBusCommand({ Name: "cdc-bus" }));
        console.log("Bus created");
    } catch (e: any) {
        if (e.name !== "ResourceAlreadyExistsException") throw e;
        console.log("Bus exists");
    }

    // 3. Create IAM Role for Pipe
    let roleArn = "";
    try {
        const role = await iam.send(new CreateRoleCommand({
            RoleName: "pipe-role",
            AssumeRolePolicyDocument: JSON.stringify({
                Version: "2012-10-17",
                Statement: [{ Effect: "Allow", Principal: { Service: "pipes.amazonaws.com" }, Action: "sts:AssumeRole" }]
            })
        }));
        roleArn = role.Role?.Arn!;
        console.log("Role created");
    } catch (e: any) {
        if (e.name !== "EntityAlreadyExistsException") throw e;
        const r = await iam.send(new GetRoleCommand({ RoleName: "pipe-role" }));
        roleArn = r.Role?.Arn!;
        console.log("Role exists");
    }

    // 4. Create Queue (Target)
    try {
        await sqs.send(new CreateQueueCommand({ QueueName: "debug-queue" }));
    } catch (e: any) { if (e.name !== "QueueAlreadyExists") throw e; }

    const qUrl = (await sqs.send(new GetQueueUrlCommand({ QueueName: "debug-queue" }))).QueueUrl!;
    const qAttr = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: qUrl, AttributeNames: ["QueueArn"] }));
    const qArn = qAttr.Attributes?.QueueArn!;
    console.log("Queue ARN:", qArn);

    // 5. Rule on Bus -> Queue
    try {
        await eb.send(new PutRuleCommand({ Name: "catch-all", EventBusName: "cdc-bus", EventPattern: JSON.stringify({ source: [{ prefix: "" }] }) }));
        await eb.send(new PutTargetsCommand({ Rule: "catch-all", EventBusName: "cdc-bus", Targets: [{ Id: "1", Arn: qArn }] }));
        console.log("Rule created");
    } catch (e) { console.log("Rule setup error", e); }

    const busArn = (await eb.send(new DescribeEventBusCommand({ Name: "cdc-bus" }))).Arn;

    // 6. Create Pipe (SKIPPED in Community Edition - using infra/local-pipe.ts)
    /*
    try {
        await pipes.send(new CreatePipeCommand({
            Name: "ddb-to-eb",
            RoleArn: roleArn,
            Source: streamArn,
            Target: busArn,
            SourceParameters: { DynamoDBStreamParameters: { StartingPosition: "TRIM_HORIZON" } }
        }));
        console.log("Pipe created");
    } catch (e: any) {
        if (e.name !== "ConflictException") throw e;
        console.log("Pipe exists");
    }
    */
    console.log("Skipping Pipe creation (requires Pro). Use 'bun run local:pipe' instead.");
}

main();
