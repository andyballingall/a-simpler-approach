import { DynamoDBClient, GetItemCommand } from "@aws-sdk/client-dynamodb";
import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";

const BASE_URL = "http://localhost:3000";
const ENDPOINT = process.env.AWS_ENDPOINT || "http://localhost:4566";
const REGION = "us-east-1";
const config = {
  region: REGION,
  endpoint: ENDPOINT,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
};
const SQS_QUEUE_URL = `${ENDPOINT}/000000000000/debug-queue`;

const sqsClient = new SQSClient(config);
const ddbClient = new DynamoDBClient(config);

async function run() {
  console.log("Starting integration test...");

  // 1. PUT Item
  const id = `test-${Date.now()}`;
  console.log(`PUT item ${id}...`);

  const res = await fetch(`${BASE_URL}/entity-x/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      id,
      jsonSchemaId: "https://json-schema.myorg.com/entity_x_1-0-0.schema.json",
      transactionId: "123e4567-e89b-12d3-a456-426614174000",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      createdBy: "test-runner",
      traceId: "0af7651916cd43dd8448eb211c80319c",
      spanId: "b7ad6b7169203331",
      propA: "test-val",
      propB: { propB1: "b1", propB2: [1] },
    }),
  });

  if (!res.ok) {
    const txt = await res.text();
    console.error(`PUT Failed: ${res.status} ${txt}`);
    process.exit(1);
  }
  console.log("PUT OK");

  // 2. Verify DDB
  console.log("Verifying DynamoDB...");
  const item = await ddbClient.send(
    new GetItemCommand({
      TableName: "entity-x",
      Key: { id: { S: id } },
    }),
  );
  if (!item.Item) {
    console.error("Item not found in DDB");
    process.exit(1);
  }
  console.log("DDB OK");

  // 3. Verify SQS (CDC)
  console.log("Polling SQS for CDC event...");
  let found = false;
  for (let i = 0; i < 20; i++) {
    const msgs = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: SQS_QUEUE_URL,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 2, // Long polling
      }),
    );

    if (msgs.Messages && msgs.Messages.length > 0) {
      const message = msgs.Messages[0];
      if (!message || !message.Body || !message.ReceiptHandle) continue;

      const body = JSON.parse(message.Body);
      console.log("Received CDC Event:", JSON.stringify(body, null, 2));
      await sqsClient.send(
        new DeleteMessageCommand({
          QueueUrl: SQS_QUEUE_URL,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
      found = true;
      break;
    }
    console.log("Waiting for event...");
  }

  if (!found) {
    console.error("CDC Event not received in time");
    process.exit(1);
  }
  console.log("Integration Test Passed!");
}

run();
