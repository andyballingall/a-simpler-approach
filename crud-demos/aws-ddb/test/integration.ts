import assert from "node:assert";
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

async function putEntity(body: Record<string, unknown>) {
  console.log(`PUT item ${body.id}...`);
  const res = await fetch(`${BASE_URL}/entity-x/${body.id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`PUT Failed: ${res.status} ${txt}`);
  }
  console.log("PUT OK");
}

async function waitForEvent() {
  console.log("Polling SQS for CDC event...");
  for (let i = 0; i < 20; i++) {
    const msgs = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: SQS_QUEUE_URL,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 2,
      }),
    );

    if (msgs.Messages && msgs.Messages.length > 0) {
      const message = msgs.Messages[0];
      if (!message || !message.Body || !message.ReceiptHandle) continue;

      const body = JSON.parse(message.Body);
      // Clean up ANY found message to effectively "consume" it
      await sqsClient.send(
        new DeleteMessageCommand({
          QueueUrl: SQS_QUEUE_URL,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );

      console.log("Received CDC Event");
      return body;
    }
  }
  throw new Error("CDC Event not received in time");
}

async function run() {
  console.log("Starting integration test...");

  const id = `test-${Date.now()}`;
  const baseItem = {
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
  };

  // 1. Initial Create
  console.log("\n--- Step 1: Create ---");
  await putEntity(baseItem);
  const event1 = await waitForEvent();

  assert.strictEqual(
    event1.detail.prev,
    null,
    "Expected prev to be null on create",
  );
  assert.deepStrictEqual(
    event1.detail.curr,
    baseItem,
    "Expected curr to match valid item",
  );
  console.log("Create Verified: prev=null, curr=item");

  // 2. Update
  console.log("\n--- Step 2: Update ---");
  const updatedItem = { ...baseItem, propA: "val-updated" };
  await putEntity(updatedItem);
  const event2 = await waitForEvent();

  assert.deepStrictEqual(
    event2.detail.prev,
    baseItem,
    "Expected prev to match original item on update",
  );
  assert.deepStrictEqual(
    event2.detail.curr,
    updatedItem,
    "Expected curr to match updated item",
  );
  console.log("Update Verified: prev=original, curr=updated");

  console.log("\nIntegration Test Passed!");
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
