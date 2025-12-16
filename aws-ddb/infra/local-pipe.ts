import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import { DescribeTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DescribeStreamCommand,
  DynamoDBStreamsClient,
  GetRecordsCommand,
  GetShardIteratorCommand,
} from "@aws-sdk/client-dynamodb-streams";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const REGION = "us-east-1";
const ENDPOINT = process.env.AWS_ENDPOINT || "http://localhost:4566";
const config = {
  region: REGION,
  endpoint: ENDPOINT,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
};

const ddb = new DynamoDBClient(config);
const streams = new DynamoDBStreamsClient(config);
const eb = new EventBridgeClient(config);

async function main() {
  console.log("Starting Local Pipe Emulator...");

  // 1. Get Stream ARN
  const table = await ddb.send(
    new DescribeTableCommand({ TableName: "entity-x" }),
  );
  const streamArn = table.Table?.LatestStreamArn;
  if (!streamArn) {
    console.error("No stream found for table entity-x");
    process.exit(1);
  }
  console.log("Polling Stream:", streamArn);

  // 2. Get Shards
  const streamDesc = await streams.send(
    new DescribeStreamCommand({ StreamArn: streamArn }),
  );
  const shards = streamDesc.StreamDescription?.Shards || [];
  console.log(`Found ${shards.length} shards`);

  // Simple implementation: tail the first open shard
  // In production pipes handle all shards and checkpoints
  const openShard = shards.find(
    (s) => !s.SequenceNumberRange?.EndingSequenceNumber,
  );
  if (!openShard) {
    console.log("No open shards found");
    return;
  }

  let iterator = (
    await streams.send(
      new GetShardIteratorCommand({
        StreamArn: streamArn,
        ShardId: openShard.ShardId,
        ShardIteratorType: "LATEST",
      }),
    )
  ).ShardIterator;

  while (true) {
    if (!iterator) break;

    const records = await streams.send(
      new GetRecordsCommand({ ShardIterator: iterator }),
    );
    iterator = records.NextShardIterator;

    if (records.Records && records.Records.length > 0) {
      console.log(`Processing ${records.Records.length} records...`);
      const entries = records.Records.map((r) => {
        // Convert DDB Stream record to unmarshalled JSON if needed,
        // but EventBridge Pipe typically puts the whole record or a subset.
        // We will match the default Pipe behavior: sending the unmarshalled image data or the whole record.
        // For ddb-to-eventbridge, typically the detail IS the record.

        // Simulating the CDC payload structure
        // Verify if r.dynamodb.NewImage/OldImage needs casting.
        // @aws-sdk/client-dynamodb-streams types NewImage as Record<string, AttributeValue> | undefined
        // However, we imported AttributeValue from client-dynamodb.
        // Let's assume they are compatible or cast to the imported type.
        const payload = {
          curr: r.dynamodb?.NewImage
            ? unmarshall(
                r.dynamodb.NewImage as unknown as Record<
                  string,
                  AttributeValue
                >,
              )
            : null,
          prev: r.dynamodb?.OldImage
            ? unmarshall(
                r.dynamodb.OldImage as unknown as Record<
                  string,
                  AttributeValue
                >,
              )
            : null,
        };

        return {
          Source: "myorg.entity-x",
          DetailType: "Entity Change",
          Detail: JSON.stringify(payload),
          EventBusName: "cdc-bus",
        };
      });

      await eb.send(new PutEventsCommand({ Entries: entries }));
      console.log("Events sent to Bus");
    }

    await new Promise((r) => setTimeout(r, 1000)); // Poll every second
  }
}

main().catch(console.error);
