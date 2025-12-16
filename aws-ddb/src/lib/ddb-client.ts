import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const region = process.env.AWS_REGION || "us-east-1";
const endpoint = process.env.DYNAMODB_ENDPOINT;

export const ddbClient = new DynamoDBClient({
  region,
  endpoint,
  credentials: {
    accessKeyId: "test",
    secretAccessKey: "test",
  },
});

export const docClient = DynamoDBDocumentClient.from(ddbClient);
