import { PutCommand } from "@aws-sdk/lib-dynamodb";
import type { NativeAttributeValue } from "@aws-sdk/util-dynamodb";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { Hono } from "hono";
import { docClient } from "../lib/ddb-client";
import baseSchema from "../schemas/entity_base_1-0-0.schema.json";
import schema from "../schemas/entity_x_1-0-0.schema.json";

const app = new Hono();
const ajv = new Ajv();
addFormats(ajv);
ajv.addSchema(baseSchema);
const validate = ajv.compile(schema);

app.put("/entity-x/:id", async (c) => {
  const id = c.req.param("id");
  let body: unknown;
  try {
    body = await c.req.json();
  } catch (_e) {
    return c.json({ error: "Invalid JSON" }, 400);
  }

  // Ensure ID matches path
  if (
    typeof body === "object" &&
    body !== null &&
    "id" in body &&
    body.id !== id
  ) {
    return c.json({ error: "ID mismatch" }, 400);
  }
  (body as Record<string, unknown>).id = id;

  const valid = validate(body);
  if (!valid) {
    return c.json({ error: validate.errors }, 400);
  }

  try {
    const command = new PutCommand({
      TableName: "entity-x",
      Item: body as Record<string, NativeAttributeValue>,
    });
    await docClient.send(command);
    return c.json({ message: "Created", item: body }, 201);
  } catch (error) {
    console.error(error);
    return c.json({ error: "Internal Server Error" }, 500);
  }
});

export default app;
