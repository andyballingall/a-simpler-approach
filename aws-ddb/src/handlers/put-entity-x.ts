import { Hono } from "hono";
import { docClient } from "../lib/ddb-client";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import schema from "../schemas/entity_x_1-0-0.schema.json";
import baseSchema from "../schemas/entity_base_1-0-0.schema.json";

const app = new Hono();
const ajv = new Ajv();
addFormats(ajv);
ajv.addSchema(baseSchema);
const validate = ajv.compile(schema);

app.put("/entity-x/:id", async (c) => {
    const id = c.req.param("id");
    let body;
    try {
        body = await c.req.json();
    } catch (e) {
        return c.json({ error: "Invalid JSON" }, 400);
    }

    // Ensure ID matches path
    if (body.id && body.id !== id) {
        return c.json({ error: "ID mismatch" }, 400);
    }
    body.id = id;

    const valid = validate(body);
    if (!valid) {
        return c.json({ error: validate.errors }, 400);
    }

    try {
        const command = new PutCommand({
            TableName: "entity-x",
            Item: body,
        });
        await docClient.send(command);
        return c.json({ message: "Created", item: body }, 201);
    } catch (error) {
        console.error(error);
        return c.json({ error: "Internal Server Error" }, 500);
    }
});

export default app;
