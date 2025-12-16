import { Hono } from "hono";
import putEntityX from "./handlers/put-entity-x";

const app = new Hono();

app.get("/health", (c) => c.text("OK"));
app.route("/", putEntityX);

export default {
    port: 3000,
    fetch: app.fetch,
};
