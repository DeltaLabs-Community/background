import { Hono } from "hono";
import { PostgreSQLJobQueue, PostgreSQLJobStorage, QueueEvent } from "../../src";
import { serve } from "@hono/node-server";
import dotenv from "dotenv";
import { honoJobs } from "./honoJobs.middleware";
import { Pool } from "pg";
dotenv.config();
const app = new Hono();

const pool = new Pool({
    connectionString: process.env.TEST_POSTGRESQL_URL!
});

const storage = new PostgreSQLJobStorage(pool, {
    tableName: "jobs"
});
const queue = new PostgreSQLJobQueue(storage, {
    concurrency: 2,
    maxRetries: 3,
    name: "test-queue",
    processingInterval: 1000
});

queue.register("test-job", async (data) => {
    await new Promise(resolve => setTimeout(resolve, 3000));
    return data;
});

queue.addEventListener("completed", async (event: QueueEvent) => {
    console.log(event.data);
});

honoJobs(app, [queue]);

app.get("/job/:id", async (c) => {
    const jobId = c.req.param("id");
    const job = await storage.getJob(jobId);
    return c.json(job);
});

app.post("/job", async (c) => {
    const data = await c.req.json();
    const queues = c.get("queues");
    const testQueue = queues?.find(q => q.getName() === "test-queue");
    if (!testQueue) {
        return c.json({ error: "Queue not found" }, 404);
    }
    const job = await testQueue.add("test-job", data);
    return c.json(job);
});

console.log("Starting server on port 3000");
serve({
    fetch: app.fetch,
    port: 3000
});


