import { Hono } from "hono";
import { MongoDBJobStorage, MongoDBJobQueue, QueueEvent } from "../../src";
import { MongoClient } from "mongodb";
import { honoJobs } from "./honoJobs.middleware";
import { serve } from "@hono/node-server";
import dotenv from "dotenv";

dotenv.config();

const app = new Hono();

const client = new MongoClient(process.env.TEST_MONGODB_URL!);
client
  .connect()
  .then(() => {
    console.log("Connected to MongoDB");
  })
  .catch((err) => {
    console.error("Error connecting to MongoDB", err);
  });

const storage = new MongoDBJobStorage(client, {
  collectionName: "jobs",
});

const queue = new MongoDBJobQueue(storage, {
  concurrency: 2,
  maxRetries: 3,
  name: "test-queue",
  processingInterval: 1000,
});

queue.register("test-job", async (data) => {
  await new Promise((resolve) => setTimeout(resolve, 3000));
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
  const testQueue = queues?.find((q) => q.getName() === "test-queue");
  if (!testQueue) {
    return c.json({ error: "Queue not found" }, 404);
  }
  const job = await testQueue.add("test-job", data);
  return c.json(job);
});

console.log("Starting server on port 3000");
serve({
  fetch: app.fetch,
  port: 3000,
});
