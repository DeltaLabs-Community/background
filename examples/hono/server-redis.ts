import { Hono } from "hono";
import { RedisJobStorage, JobQueue } from "../../src";
import { Redis } from "ioredis";
import { serve } from "@hono/node-server";

const app = new Hono();

const redis = new Redis("redis://localhost:6379");

const storage = new RedisJobStorage(redis, {
  keyPrefix: "test",
});

const queue = new JobQueue(storage, {
  name: "test",
});

queue.register("test", async (data) => {
  console.log(data);
});

app.post("/job", async (c) => {
  const { message } = await c.req.json();
  await queue.add("test", { message });
  return c.json({ message: "Hello, world!" });
});

console.log("Server is running on port 3000");
serve(app);
