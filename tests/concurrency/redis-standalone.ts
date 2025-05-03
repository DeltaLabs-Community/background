import { Redis } from "ioredis";
import { RedisJobStorage, DistributedJobQueue } from "../../src";

const redis = new Redis("redis://localhost:6379");

redis.on("error", (err) => {
  console.error(err);
});

redis.on("connect", () => {
  console.log("Connected to Redis");
});

const jobStorage = new RedisJobStorage(redis, {
  keyPrefix: "test",
});

const queue = new DistributedJobQueue(jobStorage, {
  name: "test",
  processingInterval: 1000,
  maxRetries: 3,
});

queue.register("test", async (data) => {
  console.log(data);
  await new Promise((resolve) => setTimeout(resolve, 3000));
});

queue.addEventListener("completed", (event: any) => {
  console.log(event.data?.job.id);
});

queue.start();

["SIGINT", "SIGTERM", "SIGKILL"].forEach((signal) => {
  process.on(signal, () => {
    queue.stop();
    process.exit(0);
  });
});
