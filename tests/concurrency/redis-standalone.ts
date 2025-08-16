import { Redis } from "ioredis";
import { RedisJobStorage, RedisJobQueue } from "../../src";

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

const queue = new RedisJobQueue(jobStorage, {
  name: "test",
  processingInterval: 50,
  maxRetries: 3,
  concurrency: 10,
  preFetchBatchSize:100,
  logging: false,
});

queue.register("test-job", async (data) => {
  await new Promise((resolve) => setTimeout(resolve, 100));
  return { processed: true };
});

queue.addEventListener("completed", (event: any) => {
  console.log("job completed",event.data?.job.id);
});

queue.addEventListener("buffer-refill-success", (event: any) => {
  console.log("buffer-refill-success")
});


queue.start();

for(let i = 0; i < 1000; i ++){
  queue.add("test-job",{
    i:i
  })
}

["SIGINT", "SIGTERM", "SIGKILL"].forEach((signal) => {
  process.on(signal, () => {
    queue.stop();
    process.exit(0);
  });
});
