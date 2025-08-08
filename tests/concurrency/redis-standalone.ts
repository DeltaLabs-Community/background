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
  processingInterval: 50,
  maxRetries: 3,
  concurrency: 10,
  preFetchBatchSize:50,
  logging: false,
});

queue.register("test-job", async (data) => {
  console.log(data);
  return { processed: true };
});

queue.addEventListener("completed", (event: any) => {
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
