import { MongoClient } from "mongodb";
import { MongoDBJobStorage,MongoDBJobQueue } from "../../src/index.js";
const client = new MongoClient("mongodb://localhost:27017");

client.connect().then(() => {
  console.log("Connected to MongoDB");
});


const storage = new MongoDBJobStorage(client, {
  collectionName: "jobs",
});

const storage2 = new MongoDBJobStorage(client, {
  collectionName: "jobs2",
});


const queue = new MongoDBJobQueue(storage, {
  name: "test",
  processingInterval: 100,
  concurrency:20,
  preFetchBatchSize:100,
  maxRetries: 3,
});

queue.register("test-job", async (data) => {
    await new Promise((resolve) => setTimeout(resolve, 100));
});

queue.addEventListener("completed", (event: any) => {
    console.log("job completed",event.data?.job.id);
});

const queue2 = new MongoDBJobQueue(storage2, {
  name: "test2",
  processingInterval: 100,
  concurrency:20,
  preFetchBatchSize:100,
  maxRetries: 3,
});

queue2.register("test-job", async (data) => {
    await new Promise((resolve) => setTimeout(resolve, 100));
});

queue2.addEventListener("completed", (event: any) => {
    console.log("job completed - queue2",event.data?.job.id);
})

queue.start();
queue2.start();

for (let i = 0; i < 1000; i++) {
  queue.add("test-job", {
    i: i,
  });
  queue2.add("test-job", {
    i: i,
  });
}

console.log("queues are started");

["SIGINT", "SIGTERM"].forEach((signal) => {
    process.on(signal, () => {
        queue.stop();
        queue2.stop();
        client.close();
        console.log("queues are stopped");
    });
});








