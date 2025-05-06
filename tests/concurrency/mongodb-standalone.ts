import { MongoClient } from "mongodb";
import { MongoDBJobStorage,MongoDBJobQueue } from "../../src";
const client = new MongoClient("mongodb://localhost:27017");

client.connect().then(() => {
  console.log("Connected to MongoDB");
});


const storage = new MongoDBJobStorage(client, {
  collectionName: "jobs",
});

const queue = new MongoDBJobQueue(storage, {
  name: "test",
  processingInterval: 500,
  maxRetries: 3,
});

queue.register("test", async (data) => {
    await new Promise((resolve) => setTimeout(resolve, 3000));
});

queue.addEventListener("completed", (event: any) => {
    console.log("job completed",event.data?.job.id);
});

queue.start();

console.log("queue started");

["SIGINT", "SIGTERM"].forEach((signal) => {
    process.on(signal, () => {
        queue.stop();
        client.close();
        console.log("queue stopped");
    });
});








