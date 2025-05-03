import express from "express";
import { expressJobs } from "./expressJobs.middleware";
import { InMemoryJobStorage, JobQueue } from "../../src";

const app = express();
app.use(express.json());

const storage = new InMemoryJobStorage();
const queue = new JobQueue(storage, {
  name: "test-queue",
  concurrency: 1,
  processingInterval: 200,
  logging: true,
  intelligentPolling: true,
  minInterval: 100,
  maxInterval: 5000,
  maxEmptyPolls: 5,
  loadFactor: 0.5,
  maxConcurrency: 10,
});

queue.register("test-job", async (data) => {
  await new Promise((resolve) => setTimeout(resolve, 3000));
  console.log("job completed", data);
  return data;
});

expressJobs(app, [queue]);

app.get("/job/:id", (req, res) => {
  const jobId = req.params.id;
  const job = storage.getJob(jobId);
  res.json(job);
});

app.post("/job", async (req, res) => {
  const queues = req.queues;
  const testQueue = queues?.find((q) => q.getName() === "test-queue");
  if (!testQueue) {
    res.status(404).json({ error: "Queue not found" });
    return;
  }
  const job = await testQueue.add("test-job", req.body);
  res.json(job);
});

app.listen(3000, () => {
  console.log("Server is running on port 3000");
});
