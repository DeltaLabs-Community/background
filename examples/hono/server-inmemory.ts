import { Hono } from "hono";
import { InMemoryJobStorage, JobQueue, QueueEvent } from "../../src/";
import { honoJobs } from "./honoJobs.middleware";
import { serve } from "@hono/node-server";

// Explicitly type the app with our environment
const app = new Hono();

const storage = new InMemoryJobStorage();
const queue = new JobQueue(storage, {
    concurrency: 2,
    maxRetries: 3,
    name: "test-queue",
    processingInterval: 1000
});

const jobHandler = async (data: any) => {
    await new Promise(resolve => setTimeout(resolve, 3000));
    return data;
}

const eventHandler = async (event: QueueEvent) => {
    console.log(event.type);
    console.log(event.data);
}

queue.register("test-job", jobHandler);
queue.addEventListener("completed", eventHandler);

// Start the queue and register middleware
honoJobs(app, [queue]);

// Get job by ID
app.get("/job/:id", async (c) => {
    const jobId = c.req.param("id");
    const job = await storage.getJob(jobId);
    if (!job) {
        return c.json({ error: "Job not found" }, 404);
    }
    return c.json(job);
});

// Create a new job
app.post("/job", async (c) => {
    try {
        const data = await c.req.json();
        
        // Now TypeScript will recognize this properly
        const queues = c.get("queues");
        
        const testQueue = queues?.find(q => q.getName() === "test-queue");
        
        if (!testQueue) {
            console.error("Test queue not found");
            return c.json({ error: "Queue not found" }, 404);
        }
        
        const job = await testQueue.add("test-job", data);
        return c.json(job);
    } catch (error) {
        console.error("Error creating job:", error);
        return c.json({ error: "Failed to create job" }, 500);
    }
});

console.log("Server is running on port 3000");
serve({
    fetch: app.fetch,
    port: 3000
})
