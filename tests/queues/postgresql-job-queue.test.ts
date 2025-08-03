import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  vi,
  afterAll,
} from "vitest";
import { PostgreSQLJobStorage } from "../../src/storage/postgresql-storage";
import { PostgreSQLJobQueue } from "../../src/queue/postgresql-job-queue";
import { Pool } from "pg";
import { Job, JobHandler } from "../../src/types";
import dotenv from "dotenv";
import { QueueEvent } from "../../src/utils/queue-event";
describe("PostgreSQLJobQueue", () => {
  let pool: Pool;
  let storage: PostgreSQLJobStorage;
  let queue: PostgreSQLJobQueue;
  let mockHandler: JobHandler;
  // Set up before each test
  beforeEach(async () => {
    dotenv.config();
    pool = new Pool({
      connectionString:
        process.env.TEST_POSTGRESQL_URL ||
        "postgresql://postgres:12345@localhost:5432/postgres",
    });

    storage = new PostgreSQLJobStorage(pool, { tableName: "jobs", logging: true,staleJobTimeout:1000 });
    queue = new PostgreSQLJobQueue(storage, {
      name: "test-queue",
      concurrency: 1,
      maxRetries: 2,
      processingInterval: 100,
      logging: true,
    });

    // Mock job handler
    mockHandler = vi.fn().mockImplementation(() => {
      return { success: true };
    });
    // Register job handler
    queue.register("test-job", mockHandler);
  });

  // Clean up after each test
  afterEach(async () => {
    // Reset mocks
    vi.resetAllMocks();
    await pool.query("DELETE FROM jobs");
    queue.stop();
  });

  it("should add a job to the queue", async () => {
    const job = await queue.add("test-job", { foo: "bar" });

    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe("test-job");
    expect(job.status).toBe("pending");
    expect(job.data).toEqual({ foo: "bar" });

    const jobs = await storage.getJobsByStatus("pending");
    expect(jobs.length).toBe(1);
    expect(jobs[0].id).toBe(job.id);
  });

  it("should schedule a job for later execution", async () => {
    const scheduledTime = new Date(Date.now() + 1000 * 60);
    const job = await queue.schedule(
      "test-job",
      { scheduled: true },
      scheduledTime,
    );

    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe("test-job");
    expect(job.status).toBe("pending");
    expect(job.data).toEqual({ scheduled: true });
    expect(job.scheduledAt).toEqual(scheduledTime);

    const jobs = await storage.getJobsByStatus("pending");
    expect(jobs.length).toBe(1);
    expect(jobs[0].id).toBe(job.id);
    expect(jobs[0].scheduledAt).toEqual(scheduledTime);
  });

  it("should process a job successfully", async () => {
    const job = await queue.add("test-job", { process: true });

    queue.start();

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(mockHandler).toHaveBeenCalledTimes(1);
    expect(mockHandler).toHaveBeenCalledWith({ process: true });

    const updatedJob = await queue.getJob(job.id);

    expect(updatedJob).toBeDefined();
    expect(updatedJob?.status).toBe("completed");
    expect(updatedJob?.result).toEqual({ success: true });
  });

  it("should retry a failed job", async () => {
    let attempts = 0;
    const retriableHandler = vi.fn().mockImplementation(() => {
      attempts++;
      if (attempts === 1) {
        throw new Error("Temporary failure");
      }
      return { success: true, retried: true };
    });

    queue.register("retry-job", retriableHandler);

    const job = await queue.add("retry-job", { shouldRetry: true });

    queue.start();

    await new Promise((resolve) => setTimeout(resolve, 400));

    expect(retriableHandler).toHaveBeenCalledTimes(2);

    const updatedJob = await queue.getJob(job.id);

    expect(updatedJob).toBeDefined();
    expect(updatedJob?.status).toBe("completed");
    expect(updatedJob?.result).toEqual({ success: true, retried: true });
    expect(updatedJob?.retryCount).toBe(1);
  });

  it("should mark a job as failed after max retries", async () => {
    const failingHandler = vi.fn().mockImplementation(() => {
      throw new Error("Persistent failure");
    });

    queue.register("failing-job", failingHandler);

    const job = await queue.add("failing-job", { willFail: true });

    queue.start();

    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(failingHandler).toHaveBeenCalledTimes(3);

    const updatedJob = await queue.getJob(job.id);

    expect(updatedJob).toBeDefined();
    expect(updatedJob?.status).toBe("failed");
    expect(updatedJob?.error).toContain("Failed after 2 retries");
    expect(updatedJob?.retryCount).toBe(2);
  });
  it("should handle job priority", async () => {
    await queue.add("test-job", { priority: 10 }, { priority: 10 });
    await queue.add("test-job", { priority: 1 }, { priority: 1 });

    const priorityHandler = vi
      .fn()
      .mockImplementation(({ priority }: { priority: number }) => {
        return { success: true, priority };
      });

    queue.register("test-job", priorityHandler);
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(priorityHandler).toHaveBeenNthCalledWith(1, { priority: 1 });
    expect(priorityHandler).toHaveBeenNthCalledWith(2, { priority: 10 });
  });
  it("should process repeatable jobs", async () => {
    const repeatableHandler = vi.fn().mockImplementation(async () => {
      console.log("Repeatable job executed");
    });

    const repeatableQueue = new PostgreSQLJobQueue(storage, {
      name: "repeatable-queue",
      concurrency: 1,
      processingInterval: 20,
    });
    repeatableQueue.register("repeatable-job", repeatableHandler);
    await repeatableQueue.addRepeatable(
      "repeatable-job",
      { id: 1 },
      {
        every: 1,
        unit: "seconds",
      },
    );
    repeatableQueue.start();
    await new Promise((resolve) => setTimeout(resolve, 3000));
    repeatableQueue.stop();
    expect(repeatableHandler).toHaveBeenCalledTimes(3);
  });

  it("should handle intelligent polling", async () => {
    const queue = new PostgreSQLJobQueue(storage, {
      concurrency: 1,
      processingInterval: 100,
      intelligentPolling: true,
      minInterval: 100,
      maxInterval: 225,
      maxEmptyPolls: 5,
      loadFactor: 0.5,
    });

    const handler = vi.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      return { success: true };
    });

    const eventListener = vi.fn().mockImplementation((event: QueueEvent) => {
      console.log(
        "polling-interval-updated event received:",
        event.data.message,
      );
      expect(event.data.message).toBeDefined();
    });

    queue.addEventListener("polling-interval-updated", eventListener);
    queue.register("test-job", handler);
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 3000));
    queue.stop();
    expect(eventListener).toHaveBeenCalledTimes(2);
  });

  it("should timeout a job if it takes too long", async () => {
    const timeoutHandler = vi.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10000));
      return { success: true };
    });

    const failedEventListener = vi.fn().mockImplementation((event: QueueEvent) => {
      expect(event.data.status).toBe("failed");
    });

    queue.addEventListener("failed", failedEventListener);
    queue.register("timeout-job", timeoutHandler);
    await queue.add("timeout-job", { timeout: true }, { timeout: 500 });
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 800));
    queue.stop();
    expect(timeoutHandler).toHaveBeenCalled();
    expect(failedEventListener).toHaveBeenCalled();
  });

  it("it should process stale jobs",async()=>{
    let stalted = false;
    const staleHandler = vi.fn().mockImplementation(async ({message}:{message:string}) => {
      console.log("staleHandler",message);
      return { success: true,message };
    });

    const completedEventListener = vi.fn().mockImplementation(async (event: QueueEvent) => {
      expect(event.data.status).toBe("completed");
      const jobId = event.data?.job?.id;
      if(!jobId){
        throw new Error("Job ID is required");
      }
      // stale the job manually
      const job = await storage.getJob(jobId);
      if(!job){
        throw new Error("Job not found");
      }
      if(stalted){
        return;
      }
      try {
        const pastDate = new Date(Date.now() - 3000);
        await storage.updateJob({
          id:jobId,
          status:"processing",
          startedAt: pastDate,
          completedAt:undefined,
          error:undefined,
          result:undefined,
          retryCount:0,
          repeat:false,
          timeout:1000,
          priority:1,
          name:"stale-job",
          data:{message:"test"},
          createdAt: pastDate,
        } as unknown as Job);
        
        console.log("Successfully created stale job");
        stalted = true;
      } catch (error) {
        console.error("Error staling job", error);
      }
    });

    queue.register("stale-job",staleHandler);
    queue.addEventListener("completed",completedEventListener);
    await queue.add("stale-job",{message:"test"})
    
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 3000));
    queue.stop();
    expect(completedEventListener).toHaveBeenCalledTimes(2);
    expect(staleHandler).toHaveBeenCalledTimes(2);
  })
});
