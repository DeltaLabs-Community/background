import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { RedisJobQueue, RedisJobStorage,JobHandler,QueueEvent,Job } from "../../../src";
import Redis from "ioredis";
import dotenv from "dotenv";

/**
 * Create new queues for each test
 * This is to ensure that the queues are not shared between tests
 * and that they are not affected by other tests
 */

describe("RedisJobQueue", () => {
  let redis: Redis;
  let storage: RedisJobStorage;
  let mockHandler: JobHandler;
  dotenv.config();
  redis = new Redis(process.env.TEST_REDIS_URL || "redis://localhost:6379");
  storage = new RedisJobStorage(redis, { keyPrefix: "test:",staleJobTimeout:1000,logging:true });

  beforeEach(async () => {
    await storage.clear();
    mockHandler = vi.fn().mockImplementation(() => {
      return { success: true };
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should process jobs with locking", async () => {
    const queue = new RedisJobQueue(storage, {
      concurrency: 1,
      name: "worker-1",
      processingInterval: 50,
    });

    queue.register("test-job", mockHandler);
    const job = await queue.add("test-job", { message: "Hello, World!" });
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 140));
    queue.stop();

    expect(mockHandler).toHaveBeenCalledWith({ message: "Hello, World!" });
    const processedJob = await storage.getJob(job.id);
    expect(processedJob?.status).toBe("completed");
  });

  it("should schedule jobs for later execution", async () => {
    const queue = new RedisJobQueue(storage, {
      concurrency: 1,
      name: "worker-1",
      processingInterval: 100,
    });

    queue.register("test-job", mockHandler);
    const scheduledTime = new Date(Date.now() + 1000 * 60);
    const job = await queue.schedule(
      "test-job",
      { scheduled: true },
      scheduledTime,
    );

    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 250));
    queue.stop();

    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe("test-job");
    expect(job.status).toBe("pending");
    expect(job.data).toEqual({ scheduled: true });
    expect(job.scheduledAt).toEqual(scheduledTime);
  });

  it("should process jobs concurrently", async () => {
    const concurrentQueue = new RedisJobQueue(storage, {
      concurrency: 2,
      name: "worker-2",
      processingInterval: 100,
    });

    const eventListener = vi.fn().mockImplementation((event: QueueEvent) => {});

    concurrentQueue.addEventListener("completed", eventListener);
    // Register handler for the job
    concurrentQueue.register("test-job", mockHandler);
    // Add jobs to the queue
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });
    concurrentQueue.add("test-job", { message: "Hello, World!" });

    // Start the queue
    concurrentQueue.start();
    await new Promise((resolve) => setTimeout(resolve, 200));
    concurrentQueue.stop();
    expect(mockHandler).toHaveBeenCalledTimes(2);
    expect(eventListener).toHaveBeenCalledTimes(2);
  });

  // it("should process repeatable jobs", async () => {
  //   const repeatableQueue = new DistributedJobQueue(storage, {
  //     concurrency: 1,
  //     processingInterval: 20,
  //     name: "repeatable-queue",
  //   });
  //   const repeatableHandler = vi.fn().mockImplementation(async () => {
  //     console.log("Repeatable job executed");
  //   });
  //   const scheduledEventListener = vi
  //     .fn()
  //     .mockImplementation((event: QueueEvent) => {
  //       console.log("Scheduled event", event);
  //     });

  //   repeatableQueue.register("repeatable-job", repeatableHandler);
  //   repeatableQueue.addEventListener("scheduled", scheduledEventListener);
  //   await repeatableQueue.addRepeatable(
  //     "repeatable-job",
  //     { id: 1 },
  //     {
  //       every: 1,
  //       unit: "seconds",
  //     },
  //   );
  //   repeatableQueue.start();
  //   await new Promise((resolve) => setTimeout(resolve, 3000));
  //   repeatableQueue.stop();
  //   expect(repeatableHandler).toHaveBeenCalledTimes(3);
  // });

  it("should handle intelligent polling", async () => {
    const queue = new RedisJobQueue(storage, {
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

  it("it should handle timeout",async()=>{
    const queue = new RedisJobQueue(storage,{
      concurrency:1,
      processingInterval:100
    })
    const timeoutHandler = vi.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10000));
      return { success: true };
    });
    const failedEventListener = vi.fn().mockImplementation((event: QueueEvent) => {
      expect(event.data.status).toBe("failed");
    });
    queue.addEventListener("failed",failedEventListener)
    queue.register("test-job",timeoutHandler);
    queue.add("test-job",{message:"test"},{timeout:500})
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 800));
    queue.stop();
    expect(failedEventListener).toHaveBeenCalledTimes(1);
    expect(timeoutHandler).toHaveBeenCalled();
  });

  it("it should process stale jobs",async()=>{
    let staled = false;
    const queue = new RedisJobQueue(storage,{
      concurrency:1,
      processingInterval:100
    })
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
      if(staled){
        return;
      }
      try {
        const pastDate = new Date(Date.now() - 3000);
        
        await storage.updateJob({
          id: jobId,
          status: "processing",
          startedAt: pastDate,
          completedAt: undefined,
          error: undefined,
          result: undefined,
          retryCount: 0,
          repeat: false,
          timeout: 1000,
          priority: 1,
          name: "stale-job",
          data: {message: "test"},
          createdAt: pastDate,
        } as unknown as Job);
        
        console.log("Successfully created stale job");
        staled = true;
      } catch (error) {
        console.error("Error staling job", error);
      }
    })
    queue.addEventListener("completed",completedEventListener)
    queue.register("stale-job",staleHandler);
    await queue.add("stale-job",{message:"test"})
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    queue.stop();
    expect(completedEventListener).toHaveBeenCalledTimes(2);
    expect(staleHandler).toHaveBeenCalledTimes(2);
  })
});
