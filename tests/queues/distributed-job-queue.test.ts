import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { DistributedJobQueue } from "../../src/queue/distributed-job-queue";
import { RedisJobStorage } from "../../src/storage/redis-storage";
import { JobHandler } from "../../src/types";
import Redis from "ioredis";
import { QueueEvent } from "../../src/utils/queue-event";
import dotenv from "dotenv";

/**
 * Create new queues for each test
 * This is to ensure that the queues are not shared between tests
 * and that they are not affected by other tests
 */

describe("DistributedJobQueue", () => {
  let redis: Redis;
  let storage: RedisJobStorage;
  let mockHandler: JobHandler;
  dotenv.config();
  redis = new Redis(process.env.TEST_REDIS_URL || "redis://localhost:6379");
  storage = new RedisJobStorage(redis, { keyPrefix: "test:" });

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
    const queue = new DistributedJobQueue(storage, {
      concurrency: 1,
      name: "worker-1",
      processingInterval: 100,
    });

    queue.register("test-job", mockHandler);
    const job = await queue.add("test-job", { message: "Hello, World!" });
    queue.start();
    await new Promise((resolve) => setTimeout(resolve, 250));
    queue.stop();

    expect(mockHandler).toHaveBeenCalledWith({ message: "Hello, World!" });
    const processedJob = await storage.getJob(job.id);
    expect(processedJob?.status).toBe("completed");
  });

  it("should schedule jobs for later execution", async () => {
    const queue = new DistributedJobQueue(storage, {
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
    const concurrentQueue = new DistributedJobQueue(storage, {
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

  it("should process repeatable jobs", async () => {
    const repeatableQueue = new DistributedJobQueue(storage, {
      concurrency: 1,
      processingInterval: 20,
      name: "repeatable-queue",
      logging: true,
    });
    const repeatableHandler = vi.fn().mockImplementation(async () => {
      console.log("Repeatable job executed");
    });
    const scheduledEventListener = vi
      .fn()
      .mockImplementation((event: QueueEvent) => {
        console.log("Scheduled event", event);
      });

    repeatableQueue.register("repeatable-job", repeatableHandler);
    repeatableQueue.addEventListener("scheduled", scheduledEventListener);
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
});
