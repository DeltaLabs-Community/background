import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { JobQueue } from "../../src/queue/job-queue";
import { InMemoryJobStorage } from "../../src/storage/memory-storage";
import { JobHandler } from "../../src/types";
import { QueueEvent } from "../../src/utils/queue-event";

describe("JobQueue", () => {
  let storage: InMemoryJobStorage;
  let mockHandler: JobHandler;
  let eventListener;

  beforeEach(() => {
    // Create a fresh storage instance for each test
    storage = new InMemoryJobStorage();

    mockHandler = vi.fn().mockResolvedValue({ success: true });
    // Add an event listener
    eventListener = vi.fn().mockImplementation((event: QueueEvent) => {
      console.log("Event received:", event.data);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should add a job to the queue", async () => {
    const queue = new JobQueue(storage, {
      concurrency: 1,
      name: "test-queue",
      processingInterval: 20,
    });
    queue.register("test-job", mockHandler);
    const job = await queue.add("test-job", { message: "Hello, World!" });

    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.status).toBe("pending");
    expect(job.name).toBe("test-job");
    expect(job.data).toEqual({ message: "Hello, World!" });
    const savedJob = await storage.getJob(job.id);
    expect(savedJob).toEqual(job);
  });

  it("should throw error for unregistered job handler", async () => {
    const queue = new JobQueue(storage, {
      concurrency: 1,
      name: "test-queue",
      processingInterval: 20,
    });
    queue.register("test-job", mockHandler);
    await expect(queue.add("non-existent-job", {})).rejects.toThrow();
  });

  it("should schedule a job for future execution", async () => {
    const futureDate = new Date();
    futureDate.setMinutes(futureDate.getMinutes() + 10); // 10 minutes in the future

    const queue = new JobQueue(storage, {
      concurrency: 1,
      name: "test-queue",
      processingInterval: 20,
    });
    queue.register("test-job", mockHandler);
    const job = await queue.schedule(
      "test-job",
      { message: "Future task" },
      futureDate,
    );

    expect(job).toBeDefined();
    expect(job.scheduledAt).toEqual(futureDate);

    const savedJob = await storage.getJob(job.id);
    expect(savedJob?.scheduledAt).toEqual(futureDate);
  });

  it("should retrieve a job by ID", async () => {
    const queue = new JobQueue(storage, {
      concurrency: 1,
      name: "test-queue",
      processingInterval: 20,
    });
    queue.register("test-job", mockHandler);
    const job = await queue.add("test-job", { message: "Hello, World!" });
    const retrievedJob = await queue.getJob(job.id);

    expect(retrievedJob).toEqual(job);
  });

  it("should process a job", async () => {
    const queue = new JobQueue(storage, {
      concurrency: 1,
      name: "test-queue",
      processingInterval: 20,
    });
    queue.register("test-job", mockHandler);
    // Mock Date.now for consistent testing
    const now = new Date();
    vi.spyOn(global, "Date").mockImplementation(() => now as any);

    // Add a job before starting the queue
    const job = await queue.add("test-job", { message: "Hello, World!" });

    // Start processing jobs
    queue.start();

    // Wait longer to ensure job processing
    await new Promise((resolve) => setTimeout(resolve, 25));

    // Stop the queue
    queue.stop();

    // Verify the handler was called with the correct data
    expect(mockHandler).toHaveBeenCalledWith({ message: "Hello, World!" });

    // Verify job status was updated
    const processedJob = await storage.getJob(job.id);
    expect(processedJob?.status).toBe("completed");
    expect(processedJob?.result).toEqual({ success: true });
  });

  it("should not process scheduled jobs before their time", async () => {
    // Schedule a job for the future
    const futureDate = new Date();
    futureDate.setMinutes(futureDate.getMinutes() + 10); // 10 minutes in the future

    const queue = new JobQueue(storage, {
      concurrency: 1,
      name: "test-queue",
      processingInterval: 20,
    });
    queue.register("test-job", mockHandler);
    const job = await queue.schedule(
      "test-job",
      { message: "Future task" },
      futureDate,
    );

    // Start processing jobs
    queue.start();

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 25));
    queue.stop();
    // Verify handler wasn't called for the future job
    expect(mockHandler).not.toHaveBeenCalled();
  });

  it("should process multiple jobs with configured concurrency", async () => {
    // Create a queue with concurrency of 2
    const concurrentQueue = new JobQueue(storage, {
      concurrency: 2,
      processingInterval: 20,
      name: "concurrent-queue",
    });

    // Create a handler that takes some time
    const slowHandler = vi.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return { success: true };
    });

    const completedEventListener = vi
      .fn()
      .mockImplementation((queueEvent: QueueEvent) => {
        console.log("Event received:", queueEvent.data);
      });

    // Register the slow handler
    concurrentQueue.register("slow-job", slowHandler);

    // register event listeners
    concurrentQueue.addEventListener("scheduled", eventListener);
    concurrentQueue.addEventListener("completed", completedEventListener);
    // Add multiple jobs before starting the queue
    await concurrentQueue.add("slow-job", { id: 1 });
    await concurrentQueue.add("slow-job", { id: 2 });
    await concurrentQueue.add("slow-job", { id: 3 });

    expect(eventListener).toHaveBeenCalledTimes(3);

    // Start processing
    concurrentQueue.start();

    // Wait for jobs to complete
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Stop the queue
    concurrentQueue.stop();

    // Verify all jobs were processed
    expect(slowHandler).toHaveBeenCalledTimes(2);
    expect(slowHandler).toHaveBeenCalledWith({ id: 1 });
    expect(slowHandler).toHaveBeenCalledWith({ id: 2 });
    expect(completedEventListener).toHaveBeenCalledTimes(2);
  });

  it("should process repeatable jobs", async () => {
    // Use a unique queue name to avoid collisions
    const repeatableQueue = new JobQueue(storage, {
      concurrency: 1,
      processingInterval: 20,
      name: `repeatable-queue-${Date.now()}`,
      logging: true,
    });

    const repeatableHandler = vi.fn().mockImplementation(async () => {
      console.log("Repeatable job executed");
      return { success: true };
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
    await new Promise((resolve) => setTimeout(resolve, 3000)); // Slightly longer wait
    repeatableQueue.stop();
    expect(repeatableHandler).toHaveBeenCalledTimes(3);
  });

  it("should handle intelligent polling", async () => {
    const queue = new JobQueue(storage, {
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
    // Start the queue
    queue.start();

    // Wait for jobs to complete
    await new Promise((resolve) => setTimeout(resolve, 3000));

    expect(eventListener).toHaveBeenCalledTimes(2);

    // Stop the queue
    queue.stop();
  });
});
