import { describe, it, expect, beforeEach } from "vitest";
import { InMemoryJobStorage } from "../../src/storage/memory-storage";
import { Job, JobStatus } from "../../src/types";

describe("InMemoryJobStorage", () => {
  let storage: InMemoryJobStorage;
  let testJob: Job;

  beforeEach(() => {
    storage = new InMemoryJobStorage();
    testJob = {
      id: "test-job-1",
      name: "test-job",
      data: { message: "Hello, World!" },
      status: "pending" as JobStatus,
      createdAt: new Date(),
    } as Job;
  });

  it("should save and retrieve a job", async () => {
    await storage.saveJob(testJob);
    const retrievedJob = await storage.getJob(testJob.id);
    expect(retrievedJob).toEqual(testJob);
  });

  it("should return null for non-existent job", async () => {
    const job = await storage.getJob("non-existent");
    expect(job).toBeNull();
  });

  it("should get jobs by status", async () => {
    const pendingJob1 = { ...testJob, id: "pending-1" } as Job;
    const pendingJob2 = { ...testJob, id: "pending-2" } as Job;
    const processingJob = {
      ...testJob,
      id: "processing-1",
      status: "processing",
    } as Job;

    await storage.saveJob(pendingJob1);
    await storage.saveJob(pendingJob2);
    await storage.saveJob(processingJob);

    const pendingJobs = await storage.getJobsByStatus("pending");
    expect(pendingJobs).toHaveLength(2);
    expect(pendingJobs.map((job) => job.id)).toContain("pending-1");
    expect(pendingJobs.map((job) => job.id)).toContain("pending-2");

    const processingJobs = await storage.getJobsByStatus("processing");
    expect(processingJobs).toHaveLength(1);
    expect(processingJobs[0].id).toBe("processing-1");
  });

  it("should update a job", async () => {
    await storage.saveJob(testJob);

    const updatedJob = {
      ...testJob,
      status: "completed" as JobStatus,
      completedAt: new Date(),
      result: { success: true },
    };

    await storage.updateJob(updatedJob);
    const retrievedJob = await storage.getJob(testJob.id);

    expect(retrievedJob?.status).toBe("completed");
    expect(retrievedJob?.result).toEqual({ success: true });
  });

  it("should throw error when updating non-existent job", async () => {
    const nonExistentJob = {
      ...testJob,
      id: "non-existent",
    };

    await expect(storage.updateJob(nonExistentJob)).rejects.toThrow();
  });
});
