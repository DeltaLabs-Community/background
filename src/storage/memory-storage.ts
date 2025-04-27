import { Job, JobStatus } from "../types";
import { JobStorage } from "./base-storage";

export class InMemoryJobStorage implements JobStorage {
  private jobs: Map<string, Job> = new Map();

  async saveJob(job: Job): Promise<void> {
    this.jobs.set(job.id, job);
  }

  async getJob(id: string): Promise<Job | null> {
    return this.jobs.get(id) || null;
  }

  async getJobsByStatus(status: JobStatus): Promise<Job[]> {
    return Array.from(this.jobs.values()).filter(
      (job) => job.status === status,
    );
  }

  async updateJob(job: Job): Promise<void> {
    if (!this.jobs.has(job.id)) {
      throw new Error(`Job with id ${job.id} not found`);
    }
    this.jobs.set(job.id, job);
  }

  async acquireNextJob(): Promise<Job | null> {
    // First, filter to get eligible jobs
    const pendingJobs = Array.from(this.jobs.values()).filter((job) => {
      if (job.status !== "pending") return false;
      if (job.scheduledAt) {
        const now = new Date();
        return job.scheduledAt <= now;
      }
      return true;
    });

    const sortedJobs = pendingJobs.sort((a, b) => {
      const priorityA = a.priority || 10;
      const priorityB = b.priority || 10;
      if (priorityA !== priorityB) {
        return priorityA - priorityB;
      }
      return a.createdAt.getTime() - b.createdAt.getTime();
    });

    const job = sortedJobs[0];
    if (!job) return null;

    job.status = "processing";
    job.startedAt = new Date();

    // Store the updated job
    this.jobs.set(job.id, job);

    return job;
  }

  async completeJob(jobId: string, result: any): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) {
      throw new Error(`Job with id ${jobId} not found`);
    }
    job.status = "completed";
    job.result = result;
    await this.updateJob(job);
  }

  async failJob(jobId: string, error: string): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) {
      throw new Error(`Job with id ${jobId} not found`);
    }
    job.status = "failed";
    job.error = error;
    await this.updateJob(job);
  }
}
