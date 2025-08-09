import { Job, JobStatus } from "../types.js";
import { JobStorage } from "./base-storage.js";

export class InMemoryJobStorage implements JobStorage {
  private jobs: Map<string, Job> = new Map();
  private logging: boolean = false;

  constructor(options?: { logging?: boolean }) {
    this.logging = options?.logging || false;
  }

  async saveJob(job: Job): Promise<void> {
    try {
      this.jobs.set(job.id, job);
    } catch (error) {
      if (this.logging) {
        console.error(`[InMemoryJobStorage] Error saving job:`, error);
      }
    }
  }

  async getJob(id: string): Promise<Job | null> {
    try {
      return this.jobs.get(id) || null;
    } catch (error) {
      if (this.logging) {
        console.error(`[InMemoryJobStorage] Error getting job:`, error);
      }
      return null;
    }
  }

  async getJobsByStatus(status: JobStatus): Promise<Job[]> {
    try {
      return Array.from(this.jobs.values()).filter(
        (job) => job.status === status,
      );
    } catch (error) {
      if (this.logging) {
        console.error(
          `[InMemoryJobStorage] Error getting jobs by status:`,
          error,
        );
      }
      return [];
    }
  }

  async updateJob(job: Job): Promise<void> {
    try {
      if (!this.jobs.has(job.id)) {
        throw new Error(`Job with id ${job.id} not found`);
      }
      this.jobs.set(job.id, job);
    } catch (error) {
      if (this.logging) {
        console.error(`[InMemoryJobStorage] Error updating job:`, error);
      }
    }
  }

  async acquireNextJob(handlerNames?:string []): Promise<Job | null> {
    try {
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
      if(!handlerNames || !(handlerNames.find((handlerName) => job.name === handlerName))){
        return null;
      }
      if (!job) return null;
      job.status = "processing";
      job.startedAt = new Date();
      this.jobs.set(job.id, job);
      return job;
    } catch (error) {
      if (this.logging) {
        console.error(`[InMemoryJobStorage] Error acquiring next job:`, error);
      }
      return null;
    }
  }

  async acquireNextJobs(batchSize: number,handlerNames?:string []): Promise<Job[]> {
    try {
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
      let jobList = sortedJobs.slice(0, batchSize);
      jobList = jobList.filter((job) => handlerNames ? handlerNames.find((handlerName) => job.name === handlerName) : true) 
      jobList.forEach((job) => {
        job.status = "processing";
        job.startedAt = new Date();
        this.jobs.set(job.id, job);
      });
      return jobList;
    } catch (error) {
      if (this.logging) {
        console.error(`[InMemoryJobStorage] Error acquiring next job:`, error);
      }
      return [];
    }
  }

  async completeJob(jobId: string, result: any): Promise<void> {
    try {
      const job = await this.getJob(jobId);
      if (!job) {
        throw new Error(`Job with id ${jobId} not found`);
      }
      job.status = "completed";
      job.result = result;
      await this.updateJob(job);
    } catch (error) {
      if (this.logging) {
        console.error(`[InMemoryJobStorage] Error completing job:`, error);
      }
    }
  }

  async failJob(jobId: string, error: string): Promise<void> {
    try {
      const job = await this.getJob(jobId);
      if (!job) {
        throw new Error(`Job with id ${jobId} not found`);
      }
      job.status = "failed";
      job.error = error;
      await this.updateJob(job);
    } catch (error) {
      if (this.logging) {
        console.error(`[InMemoryJobStorage] Error failing job:`, error);
      }
    }
  }
}
