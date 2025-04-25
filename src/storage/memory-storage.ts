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
      return Array.from(this.jobs.values()).filter(job => job.status === status);
    }
    
    async updateJob(job: Job): Promise<void> {
      if (!this.jobs.has(job.id)) {
        throw new Error(`Job with id ${job.id} not found`);
      }
      this.jobs.set(job.id, job);
    }
}