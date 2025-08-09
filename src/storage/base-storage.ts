import { Job, JobStatus } from "../types.js";
export { InMemoryJobStorage } from "./memory-storage.js";
export { PostgreSQLJobStorage } from "./postgresql-storage.js";
export { MongoDBJobStorage } from "./mongodb-storage.js";

export interface JobStorage {
  saveJob(job: Job): Promise<void>;
  getJob(id: string): Promise<Job | null>;
  getJobsByStatus(status: JobStatus): Promise<Job[]>;
  updateJob(job: Job): Promise<void>;
  acquireNextJob(handlerNames?:string []): Promise<Job | null>;
  acquireNextJobs(batchSize: number,handlerNames?:string []): Promise<Job[]>;
  completeJob(jobId: string, result: any): Promise<void>;
  failJob(jobId: string, error: string): Promise<void>;
}
