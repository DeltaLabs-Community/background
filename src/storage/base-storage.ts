import { Job, JobStatus } from '../types';
export { InMemoryJobStorage } from './memory-storage';
export { PostgreSQLJobStorage } from './postgresql-storage';
export { MongoDBJobStorage } from './mongodb-storage';

export interface JobStorage {
  saveJob(job: Job): Promise<void>;
  getJob(id: string): Promise<Job | null>;
  getJobsByStatus(status: JobStatus): Promise<Job[]>;
  updateJob(job: Job): Promise<void>;
  acquireNextJob(): Promise<Job | null>;
  completeJob(jobId: string, result: any): Promise<void>;
  failJob(jobId: string, error: string): Promise<void>;
}
