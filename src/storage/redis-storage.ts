import { Job, JobStatus } from "../types";
import { JobStorage } from "./base-storage";
import type { Redis } from "ioredis";

/**
 * Redis storage adapter for JobQueue
 * 
 * This storage adapter uses Redis to store jobs, making it suitable
 * for distributed environments with multiple instances/processes.
 * 
 * Note: You must install the 'ioredis' package to use this adapter:
 * npm install ioredis
 */

export interface RedisStorage extends JobStorage {
  // Atomic job acquisition
  acquireNextJob(instanceId: string, ttl?: number): Promise<Job | null>;
  // Atomic job completion
  completeJob(jobId: string, instanceId: string, result: any): Promise<void>;
  // Atomic job failure
  failJob(jobId: string, instanceId: string, error: string): Promise<void>;
}

export class RedisJobStorage implements RedisStorage {
  private readonly redis: Redis; // ioredis instance
  private readonly keyPrefix: string;
  private readonly jobListKey: string;
  private readonly statusSetKeyPrefix: string;

  /**
   * Create a new RedisJobStorage
   * 
   * @param redis - An ioredis client instance
   * @param options - Configuration options
   */
  constructor(redis: Redis, options: { keyPrefix?: string } = {}) {
    this.redis = redis;
    this.keyPrefix = options.keyPrefix || 'jobqueue:';
    this.jobListKey = `${this.keyPrefix}jobs`;
    this.statusSetKeyPrefix = `${this.keyPrefix}status:`;
  }

  /**
   * Save a job to Redis
   */
  async saveJob(job: Job): Promise<void> {
    const jobKey = this.getJobKey(job.id);
    const statusKey = this.getStatusKey(job.status);
    
    const pipeline = this.redis.pipeline();
    pipeline.set(jobKey, JSON.stringify(job));
    pipeline.sadd(statusKey, job.id);
    pipeline.sadd(this.jobListKey, job.id);
    
    await pipeline.exec();
  }

  /**
   * Get a job by ID
   */
  async getJob(id: string): Promise<Job | null> {
    const jobKey = this.getJobKey(id);
    const json = await this.redis.get(jobKey);
    
    if (!json) return null;
    
    try {
      const job = JSON.parse(json) as Job;
      // Restore dates (Redis stores them as strings)
      job.createdAt = new Date(job.createdAt);
      if (job.scheduledAt) job.scheduledAt = new Date(job.scheduledAt);
      if (job.startedAt) job.startedAt = new Date(job.startedAt);
      if (job.completedAt) job.completedAt = new Date(job.completedAt);
      return job;
    } catch (e) {
      console.error('Error parsing job from Redis:', e);
      return null;
    }
  }

  /**
   * Get jobs by status
   */
  async getJobsByStatus(status: JobStatus): Promise<Job[]> {
    const statusKey = this.getStatusKey(status);
    const jobIds = await this.redis.smembers(statusKey);
    
    if (!jobIds.length) return [];
    
    const jobs: Job[] = [];
    for (const id of jobIds) {
      const job = await this.getJob(id);
      if (job) jobs.push(job);
    }
    
    return jobs;
  }

  /**
   * Update a job
   */
  async updateJob(job: Job): Promise<void> {
    const jobKey = this.getJobKey(job.id);
    const oldJob = await this.getJob(job.id);
    
    if (!oldJob) {
      throw new Error(`Job with ID ${job.id} not found`);
    }
    
    const pipeline = this.redis.pipeline();
    
    // If status changed, update status sets
    if (oldJob.status !== job.status) {
      const oldStatusKey = this.getStatusKey(oldJob.status);
      const newStatusKey = this.getStatusKey(job.status);
      
      pipeline.srem(oldStatusKey, job.id);
      pipeline.sadd(newStatusKey, job.id);
    }
    
    // Update the job data
    pipeline.set(jobKey, JSON.stringify(job));
    
    await pipeline.exec();
  }

  // Helper method to generate Redis keys for jobs
  private getJobKey(id: string): string {
    return `${this.keyPrefix}job:${id}`;
  }

  // Helper method to generate Redis keys for status sets
  private getStatusKey(status: JobStatus): string {
    return `${this.statusSetKeyPrefix}${status}`;
  }

  /**
   * Acquire the next job from the queue
   * 
   * @param instanceId - Unique ID of this worker instance
   * @param ttl - Time-to-live for the lock in seconds
   * @returns The next job or null if no job is available
   */
  async acquireNextJob(instanceId: string, ttl: number): Promise<Job | null> {
    const script = `
      local jobId = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
      if not jobId then
        return nil
      end
      
      redis.call('SET', KEYS[3] .. jobId .. ':claimed', ARGV[1], 'EX', ARGV[2])
      return jobId
    `;
    const jobId = await this.redis.eval(
      script,
      3,  // Number of keys
      this.jobListKey,
      `${this.keyPrefix}processing`,
      `${this.keyPrefix}job:`,
      instanceId,
      ttl.toString()
    );
    
    if (!jobId) return null;
    const job = await this.getJob(jobId as string);
    return job;
  }

  /**
   * Complete a job
   * 
   * @param jobId - ID of the job to complete
   * @param result - Result of the job
   */
  async completeJob(jobId: string, result: any): Promise<void> {
    // Use MULTI/EXEC for atomic transaction
    const multi = this.redis.multi();
    multi.hset(`${this.keyPrefix}job:${jobId}`, 'status', 'completed', 'result', JSON.stringify(result));
    multi.del(`${this.keyPrefix}job:${jobId}:claimed`);
    await multi.exec();
  }

  /**
   * Fail a job
   * 
   * @param jobId - ID of the job to fail
   * @param error - Error message
   */
  async failJob(jobId: string, error: string): Promise<void> {
    // Use MULTI/EXEC for atomic transaction
    const multi = this.redis.multi();
    multi.hset(`${this.keyPrefix}job:${jobId}`, 'status', 'failed', 'error', error);
    multi.del(`${this.keyPrefix}job:${jobId}:claimed`);
    await multi.exec();
  }
} 