import { Job, JobStatus } from "../types";
import { JobStorage } from "./base-storage";
import type { Redis } from "ioredis";
import { atomicAcquireScript, completeJobScript, failJobScript, saveJobScript, updateJobScript } from "./lua-scripts/scripts";

export interface RedisStorage extends JobStorage {
  // Get jobs by priority
  getJobsByPriority(priority: number): Promise<Job[]>;
  // Get scheduled jobs within a time range
  getScheduledJobs(startTime: Date, endTime?: Date): Promise<Job[]>;
  // Clear all jobs
  clear(): Promise<void>;
}

export class RedisJobStorage implements RedisStorage {
  private readonly redis: Redis; // ioredis instance
  private readonly keyPrefix: string;
  private readonly jobListKey: string;
  private readonly priorityQueueKeys: { [priority: number]: string };
  private readonly scheduledJobsKey: string;
  private readonly logging: boolean = false;
  // Lua scripts
  private readonly saveJobScript: string;
  private readonly updateJobScript: string;
  private readonly moveScheduledJobsScript: string;
  private readonly completeJobScript: string;
  private readonly failJobScript: string;
  private readonly atomicAcquireScript: string;

  private readonly staleJobTimeout: number = 1000 * 60 * 60 * 24; // 24 hours

  /**
   * Create a new RedisJobStorage
   *
   * @param redis - An ioredis client instance
   * @param options - Configuration options
   */
  constructor(
    redis: Redis,
    options: { keyPrefix?: string; logging?: boolean; staleJobTimeout?: number } = {},
  ) {
    this.redis = redis;
    this.keyPrefix = options.keyPrefix || "jobqueue:";
    this.jobListKey = `${this.keyPrefix}jobs`;
    this.scheduledJobsKey = `${this.keyPrefix}scheduled`;
    this.priorityQueueKeys = {
      1: `${this.keyPrefix}priority:1`,
      2: `${this.keyPrefix}priority:2`,
      3: `${this.keyPrefix}priority:3`,
      4: `${this.keyPrefix}priority:4`,
      5: `${this.keyPrefix}priority:5`,
      6: `${this.keyPrefix}priority:6`,
      7: `${this.keyPrefix}priority:7`,
      8: `${this.keyPrefix}priority:8`,
      9: `${this.keyPrefix}priority:9`,
      10: `${this.keyPrefix}priority:10`,
    };
    this.logging = options.logging || false;
    this.staleJobTimeout = options.staleJobTimeout || 1000 * 60 * 60 * 24; // 24 hours
    this.saveJobScript = saveJobScript;
    this.updateJobScript = updateJobScript;
    this.moveScheduledJobsScript = this.moveScheduledJobsScript;
    this.completeJobScript = completeJobScript;
    this.failJobScript = failJobScript;
    this.atomicAcquireScript = atomicAcquireScript
  }

  async acquireNextJobs(batchSize: number): Promise<Job[]> {
    try {
      const jobs: Job[] = [];
      
      for (let i = 0; i < batchSize; i++) {
        const job = await this.acquireNextJob();
        if (!job) {
          break;
        }
        jobs.push(job);
      }

      if (this.logging && jobs.length > 0) {
        console.log(`[RedisJobStorage] Acquired ${jobs.length} jobs in batch`);
      }

      return jobs;
      
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error acquiring batch jobs (simple):`, error);
      }
      return [];
    }
  }

  /**
   * Save a job to Redis using a Lua script
   */
  async saveJob(job: Job): Promise<void> {
    try {
      const jobKey = this.getJobKey(job.id);
      const statusKey = this.getStatusKey(job.status);

      const serializedJob = this.serializeJob(job);

      const jobDataArray: string[] = [];
      Object.entries(serializedJob).forEach(([key, value]) => {
        jobDataArray.push(key, value);
      });
      const isScheduled =
        job.scheduledAt && job.scheduledAt > new Date() ? "1" : "0";
      const scheduledTime = job.scheduledAt
        ? job.scheduledAt.getTime().toString()
        : "0";
      const priority = String(job.priority || 3);

      await this.redis.eval(
        this.saveJobScript,
        4, // Number of keys
        jobKey,
        statusKey,
        this.scheduledJobsKey,
        this.keyPrefix + "priority:",
        job.id,
        job.status,
        priority,
        isScheduled,
        scheduledTime,
        ...jobDataArray,
      );
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error saving job:`, error);
      }
    }
  }

  /**
   * Get a job by ID
   */
  async getJob(id: string): Promise<Job | null> {
    try {
      const jobKey = this.getJobKey(id);
      const jobData = await this.redis.hgetall(jobKey);
      if (!jobData || Object.keys(jobData).length === 0) return null;
      return this.deserializeJob(jobData);
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error getting job:`, error);
      }
      return null;
    }
  }

  /**
   * Get jobs by status
   */
  async getJobsByStatus(status: JobStatus): Promise<Job[]> {
    try {
      const statusKey = this.getStatusKey(status);
      const jobIds = await this.redis.smembers(statusKey);
      if (!jobIds.length) return [];
      const jobs = await Promise.all(jobIds.map((id) => this.getJob(id)));
      return jobs.filter((job) => job !== null) as Job[];
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error getting jobs by status:`, error);
      }
      return [];
    }
  }

  /**
   * Update a job using a Lua script
   */
  async updateJob(job: Job): Promise<void> {
    try {
      const jobKey = this.getJobKey(job.id);
      const newStatusKey = this.getStatusKey(job.status);

      // Get current job to find its status
      const existingJob = await this.getJob(job.id);
      if (!existingJob) {
        throw new Error(`Job ${job.id} not found`);
      }

      // Serialize job data properly for Redis
      const serializedJob = this.serializeJob(job);

      // Convert serialized job to flattened array for Lua
      const jobDataArray: string[] = [];
      Object.entries(serializedJob).forEach(([key, value]) => {
        jobDataArray.push(key, value);
      });

      // Check if job is scheduled
      const isScheduled =
        job.scheduledAt && job.scheduledAt > new Date() ? "1" : "0";
      const scheduledTime = job.scheduledAt
        ? job.scheduledAt.getTime().toString()
        : "0";
      const priority = String(job.priority || 3);

      // Run the Lua script
      const result = await this.redis.eval(
        this.updateJobScript,
        5, // Number of keys
        jobKey,
        newStatusKey,
        this.scheduledJobsKey,
        this.keyPrefix + "priority:",
        this.keyPrefix + "status:", // Status set key prefix
        job.id,
        existingJob.status,
        job.status,
        priority,
        isScheduled,
        scheduledTime,
        ...jobDataArray,
      );

      // Handle error result from Lua script
      if (
        result &&
        typeof result === "object" &&
        result !== null &&
        "err" in result
      ) {
        throw new Error((result as { err: string }).err);
      }
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error updating job:`, error);
      }
      throw error;
    }
  }

  /**
   * Acquire the next job from the queue, respecting priorities and scheduled times
   * Also checks for stale jobs that have been processing for too long
   * @returns The next job or null if no job is available
   */
  async acquireNextJob(handlerNames?: string[]): Promise<Job | null> {
    try {
      const now = Date.now();
      
      const args = [
        String(now),
        String(this.staleJobTimeout),
        String(handlerNames?.length || 0),
        ...(handlerNames || [])
      ];

      const result = await this.redis.eval(
        this.atomicAcquireScript,
        4, // Number of keys
        this.keyPrefix + "priority:",     // KEYS[1]
        this.keyPrefix + "job:",          // KEYS[2] 
        this.keyPrefix + "status:",       // KEYS[3]
        this.scheduledJobsKey,            // KEYS[4]
        ...args                           // ARGV[1], ARGV[2], ARGV[3], ...
      ) as string | null;

      if (!result) {
        return null;
      }

      // Get the job data
      const job = await this.getJob(result);
      return job;

    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error acquiring next job:`, error);
      }
      return null;
    }
  }

  /**
   * Move scheduled jobs to priority queues using a Lua script
   * @private
   */
  private async moveScheduledJobs(): Promise<number> {
    try {
      const now = Date.now();

      // Run the Lua script
      const movedCount = await this.redis.eval(
        this.moveScheduledJobsScript,
        3, // Number of keys
        this.scheduledJobsKey,
        this.keyPrefix + "job:",
        this.keyPrefix + "priority:",
        String(now),
      );

      return movedCount as number;
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error moving scheduled jobs:`, error);
      }
      return 0;
    }
  }

  /**
   * Serialize a job object for Redis storage
   * @private
   */
  private serializeJob(job: Job): Record<string, string> {
    const serialized: Record<string, string> = {};

    serialized.id = job.id;
    serialized.name = job.name;
    serialized.status = job.status;
    serialized.priority = String(job.priority || 3);

    // Store dates as timestamps (milliseconds) for Lua compatibility
    if (job.createdAt) {
      serialized.createdAt = String(
        job.createdAt instanceof Date ? job.createdAt.getTime() : job.createdAt
      );
    }
    if (job.scheduledAt) {
      serialized.scheduledAt = String(
        job.scheduledAt instanceof Date ? job.scheduledAt.getTime() : job.scheduledAt
      );
    }
    if (job.startedAt) {
      serialized.startedAt = String(
        job.startedAt instanceof Date ? job.startedAt.getTime() : job.startedAt
      );
    }
    if (job.completedAt) {
      serialized.completedAt = String(
        job.completedAt instanceof Date ? job.completedAt.getTime() : job.completedAt
      );
    }

    if (job.data) serialized.data = JSON.stringify(job.data);
    if (job.result) serialized.result = JSON.stringify(job.result);
    if (job.error) serialized.error = job.error;
    if (job.retryCount !== undefined)
      serialized.retryCount = String(job.retryCount);
    if (job.repeat) serialized.repeat = JSON.stringify(job.repeat);
    if (job.timeout) serialized.timeout = String(job.timeout);

    return serialized;
  }

  /**
   * Deserialize a job object from Redis storage
   * @private
   */
  private deserializeJob(data: Record<string, string>): Job {
    const job: Job = {
      id: data.id || "",
      name: data.name,
      status: data.status as JobStatus,
      createdAt: new Date(),
      data: {},
    };

    if (data.priority) job.priority = parseInt(data.priority, 10);
    
    // Parse timestamps back to Date objects
    if (data.createdAt) {
      const timestamp = parseInt(data.createdAt, 10);
      job.createdAt = isNaN(timestamp) ? new Date(data.createdAt) : new Date(timestamp);
    }
    if (data.scheduledAt) {
      const timestamp = parseInt(data.scheduledAt, 10);
      job.scheduledAt = isNaN(timestamp) ? new Date(data.scheduledAt) : new Date(timestamp);
    }
    if (data.startedAt) {
      const timestamp = parseInt(data.startedAt, 10);
      job.startedAt = isNaN(timestamp) ? new Date(data.startedAt) : new Date(timestamp);
    }
    if (data.completedAt) {
      const timestamp = parseInt(data.completedAt, 10);
      job.completedAt = isNaN(timestamp) ? new Date(data.completedAt) : new Date(timestamp);
    }
    
    if (data.repeat) job.repeat = JSON.parse(data.repeat);
    if (data.data) {
      try {
        job.data = JSON.parse(data.data);
      } catch (e) {
        job.data = data.data;
      }
    }

    if (data.result) {
      try {
        job.result = JSON.parse(data.result);
      } catch (e) {
        job.result = data.result;
      }
    }

    if (data.error) job.error = data.error;
    if (data.retryCount) job.retryCount = parseInt(data.retryCount, 10);
    if (data.timeout) job.timeout = parseInt(data.timeout, 10);

    return job;
  }

  private getJobKey(id: string): string {
    return `${this.keyPrefix}job:${id}`;
  }
  private getStatusKey(status: JobStatus): string {
    return `${this.keyPrefix}status:${status}`;
  }

  /**
   * Get jobs by priority
   * @param priority - The priority level (1-5)
   * @returns Array of jobs with the specified priority
   */
  async getJobsByPriority(priority: number): Promise<Job[]> {
    try {
      if (!this.priorityQueueKeys[priority]) {
        throw new Error(
          `Invalid priority: ${priority}. Must be between 1 and 10.`,
        );
      }
      const jobIds = await this.redis.lrange(
        this.priorityQueueKeys[priority],
        0,
        -1,
      );
      if (!jobIds.length) return [];
      const jobs = await Promise.all(jobIds.map((id) => this.getJob(id)));
      return jobs.filter((job) => job !== null) as Job[];
    } catch (error) {
      if (this.logging) {
        console.error(
          `[RedisJobStorage] Error getting jobs by priority:`,
          error,
        );
      }
      return [];
    }
  }

  /**
   * Get scheduled jobs within a time range
   * @param startTime - Start of time range (inclusive)
   * @param endTime - End of time range (inclusive)
   * @returns Array of scheduled jobs within the time range
   */
  async getScheduledJobs(
    startTime: Date = new Date(),
    endTime?: Date,
  ): Promise<Job[]> {
    try {
      const start = startTime.getTime();
      const end = endTime ? endTime.getTime() : "+inf";

      const jobIds = await this.redis.zrangebyscore(
        this.scheduledJobsKey,
        start,
        end,
      );
      if (!jobIds.length) return [];

      const jobs = await Promise.all(jobIds.map((id) => this.getJob(id)));
      return jobs.filter((job) => job !== null) as Job[];
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error getting scheduled jobs:`, error);
      }
      return [];
    }
  }

  /**
   * Complete a job using a Lua script
   *
   * @param jobId - ID of the job to complete
   * @param result - Result of the job
   */
  async completeJob(jobId: string, result: any): Promise<void> {
    try {
      const jobKey = this.getJobKey(jobId);
      const serializedResult = JSON.stringify(result);
      const now = new Date().toISOString();

      // Run the Lua script to complete the job
      const scriptResult = await this.redis.eval(
        this.completeJobScript,
        2, // Number of keys
        jobKey,
        this.keyPrefix + "status:",
        jobId,
        serializedResult,
        now,
      );

      // Handle error result from Lua script
      if (
        scriptResult &&
        typeof scriptResult === "object" &&
        scriptResult !== null &&
        "err" in scriptResult
      ) {
        throw new Error((scriptResult as { err: string }).err);
      }
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error completing job:`, error);
      }
    }
  }

  /**
   * Fail a job using a Lua script
   *
   * @param jobId - ID of the job to fail
   * @param error - Error message
   */
  async failJob(jobId: string, error: string): Promise<void> {
    try {
      const jobKey = this.getJobKey(jobId);
      const now = new Date().toISOString();

      // Run the Lua script
      const scriptResult = await this.redis.eval(
        this.failJobScript,
        2, // Number of keys
        jobKey,
        this.keyPrefix + "status:",
        jobId,
        error,
        now,
      );

      // Handle error result from Lua script
      if (
        scriptResult &&
        typeof scriptResult === "object" &&
        scriptResult !== null &&
        "err" in scriptResult
      ) {
        throw new Error((scriptResult as { err: string }).err);
      }
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error failing job:`, error);
      }
    }
  }

  /**
   * Clear all jobs
   */
  async clear(): Promise<void> {
    try {
      await this.redis.flushall();
    } catch (error) {
      if (this.logging) {
        console.error(`[RedisJobStorage] Error clearing all jobs:`, error);
      }
    }
  }
}
