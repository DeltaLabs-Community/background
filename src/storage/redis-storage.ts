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
  acquireNextJob(ttl?: number): Promise<Job | null>;
  // Atomic job completion
  completeJob(jobId: string, result: any): Promise<void>;
  // Atomic job failure
  failJob(jobId: string, error: string): Promise<void>;
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
  // Priority queues (separate lists for different priorities)
  private readonly priorityQueueKeys: { [priority: number]: string };
  // Sorted set for scheduled jobs
  private readonly scheduledJobsKey: string;
  
  // Lua scripts
  private readonly saveJobScript: string;
  private readonly updateJobScript: string;
  private readonly acquireJobScript: string;
  private readonly moveScheduledJobsScript: string;
  private readonly completeJobScript: string;
  private readonly failJobScript: string;

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
      10: `${this.keyPrefix}priority:10`
    };
    
    // Define Lua script for saveJob
    this.saveJobScript = `
      -- Arguments:
      -- KEYS[1]: Job key (hash)
      -- KEYS[2]: Status set key
      -- KEYS[3]: Scheduled set key
      -- KEYS[4]: Priority queue key prefix
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Job status
      -- ARGV[3]: Priority (default 3)
      -- ARGV[4]: Is scheduled (1 or 0)
      -- ARGV[5]: Scheduled time (timestamp)
      -- ARGV[6+]: Key-value pairs for job data (flattened)
      
      local jobKey = KEYS[1]
      local statusKey = KEYS[2]
      local scheduledKey = KEYS[3]
      local priorityKeyPrefix = KEYS[4]
      
      local jobId = ARGV[1]
      local status = ARGV[2]
      local priority = tonumber(ARGV[3]) or 3
      local isScheduled = ARGV[4] == "1"
      local scheduledTime = tonumber(ARGV[5]) or 0
      
      -- Store job hash fields
      for i = 6, #ARGV, 2 do
        redis.call('HSET', jobKey, ARGV[i], ARGV[i+1])
      end
      
      -- Add to status set
      redis.call('SADD', statusKey, jobId)
      
      -- Add to appropriate queue based on scheduling
      if isScheduled then
        redis.call('ZADD', scheduledKey, scheduledTime, jobId)
      else
        local priorityQueueKey = priorityKeyPrefix .. priority
        redis.call('LPUSH', priorityQueueKey, jobId)
      end
      
      return 1
    `;
    
    // Define Lua script for updateJob
    this.updateJobScript = `
      -- Arguments:
      -- KEYS[1]: Job key (hash)
      -- KEYS[2]: New status set key 
      -- KEYS[3]: Scheduled set key
      -- KEYS[4]: Priority queue key prefix
      -- KEYS[5]: Status set key prefix
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Current status (for old status set)
      -- ARGV[3]: New status
      -- ARGV[4]: Priority (default 3)
      -- ARGV[5]: Is scheduled (1 or 0)
      -- ARGV[6]: Scheduled time (timestamp)
      -- ARGV[7+]: Key-value pairs for job data (flattened)
      
      local jobKey = KEYS[1]
      local newStatusKey = KEYS[2]
      local scheduledKey = KEYS[3]
      local priorityKeyPrefix = KEYS[4]
      local statusKeyPrefix = KEYS[5]
      
      local jobId = ARGV[1]
      local oldStatus = ARGV[2]
      local newStatus = ARGV[3]
      local priority = tonumber(ARGV[4]) or 3
      local isScheduled = ARGV[5] == "1"
      local scheduledTime = tonumber(ARGV[6]) or 0
      
      -- Check if job exists
      if redis.call('EXISTS', jobKey) == 0 then
        return {err = "Job not found: " .. jobId}
      end
      
      -- Update job data
      for i = 7, #ARGV, 2 do
        redis.call('HSET', jobKey, ARGV[i], ARGV[i+1])
      end
      
      -- Update status sets if status changed
      if oldStatus ~= newStatus then
        local oldStatusKey = statusKeyPrefix .. oldStatus
        redis.call('SREM', oldStatusKey, jobId)
        redis.call('SADD', newStatusKey, jobId)
      end
      
      -- Handle queue placement based on status
      if newStatus == 'pending' then
        if isScheduled then
          -- Remove from all priority queues
          for p = 1, 10 do
            redis.call('LREM', priorityKeyPrefix .. p, 0, jobId)
          end
          -- Add to scheduled set
          redis.call('ZADD', scheduledKey, scheduledTime, jobId)
        else
          -- Remove from scheduled set
          redis.call('ZREM', scheduledKey, jobId)
          
          -- Remove from all other priority queues
          for p = 1, 10 do
            if p ~= priority then
              redis.call('LREM', priorityKeyPrefix .. p, 0, jobId)
            end
          end
          
          -- Add to correct priority queue
          redis.call('LPUSH', priorityKeyPrefix .. priority, jobId)
        end
      else
        -- If not pending, remove from all queues
        redis.call('ZREM', scheduledKey, jobId)
        for p = 1, 10 do
          redis.call('LREM', priorityKeyPrefix .. p, 0, jobId)
        end
      end
      
      return 1
    `;
    
    // Define Lua script for acquireNextJob
    this.acquireJobScript = `
      -- Arguments:
      -- KEYS[1]: Priority queue key prefix
      -- KEYS[2]: Job key prefix
      -- KEYS[3]: Status set key prefix
      
      -- ARGV[1]: Current timestamp (ISO string)
      -- ARGV[2]: TTL for job lock (seconds)
      
      -- Try to get a job from each priority queue in order
      local jobId = nil
      for priority = 1, 10 do
        local queueKey = KEYS[1] .. priority
        jobId = redis.call('RPOP', queueKey)
        if jobId then
          break
        end
      end
      
      if not jobId then
        return nil
      end
      
      -- Get the job data
      local jobKey = KEYS[2] .. jobId
      
      -- Check if job exists
      if redis.call('EXISTS', jobKey) == 0 then
        return nil
      end
      
      -- Get current status
      local status = redis.call('HGET', jobKey, 'status')
      
      -- Update job status and startedAt
      redis.call('HSET', jobKey, 'status', 'processing', 'startedAt', ARGV[1])
      
      -- Update status sets
      if status then
        redis.call('SREM', KEYS[3] .. status, jobId)
      end
      redis.call('SADD', KEYS[3] .. 'processing', jobId)
      
      -- Set expiry on job key (TTL)
      redis.call('EXPIRE', jobKey, ARGV[2])
      
      -- Return the job ID
      return jobId
    `;
    
    // Define Lua script for moveScheduledJobs
    this.moveScheduledJobsScript = `
      -- Arguments:
      -- KEYS[1]: Scheduled jobs sorted set
      -- KEYS[2]: Job key prefix
      -- KEYS[3]: Priority queue key prefix
      
      -- ARGV[1]: Current timestamp
      
      local scheduledKey = KEYS[1]
      local jobKeyPrefix = KEYS[2]
      local priorityKeyPrefix = KEYS[3]
      local now = tonumber(ARGV[1])
      
      -- Get all jobs scheduled before now
      local dueJobs = redis.call('ZRANGEBYSCORE', scheduledKey, 0, now)
      if #dueJobs == 0 then
        return 0  -- No jobs due yet
      end
      
      local movedCount = 0
      
      -- Process each due job
      for i, jobId in ipairs(dueJobs) do
        -- Remove job from scheduled set
        redis.call('ZREM', scheduledKey, jobId)
        
        -- Get job details to find priority
        local jobKey = jobKeyPrefix .. jobId
        
        -- Check if job still exists
        if redis.call('EXISTS', jobKey) == 1 then
          -- Get current status and priority
          local status = redis.call('HGET', jobKey, 'status')
          local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 3
          
          -- Only move to queue if job is still pending
          if status == 'pending' then
            -- Make sure priority is within bounds
            priority = math.max(1, math.min(10, priority))
            
            -- Add to appropriate priority queue
            redis.call('LPUSH', priorityKeyPrefix .. priority, jobId)
            movedCount = movedCount + 1
          end
        end
      end
      
      return movedCount
    `;
    
    // Define Lua script for completeJob
    this.completeJobScript = `
      -- Arguments:
      -- KEYS[1]: Job key
      -- KEYS[2]: Status set key prefix 
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Result (JSON string)
      -- ARGV[3]: Completion timestamp
      
      local jobKey = KEYS[1]
      local statusKeyPrefix = KEYS[2]
      
      local jobId = ARGV[1]
      local result = ARGV[2]
      local completedAt = ARGV[3]
      
      -- Check if job exists
      if redis.call('EXISTS', jobKey) == 0 then
        return {err = "Job not found: " .. jobId}
      end
      
      -- Get current status
      local currentStatus = redis.call('HGET', jobKey, 'status')
      
      -- Update job data
      redis.call('HSET', jobKey, 
        'status', 'completed',
        'result', result,
        'completedAt', completedAt
      )
      
      -- Update status sets
      if currentStatus then
        redis.call('SREM', statusKeyPrefix .. currentStatus, jobId)
      end
      redis.call('SADD', statusKeyPrefix .. 'completed', jobId)
      
      return 1
    `;
    
    // Define Lua script for failJob
    this.failJobScript = `
      -- Arguments:
      -- KEYS[1]: Job key
      -- KEYS[2]: Status set key prefix 
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Error message
      -- ARGV[3]: Completion timestamp
      
      local jobKey = KEYS[1]
      local statusKeyPrefix = KEYS[2]
      
      local jobId = ARGV[1]
      local errorMsg = ARGV[2]
      local completedAt = ARGV[3]
      
      -- Check if job exists
      if redis.call('EXISTS', jobKey) == 0 then
        return {err = "Job not found: " .. jobId}
      end
      
      -- Get current status
      local currentStatus = redis.call('HGET', jobKey, 'status')
      
      -- Update job data
      redis.call('HSET', jobKey, 
        'status', 'failed',
        'error', errorMsg,
        'completedAt', completedAt
      )
      
      -- Update status sets
      if currentStatus then
        redis.call('SREM', statusKeyPrefix .. currentStatus, jobId)
      end
      redis.call('SADD', statusKeyPrefix .. 'failed', jobId)
      
      return 1
    `;
  }

  /**
   * Save a job to Redis using a Lua script
   */
  async saveJob(job: Job): Promise<void> {
    const jobKey = this.getJobKey(job.id);
    const statusKey = this.getStatusKey(job.status);
    
    // Serialize job data properly for Redis
    const serializedJob = this.serializeJob(job);
    
    // Convert serialized job to flattened array for Lua
    const jobDataArray: string[] = [];
    Object.entries(serializedJob).forEach(([key, value]) => {
      jobDataArray.push(key, value);
    });
    
    // Check if job is scheduled
    const isScheduled = job.scheduledAt && job.scheduledAt > new Date() ? "1" : "0";
    const scheduledTime = job.scheduledAt ? job.scheduledAt.getTime().toString() : "0";
    const priority = String(job.priority || 3);
    
    // Run the Lua script
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
      ...jobDataArray
    );
  }

  /**
   * Get a job by ID
   */
  async getJob(id: string): Promise<Job | null> {
    const jobKey = this.getJobKey(id);
    const jobData = await this.redis.hgetall(jobKey);
    if (!jobData || Object.keys(jobData).length === 0) return null;
    return this.deserializeJob(jobData);
  }

  /**
   * Get jobs by status
   */
  async getJobsByStatus(status: JobStatus): Promise<Job[]> {
    const statusKey = this.getStatusKey(status);
    const jobIds = await this.redis.smembers(statusKey);
    if (!jobIds.length) return [];
    const jobs = await Promise.all(jobIds.map(id => this.getJob(id)));
    return jobs.filter(job => job !== null) as Job[];
  }

  /**
   * Update a job using a Lua script
   */
  async updateJob(job: Job): Promise<void> {
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
    const isScheduled = job.scheduledAt && job.scheduledAt > new Date() ? "1" : "0";
    const scheduledTime = job.scheduledAt ? job.scheduledAt.getTime().toString() : "0";
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
      ...jobDataArray
    );
    
    // Handle error result from Lua script
    if (result && typeof result === 'object' && result !== null && 'err' in result) {
      throw new Error((result as {err: string}).err);
    }
  }

  /**
   * Acquire the next job from the queue, respecting priorities and scheduled times
   * @returns The next job or null if no job is available
   */
  async acquireNextJob(ttl: number = 30): Promise<Job | null> {
    // First move scheduled jobs
    await this.moveScheduledJobs();
    
    // Run the Lua script to acquire a job
    const jobId = await this.redis.eval(
      this.acquireJobScript,
      3, // Number of keys
      this.keyPrefix + "priority:",    // Priority queue key prefix
      this.keyPrefix + "job:",         // Job key prefix
      this.keyPrefix + "status:",      // Status set key prefix
      new Date().toISOString(),        // Current timestamp
      String(ttl)                      // Job TTL
    );
    
    if (!jobId) {
      return null;
    }
    
    // Get the full job data
    const job = await this.getJob(jobId as string);
    
    // Ensure we got a valid job with an ID
    if (!job) {
      return null;
    }
    
    return job;
  }
  
  /**
   * Move scheduled jobs to priority queues using a Lua script
   * @private
   */
  private async moveScheduledJobs(): Promise<number> {
    const now = Date.now();
    
    // Run the Lua script
    const movedCount = await this.redis.eval(
      this.moveScheduledJobsScript,
      3, // Number of keys
      this.scheduledJobsKey,
      this.keyPrefix + "job:",
      this.keyPrefix + "priority:",
      String(now)
    );
    
    return movedCount as number;
  }
  
  /**
   * Process a job that has been acquired
   * @private
   */
  private async processAcquiredJob(jobId: string, ttl: number): Promise<Job | null> {
    const jobKey = this.getJobKey(jobId);
    
    // Get job data
    const jobData = await this.redis.hgetall(jobKey);
    
    if (!jobData || Object.keys(jobData).length === 0) return null;
    
    const job = this.deserializeJob(jobData);
    
    // Ensure the job ID is set correctly
    job.id = jobId;
    
    // Update job status
    job.status = 'processing';
    job.startedAt = new Date();
    
    // Update job
    await this.updateJob(job);
    
    // Set expiry on job key
    await this.redis.expire(jobKey, ttl);
    
    return job;
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
    
    if (job.createdAt) serialized.createdAt = job.createdAt.toISOString();
    if (job.scheduledAt) serialized.scheduledAt = job.scheduledAt.toISOString();
    if (job.startedAt) serialized.startedAt = job.startedAt.toISOString();
    if (job.completedAt) serialized.completedAt = job.completedAt.toISOString();
    
    if (job.data) serialized.data = JSON.stringify(job.data);
    if (job.result) serialized.result = JSON.stringify(job.result);
    if (job.error) serialized.error = job.error;
    if (job.retryCount !== undefined) serialized.retryCount = String(job.retryCount);
    
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
      createdAt: data.createdAt ? new Date(data.createdAt) : new Date(),
      data: {}
    };
    
    if (data.priority) job.priority = parseInt(data.priority, 10);
    if (data.scheduledAt) job.scheduledAt = new Date(data.scheduledAt);
    if (data.startedAt) job.startedAt = new Date(data.startedAt);
    if (data.completedAt) job.completedAt = new Date(data.completedAt);
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
    if (!this.priorityQueueKeys[priority]) {
      throw new Error(`Invalid priority: ${priority}. Must be between 1 and 10.`);
    }
    const jobIds = await this.redis.lrange(this.priorityQueueKeys[priority], 0, -1);
    if (!jobIds.length) return [];
    const jobs = await Promise.all(jobIds.map(id => this.getJob(id)));
    return jobs.filter(job => job !== null) as Job[];
  }

  /**
   * Get scheduled jobs within a time range
   * @param startTime - Start of time range (inclusive)
   * @param endTime - End of time range (inclusive)
   * @returns Array of scheduled jobs within the time range
   */
  async getScheduledJobs(startTime: Date = new Date(), endTime?: Date): Promise<Job[]> {
    const start = startTime.getTime();
    const end = endTime ? endTime.getTime() : '+inf';
    
    const jobIds = await this.redis.zrangebyscore(this.scheduledJobsKey, start, end);
    if (!jobIds.length) return [];
    
    const jobs = await Promise.all(jobIds.map(id => this.getJob(id)));
    return jobs.filter(job => job !== null) as Job[];
  }

  /**
   * Complete a job using a Lua script
   * 
   * @param jobId - ID of the job to complete
   * @param result - Result of the job
   */
  async completeJob(jobId: string, result: any): Promise<void> {
    const jobKey = this.getJobKey(jobId);
    const serializedResult = JSON.stringify(result);
    const now = new Date().toISOString();
    
    // Run the Lua script
    const scriptResult = await this.redis.eval(
      this.completeJobScript,
      2, // Number of keys
      jobKey,
      this.keyPrefix + "status:",
      jobId,
      serializedResult,
      now
    );
    
    // Handle error result from Lua script
    if (scriptResult && typeof scriptResult === 'object' && scriptResult !== null && 'err' in scriptResult) {
      throw new Error((scriptResult as {err: string}).err);
    }
  }
  
  /**
   * Fail a job using a Lua script
   * 
   * @param jobId - ID of the job to fail
   * @param error - Error message
   */
  async failJob(jobId: string, error: string): Promise<void> {
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
      now
    );
    
    // Handle error result from Lua script
    if (scriptResult && typeof scriptResult === 'object' && scriptResult !== null && 'err' in scriptResult) {
      throw new Error((scriptResult as {err: string}).err);
    }
  }

  /**
   * Clear all jobs
   */
  async clear(): Promise<void> {
    await this.redis.flushall();
  }
} 