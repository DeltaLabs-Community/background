import { Job, JobHandler } from "../types";
import { JobQueue } from "./job-queue";
import { JobStorage } from "../storage";
import { generateId } from "../utils/id-generator";
import { QueueEvent } from "../utils/queue-event";

// Type for Redis storage adapter with atomic operations
interface RedisStorage extends JobStorage {
  // Atomic job acquisition
  acquireNextJob(instanceId: string, ttl?: number): Promise<Job | null>;
  // Atomic job completion
  completeJob(jobId: string, instanceId: string, result: any): Promise<void>;
  // Atomic job failure
  failJob(jobId: string, instanceId: string, error: string): Promise<void>;
}

/**
 * DistributedJobQueue extends JobQueue to provide distributed processing
 * capabilities across multiple instances/processes using Redis atomic operations.
 */
export class DistributedJobQueue extends JobQueue {
  private readonly instanceId: string;
  private readonly redisStorage: RedisStorage;
  private readonly jobTTL: number = 30; // seconds
  
  /**
   * Create a distributed job queue
   * 
   * @param storage - A storage implementation that supports atomic operations
   * @param options - Configuration options
   */
  constructor(
    storage: RedisStorage,
    options: { 
      concurrency?: number;
      name?: string;
      instanceId?: string;
      jobTTL?: number;
      processingInterval?: number;
      maxRetries?: number;
    } = {}
  ) {
    super(storage, options);
    
    this.redisStorage = storage;
    this.instanceId = options.instanceId || generateId(8);
    this.jobTTL = options.jobTTL || 30;
  }
  
  /**
   * Get the unique instance ID for this worker
   */
  getInstanceId(): string {
    return this.instanceId;
  }
  
  /**
   * Process jobs with distributed locking
   * Override the parent's protected method
   */
  protected async processNextBatch(): Promise<void> {
    // Skip if we're already processing the maximum number of jobs
    if (this.activeJobs.size >= this.concurrency) {
      return;
    }
    const availableSlots = this.concurrency - this.activeJobs.size;
    // Try to acquire jobs atomically
    for (let i = 0; i < availableSlots; i++) {
      const job = await this.redisStorage.acquireNextJob(this.instanceId, this.jobTTL);
      if (!job) {
        break; // No more jobs available
      }
      this.activeJobs.add(job.id);
      this.processJob(job).finally(() => {
        this.activeJobs.delete(job.id);
      });
    }
  }
  
  /**
   * Process a single job with locking
   * This is called by the parent class
   */
  protected async processJob(job: Job): Promise<void> {
    try {
      // Get the handler using the parent's processJob method
      await super.processJob(job);
    } catch (error) {
      // Mark job as failed atomically
      const errorMessage = error instanceof Error ? error.message : String(error);
      await this.redisStorage.failJob(job.id, this.instanceId, errorMessage);
      this.dispatchEvent(new QueueEvent('failed', job, 'failed'));
      throw error;
    }
  }
} 