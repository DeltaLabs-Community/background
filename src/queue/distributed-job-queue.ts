import { Job } from "../types";
import { JobQueue } from "./job-queue";
import { JobStorage } from "../storage";
import { generateId } from "../utils/id-generator";

// Type for Redis storage adapter with locking
interface DistributedStorage extends JobStorage {
  acquireJobLock(jobId: string, instanceId: string, ttl?: number): Promise<boolean>;
  releaseJobLock(jobId: string, instanceId: string): Promise<boolean>;
}

/**
 * DistributedJobQueue extends JobQueue to provide distributed processing
 * capabilities across multiple instances/processes using a shared storage
 * like Redis.
 * 
 * Note: This requires a storage implementation that supports locking,
 * such as RedisJobStorage.
 */
export class DistributedJobQueue extends JobQueue {
  private readonly instanceId: string;
  private readonly distributedStorage: DistributedStorage;
  private readonly lockTTL: number = 30; // seconds
  
  /**
   * Create a distributed job queue
   * 
   * @param storage - A storage implementation that supports locking
   * @param options - Configuration options
   */
  constructor(
    storage: DistributedStorage,
    options: { 
      concurrency?: number;
      name?: string;
      instanceId?: string;
      lockTTL?: number;
      processingInterval?: number;
      maxRetries?: number;
    } = {}
  ) {
    super(storage, options);
    
    // Store reference to the storage with distributed capabilities
    this.distributedStorage = storage;
    this.instanceId = options.instanceId || generateId(8);
    this.lockTTL = options.lockTTL || 30;
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
    // Use the parent implementation to get jobs that need processing
    await super.processNextBatch();
  }
  
  /**
   * Process a single job with locking
   * This is called by the parent class
   */
  protected async processJob(job: Job): Promise<void> {
    // Try to acquire a lock on the job
    const lockAcquired = await this.distributedStorage.acquireJobLock(
      job.id,
      this.instanceId,
      this.lockTTL
    );
    
    // If we couldn't acquire the lock, another instance is processing this job
    if (!lockAcquired) {
      return;
    }
    
    try {
      // Call the parent implementation to process the job
      await super.processJob(job);
    } catch (error) {
      throw error;
    } finally {
      // Always release the lock when done
      await this.distributedStorage.releaseJobLock(job.id, this.instanceId);
    }
  }
} 