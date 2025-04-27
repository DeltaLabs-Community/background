import { Job} from "../types";
import { JobQueue } from "./job-queue";
import { generateId } from "../utils/id-generator";
import { QueueEvent } from "../utils/queue-event";
import { RedisStorage } from "../storage/redis-storage";
/**
 * DistributedJobQueue extends JobQueue to provide distributed processing
 * capabilities across multiple instances/processes using Redis atomic operations.
 */
export class DistributedJobQueue extends JobQueue {
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
      jobTTL?: number;
      processingInterval?: number;
      maxRetries?: number;
    } = {}
  ) {
    super(storage, options);
    this.redisStorage = storage;
    this.jobTTL = options.jobTTL || 30;
  }
  
  /**
   * Process jobs with distributed locking
   * Override the parent's protected method
   */
  protected async processNextBatch(): Promise<void> {
    // Skip if we're already processing the maximum number of jobs
    if (this.activeJobs.size >= this.concurrency) {
      console.log(`Already at max concurrency (${this.concurrency}), skipping batch.`);
      return;
    }
    
    const availableSlots = this.concurrency - this.activeJobs.size;

    // Try to acquire jobs atomically
    const processingPromises: Promise<void>[] = [];
    
    for (let i = 0; i < availableSlots; i++) {
      try {
        const job = await this.redisStorage.acquireNextJob(this.jobTTL);
        if (!job) {
          break; // No more jobs available
        }
        this.activeJobs.add(job.id);
        // Create a promise for processing this job
        const processingPromise = this.processJob(job)
          .catch(error => {
            console.error(`Error processing job ${job.id}:`, error);
          })
          .finally(() => {
            this.activeJobs.delete(job.id);
          });
        
        processingPromises.push(processingPromise);
      } catch (error) {
        console.error('Error acquiring job:', error);
      }
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
      await this.redisStorage.failJob(job.id, errorMessage);
      throw error;
    }
  }
} 