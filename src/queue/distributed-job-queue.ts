import { Job } from "../types";
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
  private queueName: string = "distributed-queue";

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
      logging?: boolean;
      intelligentPolling?: boolean;
      minInterval?: number;
      maxInterval?: number;
      maxEmptyPolls?: number;
      loadFactor?: number;
    } = {},
  ) {
    super(storage, options);
    this.redisStorage = storage;
    this.jobTTL = options.jobTTL || 30;
    this.logging = options.logging || false;
    this.queueName = options.name || "distributed-queue";
  }

  /**
   * Process jobs with distributed locking
   * Override the parent's protected method
   */
  protected async processNextBatch(): Promise<void> {
    try {
      if (this.activeJobs.size >= this.concurrency) {
        return;
      }
      const availableSlots = this.concurrency - this.activeJobs.size;
      let jobsProcessed = 0;
      for (let i = 0; i < availableSlots; i++) {
        const job = await this.redisStorage.acquireNextJob(this.jobTTL);
        if (!job) {
          break; // No more jobs available
        }
        if (this.logging) {
          console.log(`[${this.queueName}] Processing job:`, job);
          console.log(
            `[${this.queueName}] Available handlers:`,
            Array.from(this.handlers.keys()),
          );
          console.log(
            `[${this.queueName}] Has handler for ${job.name}:`,
            this.handlers.has(job.name),
          );
        }
        this.activeJobs.add(job.id);
        // Create a promise for processing this job
        this.processJob(job)
          .catch((error) => {
            console.error(
              `[${this.queueName}] Error processing job ${job.id}:`,
              error,
            );
          })
          .finally(() => {
            this.activeJobs.delete(job.id);
          });
        jobsProcessed++;
      }
      this.updatePollingInterval(jobsProcessed > 0);
    } catch (error) {
      console.error(`[${this.queueName}] Error in processNextBatch:`, error);
    }
  }

  /**
   * Process a single job with locking
   * This is called by the parent class
   */
  protected async processJob(job: Job): Promise<void> {
    try {
      if (this.logging) {
        console.log(
          `[${this.queueName}] Starting to process job ${job.id} (${job.name})`,
        );
      }
      // Check if handler exists before trying to process
      if (!this.handlers.has(job.name)) {
        throw new Error(`Handler for job "${job.name}" not found`);
      }
      // Get the handler using the parent's processJob method
      await super.processJob(job);
      if (this.logging && job.repeat) {
        console.log(`[${this.queueName}] Completed repeatable job ${job.id}`);
      }
    } catch (error) {
      // Log the error
      if (this.logging) {
        console.error(`[${this.queueName}] Error in processJob:`, error);
      }
      // Mark job as failed atomically
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      await this.redisStorage.failJob(job.id, errorMessage);
      throw error;
    }
  }
}
