import { Job, JobStatus } from "../types";
import { JobQueue } from "./job-queue";
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
      standAlone?: boolean;
      preFetchBatchSize?: number;
    } = {},
  ) {
    super(storage, options);
    this.redisStorage = storage;
    this.jobTTL = options.jobTTL || 30;
    this.logging = options.logging || false;
    this.queueName = options.name || "distributed-queue";
    this.standAlone = options.standAlone ?? true;
    this.preFetchBatchSize = options.preFetchBatchSize;
  }

    /**
   * Process jobs with prefetching
   * Override the parent's protected method
   */
  protected async processNextBatch(): Promise<void> {
    try {
      if (this.isStopping && this.logging) {
        console.log(`[${this.name}] Stopping job queue ... skipping`);
        return;
      }

      if (this.activeJobs.size >= this.concurrency || this.isStopping) {
        return;
      }

      if (this.preFetchBatchSize) {
        await this.refillJobBuffer();
      }

      const availableSlots = this.concurrency - this.activeJobs.size;
      let jobsProcessed = 0;

      const handlers = Array.from(this.handlers.keys());

      if (this.preFetchBatchSize) {
        // Process jobs from buffer
        for (let i = 0; i < availableSlots && this.jobBuffer.length > 0; i++) {
          const job = this.jobBuffer.shift()!;
          
          if (this.logging) {
            console.log(`[${this.name}] Processing prefetched job:`, job.id);
          }

          this.activeJobs.add(job.id);
          this.processJob(job)
            .catch((error) => {
              if (this.logging) {
                console.error("Error processing job", error);
              }
            })
            .finally(() => {
              this.activeJobs.delete(job.id);
            });
          jobsProcessed++;
        }
      } else {
        // Original single job processing
        for (let i = 0; i < availableSlots; i++) {
          const job = await this.redisStorage.acquireNextJob(handlers,this.jobTTL);
          if (!job) {
            break;
          }

          if (this.logging) {
            console.log(`[${this.name}] Processing job:`, job);
            console.log(
              `[${this.name}] Available handlers:`,
              Array.from(this.handlers.keys()),
            );
            console.log(
              `[${this.name}] Has handler for ${job.name}:`,
              this.handlers.has(job.name),
            );
          }

          this.activeJobs.add(job.id);
          this.processJob(job)
            .catch((error) => {
              if (this.logging) {
                console.error("Error processing job", error);
              }
            })
            .finally(() => {
              this.activeJobs.delete(job.id);
            });
          jobsProcessed++;
        }
      }

      this.updatePollingInterval(jobsProcessed > 0);
    } catch (error) {
      if (this.logging) {
        console.error(`[${this.name}] Error in processNextBatch:`, error);
      }
    }
  }
  

  protected async refillJobBuffer(): Promise<void> {
    const bufferThreshold = Math.max(1, Math.floor((this.preFetchBatchSize ?? 1) / 3));
    
    if (this.jobBuffer.length <= bufferThreshold) {
      const neededJobs = (this.preFetchBatchSize ?? 1) - this.jobBuffer.length;
      
      if (this.logging) {
        console.log(`[${this.name}] Refilling job buffer, need ${neededJobs} jobs`);
      }

      const newJobs = await this.redisStorage.acquireNextJobs(neededJobs);
      this.jobBuffer.push(...newJobs);

      if (this.logging && newJobs.length > 0) {
        console.log(`[${this.name}] Prefetched ${newJobs.length} jobs, buffer size: ${this.jobBuffer.length}`);
      }
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
  /**
  * Override stop to handle buffered jobs
  */
  async stop(): Promise<void> {
    if (this.logging) {
      console.log(`[${this.name}] Stopping queue, ${this.jobBuffer.length} jobs in buffer`);
    }
    
    for (const job of this.jobBuffer) {
      job.status = "pending" as JobStatus;
      job.startedAt = undefined;
      await this.redisStorage.updateJob(job);
    }
    this.jobBuffer.length = 0;

    await super.stop();
  }
}
