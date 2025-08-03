import { JobStorage } from "../storage/base-storage";
import { JobQueue } from "./job-queue";
import { PostgreSQLJobStorage } from "../storage/postgresql-storage";
import { Job, JobStatus } from "../types";

export class PostgreSQLJobQueue extends JobQueue {
  private readonly postgresStorage: PostgreSQLJobStorage;
  private readonly jobBuffer: Job[] = []; // Prefetch buffer
  private readonly preFetchBatchSize: number | undefined;
  /**
   * Create a PostgreSQL job queue
   *
   * @param storage - PostgreSQL storage implementation
   * @param options - Configuration options
   */
  constructor(
    storage: JobStorage,
    options: {
      concurrency?: number;
      maxRetries?: number;
      name?: string;
      processingInterval?: number;
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
    this.postgresStorage = storage as PostgreSQLJobStorage;
    this.concurrency = options.concurrency || 1;
    this.logging = options.logging || false;
    this.standAlone = options.standAlone ?? true;
    this.preFetchBatchSize = options.preFetchBatchSize;
  }

  /**
   * Process jobs with distributed locking
   * Override the parent's protected method
   */
  protected async processNextBatch(): Promise<void> {
    try {
      if (this.isStopping && this.logging) {
        console.log(`[${this.name}] Stopping job queue ... skipping`);
      }
      if (this.activeJobs.size >= this.concurrency || this.isStopping) {
        return;
      }
      if(this.preFetchBatchSize){
        await this.refillJobBuffer();
      }
      const availableSlots = this.concurrency - this.activeJobs.size;
      let jobsProcessed = 0;
      if(this.preFetchBatchSize){
        for (let i = 0; i < availableSlots && this.jobBuffer.length > 0; i++) {
          const job = this.jobBuffer.shift()!;
          
          if (this.logging) {
            console.log(`[${this.name}] Processing prefetched job:`, job.id);
          }

          this.activeJobs.add(job.id);
          this.processJob(job).finally(() => {
            this.activeJobs.delete(job.id);
          });
          jobsProcessed++;
        }
      }
      else{
        for (let i = 0; i < availableSlots; i++) {
          const job = await this.postgresStorage.acquireNextJob();
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
          this.processJob(job).finally(() => {
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

   /**
   * Refill the job buffer when it's running low
   */
  private async refillJobBuffer(): Promise<void> {
    const bufferThreshold = Math.max(1, Math.floor(this.preFetchBatchSize ?? 1 / 3));
    
    if (this.jobBuffer.length <= bufferThreshold) {
      const neededJobs = this.preFetchBatchSize ?? 1 - this.jobBuffer.length;
      
      if (this.logging) {
        console.log(`[${this.name}] Refilling job buffer, need ${neededJobs} jobs`);
      }

      const newJobs = await this.postgresStorage.acquireNextJobs(neededJobs);
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
          `[${this.name}] Starting to process job ${job.id} (${job.name})`,
        );
      }

      await super.processJob(job);
      if (this.logging && job.repeat) {
        console.log(`[${this.name}] Completed repeatable job ${job.id}`);
      }
    } catch (error) {
      const retryCount = job.retryCount || 0;
      if (retryCount < this.maxRetries) {
        const updatedJob: Job = {
          ...job,
          status: "pending" as JobStatus,
          retryCount: retryCount + 1,
          error: `${error instanceof Error ? error.message : String(error)} (Retry ${retryCount + 1}/${this.maxRetries})`,
        };
        await this.postgresStorage.updateJob(updatedJob);
      } else {
        job.status = "failed";
        job.completedAt = new Date();
        job.error = `Failed after ${this.maxRetries} retries. Last error: ${error instanceof Error ? error.message : String(error)}`;
        await this.postgresStorage.updateJob(job);
      }
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
      await this.postgresStorage.updateJob(job);
    }
    this.jobBuffer.length = 0;

    await super.stop();
  }
}
