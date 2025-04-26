import { JobStorage } from "../storage/base-storage";
import { JobQueue } from "./job-queue";
import { MongoDBJobStorage } from "../storage/mongodb-storage";
import { Job, JobStatus } from "../types";

export class MongoDBJobQueue extends JobQueue {
    private readonly mongodbStorage: MongoDBJobStorage;
    constructor(storage: JobStorage, options: {concurrency?: number, maxRetries?: number,name?: string, processingInterval?: number } = {}) {
        super(storage, options);
        this.mongodbStorage = storage as MongoDBJobStorage;
        this.concurrency = options.concurrency || 1;
    }
    /**
     * Process jobs with distributed locking
     * Override the parent's protected method
     */
    protected async processNextBatch(): Promise<void> {
        if(this.activeJobs.size >= this.concurrency) {
            return;
        }
        const availableSlots = this.concurrency - this.activeJobs.size;
        for(let i = 0; i < availableSlots; i++) {
            const job = await this.mongodbStorage.acquireNextJob();
            if(!job) {
              break;
            }
            this.activeJobs.add(job.id);
            this.processJob(job)
            .catch((error) => {
                console.error("Error processing job", error);
            })
            .finally(() => {
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
      await super.processJob(job);
    } catch (error) {
      const retryCount = job.retryCount || 0;
      // Retry job if it has not failed too many times
      if (retryCount < this.maxRetries) {
        const updatedJob: Job = { 
          ...job, 
          status: 'pending' as JobStatus,
          retryCount: retryCount + 1,
          error: `${error instanceof Error ? error.message : String(error)} (Retry ${retryCount + 1}/${this.maxRetries})`
        };
        await this.mongodbStorage.updateJob(updatedJob);
      } else {
        job.status = 'failed';
        job.completedAt = new Date();
        job.error = `Failed after ${this.maxRetries} retries. Last error: ${error instanceof Error ? error.message : String(error)}`;
        await this.mongodbStorage.updateJob(job);
      }
    } 
  }
}