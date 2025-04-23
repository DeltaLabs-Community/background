import { JobStorage } from "../storage";
import { JobQueue } from "./job-queue";
import { PostgreSQLJobStorage } from "../storage/postgresql-storage";
import { Job, JobStatus } from "../types";

export class PostgreSQLJobQueue extends JobQueue {
    private readonly postgresStorage: PostgreSQLJobStorage;
    private readonly lockTTL: number = 30; // seconds
    
    /**
     * Create a PostgreSQL job queue
     * 
     * @param storage - PostgreSQL storage implementation
     * @param options - Configuration options
     */
    constructor(storage: JobStorage, options: { 
        lockTTL?: number, 
        concurrency?: number, 
        maxRetries?: number,
        name?: string,
        processingInterval?: number 
    } = {}) {
        super(storage, options);
        this.postgresStorage = storage as PostgreSQLJobStorage;
        this.lockTTL = options.lockTTL || 30;
        this.concurrency = options.concurrency || 1;
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
        const lockAcquired = await this.postgresStorage.acquireJobLock(
            job.id,
            this.lockTTL
        );
        if (!lockAcquired) {
            return;
        }
        try {
            await super.processJob(job);
        } catch (error) {
            const retryCount = job.retryCount || 0;
            
            if (retryCount < this.maxRetries) {
                const updatedJob: Job = { 
                    ...job, 
                    status: 'pending' as JobStatus,
                    retryCount: retryCount + 1,
                    error: `${error instanceof Error ? error.message : String(error)} (Retry ${retryCount + 1}/${this.maxRetries})`
                };
                await this.postgresStorage.updateJob(updatedJob);
            } else {
                job.status = 'failed';
                job.completedAt = new Date();
                job.error = `Failed after ${this.maxRetries} retries. Last error: ${error instanceof Error ? error.message : String(error)}`;
                await this.postgresStorage.updateJob(job);
            }
        } finally {
            await this.postgresStorage.releaseJobLock(job.id);
        }
    }
} 