import { Job, JobStatus } from "../types";
import { JobStorage } from "../storage/base-storage";
import { JobHandler } from "../types";
import { generateId } from "../utils/id-generator";
import { QueueEvent } from "../utils/queue-event";
export class JobQueue extends EventTarget {
  /**
   * Job handlers registered with this queue
   */
  protected handlers: Map<string, JobHandler> = new Map();
  protected storage: JobStorage;
  /**
   * Set of job IDs that are currently being processed
   */
  protected activeJobs: Set<string> = new Set();
  protected readonly jobBuffer: Job[] = []; // Prefetch buffer
  protected preFetchBatchSize: number | undefined;
  /**
   * Number of jobs that can be processed concurrently
   */
  protected concurrency: number;
  /**
   * Interval in milliseconds at which to check for new jobs
   */
  private processing: boolean = false;
  private processingInterval: number = 1000; // 1 second
  private intervalId?: any = null; // For Universal JS
  protected name: string;
  protected maxRetries: number = 3;
  protected logging: boolean = false;
  private lastPollingInterval: number = 0;
  private pollingErrorCount: number = 0;
  // Intelligent polling properties
  private intelligentPolling: boolean = false;
  private minInterval: number = 100; // Minimum polling interval (ms)
  private maxInterval: number = 5000; // Maximum polling interval (ms)
  private emptyPollsCount: number = 0;
  private maxEmptyPolls: number = 5; // Number of empty polls before increasing interval
  private loadFactor: number = 0.5; // Target load factor (0.0 to 1.0)
  private maxConcurrency: number = 10; // Maximum number of jobs that can be processed concurrently

  // Stopping properties
  protected isStopping: boolean = false;
  private isUpdatingInterval = false;
  private isStopped: boolean = false;


  constructor(
    storage: JobStorage,
    options: {
      concurrency?: number;
      name?: string;
      processingInterval?: number;
      maxRetries?: number;
      logging?: boolean;
      intelligentPolling?: boolean;
      minInterval?: number;
      maxInterval?: number;
      maxEmptyPolls?: number;
      loadFactor?: number;
      maxConcurrency?: number;
      preFetchBatchSize?: number;
    } = {},
  ) {
    super();
    this.storage = storage;
    this.concurrency = options.concurrency || 1;
    this.name = options.name || "default";
    this.processingInterval = options.processingInterval || 1000;
    this.maxRetries = options.maxRetries || 3;
    this.logging = options.logging || false;
    this.lastPollingInterval = this.processingInterval;
    this.pollingErrorCount = 0;
    this.preFetchBatchSize = options.preFetchBatchSize;
    // Intelligent polling configuration
    this.intelligentPolling = options.intelligentPolling || false;
    if (this.intelligentPolling) {
      this.minInterval = options.minInterval || 100;
      this.maxInterval = options.maxInterval || 5000;
      this.maxEmptyPolls = options.maxEmptyPolls || 5;
      this.loadFactor = options.loadFactor || 0.5;
      this.maxConcurrency = options.maxConcurrency || 10;
    }
  }

  // Register a job handler
  register<T, R>(name: string, handler: JobHandler<T, R>): void {
    this.handlers.set(name, handler);
  }

  // Add a job to the queue
  async add<T>(
    name: string,
    data: T,
    options?: { priority?: number , timeout?: number},
  ): Promise<Job<T>> {
    if (this.isStopped) {
      throw new Error("Queue is stopped");
    }
    const priority = options?.priority || 1;
    const job: Job<T> = {
      id: generateId(),
      name,
      data,
      status: "pending",
      createdAt: new Date(),
      priority,
      timeout: options?.timeout || 10000,
    };

    if (this.logging) {
      console.log(
        `[${this.name}] Scheduled job ${job.id} to run at ${job.createdAt}`,
      );
    }

    await this.storage.saveJob(job);
    this.dispatchEvent(new QueueEvent("scheduled", { job, status: "pending" }));
    return job;
  }

  // Schedule a job to run at a specific time
  async schedule<T>(name: string, data: T, scheduledAt: Date, options?: { timeout?: number }): Promise<Job<T>> {
    if (this.isStopped) {
      throw new Error("Queue is stopped");
    }

    if (scheduledAt < new Date()) {
      throw new Error("Scheduled time must be in the future");
    }

    const job: Job<T> = {
      id: generateId(),
      name,
      data,
      status: "pending",
      createdAt: new Date(),
      scheduledAt,
      timeout: options?.timeout || 10000,
    };

    if (this.logging) {
      console.log(
        `[${this.name}] Scheduled job ${job.id} to run at ${scheduledAt}`,
      );
    }

    await this.storage.saveJob(job);
    this.dispatchEvent(new QueueEvent("scheduled", { job, status: "pending" }));
    return job;
  }

  // Schedule a job to run after a delay (in milliseconds)
  async scheduleIn<T>(name: string, data: T, delayMs: number): Promise<Job<T>> {
    const scheduledAt = new Date(Date.now() + delayMs);
    return this.schedule(name, data, scheduledAt);
  }

  // Get a job by ID
  async getJob<T>(id: string): Promise<Job<T> | null> {
    return this.storage.getJob(id);
  }

  // Get the name of the queue
  getName(): string {
    return this.name;
  }

  // Start processing jobs
  start(): void {
    if (this.processing || this.isStopping) return;

    if (this.logging) {
      console.log(`[${this.name}] Starting job queue`);
    }

    this.processing = true;
    this.intervalId = setInterval(
      () => this.processNextBatch(),
      this.processingInterval,
    );
  }

  async rollbackActiveJobs(): Promise<void> {
    if (this.logging) {
      console.log(`[${this.name}] Rolling back ${this.activeJobs.size} active jobs`);
    }
    
    const activeJobIds = [...this.activeJobs];
    
    for (const jobId of activeJobIds) {
      try {
        const job = await this.storage.getJob(jobId);
        if (job) {
          job.status = "pending";
          await this.storage.updateJob(job);
          if (this.logging) {
            console.log(`[${this.name}] Rolled back job ${jobId}`);
          }
        } else {
          if (this.logging) {
            console.log(`[${this.name}] Could not find job ${jobId} to roll back`);
          }
        }
      } catch (error) {
        if (this.logging) {
          console.error(`[${this.name}] Error rolling back job ${jobId}:`, error);
        }
      }
    }
  }

  // Stop processing jobs
  async stop(): Promise<void> {
    if (!this.processing) return;
    this.isStopping = true;
    if (this.logging) {
      console.log(`[${this.name}] Stopping job queue`);
    }
    
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }

    if (this.logging) {
      console.log(`[${this.name}] Job queue stopped`);
    }

    for (const job of this.jobBuffer) {
      job.status = "pending" as JobStatus;
      job.startedAt = undefined;
      await this.storage.updateJob(job);
    }
    this.jobBuffer.length = 0;

    this.processing = false;
    this.isStopping = false;
    this.isStopped = true;
  }

  // Set processing interval
  setProcessingInterval(ms: number): void {
    this.processingInterval = ms;
    if (this.processing && this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = setInterval(
        () => this.processNextBatch(),
        this.processingInterval,
      );
    }
  }

  // Set concurrency level
  setConcurrency(level: number): void {
    if (level < 1) {
      throw new Error("Concurrency level must be at least 1");
    }
    this.concurrency = level;
  }

  // Process the next batch of pending jobs
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

      if (this.preFetchBatchSize) {
        // Process jobs from buffer
        for (let i = 0; i < availableSlots && this.jobBuffer.length > 0; i++) {
          const job = this.jobBuffer.shift()!;
          
          if (this.logging) {
            console.log(`[${this.name}] Processing prefetched job:`, job.id);
          }

          if(!this.handlers.has(job.name)){
            if (this.logging){
              console.log(`[${this.name}] Job with no handler found: ${job.id}`)
              console.log(`[${this.name}] Resetting Job status...`)
            }
            job.status = "pending" as JobStatus;
            job.startedAt = undefined;
            this.storage.updateJob(job)
            .then(() => {
              if (this.logging) {
                console.log(`[${this.name}] Job status reset: ${job.id}`);
              }
            })
            .catch((error) => {
              if (this.logging) {
                console.error("Error resetting job status", error);
              }
            })
            this.activeJobs.delete(job.id);
            continue;
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
        for (let i = 0; i < availableSlots; i++) {
          const job = await this.storage.acquireNextJob();
          if (!job) {
            break;
          }

          if(!this.handlers.has(job.name)){
            if (this.logging){
              console.log(`[${this.name}] Job with no handler found: ${job.id}`)
              console.log(`[${this.name}] Resetting Job status...`)
            }
            job.status = "pending" as JobStatus;
            job.startedAt = undefined;
            this.storage.updateJob(job)
            .then(() => {
              if (this.logging) {
                console.log(`[${this.name}] Job status reset: ${job.id}`);
              }
            })
            .catch((error) => {
              if (this.logging) {
                console.error("Error resetting job status", error);
              }
            })
            this.activeJobs.delete(job.id);
            continue;
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

  /**
   * Refill the job buffer when it's running low
   */
  protected async refillJobBuffer(): Promise<void> {
    const bufferThreshold = Math.max(1, Math.floor((this.preFetchBatchSize ?? 1) / 3));
    
    if (this.jobBuffer.length <= bufferThreshold) {
      const neededJobs = (this.preFetchBatchSize ?? 1) - this.jobBuffer.length;
      
      if (this.logging) {
        console.log(`[${this.name}] Refilling job buffer, need ${neededJobs} jobs`);
      }

      const newJobs = await this.storage.acquireNextJobs(neededJobs);
      this.jobBuffer.push(...newJobs);

      if (this.logging && newJobs.length > 0) {
        console.log(`[${this.name}] Prefetched ${newJobs.length} jobs, buffer size: ${this.jobBuffer.length}`);
      }
    }
  }

  // Update polling interval based on processing results
  protected updatePollingInterval(hadJobs: boolean): void {
    if (this.isStopped) {
      if (this.logging) {
        console.log(`[${this.name}] Queue is stopped, skipping`);
      }
      return;
    }
    if (this.isUpdatingInterval) return;

    try {
      this.isUpdatingInterval = true;
      if (!this.intelligentPolling) {
        return; // Skip intelligent polling if disabled
      }
      if (hadJobs) {
        // Jobs were found and processed
        this.emptyPollsCount = 0;

        // Calculate current load factor
        const currentLoad = this.activeJobs.size / this.concurrency;

        // Adjust interval based on load
        if (currentLoad > this.loadFactor) {
          // System is busy, poll more frequently
          this.processingInterval = Math.max(
            this.minInterval,
            this.lastPollingInterval * 0.8,
          );
          if (this.concurrency < this.maxConcurrency) {
            this.concurrency = Math.min(
              this.maxConcurrency,
              Math.ceil(this.concurrency * 1.2),
            );
          }
        } else {
          // System is underutilized, poll less frequently
          this.processingInterval = Math.min(
            this.maxInterval,
            this.lastPollingInterval * 1.2,
          );
          this.concurrency = Math.max(1, Math.floor(this.concurrency * 0.8));
        }
      } else {
        // No jobs were found
        this.emptyPollsCount++;

        if (this.emptyPollsCount >= this.maxEmptyPolls) {
          // Gradually increase interval when queue is empty
          this.processingInterval = Math.min(
            this.maxInterval,
            this.lastPollingInterval * 1.5,
          );
          this.concurrency = Math.max(1, Math.floor(this.concurrency * 0.8));
          this.emptyPollsCount = 0;
        }
      }

      // Update the interval if queue is running
      if (
        this.processing &&
        this.intervalId &&
        this.processingInterval !== this.lastPollingInterval
      ) {
        clearInterval(this.intervalId);
        this.lastPollingInterval = this.processingInterval;
        this.intervalId = setInterval(
          () => this.processNextBatch(),
          this.processingInterval,
        );
        this.dispatchEvent(
          new QueueEvent("polling-interval-updated", {
            message: `Polling interval adjusted to: ${this.processingInterval}ms. Concurrency: ${this.concurrency}`,
          }),
        );
        if (this.logging) {
          console.log(
            `[${this.name}] Polling interval adjusted to: ${this.processingInterval}ms. Concurrency: ${this.concurrency}`,
          );
        }
      }
    } catch (error) {
      if (this.logging) {
        console.error(`[${this.name}] Error in updatePollingInterval:`, error);
        this.pollingErrorCount++;
        if (this.pollingErrorCount >= 5) {
          this.intelligentPolling = false;
          console.log(
            `[${this.name}] Intelligent polling disabled due to errors`,
          );
        }
        this.dispatchEvent(
          new QueueEvent("polling-interval-error", {
            message: `Polling interval error: ${error}`,
          }),
        );
      }
    } finally {
      this.isUpdatingInterval = false;
    }
  }

  // Process a single job
  protected async processJob(job: Job): Promise<void> {
    try {
      if (this.isStopping) {
        console.log(`[${this.name}] Queue is stopping, skipping job ${job.id}`);
        return;
      }
      
      if (job.status !== "processing") {
        // Mark job as processing
        job.status = "processing";
        job.startedAt = new Date();
        await this.storage.updateJob(job);
      }

      // Get the handler
      const handler = this.handlers.get(job.name);
      if (!handler) {
        throw new Error(`Handler for job "${job.name}" not found`);
      }


      const result = await Promise.race([
        handler(job.data),
        new Promise((_, reject) => {
          const timeoutMs = job.timeout || 10000;
          const timeoutId = setTimeout(() => {
            reject(new Error(`Job timeout exceeded (${timeoutMs}ms)`));
          }, timeoutMs);
          // Ensure timeoutId is used to prevent optimization
          if (this.logging) {
            console.log(`[${this.name}] Set timeout ${timeoutId} for job ${job.id} (${timeoutMs}ms)`);
          }
        })
      ]);

      if (this.isStopping) {
        console.log(`[${this.name}] Queue is stopping, skipping job ${job.id}`);
        return;
      }

      await this.storage.completeJob(job.id, result);
      this.dispatchEvent(
        new QueueEvent("completed", { job, status: "completed" }),
      );

      if (this.logging) {
        console.log(`[${this.name}] Completed job ${job.id}`);
      }
        
      if (job.repeat && !this.isStopping) {
        await this.scheduleNextRepeat(job);
      }

    } catch (error) {
      if (this.logging) {
        console.error(`[${this.name}] Error processing job`);
      }
      this.dispatchEvent(new QueueEvent("failed", { job, status: "failed" }));
      throw error;
    }
  }

  // Schedule the next occurrence of a repeatable job
  protected async scheduleNextRepeat(job: Job): Promise<void> {
    try {
      if (!job.repeat) return;

      // Check if we've reached the repeat limit
      if (job.repeat.limit) {
        const executionCount = (job.retryCount || 0) + 1;
        if (executionCount >= job.repeat.limit) {
          return; // Don't schedule another repeat
        }
      }
      // Check if we've passed the end date
      if (job.repeat.endDate && new Date() > job.repeat.endDate) {
        return; // Don't schedule another repeat
      }

      const nextExecutionTime = this.calculateNextExecutionTime(job);

      if (!nextExecutionTime) {
        return;
      }

      // Create a new job with the same parameters
      const newJob: Job = {
        id: generateId(),
        name: job.name,
        data: job.data,
        status: "pending",
        createdAt: new Date(),
        scheduledAt: nextExecutionTime,
        priority: job.priority,
        retryCount: (job.retryCount || 0) + 1,
        repeat: job.repeat,
        timeout: job.timeout,
      };
      if (this.logging) {
        console.log(
          `[${this.name}] Scheduled repeatable job ${newJob.id} to run at ${nextExecutionTime}`,
        );
      }
      await this.storage.saveJob(newJob);
      this.dispatchEvent(
        new QueueEvent("scheduled", { job: newJob, status: "pending" }),
      );
    } catch (error) {
      if (this.logging) {
        console.error(`[${this.name}] Error in scheduleNextRepeat:`, error);
      }
      this.dispatchEvent(
        new QueueEvent("scheduled-repeat-error", {
          message: `Scheduled repeat error: ${error}`,
        }),
      );
    }
  }

  // Calculate the next execution time based on the repeat configuration
  protected calculateNextExecutionTime(job: Job): Date | undefined {
    try {
      if (!job.repeat) {
        throw new Error("Job does not have repeat configuration");
      }

      const now = new Date();
      const { every, unit } = job.repeat;

      if (every === undefined || unit === undefined) {
        throw new Error("Invalid repeat configuration: missing every or unit");
      }

      let nextTime = new Date(now);

      switch (unit) {
        case "seconds":
          nextTime.setSeconds(nextTime.getSeconds() + every);
          break;
        case "minutes":
          nextTime.setMinutes(nextTime.getMinutes() + every);
          break;
        case "hours":
          nextTime.setHours(nextTime.getHours() + every);
          break;
        case "days":
          nextTime.setDate(nextTime.getDate() + every);
          break;
        case "weeks":
          nextTime.setDate(nextTime.getDate() + every * 7);
          break;
        case "months":
          nextTime.setMonth(nextTime.getMonth() + every);
          break;
        default:
          throw new Error(`Unsupported repeat unit: ${unit}`);
      }

      return nextTime;
    } catch (error) {
      if (this.logging) {
        console.error(
          `[${this.name}] Error in calculateNextExecutionTime:`,
          error,
        );
      }
      this.dispatchEvent(
        new QueueEvent("calculate-next-execution-time-error", {
          message: `Calculate next execution time error: ${error}`,
        }),
      );
    }
  }

  // Add a repeatable job to the queue
  async addRepeatable<T>(
    name: string,
    data: T,
    options: {
      every: number;
      unit: "seconds" | "minutes" | "hours" | "days" | "weeks" | "months";
      startDate?: Date;
      endDate?: Date;
      limit?: number;
      priority?: number;
      timeout?: number;
    },
  ): Promise<Job<T>> {
    if (this.isStopped) {
      throw new Error("Queue is stopped");
    }

    // Validate options
    if (options.every <= 0) {
      if (this.logging) {
        console.log(`[${this.name}] Repeat interval must be greater than 0`);
      }
      throw new Error("Repeat interval must be greater than 0");
    }

    const priority = options.priority || 0;
    const job: Job<T> = {
      id: generateId(),
      name,
      data,
      status: "pending",
      createdAt: new Date(),
      priority,
      repeat: {
        every: options.every,
        unit: options.unit,
        startDate: options.startDate || undefined,
        endDate: options.endDate || undefined,
        limit: options.limit || undefined,
      },
      timeout: options.timeout || undefined,
    };

    if (this.logging) {
      console.log(
        `[${this.name}] Scheduled repeatable job ${job.id} to run at ${job.createdAt}`,
      );
    }

    // Schedule the job to start at the specified time or now
    if (options.startDate && options.startDate > new Date()) {
      job.scheduledAt = options.startDate;
    }

    await this.storage.saveJob(job);
    this.dispatchEvent(new QueueEvent("scheduled", { job, status: "pending" }));
    return job;
  }
}
