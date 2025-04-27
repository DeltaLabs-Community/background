import { Job } from "../types";
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

  constructor(
    storage: JobStorage,
    options: {
      concurrency?: number;
      name?: string;
      processingInterval?: number;
      maxRetries?: number;
      logging?: boolean;
    } = {},
  ) {
    super();
    this.storage = storage;
    this.concurrency = options.concurrency || 1;
    this.name = options.name || "default";
    this.processingInterval = options.processingInterval || 1000;
    this.maxRetries = options.maxRetries || 3;
    this.logging = options.logging || false;
  }

  // Register a job handler
  register<T, R>(name: string, handler: JobHandler<T, R>): void {
    this.handlers.set(name, handler);
  }

  // Add a job to the queue
  async add<T>(
    name: string,
    data: T,
    options?: { priority?: number },
  ): Promise<Job<T>> {
    if (!this.handlers.has(name)) {
      throw new Error(`Job handler for "${name}" not registered`);
    }
    const priority = options?.priority || 1;
    const job: Job<T> = {
      id: generateId(),
      name,
      data,
      status: "pending",
      createdAt: new Date(),
      priority,
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
  async schedule<T>(name: string, data: T, scheduledAt: Date): Promise<Job<T>> {
    if (!this.handlers.has(name)) {
      throw new Error(`Job handler for "${name}" not registered`);
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
    if (this.processing) return;

    if (this.logging) {
      console.log(`[${this.name}] Starting job queue`);
    }

    this.processing = true;
    this.intervalId = setInterval(
      () => this.processNextBatch(),
      this.processingInterval,
    );
  }

  // Stop processing jobs
  stop(): void {
    if (!this.processing) return;

    if (this.logging) {
      console.log(`[${this.name}] Stopping job queue`);
    }

    this.processing = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
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
  protected async processNextBatch(): Promise<void> {
    try {
      // Skip if we're already processing the maximum number of jobs
      if (this.activeJobs.size >= this.concurrency) {
        return;
      }
      const availableSlots = this.concurrency - this.activeJobs.size;
      for (let i = 0; i < availableSlots; i++) {
        const job = await this.storage.acquireNextJob();
        if (!job) {
          break;
        }
        // Skip if job is already being processed
        if (this.activeJobs.has(job.id)) {
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
        this.processJob(job).finally(() => {
          this.activeJobs.delete(job.id);
        });
      }
    } catch (error) {
      if (this.logging) {
        console.error(`[${this.name}] Error in processNextBatch:`, error);
      }
    }
  }

  // Process a single job
  protected async processJob(job: Job): Promise<void> {
    try {
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

      // Execute the handler
      const result = await handler(job.data);

      // Mark job as completed
      await this.storage.completeJob(job.id, result);
      this.dispatchEvent(
        new QueueEvent("completed", { job, status: "completed" }),
      );

      if (this.logging) {
        console.log(`[${this.name}] Completed job ${job.id}`);
      }
      // Handle repeatable jobs
      if (job.repeat) {
        await this.scheduleNextRepeat(job);
      }
    } catch (error) {
      this.dispatchEvent(new QueueEvent("failed", { job, status: "failed" }));
      if (this.logging) {
        console.log(`[${this.name}] Failed job ${job.id}`);
      }
      throw error;
    }
  }

  // Schedule the next occurrence of a repeatable job
  protected async scheduleNextRepeat(job: Job): Promise<void> {
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
  }

  // Calculate the next execution time based on the repeat configuration
  protected calculateNextExecutionTime(job: Job): Date {
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
    },
  ): Promise<Job<T>> {
    if (!this.handlers.has(name)) {
      throw new Error(`Job handler for "${name}" not registered`);
    }

    // Validate options
    if (options.every <= 0) {
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
