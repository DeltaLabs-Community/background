import { Job } from "../types";
import { JobStorage } from "../storage/index";
import { JobHandler } from "../types";
import { generateId } from "../utils/id-generator";
import { QueueEvent } from "../utils/queue-event";
export class JobQueue extends EventTarget{
    /**
     * Job handlers registered with this queue
     */
    private handlers: Map<string, JobHandler> = new Map();
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
    private name: string;
    protected maxRetries: number = 3;
    
    constructor(storage: JobStorage, options: { concurrency?: number, name?: string, processingInterval?: number, maxRetries?: number } = {}) {
      super();
      this.storage = storage;
      this.concurrency = options.concurrency || 1;
      this.name = options.name || 'default';
      this.processingInterval = options.processingInterval || 1000;
      this.maxRetries = options.maxRetries || 3;
    }
    
    // Register a job handler
    register<T, R>(name: string, handler: JobHandler<T, R>): void {
      this.handlers.set(name, handler);
    }
    
    // Add a job to the queue
    async add<T>(name: string, data: T): Promise<Job<T>> {
      if (!this.handlers.has(name)) {
        throw new Error(`Job handler for "${name}" not registered`);
      }
      
      const job: Job<T> = {
        id: generateId(),
        name,
        data,
        status: 'pending',
        createdAt: new Date(),
      };
      
      await this.storage.saveJob(job);
      this.dispatchEvent(new QueueEvent('scheduled', job, 'pending'));
      return job;
    }
    
    // Schedule a job to run at a specific time
    async schedule<T>(name: string, data: T, scheduledAt: Date): Promise<Job<T>> {
      if (!this.handlers.has(name)) {
        throw new Error(`Job handler for "${name}" not registered`);
      }
      
      if (scheduledAt < new Date()) {
        throw new Error('Scheduled time must be in the future');
      }
      
      const job: Job<T> = {
        id: generateId(),
        name,
        data,
        status: 'pending',
        createdAt: new Date(),
        scheduledAt,
      };
      
      await this.storage.saveJob(job);
      this.dispatchEvent(new QueueEvent('scheduled', job, 'pending'));
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
      
      this.processing = true;
      this.intervalId = setInterval(() => this.processNextBatch(), this.processingInterval);
    }
    
    // Stop processing jobs
    stop(): void {
      if (!this.processing) return;
      
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
        this.intervalId = setInterval(() => this.processNextBatch(), this.processingInterval);
      }
    }

    // Set concurrency level
    setConcurrency(level: number): void {
      if (level < 1) {
        throw new Error('Concurrency level must be at least 1');
      }
      this.concurrency = level;
    }
    
    // Process the next batch of pending jobs
    protected async processNextBatch(): Promise<void> {
      // Skip if we're already processing the maximum number of jobs
      if (this.activeJobs.size >= this.concurrency) {
        return;
      }
      const pendingJobs = await this.storage.getJobsByStatus('pending');
      const now = new Date();
      
      // Filter out jobs that are scheduled for the future
      const readyJobs = pendingJobs.filter(job => 
        !job.scheduledAt || job.scheduledAt <= now
      );
      
      const availableSlots = this.concurrency - this.activeJobs.size;
      const jobsToProcess = readyJobs.slice(0, availableSlots);
      
      for (const job of jobsToProcess) {
        // Skip if job is already being processed
        if (this.activeJobs.has(job.id)) {
          continue;
        }

        this.activeJobs.add(job.id);
        this.processJob(job).finally(() => {
          this.activeJobs.delete(job.id);
        });
      }
    }

    // Process a single job
    protected async processJob(job: Job): Promise<void> {
      try {
        // Mark job as processing
        job.status = 'processing';
        job.startedAt = new Date();
        await this.storage.updateJob(job);
        
        // Get the handler
        const handler = this.handlers.get(job.name);
        if (!handler) {
          throw new Error(`Handler for job "${job.name}" not found`);
        }
        
        // Execute the handler
        const result = await handler(job.data);

        // Mark job as completed
        job.status = 'completed';
        job.completedAt = new Date();
        job.result = result;
        await this.storage.updateJob(job);
        console.log('completed', job);
        this.dispatchEvent(new QueueEvent('completed', job, 'completed'));
      } catch (error) {
        // Mark job as failed
        job.status = 'failed';
        job.completedAt = new Date();
        job.error = error instanceof Error ? error.message : String(error);
        await this.storage.updateJob(job);
        this.dispatchEvent(new QueueEvent('failed', job, 'failed'));
        throw error;
      }
    }
}