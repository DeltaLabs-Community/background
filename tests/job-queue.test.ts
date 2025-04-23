import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { JobQueue } from '../src/queue/job-queue';
import { InMemoryJobStorage } from '../src/storage/memory-storage';
import { JobHandler } from '../src/types';

describe('JobQueue', () => {
  let queue: JobQueue;
  let storage: InMemoryJobStorage;
  let mockHandler: JobHandler;

  beforeEach(() => {
    // Create fresh storage and queue for each test
    storage = new InMemoryJobStorage();
    mockHandler = vi.fn().mockResolvedValue({ success: true });
    queue = new JobQueue(storage, { concurrency: 1, name: 'test-queue' });
    
    // Register a mock handler
    queue.register('test-job', mockHandler);
  });

  afterEach(() => {
    // Stop the queue after each test
    queue.stop();
    vi.restoreAllMocks();
  });

  it('should add a job to the queue', async () => {
    const job = await queue.add('test-job', { message: 'Hello, World!' });
    
    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.status).toBe('pending');
    expect(job.name).toBe('test-job');
    expect(job.data).toEqual({ message: 'Hello, World!' });
    
    // Verify it's saved to storage
    const savedJob = await storage.getJob(job.id);
    expect(savedJob).toEqual(job);
  });

  it('should throw error for unregistered job handler', async () => {
    await expect(queue.add('non-existent-job', {})).rejects.toThrow();
  });

  it('should schedule a job for future execution', async () => {
    const futureDate = new Date();
    futureDate.setMinutes(futureDate.getMinutes() + 10); // 10 minutes in the future
    
    const job = await queue.schedule('test-job', { message: 'Future task' }, futureDate);
    
    expect(job).toBeDefined();
    expect(job.scheduledAt).toEqual(futureDate);
    
    // Verify it's saved to storage with scheduled time
    const savedJob = await storage.getJob(job.id);
    expect(savedJob?.scheduledAt).toEqual(futureDate);
  });

  it('should retrieve a job by ID', async () => {
    const job = await queue.add('test-job', { message: 'Hello, World!' });
    const retrievedJob = await queue.getJob(job.id);
    
    expect(retrievedJob).toEqual(job);
  });

  it('should process a job', async () => {
    // Mock Date.now for consistent testing
    const now = new Date();
    vi.spyOn(global, 'Date').mockImplementation(() => now as any);
    
    console.log('Adding job to queue');
    // Add a job before starting the queue
    const job = await queue.add('test-job', { message: 'Hello, World!' });
    
    console.log('Starting queue');
    // Start processing jobs
    queue.start();

    console.log('Waiting for job to be processed');
    // Wait longer to ensure job processing
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    console.log('Checking job status');
    // Verify the handler was called with the correct data
    expect(mockHandler).toHaveBeenCalledWith({ message: 'Hello, World!' });
    
    // Verify job status was updated
    const processedJob = await storage.getJob(job.id);
    expect(processedJob?.status).toBe('completed');
    expect(processedJob?.result).toEqual({ success: true });
  });

  it('should not process scheduled jobs before their time', async () => {
    // Schedule a job for the future
    const futureDate = new Date();
    futureDate.setMinutes(futureDate.getMinutes() + 10); // 10 minutes in the future
    
    console.log('Scheduling future job');
    await queue.schedule('test-job', { message: 'Future task' }, futureDate);
    
    console.log('Starting queue');
    // Start processing jobs
    queue.start();
    
    console.log('Waiting to verify job is not processed');
    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Verify handler wasn't called for the future job
    expect(mockHandler).not.toHaveBeenCalled();
  });

  it('should process multiple jobs with configured concurrency', async () => {
    // Create a queue with concurrency of 2
    const concurrentQueue = new JobQueue(storage, { concurrency: 2 });
    
    // Create a handler that takes some time
    const slowHandler = vi.fn().mockImplementation(async () => {
      await new Promise(resolve => setTimeout(resolve, 20));
      return { success: true };
    });
    
    concurrentQueue.register('slow-job', slowHandler);
    
    console.log('Adding multiple jobs');
    // Add multiple jobs before starting the queue
    await concurrentQueue.add('slow-job', { id: 1 });
    await concurrentQueue.add('slow-job', { id: 2 });
    await concurrentQueue.add('slow-job', { id: 3 });
    
    console.log('Starting queue with concurrency 2');
    // Start processing
    concurrentQueue.start();
    
    console.log('Waiting for jobs to be processed');
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Stop the queue
    concurrentQueue.stop();
    
    // Verify all jobs were processed
    expect(slowHandler).toHaveBeenCalledTimes(2);
    expect(slowHandler).toHaveBeenCalledWith({ id: 1 });
    expect(slowHandler).toHaveBeenCalledWith({ id: 2 });
  });
}); 