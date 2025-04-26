import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { JobQueue } from '../../src/queue/job-queue';
import { InMemoryJobStorage } from '../../src/storage/memory-storage';
import { JobHandler } from '../../src/types';
import { QueueEventData } from '../../src/utils/queue-event';

describe('JobQueue', () => {
  let queue: JobQueue;
  let storage: InMemoryJobStorage;
  let mockHandler: JobHandler;
  let eventListener: EventListener;

  beforeEach(() => {
    // Create fresh storage and queue for each test
    storage = new InMemoryJobStorage();
    mockHandler = vi.fn().mockResolvedValue({ success: true });
    queue = new JobQueue(storage, { concurrency: 1, name: 'test-queue', processingInterval: 20 });
    
    // Register a mock handler
    queue.register('test-job', mockHandler);

    // Add an event listener
    eventListener = vi.fn().mockImplementation((eventData: QueueEventData) => {
      console.log('Event received:', eventData);
    });
    
    // register event listeners
    queue.addEventListener('scheduled', eventListener);
    queue.addEventListener('completed', eventListener);
    queue.addEventListener('failed', eventListener);
  });

  afterEach(() => {
    // Stop the queue after each test
    queue.stop();
    vi.restoreAllMocks();
    queue.removeEventListener('scheduled', eventListener);
    queue.removeEventListener('completed', eventListener);
    queue.removeEventListener('failed', eventListener);
  });

  it('should add a job to the queue', async () => {
    const job = await queue.add('test-job', { message: 'Hello, World!' });
    
    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.status).toBe('pending');
    expect(job.name).toBe('test-job');
    expect(job.data).toEqual({ message: 'Hello, World!' });
    expect(eventListener).toHaveBeenCalledWith(expect.objectContaining({
      job: expect.objectContaining({
        id: job.id,
        name: 'test-job',
        data: { message: 'Hello, World!' },
        status: 'pending'
      }),
      status: 'pending'
    }));
    
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
    expect(eventListener).toHaveBeenCalledWith(expect.objectContaining({
      job: expect.objectContaining({
        id: job.id,
        name: 'test-job',
        data: { message: 'Future task' },
        status: 'pending'
      }),
      status: 'pending'
    }));
    
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
    
    // Add a job before starting the queue
    const job = await queue.add('test-job', { message: 'Hello, World!' });
    
    // Start processing jobs
    queue.start();

    // Wait longer to ensure job processing
    await new Promise(resolve => setTimeout(resolve, 25));
    
    // Verify the handler was called with the correct data
    expect(mockHandler).toHaveBeenCalledWith({ message: 'Hello, World!' });
    
    // Verify job status was updated
    const processedJob = await storage.getJob(job.id);
    expect(processedJob?.status).toBe('completed');
    expect(processedJob?.result).toEqual({ success: true });

    expect(eventListener).toHaveBeenCalledWith(expect.objectContaining({
      job: expect.objectContaining({
        id: job.id,
        name: 'test-job',
        data: { message: 'Hello, World!' },
        status: 'completed'
      }),
      status: 'completed'
    }));
  });

  it('should not process scheduled jobs before their time', async () => {
    // Schedule a job for the future
    const futureDate = new Date();
    futureDate.setMinutes(futureDate.getMinutes() + 10); // 10 minutes in the future
    
    const job = await queue.schedule('test-job', { message: 'Future task' }, futureDate);
    
    // Start processing jobs
    queue.start();
    
    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 25));
    
    // Verify handler wasn't called for the future job
    expect(mockHandler).not.toHaveBeenCalled();
    expect(eventListener).toHaveBeenCalledWith(expect.objectContaining({
      job: expect.objectContaining({
        id: job.id,
        name: 'test-job',
        data: { message: 'Future task' },
        status: 'pending'
      }),
      status: 'pending'
    }));
  });

  it('should process multiple jobs with configured concurrency', async () => {
    // Create a queue with concurrency of 2
    const concurrentQueue = new JobQueue(storage, {
      concurrency: 2,
      processingInterval: 20,
      name: 'concurrent-queue'
     });
    
    // Create a handler that takes some time
    const slowHandler = vi.fn().mockImplementation(async () => {
      await new Promise(resolve => setTimeout(resolve, 20));
      return { success: true };
    });

    const completedEventListener = vi.fn().mockImplementation((eventData: QueueEventData) => {
      console.log('completed event received:', eventData);
    });
    
    // Register the slow handler
    concurrentQueue.register('slow-job', slowHandler);
    
    // unregister event listeners
    concurrentQueue.removeEventListener('scheduled', eventListener);
    concurrentQueue.removeEventListener('completed', completedEventListener);
    
    // register event listeners
    concurrentQueue.addEventListener('scheduled', eventListener);
    concurrentQueue.addEventListener('completed', completedEventListener);
    // Add multiple jobs before starting the queue
    await concurrentQueue.add('slow-job', { id: 1 });
    await concurrentQueue.add('slow-job', { id: 2 });
    await concurrentQueue.add('slow-job', { id: 3 });

    expect(eventListener).toHaveBeenCalledTimes(3);

    // Start processing
    concurrentQueue.start();
    
    // Wait for jobs to complete
    await new Promise(resolve => setTimeout(resolve, 50));
    
    // Stop the queue
    concurrentQueue.stop();
    
    // Verify all jobs were processed
    expect(slowHandler).toHaveBeenCalledTimes(2);
    expect(slowHandler).toHaveBeenCalledWith({ id: 1 });
    expect(slowHandler).toHaveBeenCalledWith({ id: 2 });
    expect(completedEventListener).toHaveBeenCalledTimes(2);
  });
}); 