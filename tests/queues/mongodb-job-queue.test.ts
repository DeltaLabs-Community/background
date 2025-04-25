import { describe, test, expect, beforeEach, afterEach, vi, afterAll } from 'vitest';
import { MongoDBJobStorage } from '../../src/storage/mongodb-storage';
import { MongoDBJobQueue } from '../../src/queue/mongodb-job-queue';
import { MongoClient } from 'mongodb';
import { JobHandler } from '../../src/types';
describe('MongoDBJobQueue',() => {
  let storage: MongoDBJobStorage;
  let queue: MongoDBJobQueue;
  let mockHandler: JobHandler;
  const mongoClient = new MongoClient('mongodb://localhost:27017');
  
  beforeEach(async () => {
    await mongoClient.connect();
    await mongoClient.db().collection('jobs').deleteMany({});

    storage = new MongoDBJobStorage(mongoClient, { collectionName: 'jobs'});
    queue = new MongoDBJobQueue(storage, {
      name: 'test-queue',
      concurrency: 2,
      maxRetries: 2,
      processingInterval:100
    });

    // Mock job handler
    mockHandler = vi.fn().mockImplementation(() => {
      return { success: true };
    });
    
    // Register job handler
    queue.register('test-job', mockHandler);
  });
  
  afterEach(async () => {
    vi.resetAllMocks();
    queue.stop();
  });

  afterAll(async () => {
    await mongoClient.close();
  });
  
  test('should add a job to the queue', async () => {
    const job = await queue.add('test-job', { foo: 'bar' });
    
    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe('test-job');
    expect(job.status).toBe('pending');
    expect(job.data).toEqual({ foo: 'bar' });
    
    const savedJob = await storage.getJob(job.id);
    expect(savedJob).toBeDefined();
    expect(savedJob?.id).toBe(job.id);
  });
  
  test('should schedule a job for later execution', async () => {
    const scheduledTime = new Date(Date.now() + 1000 * 60);
    const job = await queue.schedule('test-job', { scheduled: true }, scheduledTime);
    
    // Expectations
    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe('test-job');
    expect(job.status).toBe('pending');
    expect(job.data).toEqual({ scheduled: true });
    expect(job.scheduledAt).toEqual(scheduledTime);
    
    const savedJob = await storage.getJob(job.id);
    expect(savedJob).toBeDefined();
    expect(savedJob?.scheduledAt).toEqual(scheduledTime);
  });
  
  test('should process a job successfully', async () => {
    const job = await queue.add('test-job', { process: true });
    
    queue.start();
    
    await new Promise(resolve => setTimeout(resolve, 200));
    
    expect(mockHandler).toHaveBeenCalledTimes(1);
    expect(mockHandler).toHaveBeenCalledWith({ process: true });
    
    const updatedJob = await queue.getJob(job.id);
    
    expect(updatedJob).toBeDefined();
    expect(updatedJob?.status).toBe('completed');
  });
  
  test('should retry a failed job', async () => {
    let attempts = 0;
    const retriableHandler = vi.fn().mockImplementation(() => {
      attempts++;
      if (attempts === 1) {
        throw new Error('Temporary failure');
      }
      return { success: true, retried: true };
    });

    queue.register('retry-job', retriableHandler);
    
    const job = await queue.add('retry-job', { shouldRetry: true });
    
    queue.start();
    
    await new Promise(resolve => setTimeout(resolve, 300));
    
    expect(retriableHandler).toHaveBeenCalledTimes(2);
    
    const updatedJob = await queue.getJob(job.id);
    
    expect(updatedJob).toBeDefined();
    expect(updatedJob?.status).toBe('completed');
    expect(updatedJob?.result).toEqual({ success: true, retried: true });
    expect(updatedJob?.retryCount).toBe(1);
  });
  
  test('should mark a job as failed after max retries', async () => {
    const failingHandler = vi.fn().mockImplementation(() => {
      throw new Error('Persistent failure');
    });
    
    queue.register('failing-job', failingHandler);
    
    const job = await queue.add('failing-job', { willFail: true });
    
    queue.start();
    
    await new Promise(resolve => setTimeout(resolve, 400));
    
    expect(failingHandler).toHaveBeenCalledTimes(3);
    
    const updatedJob = await queue.getJob(job.id);
    
    expect(updatedJob).toBeDefined();
    expect(updatedJob?.status).toBe('failed');
    expect(updatedJob?.error).toContain('Failed after 2 retries');
    expect(updatedJob?.retryCount).toBe(2);
  });
  
  test('should use distributed locking to prevent concurrent processing', async () => {
    const job = await queue.add('test-job', { concurrent: true });
    
    const originalAcquireLock = storage.acquireJobLock;
    storage.acquireJobLock = vi.fn().mockResolvedValue(false);
    
    queue.start();
    
    await new Promise(resolve => setTimeout(resolve, 200));
    
    expect(mockHandler).not.toHaveBeenCalled();
    expect(storage.acquireJobLock).toHaveBeenCalledWith(job.id, expect.any(Number));
    
    storage.acquireJobLock = originalAcquireLock;
  });
}); 