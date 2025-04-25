import { describe, test, expect, beforeEach, afterEach, vi } from 'vitest';
import { PostgreSQLJobStorage } from '../../src/storage/postgresql-storage';
import { PostgreSQLJobQueue } from '../../src/queue/postgresql-job-queue';
import {Pool} from "pg"
import { JobHandler } from '../../src/types';

describe('PostgreSQLJobQueue', () => {
  let pool: Pool;
  let storage: PostgreSQLJobStorage;
  let queue: PostgreSQLJobQueue;
  let mockHandler: JobHandler;
  // Set up before each test
  beforeEach(() => {
    // Create a fresh mock pool for each test
    pool = new Pool({
      connectionString: 'postgresql://postgres:12345@localhost:5432/postgres',
    });
    
    // Create storage with mock pool
    storage = new PostgreSQLJobStorage(pool, { tableName: 'jobs' });
    
    pool.query(`DELETE FROM jobs`);

    // Create queue with storage
    queue = new PostgreSQLJobQueue(storage, {
      name: 'test-queue',
      concurrency: 1,
      maxRetries: 2,
      processingInterval: 100
    });
    
    // Mock job handler
    mockHandler = vi.fn().mockImplementation(() => {
        return { success: true };
    });
    // Register job handler
    queue.register('test-job', mockHandler);
  });
  
  // Clean up after each test
  afterEach(() => {
    // Reset mocks
    vi.resetAllMocks();
    
    // Stop the queue if it's processing
    queue.stop();
  });
  
  test('should add a job to the queue', async () => {
    const job = await queue.add('test-job', { foo: 'bar' });
    
    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe('test-job');
    expect(job.status).toBe('pending');
    expect(job.data).toEqual({ foo: 'bar' });
    
    const jobs = await storage.getJobsByStatus('pending');
    expect(jobs.length).toBe(1);
    expect(jobs[0].id).toBe(job.id);
  });
  
  test('should schedule a job for later execution', async () => {
    const scheduledTime = new Date(Date.now() + 1000 * 60);
    const job = await queue.schedule('test-job', { scheduled: true }, scheduledTime);
    
    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe('test-job');
    expect(job.status).toBe('pending');
    expect(job.data).toEqual({ scheduled: true });
    expect(job.scheduledAt).toEqual(scheduledTime);
    
    const jobs = await storage.getJobsByStatus('pending');
    expect(jobs.length).toBe(1);
    expect(jobs[0].id).toBe(job.id);
    expect(jobs[0].scheduledAt).toEqual(scheduledTime);
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
    expect(updatedJob?.result).toEqual({ success: true });
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
    
    await new Promise(resolve => setTimeout(resolve, 400));
    
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
    
    await new Promise(resolve => setTimeout(resolve, 500));
    
    expect(failingHandler).toHaveBeenCalledTimes(3);
    
    const updatedJob = await queue.getJob(job.id);
    
    expect(updatedJob).toBeDefined();
    expect(updatedJob?.status).toBe('failed');
    expect(updatedJob?.error).toContain('Failed after 2 retries');
    expect(updatedJob?.retryCount).toBe(2);
  });
}); 