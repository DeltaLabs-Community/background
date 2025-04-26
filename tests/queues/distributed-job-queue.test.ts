import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { DistributedJobQueue } from '../../src/queue/distributed-job-queue';
import { RedisJobStorage } from '../../src/storage/redis-storage';
import { JobHandler } from '../../src/types';
import Redis from 'ioredis';
import { QueueEvent } from '../../src/utils/queue-event';
import dotenv from 'dotenv';

describe('DistributedJobQueue', () => {
  let redis: Redis;
  let storage: RedisJobStorage;
  let queue: DistributedJobQueue;
  let mockHandler: JobHandler;

  beforeEach(async () => {
    dotenv.config();
    redis = new Redis(process.env.TEST_REDIS_URL || 'redis://localhost:6379');
    storage = new RedisJobStorage(redis, { keyPrefix: 'test:' });
    await storage.clear();
    mockHandler = vi.fn().mockImplementation(() => {
        return { success: true };
    });
    queue = new DistributedJobQueue(storage, { 
      concurrency: 1, 
      name: 'worker-1',
      processingInterval: 100
    });    
    queue.register('test-job', mockHandler);
  });

  afterEach(() => {
    queue.stop();
    vi.restoreAllMocks();
  });

  it('should process jobs with locking', async () => {
    const job = await queue.add('test-job', { message: 'Hello, World!' });
    queue.start();
    await new Promise(resolve => setTimeout(resolve, 250));
    expect(mockHandler).toHaveBeenCalledWith({ message: 'Hello, World!' });
    const processedJob = await storage.getJob(job.id);
    expect(processedJob?.status).toBe('completed');
  });

  it('should schedule jobs for later execution', async () => {
    const scheduledTime = new Date(Date.now() + 1000 * 60);
    const job = await queue.schedule('test-job', { scheduled: true }, scheduledTime);
    
    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.name).toBe('test-job');
    expect(job.status).toBe('pending');
    expect(job.data).toEqual({ scheduled: true });
    expect(job.scheduledAt).toEqual(scheduledTime);
  });

  it('should process jobs concurrently', async () => {
    const concurrentQueue = new DistributedJobQueue(storage, {
      concurrency: 2,
      name: 'worker-2',
      processingInterval: 100
    });

    const eventListener = vi.fn().mockImplementation((event: QueueEvent) => {
    });

    concurrentQueue.addEventListener('completed', eventListener);
    // Register handler for the job
    concurrentQueue.register('test-job', mockHandler);
    // Add jobs to the queue
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    concurrentQueue.add('test-job', { message: 'Hello, World!' });
    
    // Start the queue
    concurrentQueue.start();
    await new Promise(resolve => setTimeout(resolve, 200));
    expect(mockHandler).toHaveBeenCalledTimes(2);
    expect(eventListener).toHaveBeenCalledTimes(2);
  });
}); 