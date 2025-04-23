import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { DistributedJobQueue } from '../src/queue/distributed-job-queue';
import { RedisJobStorage } from '../src/storage/redis-storage';
import { JobHandler } from '../src/types';
import Redis from 'ioredis';

describe('DistributedJobQueue', () => {
  let redis: Redis;
  let storage: RedisJobStorage;
  let queue: DistributedJobQueue;
  let mockHandler: JobHandler;

  beforeEach(() => {
    redis = new Redis('redis://localhost:6379');
    storage = new RedisJobStorage(redis, { keyPrefix: 'test:' });
    mockHandler = vi.fn().mockImplementation(() => {
        console.log('mockHandler called');
        return { success: true };
    });
    queue = new DistributedJobQueue(storage, { 
      concurrency: 1, 
      name: 'worker-1',
      instanceId: 'instance-1',
      processingInterval: 100
    });    
    queue.register('test-job', mockHandler);
  });

  afterEach(() => {
    queue.stop();
    vi.restoreAllMocks();
  });

  it('should have a unique instance ID', () => {
    expect(queue.getInstanceId()).toBe('instance-1');
    
    const autoIdQueue = new DistributedJobQueue(storage);
    expect(autoIdQueue.getInstanceId()).toBeDefined();
    expect(autoIdQueue.getInstanceId().length).toBeGreaterThan(0);
  });

  it('should process jobs with locking', async () => {
    const job = await queue.add('test-job', { message: 'Hello, World!' });
    
    queue.start();

    await new Promise(resolve => setTimeout(resolve, 1000));

    expect(mockHandler).toHaveBeenCalledWith({ message: 'Hello, World!' });
    
    const processedJob = await storage.getJob(job.id);
    expect(processedJob?.status).toBe('completed');
  });

  it('should prevent duplicate processing across instances', async () => {
    storage.acquireJobLock = vi.fn().mockResolvedValue(false);

    const job = await queue.add('test-job', { message: 'Hello, World!' });
    
    queue.start();
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    expect(mockHandler).not.toHaveBeenCalled();
    
    expect(storage.acquireJobLock).toHaveBeenCalled();
  });

  it('should always release locks after processing', async () => {
    const releaseSpy = vi.spyOn(storage, 'releaseJobLock');
    
    await queue.add('test-job', { message: 'Hello, World!' });
    
    queue.start();
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    expect(releaseSpy).toHaveBeenCalledWith(expect.any(String), 'instance-1');
  });

  it('should release locks even when job processing fails', async () => {
    const errorHandler = vi.fn().mockRejectedValue(new Error('Processing failed'));
    queue.register('error-job', errorHandler);
    
    const releaseSpy = vi.spyOn(storage, 'releaseJobLock');
    
    const job = await queue.add('error-job', { message: 'Will fail' });
    
    queue.start();
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    expect(errorHandler).toHaveBeenCalled();
    
    const failedJob = await storage.getJob(job.id);
    expect(failedJob?.status).toBe('failed');
    expect(failedJob?.error).toBe('Processing failed');
    
    expect(releaseSpy).toHaveBeenCalled();
  });
}); 