import { describe, it, expect, beforeEach } from 'vitest';
import { RedisJobStorage } from '../../src/storage/redis-storage';
import { Job, JobStatus } from '../../src/types';
import Redis from 'ioredis';

describe('RedisJobStorage', () => {
  let redis: Redis;
  let storage: RedisJobStorage;
  let testJob: Job;

  beforeEach(() => {
    redis = new Redis({
      host: 'localhost',
      port: 6379,
      db: 0,
    });
    storage = new RedisJobStorage(redis, { keyPrefix: 'test:' });
    testJob = {
      id: 'test-job-1',
      name: 'test-job',
      data: { message: 'Hello, World!' },
      status: 'pending' as JobStatus,
      createdAt: new Date(),
    };
  });

  it('should save and retrieve a job', async () => {
    await storage.saveJob(testJob);
    const retrievedJob = await storage.getJob(testJob.id);
    
    // Convert dates for comparison since Redis serializes them
    expect(retrievedJob?.id).toBe(testJob.id);
    expect(retrievedJob?.name).toBe(testJob.name);
    expect(retrievedJob?.status).toBe(testJob.status);
    expect(retrievedJob?.data).toEqual(testJob.data);
    expect(retrievedJob?.createdAt.getTime()).toEqual(testJob.createdAt.getTime());
  });

  it('should return null for non-existent job', async () => {
    const job = await storage.getJob('non-existent');
    expect(job).toBeNull();
  });

  it('should get jobs by status', async () => {
    const pendingJob1 = { ...testJob, id: 'pending-1' };
    const pendingJob2 = { ...testJob, id: 'pending-2' };
    const processingJob = { ...testJob, id: 'processing-1', status: 'processing' as JobStatus };

    await storage.saveJob(pendingJob1);
    await storage.saveJob(pendingJob2);
    await storage.saveJob(processingJob);

    const pendingJobs = await storage.getJobsByStatus('pending');
    expect(pendingJobs).toHaveLength(2);
    expect(pendingJobs.map(job => job.id)).toContain('pending-1');
    expect(pendingJobs.map(job => job.id)).toContain('pending-2');

    const processingJobs = await storage.getJobsByStatus('processing');
    expect(processingJobs).toHaveLength(1);
    expect(processingJobs[0].id).toBe('processing-1');
  });

  it('should update a job', async () => {
    await storage.saveJob(testJob);
    
    const updatedJob = {
      ...testJob,
      status: 'completed' as JobStatus,
      completedAt: new Date(),
      result: { success: true }
    };
    
    await storage.updateJob(updatedJob);
    const retrievedJob = await storage.getJob(testJob.id);
    
    expect(retrievedJob?.status).toBe('completed');
    expect(retrievedJob?.result).toEqual({ success: true });
  });

  it('should throw error when updating non-existent job', async () => {
    const nonExistentJob = {
      ...testJob,
      id: 'non-existent'
    };
    
    await expect(storage.updateJob(nonExistentJob)).rejects.toThrow();
  });
}); 