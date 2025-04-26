// Re-export all types
export type { Job, JobStatus, JobHandler } from "./types";

// Re-export storage interfaces and classes
export type { JobStorage } from "./storage/base-storage";
export { InMemoryJobStorage } from "./storage/memory-storage";
export { PostgreSQLJobStorage } from "./storage/postgresql-storage";
export { MongoDBJobStorage } from "./storage/mongodb-storage";
export { RedisJobStorage } from "./storage/redis-storage";

// Re-export queue implementation
export { JobQueue } from "./queue/job-queue";
export { DistributedJobQueue } from "./queue/distributed-job-queue";
export { MongoDBJobQueue } from "./queue/mongodb-job-queue";
export { PostgreSQLJobQueue } from "./queue/postgresql-job-queue";

// Helper factory function
import { JobQueue } from "./queue/job-queue";
import { DistributedJobQueue } from "./queue/distributed-job-queue";
import type { JobStorage } from "./storage/base-storage";
import { InMemoryJobStorage } from "./storage/memory-storage";
export { QueueEvent } from "./utils/queue-event";
export type { QueueEventData } from "./utils/queue-event";

export const createJobQueue = (
  storage?: JobStorage,
  options?: { concurrency?: number; name?: string; distributed?: boolean }
): JobQueue => {
  const jobStorage = storage || new InMemoryJobStorage();
  
  if (options?.distributed) {
    // Validate the storage has distributed capabilities
    if (!('acquireJobLock' in jobStorage) || !('releaseJobLock' in jobStorage)) {
      throw new Error('Distributed job queue requires a storage with locking capabilities (e.g., RedisJobStorage)');
    }
    return new DistributedJobQueue(jobStorage as any, options);
  }
  
  return new JobQueue(jobStorage, options);
};
