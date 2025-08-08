  import { Collection, MongoClient } from "mongodb";
  import { JobStorage } from "./base-storage";
  import { Job, JobStatus } from "../types";

  /**
   * MongoDB storage adapter for JobQueue
   *
   * This storage adapter uses MongoDB to store jobs, making it suitable
   * for distributed environments with multiple instances/processes.
   *
   * Note: You must install the 'mongodb' package to use this adapter:
   * npm install mongodb
   */

  export class MongoDBJobStorage implements JobStorage {
    private readonly collection: Collection<Job>;
    private readonly mongoClient: MongoClient;
    private readonly logging: boolean = false;
    private readonly staleJobTimeout: number = 1000 * 60 * 60 * 24; // 24 hours

    constructor(
      mongoClient: MongoClient,
      options: { collectionName?: string; logging?: boolean; staleJobTimeout?: number } = {},
    ) {
      this.mongoClient = mongoClient;
      this.collection = this.mongoClient
        .db()
        .collection(options.collectionName || "jobs");
      this.logging = options.logging || false;
      this.staleJobTimeout = options.staleJobTimeout || 1000 * 60 * 60 * 24; // 24 hours
    }
    /**
     * Save a job
     * @param job - The job to save
     */
    async saveJob(job: Job): Promise<void> {
      try {
        await this.collection.insertOne(job);
      } catch (error) {
        if (this.logging) {
          console.error("Error saving job", error);
        }
      }
    }

    /**
     * Get a job by ID
     * @param id - The ID of the job to get
     * @returns The job with the given ID, or null if the job does not exist
     */
    async getJob(id: string): Promise<Job | null> {
      try {
        return await this.collection.findOne({ id });
      } catch (error) {
        if (this.logging) {
          console.error("Error getting job", error);
        }
        return null;
      }
    }

    /**
     * Get all jobs by status
     * @param status - The status of the jobs to get
     * @returns An array of jobs with the given status
     */
    async getJobsByStatus(status: JobStatus): Promise<Job[]> {
      try {
        return await this.collection.find({ status }).toArray();
      } catch (error) {
        if (this.logging) {
          console.error("Error getting jobs by status", error);
        }
        return [];
      }
    }

    /**
     * Update a job
     * @param job - The job to update
     * @throws Error if the job is not found
     */
    async updateJob(job: Job): Promise<void> {
      try {
        await this.collection.findOneAndUpdate({ id: job.id }, { $set: job });
      } catch (error) {
        if (this.logging) {
          console.error("Error updating job", error);
        }
        throw error;
      }
    }

    /**
     * Acquire the next pending job
     * @returns The next pending job, or null if no pending jobs are available
     */
    async acquireNextJob(handlerNames?:string []): Promise<Job | null> {
      try {
        const staleThreshold = new Date(Date.now() - this.staleJobTimeout);
        // First try to find pending jobs
        let job = await this.collection.findOneAndUpdate(
          {
            ...(handlerNames && { name: { $in: handlerNames } }),
            status: "pending",
            $or: [
              { scheduledAt: { $exists: false } },
              { scheduledAt: { $lte: new Date() } },
            ],
          },
          { $set: { status: "processing", startedAt: new Date() } },
          { sort: { priority: 1, createdAt: 1 }, returnDocument: "after" }
        );
        
        if (!job) {
          job = await this.collection.findOneAndUpdate(
            {
              ...(handlerNames && { name: { $in: handlerNames } }),
              status: "processing",
              startedAt: { $lte: staleThreshold } as any,
              $or: [
                { completedAt: { $exists: false } } as any,
                { completedAt: null } as any
              ]
            },
            { $set: { startedAt: new Date() } },
            { sort: { priority: 1, createdAt: 1 }, returnDocument: "after" }
          );
        }
        return job;
      } catch (error) {
        if (this.logging) {
          console.error(`[MongoDBJobStorage] Error acquiring next job:`, error);
        }
        return null;
      }
    }

  /**
   * Acquire multiple pending jobs for prefetching
   * @param batchSize - Number of jobs to prefetch
   * @returns Array of acquired jobs
   */
    async acquireNextJobs(batchSize: number,handlerNames?:string []): Promise<Job[]> {
      const session = this.mongoClient.startSession();
      try {
        let jobs: Job[] = [];
        
        await session.withTransaction(async () => {
          const now = new Date();
          const staleThreshold = new Date(Date.now() - this.staleJobTimeout);
          
          const availableJobs = await this.collection
            .find(
              {
                ...(handlerNames && { name: { $in: handlerNames } }),
                $or: [
                  {
                    status: 'pending',
                    $or: [
                      { scheduledAt: { $exists: false } },
                      { scheduledAt: { $lte: now } }
                    ]
                  },
                  {
                    status: 'processing',
                    startedAt: { $exists: true,$ne: undefined,$lt: staleThreshold},
                    completedAt: { $exists: false },
                  }
                ]
              },
              { session }
            )
            .sort({ priority: 1, createdAt: 1 })
            .limit(batchSize)
            .toArray();

          if (availableJobs.length === 0) {
            return;
          }
          const bulkOps = availableJobs.map(job => ({
            updateOne: {
              filter: { _id: job._id },
              update: {
                $set: {
                  status: 'processing' as JobStatus, 
                  startedAt: now,
                }
              }
            }
          }));

          const bulkResult = await this.collection.bulkWrite(bulkOps, { session });
          
          if (bulkResult.modifiedCount > 0) {
            const updatedJobs = await this.collection
              .find(
                { 
                  _id: { $in: availableJobs.map(job => job._id) },
                  status: 'processing',
                  startedAt: now
                },
                { session }
              )
              .toArray();
            jobs = updatedJobs
          }
        });
        if (this.logging && jobs.length > 0) {
          console.log(`[MongoDBJobStorage] Acquired ${jobs.length} jobs in batch`);
        }
        return jobs;
      } catch (error) {
        if (this.logging) {
          console.error(`[MongoDBJobStorage] Error acquiring batch jobs:`, error);
        }
        return [];
      } finally {
        await session.endSession();
      }
    }

    /**
     * Complete a job
     * @param jobId - The ID of the job to complete
     * @param result - The result of the job
     */
    async completeJob(jobId: string, result: any): Promise<void> {
      try {
        await this.collection.findOneAndUpdate(
          { id: jobId, status: "processing" },
          { $set: { status: "completed", result, completedAt: new Date() } },
        );
      } catch (error) {
        if (this.logging) {
          console.error(`[MongoDBJobStorage] Error completing job:`, error);
        }
      }
    }
    /**
     * Fail a job
     * @param jobId - The ID of the job to fail
     * @param error - The error message
     */
    async failJob(jobId: string, error: string): Promise<void> {
      try {
        await this.collection.findOneAndUpdate(
          { id: jobId, status: "processing" },
          { $set: { status: "failed", error, completedAt: new Date() } },
        );
      } catch (error) {
        if (this.logging) {
          console.error(`[MongoDBJobStorage] Error failing job:`, error);
        }
      }
    }
  }
