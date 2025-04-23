import { Collection, MongoClient } from "mongodb";
import { JobStorage } from ".";
import { Job, JobStatus } from "../types";

export class MongoDBJobStorage implements JobStorage {
    private readonly collection: Collection<Job>;
    private readonly mongoClient: MongoClient;
    
    constructor(mongoClient: MongoClient, options: { collectionName?: string } = {}) {
        this.mongoClient = mongoClient;
        this.collection = this.mongoClient.db().collection(options.collectionName || "jobs");
    }

    /**
     * Save a job
     * @param job - The job to save
     */
    async saveJob(job: Job): Promise<void> {
        await this.collection.insertOne(job);
    }

    /**
     * Get a job by ID
     * @param id - The ID of the job to get
     * @returns The job with the given ID, or null if the job does not exist
     */
    async getJob(id: string): Promise<Job | null> {
        return await this.collection.findOne({ id });
    }

    /**
     * Get all jobs by status
     * @param status - The status of the jobs to get
     * @returns An array of jobs with the given status
     */
    async getJobsByStatus(status: JobStatus): Promise<Job[]> {
        return await this.collection.find({ status }).toArray();
    }

    /**
     * Update a job
     * @param job - The job to update
     * @throws Error if the job is not found
     */
    async updateJob(job: Job): Promise<void> {
        const oldJob = await this.getJob(job.id);
        if (!oldJob) {
            throw new Error(`Job with ID ${job.id} not found`);
        }
        await this.collection.updateOne({ id: job.id }, { $set: job });
    }

    /**
     * Acquire a job lock
     * @param jobId - The ID of the job to acquire the lock for
     * @param ttl - The time to live for the lock in seconds
     * @returns True if the lock was acquired, false if the lock already exists
     */
    async acquireJobLock(jobId: string, ttl: number = 30): Promise<boolean> {
        const lock = await this.collection.findOne({ id: jobId, lock: { $exists: true } });
        if (lock) {
            return false;
        }
        await this.collection.updateOne({ id: jobId }, { $set: { lock: {expiresAt: new Date(Date.now() + ttl * 1000)}}});
        return true;
    }

    /**
     * Release a job lock
     * @param jobId - The ID of the job to release the lock for
     * @returns True if the lock was released, false if the lock did not exist
     */
    async releaseJobLock(jobId: string): Promise<boolean> {
        await this.collection.updateOne({ id: jobId }, { $unset: { lock: 1 } });
        return true;
    }
}
