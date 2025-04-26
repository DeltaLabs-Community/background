import { ClientSession, Collection, MongoClient } from "mongodb";
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

export interface MongoDBStorage extends JobStorage {
    acquireNextJob(): Promise<Job | null>;
    completeJob(jobId: string, result: any): Promise<void>;
    failJob(jobId: string, error: string): Promise<void>;
}

export class MongoDBJobStorage implements MongoDBStorage {
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
        await this.collection.findOneAndUpdate({ id: job.id }, { $set: job });
    }

    /**
     * Acquire the next pending job
     * @returns The next pending job, or null if no pending jobs are available
     */
    async acquireNextJob(): Promise<Job | null> {
        try {
            const result = await this.collection.findOneAndUpdate(
                { 
                    status: "pending",
                    $or: [
                        { scheduledAt: { $exists: false } },
                        { scheduledAt: { $lte: new Date() } }
                    ]
                },
                {
                    $set: {
                        status: "processing",
                        startedAt: new Date()
                    }
                },
                { 
                    sort: { priority: 1, createdAt: 1 },
                    returnDocument: "before" 
                }
            );
            
            return result || null;
        } catch (error) {
            console.error("Error acquiring next job", error);
            return null;
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
                { $set: { status: "completed", result, completedAt: new Date() } }
            );
        } catch (error) {
            throw error;
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
                { $set: { status: "failed", error, completedAt: new Date() } }
            );
        } catch (error) {
            throw error;
        }
    }
}
