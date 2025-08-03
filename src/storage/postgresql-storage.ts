import { Pool } from "pg";
import { JobStorage } from "./base-storage";
import { Job, JobStatus } from "../types";

export interface PostgreSQLStorage extends JobStorage {
  acquireNextJob(): Promise<Job | null>;
  completeJob(jobId: string, result: any): Promise<void>;
  acquireNextJobs(batchSize: number): Promise<Job[]>; // Add this
  failJob(jobId: string, error: string): Promise<void>;
}

/**
 * PostgreSQL storage adapter for JobQueue
 *
 * This storage adapter uses PostgreSQL to store jobs, making it suitable
 * for distributed environments with multiple instances/processes.
 */
export class PostgreSQLJobStorage implements PostgreSQLStorage {
  private readonly pool: Pool;
  private readonly tableName: string;
  private initialized: boolean = false;
  private readonly logging: boolean = false;
  private readonly staleJobTimeout: number = 1000 * 60 * 60 * 24; // 24 hours
  /**
   * Create a new PostgreSQL job storage
   *
   * @param pool - A pg Pool instance
   * @param options - Configuration options
   */
  constructor(
    pool: Pool,
    options: { tableName?: string; logging?: boolean; staleJobTimeout?: number } = {},
  ) {
    this.pool = pool;
    this.tableName = options.tableName || "jobs";
    this.logging = options.logging || false;
    this.staleJobTimeout = options.staleJobTimeout || 1000 * 60 * 60 * 24; // 24 hours
  }

  /**
   * Initialize the database tables if they don't exist
   */
  async initialize(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          data JSONB NOT NULL,
          status TEXT NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL,
          scheduled_at TIMESTAMP WITH TIME ZONE,
          started_at TIMESTAMP WITH TIME ZONE,
          completed_at TIMESTAMP WITH TIME ZONE,
          error TEXT,
          priority INTEGER DEFAULT 0,
          result JSONB,
          repeat JSONB,
          retry_count INTEGER DEFAULT 0,
          timeout INTEGER
        )
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS ${this.tableName}_status_idx 
        ON ${this.tableName} (status)
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS ${this.tableName}_scheduled_at_idx 
        ON ${this.tableName} (scheduled_at) 
        WHERE scheduled_at IS NOT NULL
      `);

      this.initialized = true;
    } finally {
      client.release();
    }
  }

  /**
   * Save a job to PostgreSQL
   */
  async saveJob(job: Job): Promise<void> {
    try {
      if (!this.initialized) {
        await this.initialize();
      }
      await this.pool.query(
        `INSERT INTO ${this.tableName} (
          id, name, data, status, created_at, scheduled_at, retry_count, priority, repeat, timeout
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [
          job.id,
          job.name,
          JSON.stringify(job.data),
          job.status,
          job.createdAt,
          job.scheduledAt || null,
          job.retryCount || 0,
          job.priority || 0,
          JSON.stringify(job.repeat),
          job.timeout || null,
        ],
      );
    } catch (error) {
      if (this.logging) {
        console.error(`[PostgreSQLJobStorage] Error saving job:`, error);
      }
    }
  }

  /**
   * Get a job by ID
   */
  async getJob(id: string): Promise<Job | null> {
    try {
      if (!this.initialized) {
        await this.initialize();
      }
      const result = await this.pool.query(
        `SELECT * FROM ${this.tableName} WHERE id = $1`,
        [id],
      );

      if (result.rows.length === 0) {
        return null;
      }

      return this.mapRowToJob(result.rows[0]);
    } catch (error) {
      if (this.logging) {
        console.error(`[PostgreSQLJobStorage] Error getting job:`, error);
      }
      return null;
    }
  }

  /**
   * Get jobs by status
   */
  async getJobsByStatus(status: JobStatus): Promise<Job[]> {
    try {
      if (!this.initialized) {
        await this.initialize();
      }
      const result = await this.pool.query(
        `SELECT * FROM ${this.tableName} WHERE status = $1`,
        [status],
      );

      return result.rows.map((row) => this.mapRowToJob(row));
    } catch (error) {
      if (this.logging) {
        console.error(
          `[PostgreSQLJobStorage] Error getting jobs by status:`,
          error,
        );
      }
      return [];
    }
  }

  /**
   * Update a job
   */
  async updateJob(job: Job): Promise<void> {
    try {
      if (!this.initialized) {
        await this.initialize();
      }
      const result = await this.pool.query(
        `UPDATE ${this.tableName} SET
          name = $2,
          data = $3,
          status = $4,
          scheduled_at = $5,
          started_at = $6,
          completed_at = $7,
          error = $8,
          result = $9,
          repeat = $10,
          retry_count = $11,
          timeout = $12,
          priority = $13
        WHERE id = $1`,
        [
          job.id,
          job.name,
          JSON.stringify(job.data),
          job.status,
          job.scheduledAt || null,
          job.startedAt || null,
          job.completedAt || null,
          job.error || null,
          job.result ? JSON.stringify(job.result) : null,
          job.repeat ? JSON.stringify(job.repeat) : null,
          job.retryCount || 0,
          job.timeout || null,
          job.priority || 0,
        ],
      );

      if (result.rowCount === 0) {
        throw new Error(`Job with ID ${job.id} not found`);
      }
    } catch (error) {
      if (this.logging) {
        console.error(`[PostgreSQLJobStorage] Error updating job:`, error);
      }
    }
  }

  /**
   * Acquire the next pending job
   * @returns The next pending job, or null if no pending jobs are available
   */
  async acquireNextJob(): Promise<Job | null> {
    if (!this.initialized) {
      await this.initialize();
    }
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      // Use the server's current time for the scheduled_at check
      const query = await client.query(
        `SELECT * FROM ${this.tableName} 
        WHERE (
          status = 'pending' 
          AND (scheduled_at IS NULL OR scheduled_at <= $1)
        )
        OR (
          status = 'processing' 
          AND started_at IS NOT NULL 
          AND completed_at IS NULL 
          AND started_at < $2
        )
        ORDER BY priority ASC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`,
        [new Date(), new Date(Date.now() - this.staleJobTimeout)],
      );

      if (query.rows.length === 0) {
        await client.query("ROLLBACK");
        return null;
      }

      let job = query.rows[0];
      const now = new Date();
      await client.query(
        `UPDATE ${this.tableName} SET status = 'processing', started_at = $1 WHERE id = $2`,
        [now, job.id],
      );
      job = this.mapRowToJob(job) as Job;
      // Add so that we do not update it again in the processJob method
      if (job) {
        job.status = "processing";
        job.startedAt = now;
      }
      await client.query("COMMIT");
      return job;
    } catch (error) {
      if (this.logging) {
        console.error(
          `[PostgreSQLJobStorage] Error acquiring next job:`,
          error,
        );
      }
      await client.query("ROLLBACK");
      return null;
    } finally {
      client.release();
    }
  }

async acquireNextJobs(batchSize: number): Promise<Job[]> {
  if (!this.initialized) {
    await this.initialize();
  }
  
  const client = await this.pool.connect();
  let committed = false;
  
  try {
    await client.query("BEGIN");

    // Acquire multiple jobs atomically
    const query = await client.query(
      `UPDATE ${this.tableName} 
      SET status = 'processing', started_at = $1
      WHERE id IN (
        SELECT id FROM ${this.tableName} 
        WHERE (
          status = 'pending' 
          AND (scheduled_at IS NULL OR scheduled_at <= $1)
        )
        OR (
          status = 'processing' 
          AND started_at IS NOT NULL 
          AND completed_at IS NULL 
          AND started_at < $2
        )
        ORDER BY priority ASC, created_at ASC 
        LIMIT $3
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *`,
      [new Date(), new Date(Date.now() - this.staleJobTimeout), batchSize]
    );

    await client.query("COMMIT");
    committed = true;
    
    return query.rows.map(row => {
      const job = this.mapRowToJob(row);
      job.status = "processing";
      job.startedAt = new Date();
      return job;
    });
    
  } catch (error) {
    if (this.logging) {
      console.error(`[PostgreSQLJobStorage] Error acquiring batch jobs:`, error);
    }
    
    if (!committed) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        if (this.logging) {
          console.error(`[PostgreSQLJobStorage] Error during rollback:`, rollbackError);
        }
      }
    }
    
    return [];
  } finally {
    try {
      client.release();
    } catch (releaseError) {
      if (this.logging) {
        console.error(`[PostgreSQLJobStorage] Error releasing client:`, releaseError);
      }
    }
  }
}

  /**
   * Complete a job
   * @param jobId - The ID of the job to complete
   * @param result - The result of the job
   */
  async completeJob(jobId: string, result: any): Promise<void> {
    try {
      if (!this.initialized) {
        await this.initialize();
      }
      await this.pool.query(
        `UPDATE ${this.tableName} SET status = 'completed', completed_at = NOW(), result = $2 WHERE id = $1`,
        [jobId, JSON.stringify(result)],
      );
    } catch (error) {
      if (this.logging) {
        console.error(`[PostgreSQLJobStorage] Error completing job:`, error);
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
      if (!this.initialized) {
        await this.initialize();
      }
      await this.pool.query(
        `UPDATE ${this.tableName} SET status = 'failed', completed_at = NOW(), error = $2 WHERE id = $1`,
        [jobId, error],
      );
    } catch (error) {
      if (this.logging) {
        console.error(`[PostgreSQLJobStorage] Error failing job:`, error);
      }
    }
  }
  /**
   * Map a database row to a Job object
   * @param row - The database row to map
   * @returns The mapped Job object
   */
  private mapRowToJob(row: any): Job {
    return {
      id: row.id,
      name: row.name,
      data: typeof row.data === "string" ? JSON.parse(row.data) : row.data,
      status: row.status as JobStatus,
      createdAt: new Date(row.created_at),
      scheduledAt: row.scheduled_at ? new Date(row.scheduled_at) : undefined,
      startedAt: row.started_at ? new Date(row.started_at) : undefined,
      completedAt: row.completed_at ? new Date(row.completed_at) : undefined,
      error: row.error || undefined,
      result: row.result
        ? typeof row.result === "string"
          ? JSON.parse(row.result)
          : row.result
        : undefined,
      repeat: row.repeat
        ? typeof row.repeat === "string"
          ? JSON.parse(row.repeat)
          : row.repeat
        : undefined,
      retryCount: row.retry_count || 0,
      timeout: row.timeout || undefined,
    };
  }
}
