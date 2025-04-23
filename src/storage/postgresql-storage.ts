import { Pool, PoolClient } from 'pg';
import { JobStorage } from './index';
import { Job, JobStatus } from '../types';

export class PostgreSQLJobStorage implements JobStorage {
  private readonly pool: Pool;
  private readonly tableName: string;
  private initialized: boolean = false;

  /**
   * Create a new PostgreSQL job storage
   * 
   * @param pool - A pg Pool instance
   * @param options - Configuration options
   */
  constructor(pool: Pool, options: { tableName?: string } = {}) {
    this.pool = pool;
    this.tableName = options.tableName || 'jobs';
  }

  /**
   * Initialize the database tables if they don't exist
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

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
          result JSONB,
          retry_count INTEGER DEFAULT 0,
          lock_id TEXT,
          lock_expires_at TIMESTAMP WITH TIME ZONE
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
    await this.initialize();
    await this.pool.query(
      `INSERT INTO ${this.tableName} (
        id, name, data, status, created_at, scheduled_at, retry_count
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        job.id,
        job.name,
        JSON.stringify(job.data),
        job.status,
        job.createdAt,
        job.scheduledAt || null,
        job.retryCount || 0
      ]
    );
  }

  /**
   * Get a job by ID
   */
  async getJob(id: string): Promise<Job | null> {
    await this.initialize();
    const result = await this.pool.query(
      `SELECT * FROM ${this.tableName} WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToJob(result.rows[0]);
  }

  /**
   * Get jobs by status
   */
  async getJobsByStatus(status: JobStatus): Promise<Job[]> {
    await this.initialize();
    const result = await this.pool.query(
      `SELECT * FROM ${this.tableName} WHERE status = $1`,
      [status]
    );

    return result.rows.map(row => this.mapRowToJob(row));
  }

  /**
   * Update a job
   */
  async updateJob(job: Job): Promise<void> {
    await this.initialize();
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
        retry_count = $10
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
        job.retryCount || 0
      ]
    );

    if (result.rowCount === 0) {
      throw new Error(`Job with ID ${job.id} not found`);
    }
  }

  /**
   * Acquire a job lock
   * @param jobId - The ID of the job to lock
   * @param ttl - Time to live in seconds
   */
  async acquireJobLock(jobId: string, ttl: number = 30): Promise<boolean> {
    await this.initialize();
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const checkResult = await client.query(
        `SELECT id FROM ${this.tableName} 
         WHERE id = $1 AND (
           lock_id IS NOT NULL AND 
           lock_expires_at > NOW()
         )`,
        [jobId]
      );

      // If job is locked, return false
      if (checkResult.rows.length > 0) {
        await client.query('ROLLBACK');
        return false;
      }

      // Lock the job
      const lockId = Math.random().toString(36).substring(2, 15);
      const expiresAt = new Date(Date.now() + ttl * 1000);

      await client.query(
        `UPDATE ${this.tableName} SET
          lock_id = $2,
          lock_expires_at = $3
         WHERE id = $1`,
        [jobId, lockId, expiresAt]
      );

      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      return false;
    } finally {
      client.release();
    }
  }

  /**
   * Release a job lock
   * @param jobId - The ID of the job to unlock
   */
  async releaseJobLock(jobId: string): Promise<boolean> {
    await this.initialize();
    try {
      const result = await this.pool.query(
        `UPDATE ${this.tableName} SET
          lock_id = NULL,
          lock_expires_at = NULL
         WHERE id = $1`,
        [jobId]
      );
      return result.rowCount > 0;
    } catch (error) {
      return false;
    }
  }

  /**
   * Map a database row to a Job object
   */
  private mapRowToJob(row: any): Job {
    return {
      id: row.id,
      name: row.name,
      data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
      status: row.status as JobStatus,
      createdAt: new Date(row.created_at),
      scheduledAt: row.scheduled_at ? new Date(row.scheduled_at) : undefined,
      startedAt: row.started_at ? new Date(row.started_at) : undefined,
      completedAt: row.completed_at ? new Date(row.completed_at) : undefined,
      error: row.error || undefined,
      result: row.result ? (typeof row.result === 'string' ? JSON.parse(row.result) : row.result) : undefined,
      retryCount: row.retry_count || 0
    };
  }
} 