// Define job status types
export type JobStatus = "pending" | "processing" | "completed" | "failed";

// Define job interface
export interface Job<T = any> {
  id: string;
  name: string;
  data: T;
  status: JobStatus;
  createdAt: Date;
  scheduledAt?: Date;
  startedAt?: Date;
  completedAt?: Date;
  error?: string;
  priority?: number;
  result?: any;
  retryCount?: number;
  timeout?: number;
  repeat?: {
    every?: number;
    unit?: "seconds" | "minutes" | "hours" | "days" | "weeks" | "months";
    startDate?: Date;
    endDate?: Date;
    limit?: number;
  };
}

// Define job handler type
export type JobHandler<T = any, R = any> = (data: T) => Promise<R>;
