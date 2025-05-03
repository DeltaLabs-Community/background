import { JobQueue } from "../../src";

declare global {
  namespace Express {
    interface Request {
      queues?: JobQueue[];
    }
  }
}
