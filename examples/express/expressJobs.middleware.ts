import { Express } from "express";
import { JobQueue } from "../../src/queue/job-queue";

export const expressJobs = (app: Express, queues: JobQueue[]) => {
  queues.forEach((queue) => {
    queue.start();
  });
  app.use((req, res, next) => {
    req.queues = queues;
    next();
  });

  ["SIGINT", "SIGTERM"].forEach((signal) => {
    process.on(signal, () => {
      for (const queue of queues) {
        queue.stop();
      }
    });
  });
};
