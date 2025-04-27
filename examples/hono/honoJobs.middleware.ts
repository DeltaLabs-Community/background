import { Hono } from "hono";
import { JobQueue } from "../../src";

export const honoJobs = (app: Hono, queues: JobQueue[]) => {
    // Start all queues first
    queues.forEach((queue) => {
        queue.start();
    });
    
    // Add middleware to make queues available in context via Variables API
    app.use(async (c, next) => {
        // Using set() is the correct way to set context variables
        c.set('queues', queues);
        await next();
    });
};