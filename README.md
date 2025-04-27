# Pulse

An extendible background job queue for Nodejs and Bun

## Features

- 🚀 **Performance**: Parallel job processing with configurable concurrency
- 💾 **Multiple Storage Options**: In-memory (default) , Redis (for distributed processing) or PostgeSQL and MongoDB
- 📅 **Job Scheduling**: Schedule jobs to run at specific times or after delays
- 🔌 **Middleware**: Easy integration with many backend frameworks.
- ⚡ **Distributed Processing**: Support for multiple instances with Redis backend
- 🔄 **Type Safety**: Built with TypeScript for excellent developer experience
- 🔥 **Extendible**: Easily extend and write your own logic

## Project Structure

```
.
├── src/                    # Source code
│   ├── queue/             # Job queue implementation
│   ├── storage/           # Storage implementations
│   └── types.ts           # TypeScript types
├── tests/                 # Test files
├── dist/                  # Build output
│   ├── node/             # Node.js build
│   └── bun/              # Bun build
├── tsconfig.base.json     # Base TypeScript config
├── tsconfig.node.json     # Node.js specific config
└── tsconfig.bun.json      # Bun specific config
```

## Installation

### Node.js

```bash
npm install pulse
```

### Bun

```bash
bun add pulse
```

## Usage

### Basic Usage (In-Memory)

```typescript
// types.ts
import { ContextVariableMap } from "hono";
import type { JobQueue } from "pulse";

declare module "hono" {
  interface ContextVariableMap {
    queues?: JobQueue[];
  }
}

//honoJobs.middleware.ts
export const honoJobs = (app: Hono, queues: JobQueue[]) => {
  queues.forEach((queue) => {
    queue.start();
    app.get(`/jobs/${queue.getName()}/:jobId`, async (c, next) => {
      const { jobId } = c.req.param();
      const job = await queue.getJob(jobId);
      if (!job) {
        c.status(404);
        return c.json({ error: "Job not found" });
      }
      c.json({ job: job });
    });
  });
  return async (c: Context, next: Next) => {
    c.set("queues", queues);
    await next();
  };
};

// server.ts
import { Hono } from "hono";
import { JobQueue, InMemoryJobStorage, honoJobs } from "pulse";

const app = new Hono();
const storage = new InMemoryJobStorage();
const queue = new JobQueue(storage, { concurrency: 5 });

// Register a job handler
queue.register("email-job", async (data) => {
  console.log("Sending email to:", data.to);
  // Send email logic here
  return { success: true };
});

// Add the job queue middleware
// You can add multiple queues
app.use(honoJobs(app, [queue]));

// Add a job
app.post("/send-email", async (c) => {
  const { to, subject } = await c.req.json();
  const job = await c
    .get("queues")
    .find((q) => q.getName() == "default")
    .add("email-job", { to, subject });
  return c.json({ jobId: job.id });
});
```

### Distributed Processing with Redis

For distributed processing across multiple instances, use the Redis storage adapter:

```typescript
import { DistributedJobQueue, RedisJobStorage } from "pulse";
import Redis from "ioredis"; // You need to install this separately

// Create Redis client
const redis = new Redis("redis://localhost:6379");

// Create Redis storage
const storage = new RedisJobStorage(redis, {
  keyPrefix: "myapp:",
});

// Create distributed job queue
const queue = new DistributedJobQueue(storage, {
  concurrency: 5,
  name: "worker-1",
});

// ... Register handlers and use as before
```

### Job Scheduling

```typescript
// Schedule a job for a specific time
const futureDate = new Date();
futureDate.setHours(futureDate.getHours() + 1); // 1 hour from now
const job = await queue.schedule("reminder-job", { userId: 123 }, futureDate);

// Schedule a job with a delay (in milliseconds)
const delayedJob = await queue.scheduleIn(
  "cleanup-job",
  { path: "/tmp" },
  30 * 60 * 1000,
); // 30 minutes
```

## Development

1. Install dependencies:

```bash
npm install
```

2. Build for all runtimes:

```bash
npm run build
```

3. Run tests:

```bash
npm test
```

Or test specific runtime:

```bash
npm run test:node
npm run test:deno
npm run test:bun
```

## License

MIT
