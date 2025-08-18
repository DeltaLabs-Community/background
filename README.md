# Background

An extendible background job queue for Nodejs and Bun

## Features

- 🚀 **Performance**: Parallel job processing with configurable concurrency
- 💾 **Multiple Storage Options**: In-memory (default) , Redis  or PostgeSQL and MongoDB
- 📅 **Job Scheduling**: Schedule jobs to run at specific times or after delays
- 🔄 **Repeatable Jobs**:You can repeat jobs to run the specific intervals.
- ⚡ **Distributed Processing**: Support for multiple instances with Redis backend
- 🔄 **Type Safety**: Built with TypeScript for excellent developer experience
- 🔥 **Extendible**: Easily extend and write your own logic
- 🔄 **Prefetching** : Fetch jobs in batches for performance
- ⚡ **Intelligent Polling** : Enable intelligent polling for reducing cpu usage, and increase polling and concurrency among high loads.

## Documentation

Please refer to the documentation site for detailed explanation.
[https://backgroundjs.dev/](https://backgroundjs.dev/)
## Installation

### Node.js

```bash
npm install @backgroundjs/core
```

### Bun

```bash
bun add @backgroundjs/core
```

## Example Usage(In Memory)
```typescript
import {InMemoryJobStorage,JobQueue} from "@backgroundjs/core";

const storage = new InMemoryJobStorage();

const queue = new JobQueue(storage,{
    concurrency:10,
    preFetchBatchSize:100,
    maxRetries:3,
    processingInterval:50,
})

queue.register("test-job",async (data)=>{
    await new Promise((resolve) => setTimeout(resolve, 100));
    return {processed:true}
})

queue.addEventListener("completed", (event: any) => {
    console.log("job completed",event.data?.job.id);
});

queue.start();

for (let i = 0; i < 1000; i++) {
  queue.add("test-job", {
    i: i,
  });
}
```

## Example Redis
```typescript
import { Redis } from "ioredis";
import { RedisJobStorage, RedisJobQueue } from "@backgroundjs/core";

const redis = new Redis("redis://localhost:6379");

redis.on("error", (err) => {
  console.error(err);
});

redis.on("connect", () => {
  console.log("Connected to Redis");
});

const jobStorage = new RedisJobStorage(redis, {
  keyPrefix: "test",
});

const queue = new RedisJobQueue(jobStorage, {
  name: "test",
  processingInterval: 50,
  maxRetries: 3,
  concurrency: 10,
  preFetchBatchSize:100,
  logging: false,
});

queue.register("test-job", async (data) => {
  await new Promise((resolve) => setTimeout(resolve, 100));
  return { processed: true };
});

queue.addEventListener("completed", (event: any) => {
  console.log("job completed",event.data?.job.id);
});

queue.addEventListener("buffer-refill-success", (event: any) => {
  console.log("buffer-refill-success")
});

queue.start();

for(let i = 0; i < 1000; i ++){
  queue.add("test-job",{
    i:i
  })
}

["SIGINT", "SIGTERM", "SIGKILL"].forEach((signal) => {
  process.on(signal, () => {
    queue.stop();
    process.exit(0);
  });
});
```

## License
MIT
