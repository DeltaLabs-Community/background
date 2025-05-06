import { Pool } from "pg";
import { PostgreSQLJobStorage, PostgreSQLJobQueue } from "../../src";

const pool = new Pool({
  connectionString: "postgresql://postgres:12345@localhost:5432/postgres",
});

const storage = new PostgreSQLJobStorage(pool, {
  tableName: "jobs",
});

const queue = new PostgreSQLJobQueue(storage, {
  name: "test",
  processingInterval: 500,
  maxRetries: 3,
});

queue.register("test-job", async (data) => {
  await new Promise((resolve) => setTimeout(resolve, 3000));
});

queue.addEventListener("completed", (event: any) => {
  console.log("job completed", event.data?.job.id);
});

queue.start();

console.log("queue started");
["SIGINT", "SIGTERM"].forEach((signal) => {
    process.on(signal, () => {
        queue.stop();
        pool.end();
        console.log("queue stopped");
    });
});


