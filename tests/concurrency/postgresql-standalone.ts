import { Pool } from "pg";
import { PostgreSQLJobStorage, PostgreSQLJobQueue } from "../../src";


async function addJobsToQueue(queue:PostgreSQLJobQueue,count:number,waitbeforeTimeout?:number) {
  if(waitbeforeTimeout){
    await new Promise((resolve) => setTimeout(resolve, waitbeforeTimeout));
  }
  for (let i = 0; i < count; i++) {
    queue.add("stress-test-job", {
      i: i,
    });
  }
}


const pool = new Pool({
  connectionString: "postgresql://postgres:12345@localhost:5432/postgres",
});

const storage = new PostgreSQLJobStorage(pool, {
  tableName: "jobs",
});

const queue = new PostgreSQLJobQueue(storage, {
  name: "test",
  processingInterval: 50,
  concurrency : 20,
  preFetchBatchSize:100,
  maxRetries: 3,
  intelligentPolling:true,
  loadFactor:0.5,
  maxEmptyPolls:10,
  maxInterval:2000,
  minInterval:50,
  logging:true
});

queue.register("stress-test-job", async (data) => {
  await new Promise((resolve) => setTimeout(resolve, 40));
});

// queue.addEventListener("completed", (event: any) => {
//   console.log("job completed", event.data?.job.id);
// });

queue.start();

addJobsToQueue(queue,1000);

addJobsToQueue(queue,100,20000);

console.log("queue started");
["SIGINT", "SIGTERM"].forEach((signal) => {
    process.on(signal, () => {
        queue.stop();
        pool.end();
        console.log("queue stopped");
    });
});


