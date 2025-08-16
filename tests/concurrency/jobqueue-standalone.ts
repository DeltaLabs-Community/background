import {InMemoryJobStorage,JobQueue} from "../../src";
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

console.log("queues are started");

["SIGINT", "SIGTERM"].forEach((signal) => {
    process.on(signal, () => {
        queue.stop();
        console.log("queues are stopped");
    });
});
