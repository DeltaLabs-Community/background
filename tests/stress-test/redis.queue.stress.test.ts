import readline from "readline";
import Redis from "ioredis";
import { DistributedJobQueue, RedisJobStorage } from "../../src";
import dotenv from "dotenv";

dotenv.config();

let testConfig = {
    messagePerSecond: 40000,
    redisHost: "localhost",
    redisPort: 6379,
    concurrency: 8,
    rateLimitingEnabled: true,
    redisPassword: "",
    testDurationSeconds: 30 // Added test duration
};

// Analytics tracking
let analytics = {
    totalJobsAdded: 0,
    totalJobsProcessed: 0,
    startTime: 0,
    endTime: 0,
    errors: 0,
    avgResponseTime: 0,
    responseTimes: [] as number[],
    jobsAddedThisSecond: 0,
    jobsProcessedThisSecond: 0,
    errorsThisSecond: 0,
    lastSecondTimestamp: 0,
    peakAddRate: 0,
    peakProcessRate: 0
};

async function reportAnalytics() {
    const duration = (analytics.endTime - analytics.startTime) / 1000;
    const actualJobsPerSecond = analytics.totalJobsAdded / duration;
    const processedJobsPerSecond = analytics.totalJobsProcessed / duration;
    const avgResponseTime = analytics.responseTimes.length > 0 
        ? analytics.responseTimes.reduce((a, b) => a + b, 0) / analytics.responseTimes.length 
        : 0;

    console.log("\n=== STRESS TEST RESULTS ===");
    console.log(`Test Duration: ${duration.toFixed(2)} seconds`);
    console.log(`Target Messages/Second: ${testConfig.messagePerSecond}`);
    console.log(`Actual Messages/Second: ${actualJobsPerSecond.toFixed(2)}`);
    console.log(`Peak Add Rate: ${analytics.peakAddRate}/s`);
    console.log(`Peak Process Rate: ${analytics.peakProcessRate}/s`);
    console.log(`Total Jobs Added: ${analytics.totalJobsAdded}`);
    console.log(`Total Jobs Processed: ${analytics.totalJobsProcessed}`);
    console.log(`Processed Jobs/Second: ${processedJobsPerSecond.toFixed(2)}`);
    console.log(`Errors: ${analytics.errors}`);
    console.log(`Average Response Time: ${avgResponseTime.toFixed(2)}ms`);
    console.log(`Concurrency: ${testConfig.concurrency}`);
    console.log(`Rate Limiting Enabled: ${testConfig.rateLimitingEnabled}`);
    console.log("===========================\n");
}

function updateRealTimeStats() {
    const currentTime = Date.now();
    const currentSecond = Math.floor(currentTime / 1000);
    
    if (analytics.lastSecondTimestamp !== currentSecond) {
        // Update peak rates
        if (analytics.jobsAddedThisSecond > analytics.peakAddRate) {
            analytics.peakAddRate = analytics.jobsAddedThisSecond;
        }
        if (analytics.jobsProcessedThisSecond > analytics.peakProcessRate) {
            analytics.peakProcessRate = analytics.jobsProcessedThisSecond;
        }
        
        // Reset counters for new second
        analytics.jobsAddedThisSecond = 0;
        analytics.jobsProcessedThisSecond = 0;
        analytics.errorsThisSecond = 0;
        analytics.lastSecondTimestamp = currentSecond;
    }
}

function displayRealTimeProgress() {
    const elapsed = (Date.now() - analytics.startTime) / 1000;
    const avgAddRate = analytics.totalJobsAdded / elapsed;
    const avgProcessRate = analytics.totalJobsProcessed / elapsed;
    const queueBacklog = analytics.totalJobsAdded - analytics.totalJobsProcessed;
    
    // Clear the line and move cursor to beginning
    process.stdout.write('\r\x1b[K');
    
    const progressLine = `⏱️  ${elapsed.toFixed(1)}s | ➕ ${analytics.totalJobsAdded} (${avgAddRate.toFixed(0)}/s, peak: ${analytics.peakAddRate}/s) | ✅ ${analytics.totalJobsProcessed} (${avgProcessRate.toFixed(0)}/s, peak: ${analytics.peakProcessRate}/s) | 📋 Queue: ${queueBacklog} | ❌ ${analytics.errors}`;
    
    process.stdout.write(progressLine);
}

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

function askQuestion(question: string): Promise<string> {
    return new Promise((resolve) => {
        rl.question(question, (answer) => {
            resolve(answer);
        });
    });
}

async function collectUserInput() {
    try {
        const messagesAnswer = await askQuestion("How many messages you want per second? (default is 40,000): ");
        const messages = parseInt(messagesAnswer) || testConfig.messagePerSecond;
        if (messages > 0) {
            testConfig.messagePerSecond = messages;
        }

        const concurrencyAnswer = await askQuestion("Please specify the concurrency (default is 8): ");
        const concurrency = parseInt(concurrencyAnswer) || testConfig.concurrency;
        if (concurrency > 0) {
            testConfig.concurrency = concurrency;
        }

        const hostAnswer = await askQuestion("Please specify the redis host (default is localhost): ");
        testConfig.redisHost = hostAnswer || testConfig.redisHost;

        const portAnswer = await askQuestion("Please specify the redis port (default is 6379): ");
        const port = parseInt(portAnswer) || testConfig.redisPort;
        if (port > 0) {
            testConfig.redisPort = port;
        }

        const passwordAnswer = await askQuestion("Please specify the redis password (default is empty): ");
        testConfig.redisPassword = passwordAnswer || testConfig.redisPassword;

        const rateLimitAnswer = await askQuestion("Do you want rate limiting to be enabled? (true/false, default is true): ");
        testConfig.rateLimitingEnabled = rateLimitAnswer === "false" ? false : true;

        const durationAnswer = await askQuestion("Test duration in seconds (default is 30): ");
        const duration = parseInt(durationAnswer) || 30;
        if (duration > 0) {
            testConfig.testDurationSeconds = duration;
        }

    } catch (error) {
        console.error("Error collecting input:", error);
        rl.close();
        process.exit(1);
    }
}

async function runStressTest() {
    const redis = new Redis({
        host: testConfig.redisHost,
        port: testConfig.redisPort,
        password: testConfig.redisPassword || undefined,
        lazyConnect: true
    });

    console.log("Please wait while connecting to redis...");
    
    try {
        await redis.connect();
        console.log("Redis connected successfully.");
        console.log("Stress test starting...");
        console.log(`Configuration: ${JSON.stringify(testConfig, null, 2)}`);

        const storage = new RedisJobStorage(redis);
        const queue = new DistributedJobQueue(storage, {
            concurrency: testConfig.concurrency,
            logging: false,
            standAlone: false,
            processingInterval:50,
            preFetchBatchSize:1000,
        });

        // Register a simple job handler for stress testing
        queue.register("stress-test-job", async (data: any) => {
            const startTime = Date.now();
            // Simulate some work
            await new Promise(resolve => setTimeout(resolve, Math.random() * 10));
            const endTime = Date.now();
            
            analytics.totalJobsProcessed++;
            analytics.jobsProcessedThisSecond++;
            analytics.responseTimes.push(endTime - startTime);
            updateRealTimeStats();
            
            return { processed: true, timestamp: Date.now() };
        });

        // Start the queue processing
        await queue.start();

        analytics.startTime = Date.now();
        analytics.lastSecondTimestamp = Math.floor(Date.now() / 1000);
        
        const intervalMs = 1000; // Add jobs every second
        const jobsPerInterval = Math.ceil(testConfig.messagePerSecond / (1000 / intervalMs));
        
        console.log(`Adding ${jobsPerInterval} jobs every ${intervalMs}ms for ${testConfig.testDurationSeconds} seconds...`);
        console.log("Real-time progress (Time | Added (rate, peak) | Processed (rate, peak) | Queue backlog | Errors):");

        const addJobsInterval = setInterval(async () => {
            const promises : Promise<any>[] = [];
            
            for (let i = 0; i < jobsPerInterval; i++) {
                const jobPromise = queue.add("stress-test-job", {
                    id: analytics.totalJobsAdded + i,
                    timestamp: Date.now(),
                    data: `test-data-${analytics.totalJobsAdded + i}`
                }, { 
                    priority: Math.floor(Math.random() * 5) + 1,
                    timeout: 30000 
                }).then(() => {
                    analytics.totalJobsAdded++;
                    analytics.jobsAddedThisSecond++;
                    updateRealTimeStats();
                }).catch((error) => {
                    analytics.errors++;
                    analytics.errorsThisSecond++;
                    updateRealTimeStats();
                    if (analytics.errors <= 10) { // Only log first 10 errors to avoid spam
                        console.error(`\nError adding job: ${error.message}`);
                    }
                });
                
                promises.push(jobPromise);
            }
            
            await Promise.allSettled(promises);
        }, intervalMs);

        // Real-time display update (every 100ms for smooth updates)
        const displayInterval = setInterval(() => {
            displayRealTimeProgress();
        }, 50);

        // Stop the test after the specified duration
        setTimeout(async () => {
            clearInterval(addJobsInterval);
            clearInterval(displayInterval);
            analytics.endTime = Date.now();
            
            console.log("\n\nStopping stress test...");
            console.log("Waiting for remaining jobs to process...");
            
            // Wait a bit for remaining jobs to process with progress updates
            const waitStart = Date.now();
            const waitInterval = setInterval(() => {
                const waitTime = (Date.now() - waitStart) / 1000;
                const remaining = analytics.totalJobsAdded - analytics.totalJobsProcessed;
                process.stdout.write(`\rWaiting... ${waitTime.toFixed(1)}s | Remaining jobs: ${remaining}`);
                
                if (remaining === 0 || waitTime > 10) {
                    clearInterval(waitInterval);
                    console.log("\n");
                }
            }, 200);
            
            setTimeout(async () => {
                clearInterval(waitInterval);
                await queue.stop();
                await redis.disconnect();
                await reportAnalytics();
                rl.close();
                process.exit(0);
            }, 5000);
            
        }, testConfig.testDurationSeconds * 1000);

        // Detailed progress every 10 seconds
        const detailedProgressInterval = setInterval(() => {
            const elapsed = (Date.now() - analytics.startTime) / 1000;
            const currentAddRate = analytics.totalJobsAdded / elapsed;
            const currentProcessRate = analytics.totalJobsProcessed / elapsed;
            const queueBacklog = analytics.totalJobsAdded - analytics.totalJobsProcessed;
            const targetProgress = (elapsed / testConfig.testDurationSeconds) * 100;
            
            console.log(`\n📊 [${elapsed.toFixed(0)}s/${testConfig.testDurationSeconds}s - ${targetProgress.toFixed(1)}%] Detailed Stats:`);
            console.log(`   ➕ Added: ${analytics.totalJobsAdded} (avg: ${currentAddRate.toFixed(0)}/s, peak: ${analytics.peakAddRate}/s)`);
            console.log(`   ✅ Processed: ${analytics.totalJobsProcessed} (avg: ${currentProcessRate.toFixed(0)}/s, peak: ${analytics.peakProcessRate}/s)`);
            console.log(`   📋 Queue Backlog: ${queueBacklog} jobs`);
            console.log(`   ❌ Errors: ${analytics.errors}`);
            if (analytics.responseTimes.length > 0) {
                const recentResponses = analytics.responseTimes.slice(-1000);
                const avgResponseTime = recentResponses.reduce((a, b) => a + b, 0) / recentResponses.length;
                console.log(`   ⏱️  Avg Response Time (last 1000): ${avgResponseTime.toFixed(2)}ms`);
            }
            console.log(""); // Extra line for readability
        }, 10000);

        // Clean up detailed progress interval when test ends
        setTimeout(() => {
            clearInterval(detailedProgressInterval);
        }, testConfig.testDurationSeconds * 1000);

    } catch (error) {
        console.error("Redis connection failed:", error);
        await redis.disconnect();
        rl.close();
        process.exit(1);
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT. Graceful shutdown...');
    analytics.endTime = Date.now();
    await reportAnalytics();
    rl.close();
    process.exit(0);
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    analytics.errors++;
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    analytics.errors++;
});

// Main execution
async function main() {
    console.log("Redis Job Queue Stress Test");
    console.log("==========================");
    
    await collectUserInput();
    rl.close();
    
    await runStressTest();
}

main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
});