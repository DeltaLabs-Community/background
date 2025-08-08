import { spawn } from 'child_process';
import { platform } from 'os';
import { createWriteStream, mkdirSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const logsDir = join(__dirname, 'logs');
if (!existsSync(logsDir)) {
  mkdirSync(logsDir);
}
const numberOfProcesses = 4;
const messagesPerProcess = 40000;

console.log(`Starting ${numberOfProcesses} processes...`);

const npmCommand = platform() === 'win32' ? 'npm.cmd' : 'npm';

for (let i = 0; i < numberOfProcesses; i++) {
    const workerId = i + 1;
    const child = spawn("npm", ['run', 'stress-test:distributed'], {
        shell:true,
        env: {
            ...process.env,
            WORKER_ID: workerId.toString(),
            MESSAGES_PER_SECOND: messagesPerProcess.toString(),
            AUTO_START: 'true'
        }
    });

    // Prefix all output with worker ID
    const logFile = join(logsDir, `worker_${workerId}.log`);
    const logStream = createWriteStream(logFile);

    child.stdout?.pipe(logStream);
    child.stderr?.pipe(logStream);

    child.on('close', (code) => {
        console.log(`[Worker ${workerId}] Process finished with code ${code}`);
    });
}
