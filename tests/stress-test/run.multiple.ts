import { spawn } from 'child_process';
import { platform } from 'os';
const numberOfProcesses = 4;
const messagesPerProcess = 40000;

console.log(`Starting ${numberOfProcesses} processes...`);

const npmCommand = platform() === 'win32' ? 'npm.cmd' : 'npm';

for (let i = 0; i < numberOfProcesses; i++) {
    const workerId = i + 1;
    const child = spawn("npm", ['run', 'stress-test:postgres'], {
        shell:true,
        env: {
            ...process.env,
            WORKER_ID: workerId.toString(),
            MESSAGES_PER_SECOND: messagesPerProcess.toString(),
            AUTO_START: 'true'
        }
    });

    // Prefix all output with worker ID
    child.stdout?.on('data', (data) => {
        const lines = data.toString().split('\n').filter(line => line.trim());
        lines.forEach(line => {
            console.log(`[Worker ${workerId}] ${line}`);
        });
    });

    child.stderr?.on('data', (data) => {
        const lines = data.toString().split('\n').filter(line => line.trim());
        lines.forEach(line => {
            console.error(`[Worker ${workerId} ERROR] ${line}`);
        });
    });

    child.on('close', (code) => {
        console.log(`[Worker ${workerId}] Process finished with code ${code}`);
    });
}