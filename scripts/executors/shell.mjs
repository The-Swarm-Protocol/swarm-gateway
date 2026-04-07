/**
 * Shell Executor — Runs shell commands with timeout and output capture.
 *
 * Common Executor Interface:
 *   execute(task, logCallback) → { data, artifacts, executionTimeMs, exitCode }
 *   cancel() → void
 *   getStatus() → { running, pid?, progress? }
 */

import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const AGENT_DIR = join(__dirname, "../..");

let activeProc = null;

/**
 * Execute a shell command with stdout/stderr streaming.
 *
 * @param {Object} task - The gateway task
 * @param {Function} logCallback - Called with log lines: logCallback(lines: string[])
 * @returns {{ data: Object, artifacts: Array, executionTimeMs: number, exitCode: number }}
 */
export async function execute(task, logCallback) {
  const { payload, timeoutMs = 60000 } = task;
  const { command, args = [], cwd, env } = payload;

  if (!command) throw new Error("Shell task missing 'command' in payload");

  const startTime = Date.now();

  return new Promise((resolve, reject) => {
    const proc = spawn(command, args, {
      shell: true,
      cwd: cwd || AGENT_DIR,
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    activeProc = proc;

    let stdout = "";
    let stderr = "";
    let killed = false;
    const logBuffer = [];
    let flushTimer = null;

    // Batch log lines and flush every 200ms for efficiency
    function flushLogs() {
      if (logBuffer.length > 0 && logCallback) {
        logCallback([...logBuffer]);
        logBuffer.length = 0;
      }
    }

    flushTimer = setInterval(flushLogs, 200);

    const timer = setTimeout(() => {
      killed = true;
      proc.kill("SIGKILL");
    }, timeoutMs);

    proc.stdout.on("data", (data) => {
      const chunk = data.toString();
      stdout += chunk;
      const lines = chunk.split("\n").filter(Boolean);
      logBuffer.push(...lines.map((l) => `[stdout] ${l}`));
    });

    proc.stderr.on("data", (data) => {
      const chunk = data.toString();
      stderr += chunk;
      const lines = chunk.split("\n").filter(Boolean);
      logBuffer.push(...lines.map((l) => `[stderr] ${l}`));
    });

    proc.on("error", (err) => {
      clearTimeout(timer);
      clearInterval(flushTimer);
      flushLogs();
      activeProc = null;
      reject(new Error(`Spawn error: ${err.message}`));
    });

    proc.on("close", (code) => {
      clearTimeout(timer);
      clearInterval(flushTimer);
      flushLogs();
      activeProc = null;

      if (killed) {
        reject(new Error(`Process killed: execution timed out after ${timeoutMs}ms`));
        return;
      }

      resolve({
        data: {
          stdout: stdout.trim(),
          stderr: stderr.trim(),
        },
        artifacts: [],
        executionTimeMs: Date.now() - startTime,
        exitCode: code || 0,
      });
    });
  });
}

export function cancel() {
  if (activeProc) {
    activeProc.kill("SIGTERM");
    setTimeout(() => {
      if (activeProc) activeProc.kill("SIGKILL");
    }, 5000);
  }
}

export function getStatus() {
  return {
    running: !!activeProc,
    pid: activeProc?.pid,
  };
}
