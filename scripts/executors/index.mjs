/**
 * Executor Registry — Maps task types to their executor modules.
 *
 * Each executor implements the common interface:
 *   execute(task, logCallback) → Promise<{ data, artifacts, executionTimeMs, exitCode }>
 *   cancel() → Promise<void>
 *   getStatus() → { running, pid?, progress? }
 */

import * as shellExecutor from "./shell.mjs";
import * as dockerExecutor from "./docker.mjs";
import * as comfyuiExecutor from "./comfyui.mjs";
import * as workflowExecutor from "./workflow.mjs";

/** Built-in executor registry */
const executors = new Map([
  ["shell", shellExecutor],
  ["docker", dockerExecutor],
  ["node", {
    // Node executor is a thin wrapper around shell
    execute: async (task, logCallback) => {
      const { payload, timeoutMs = 60000 } = task;
      const { script } = payload;
      if (!script) throw new Error("Node task missing 'script' in payload");
      return shellExecutor.execute(
        { payload: { command: "node", args: ["-e", script] }, timeoutMs },
        logCallback,
      );
    },
    cancel: () => shellExecutor.cancel(),
    getStatus: () => shellExecutor.getStatus(),
  }],
  ["comfyui", comfyuiExecutor],
  ["workflow", workflowExecutor],
]);

/**
 * Get an executor by task type.
 * @param {string} taskType
 * @returns {Object|null} The executor module or null
 */
export function getExecutor(taskType) {
  return executors.get(taskType) || null;
}

/**
 * Register a custom executor for a task type.
 * @param {string} taskType
 * @param {Object} executor — Must implement { execute, cancel, getStatus }
 */
export function registerExecutor(taskType, executor) {
  if (!executor.execute || typeof executor.execute !== "function") {
    throw new Error(`Executor for "${taskType}" must have an execute() method`);
  }
  executors.set(taskType, executor);
}

/**
 * List all registered task types.
 * @returns {string[]}
 */
export function listTaskTypes() {
  return [...executors.keys()];
}

/**
 * Execute a task by looking up the appropriate executor.
 *
 * @param {Object} task — Must include taskType
 * @param {Function} logCallback — Called with batches of log lines
 * @returns {Promise<{ data, artifacts, executionTimeMs, exitCode }>}
 */
export async function executeTask(task, logCallback) {
  const executor = getExecutor(task.taskType);
  if (!executor) {
    throw new Error(`Unknown task type: ${task.taskType}. Available: ${listTaskTypes().join(", ")}`);
  }
  return executor.execute(task, logCallback);
}

/**
 * Cancel the active executor for a given task type.
 * @param {string} taskType
 */
export async function cancelTask(taskType) {
  const executor = getExecutor(taskType);
  if (executor?.cancel) {
    await executor.cancel();
  }
}
