/**
 * Workflow Executor — Runs multi-step workflows with inter-step data passing.
 *
 * A workflow is an ordered list of steps, each specifying a taskType and payload.
 * Steps can reference outputs from previous steps using {{stepN.field}} syntax.
 * If any step fails, the workflow fails (unless continueOnError is set).
 *
 * Common Executor Interface:
 *   execute(task, logCallback) → { data, artifacts, executionTimeMs, exitCode }
 *   cancel() → void
 *   getStatus() → { running, progress? }
 */

import { getExecutor } from "./index.mjs";

let running = false;
let currentStep = 0;
let totalSteps = 0;
let cancelRequested = false;
let activeExecutor = null;

/**
 * Interpolate {{stepN.path}} references in an object using previous step results.
 */
function interpolate(obj, stepResults) {
  if (typeof obj === "string") {
    return obj.replace(/\{\{step(\d+)\.([^}]+)\}\}/g, (_, stepIdx, path) => {
      const idx = parseInt(stepIdx, 10);
      const result = stepResults[idx];
      if (!result) return `{{step${stepIdx}.${path}}}`;

      // Navigate the path (e.g., "data.stdout" → result.data.stdout)
      const parts = path.split(".");
      let val = result;
      for (const p of parts) {
        if (val == null) return "";
        val = val[p];
      }
      return val != null ? String(val) : "";
    });
  }

  if (Array.isArray(obj)) {
    return obj.map((item) => interpolate(item, stepResults));
  }

  if (obj && typeof obj === "object") {
    const result = {};
    for (const [key, val] of Object.entries(obj)) {
      result[key] = interpolate(val, stepResults);
    }
    return result;
  }

  return obj;
}

/**
 * Execute a multi-step workflow.
 *
 * Payload fields:
 *   steps: Array<{ taskType, payload, name?, continueOnError? }> (required)
 *   continueOnError: boolean — Global flag to continue on step failure
 *
 * @param {Object} task
 * @param {Function} logCallback
 */
export async function execute(task, logCallback) {
  const { payload, timeoutMs = 600000 } = task;
  const { steps, continueOnError = false } = payload;

  if (!steps || !Array.isArray(steps) || steps.length === 0) {
    throw new Error("Workflow task missing 'steps' array in payload");
  }

  running = true;
  cancelRequested = false;
  currentStep = 0;
  totalSteps = steps.length;
  const startTime = Date.now();
  const deadline = startTime + timeoutMs;
  const log = (msg) => logCallback?.([`[workflow] ${msg}`]);

  const stepResults = [];
  const allArtifacts = [];
  let lastExitCode = 0;

  try {
    log(`Starting workflow with ${steps.length} steps`);

    for (let i = 0; i < steps.length; i++) {
      if (cancelRequested) {
        throw new Error("Workflow cancelled");
      }

      if (Date.now() >= deadline) {
        throw new Error(`Workflow timed out after ${timeoutMs}ms`);
      }

      currentStep = i;
      const step = steps[i];
      const stepName = step.name || `Step ${i}`;
      const stepTaskType = step.taskType;

      if (!stepTaskType) {
        throw new Error(`${stepName}: missing taskType`);
      }

      log(`[${i}/${steps.length}] ${stepName} (${stepTaskType})`);

      // Interpolate payload with previous step results
      const interpolatedPayload = interpolate(step.payload || {}, stepResults);

      // Get the executor for this step's task type
      const executor = getExecutor(stepTaskType);
      if (!executor) {
        throw new Error(`${stepName}: unknown task type "${stepTaskType}"`);
      }

      activeExecutor = executor;

      // Calculate remaining time for this step
      const remainingMs = deadline - Date.now();
      const stepTimeoutMs = step.timeoutMs
        ? Math.min(step.timeoutMs, remainingMs)
        : remainingMs;

      const stepTask = {
        ...task,
        taskType: stepTaskType,
        payload: interpolatedPayload,
        timeoutMs: stepTimeoutMs,
      };

      try {
        const result = await executor.execute(stepTask, (lines) => {
          logCallback?.(lines.map((l) => `  [${stepName}] ${l}`));
        });

        stepResults.push(result);
        if (result.artifacts) {
          allArtifacts.push(...result.artifacts);
        }
        lastExitCode = result.exitCode || 0;

        log(`[${i}/${steps.length}] ${stepName} completed (exit: ${lastExitCode})`);

        // Check if step failed (non-zero exit) and we should stop
        if (lastExitCode !== 0 && !step.continueOnError && !continueOnError) {
          log(`Step "${stepName}" failed with exit code ${lastExitCode} — stopping workflow`);
          break;
        }
      } catch (err) {
        log(`[${i}/${steps.length}] ${stepName} FAILED: ${err.message}`);
        stepResults.push({ error: err.message, exitCode: 1 });

        if (!step.continueOnError && !continueOnError) {
          throw err;
        }
        lastExitCode = 1;
      } finally {
        activeExecutor = null;
      }
    }

    log(`Workflow completed — ${stepResults.length}/${steps.length} steps executed`);

    return {
      data: {
        steps: stepResults,
        completedSteps: stepResults.length,
        totalSteps: steps.length,
      },
      artifacts: allArtifacts,
      executionTimeMs: Date.now() - startTime,
      exitCode: lastExitCode,
    };
  } finally {
    running = false;
    activeExecutor = null;
  }
}

export async function cancel() {
  cancelRequested = true;
  if (activeExecutor?.cancel) {
    await activeExecutor.cancel();
  }
}

export function getStatus() {
  return {
    running,
    progress: totalSteps > 0 ? `${currentStep + 1}/${totalSteps}` : undefined,
  };
}
