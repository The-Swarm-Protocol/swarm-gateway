/**
 * ComfyUI Executor — Submits ComfyUI workflows to a local ComfyUI instance.
 *
 * Connects to a ComfyUI HTTP API running on the gateway machine (default: localhost:8188).
 * Submits the workflow prompt, polls for completion, and collects output images/artifacts.
 *
 * Common Executor Interface:
 *   execute(task, logCallback) → { data, artifacts, executionTimeMs, exitCode }
 *   cancel() → void
 *   getStatus() → { running, progress? }
 */

import http from "node:http";

const DEFAULT_COMFYUI_URL = "http://127.0.0.1:8188";
let activePromptId = null;
let running = false;
let cancelRequested = false;

/**
 * Simple HTTP request helper (no external deps).
 */
function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const reqOptions = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port,
      path: parsedUrl.pathname + parsedUrl.search,
      method: options.method || "GET",
      headers: options.headers || {},
    };

    const req = http.request(reqOptions, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        resolve({ status: res.statusCode, data, headers: res.headers });
      });
    });

    req.on("error", reject);

    if (options.body) {
      req.write(typeof options.body === "string" ? options.body : JSON.stringify(options.body));
    }
    req.end();
  });
}

/**
 * Execute a ComfyUI workflow.
 *
 * Payload fields:
 *   workflow: object (required) — ComfyUI workflow/prompt JSON
 *   comfyuiUrl: string — ComfyUI API URL (default: http://127.0.0.1:8188)
 *   clientId: string — Client ID for ComfyUI WS tracking
 *   pollIntervalMs: number — Polling interval (default: 2000)
 *
 * @param {Object} task
 * @param {Function} logCallback
 */
export async function execute(task, logCallback) {
  const { payload, timeoutMs = 300000 } = task;
  const {
    workflow,
    comfyuiUrl = DEFAULT_COMFYUI_URL,
    clientId = `swarm-gw-${Date.now()}`,
    pollIntervalMs = 2000,
  } = payload;

  if (!workflow) throw new Error("ComfyUI task missing 'workflow' in payload");

  running = true;
  cancelRequested = false;
  const startTime = Date.now();
  const log = (msg) => logCallback?.([`[comfyui] ${msg}`]);

  try {
    // 1. Check ComfyUI is reachable
    log(`Connecting to ComfyUI at ${comfyuiUrl}...`);
    try {
      const health = await httpRequest(`${comfyuiUrl}/system_stats`);
      if (health.status !== 200) {
        throw new Error(`ComfyUI returned status ${health.status}`);
      }
      const stats = JSON.parse(health.data);
      log(`ComfyUI online — VRAM: ${stats.devices?.[0]?.vram_total || "unknown"}`);
    } catch (err) {
      throw new Error(`ComfyUI not reachable at ${comfyuiUrl}: ${err.message}`);
    }

    // 2. Submit the workflow prompt
    log("Submitting workflow prompt...");
    const submitResp = await httpRequest(`${comfyuiUrl}/prompt`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: workflow, client_id: clientId }),
    });

    if (submitResp.status !== 200) {
      const errData = JSON.parse(submitResp.data || "{}");
      throw new Error(`ComfyUI prompt submission failed: ${errData.error || submitResp.status}`);
    }

    const submitData = JSON.parse(submitResp.data);
    const promptId = submitData.prompt_id;
    activePromptId = promptId;
    log(`Prompt queued: ${promptId}`);

    // 3. Poll for completion
    const deadline = startTime + timeoutMs;
    let completed = false;
    let outputs = {};

    while (!completed && !cancelRequested && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, pollIntervalMs));

      // Check history for this prompt
      const histResp = await httpRequest(`${comfyuiUrl}/history/${promptId}`);
      if (histResp.status === 200) {
        const history = JSON.parse(histResp.data);
        const promptHistory = history[promptId];

        if (promptHistory) {
          if (promptHistory.status?.status_str === "error") {
            const errorMsgs = promptHistory.status?.messages || [];
            throw new Error(`ComfyUI execution error: ${JSON.stringify(errorMsgs)}`);
          }

          if (promptHistory.outputs) {
            outputs = promptHistory.outputs;
            completed = true;
            log("Workflow completed successfully");
          }
        }
      }

      // Also check queue position
      if (!completed) {
        try {
          const queueResp = await httpRequest(`${comfyuiUrl}/queue`);
          if (queueResp.status === 200) {
            const queue = JSON.parse(queueResp.data);
            const runningCount = queue.queue_running?.length || 0;
            const pendingCount = queue.queue_pending?.length || 0;
            log(`Queue: ${runningCount} running, ${pendingCount} pending`);
          }
        } catch {
          // Non-fatal
        }
      }
    }

    if (cancelRequested) {
      throw new Error("Execution cancelled");
    }

    if (!completed) {
      throw new Error(`ComfyUI execution timed out after ${timeoutMs}ms`);
    }

    // 4. Collect artifacts (output images)
    const artifacts = [];
    for (const [nodeId, nodeOutput] of Object.entries(outputs)) {
      const images = nodeOutput.images || [];
      for (const img of images) {
        const filename = img.filename || `output_${nodeId}.png`;
        const subfolder = img.subfolder || "";
        const viewUrl = `${comfyuiUrl}/view?filename=${encodeURIComponent(filename)}&subfolder=${encodeURIComponent(subfolder)}&type=output`;

        artifacts.push({
          name: filename,
          url: viewUrl,
          type: img.type || "image/png",
          size: 0, // Not available from ComfyUI API directly
          nodeId,
        });

        log(`Artifact: ${filename} (node ${nodeId})`);
      }
    }

    return {
      data: {
        promptId,
        outputs,
        nodeCount: Object.keys(outputs).length,
      },
      artifacts,
      executionTimeMs: Date.now() - startTime,
      exitCode: 0,
    };
  } finally {
    running = false;
    activePromptId = null;
  }
}

export async function cancel() {
  cancelRequested = true;
  if (activePromptId) {
    try {
      await httpRequest(`${DEFAULT_COMFYUI_URL}/queue`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ delete: [activePromptId] }),
      });
    } catch {
      // Best effort
    }
  }
}

export function getStatus() {
  return {
    running,
    promptId: activePromptId,
  };
}
