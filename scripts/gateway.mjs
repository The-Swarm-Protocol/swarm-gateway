#!/usr/bin/env node

/**
 * @swarmprotocol/gateway-agent — Distributed execution node for Swarm.
 *
 * Lightweight service that runs on gateway machines. Registers with the
 * Swarm hub, pulls jobs from the task queue, executes them locally
 * (shell / Docker / Node), streams logs back, and reports results.
 *
 * Uses Ed25519 keypair for authentication — same pattern as SwarmConnect.
 * Zero external dependencies — Node.js built-ins only.
 *
 * Commands:
 *   gateway-agent register  --hub <url> --org <orgId> --name <name> [--region <region>] [--runtimes shell,docker,node] [--tags gpu,comfyui] [--max-concurrent 4]
 *   gateway-agent status    — show config + worker status
 *   gateway-agent daemon    [--interval <seconds>] [--drain-timeout <seconds>]
 *   gateway-agent run       <taskType> <payloadJson>  — local test execution
 */

import crypto from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync, existsSync, unlinkSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import os from "node:os";
import { spawn } from "node:child_process";

// ---------------------------------------------------------------------------
// Paths — everything within skill directory, never outside
// ---------------------------------------------------------------------------
const __dirname = dirname(fileURLToPath(import.meta.url));
const AGENT_DIR = join(__dirname, "..");
const KEYS_DIR = join(AGENT_DIR, "keys");
const PRIVATE_KEY_PATH = join(KEYS_DIR, "private.pem");
const PUBLIC_KEY_PATH = join(KEYS_DIR, "public.pem");
const CONFIG_PATH = join(AGENT_DIR, "config.json");
const STATE_PATH = join(AGENT_DIR, "state.json");
const PENDING_REG_PATH = join(AGENT_DIR, "pending-registration.json");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function arg(flag) {
  const idx = process.argv.indexOf(flag);
  return idx !== -1 && idx + 1 < process.argv.length
    ? process.argv[idx + 1]
    : undefined;
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function loadConfig() {
  if (!existsSync(CONFIG_PATH)) {
    console.error("Not registered. Run `gateway-agent register` first.");
    process.exit(1);
  }
  return JSON.parse(readFileSync(CONFIG_PATH, "utf-8"));
}

function saveConfig(config) {
  writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2) + "\n");
}

function loadState() {
  if (!existsSync(STATE_PATH)) return { activeTasks: 0, lastHeartbeat: 0, totalCompleted: 0, totalFailed: 0 };
  try { return JSON.parse(readFileSync(STATE_PATH, "utf-8")); } catch { return { activeTasks: 0, lastHeartbeat: 0, totalCompleted: 0, totalFailed: 0 }; }
}

function saveState(state) {
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2) + "\n");
}

function log(level, msg, meta = {}) {
  const ts = new Date().toISOString();
  const extra = Object.keys(meta).length ? " " + JSON.stringify(meta) : "";
  console.log(`[${ts}] [${level.toUpperCase()}] ${msg}${extra}`);
}

// ---------------------------------------------------------------------------
// Retry with Exponential Backoff
// ---------------------------------------------------------------------------

const MAX_RETRIES = 5;
const BASE_DELAY_MS = 1000;
const MAX_DELAY_MS = 30000;
const RETRYABLE_STATUSES = new Set([429, 502, 503, 504]);

async function fetchWithRetry(url, options = {}, { maxRetries = MAX_RETRIES, label = "request" } = {}) {
  let lastError;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const resp = await fetch(url, options);
      if (resp.ok || !RETRYABLE_STATUSES.has(resp.status)) {
        return resp;
      }

      const retryAfter = resp.headers.get("retry-after");
      let delayMs = Math.min(BASE_DELAY_MS * Math.pow(2, attempt), MAX_DELAY_MS);
      if (retryAfter) {
        const parsed = parseInt(retryAfter, 10);
        if (!isNaN(parsed)) delayMs = Math.max(delayMs, parsed * 1000);
      }
      delayMs += Math.floor(Math.random() * delayMs * 0.25);

      if (attempt < maxRetries) {
        log("warn", `${label}: ${resp.status} — retrying in ${Math.round(delayMs / 1000)}s (${attempt + 1}/${maxRetries})`);
        await new Promise(r => setTimeout(r, delayMs));
      } else {
        return resp;
      }
    } catch (err) {
      lastError = err;
      if (attempt < maxRetries) {
        const delayMs = Math.min(BASE_DELAY_MS * Math.pow(2, attempt), MAX_DELAY_MS);
        log("warn", `${label}: network error — retrying in ${Math.round(delayMs / 1000)}s (${attempt + 1}/${maxRetries})`);
        await new Promise(r => setTimeout(r, delayMs));
      }
    }
  }
  throw lastError || new Error(`${label}: all ${maxRetries} retries exhausted`);
}

// ---------------------------------------------------------------------------
// Offline Bootstrap Mode
// ---------------------------------------------------------------------------

function savePendingRegistration(params) {
  writeFileSync(PENDING_REG_PATH, JSON.stringify({ ...params, savedAt: new Date().toISOString() }, null, 2) + "\n");
}

function loadPendingRegistration() {
  if (!existsSync(PENDING_REG_PATH)) return null;
  try { return JSON.parse(readFileSync(PENDING_REG_PATH, "utf-8")); } catch { return null; }
}

function clearPendingRegistration() {
  if (existsSync(PENDING_REG_PATH)) {
    try { unlinkSync(PENDING_REG_PATH); } catch { /* ignore */ }
  }
}

// ---------------------------------------------------------------------------
// Ed25519 Keypair Management
// ---------------------------------------------------------------------------

function ensureKeypair() {
  if (existsSync(PRIVATE_KEY_PATH) && existsSync(PUBLIC_KEY_PATH)) {
    return {
      privateKey: readFileSync(PRIVATE_KEY_PATH, "utf-8"),
      publicKey: readFileSync(PUBLIC_KEY_PATH, "utf-8"),
    };
  }

  log("info", "Generating Ed25519 keypair...");
  mkdirSync(KEYS_DIR, { recursive: true });

  const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519", {
    publicKeyEncoding: { type: "spki", format: "pem" },
    privateKeyEncoding: { type: "pkcs8", format: "pem" },
  });

  writeFileSync(PRIVATE_KEY_PATH, privateKey);
  writeFileSync(PUBLIC_KEY_PATH, publicKey);
  log("info", "Keypair saved to ./keys/");

  return { privateKey, publicKey };
}

function sign(message, privateKeyPem) {
  const privateKey = crypto.createPrivateKey({
    key: privateKeyPem,
    format: "pem",
    type: "pkcs8",
  });
  const sig = crypto.sign(null, Buffer.from(message, "utf-8"), privateKey);
  return sig.toString("base64");
}

// ---------------------------------------------------------------------------
// System Metrics
// ---------------------------------------------------------------------------

function getSystemMetrics() {
  const cpus = os.cpus();
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;
  const loadAvg = os.loadavg();

  return {
    cpuUsagePercent: Math.round((loadAvg[0] / cpus.length) * 100),
    memoryUsageMb: Math.round(usedMem / (1024 * 1024)),
    maxCpuCores: cpus.length,
    maxMemoryMb: Math.round(totalMem / (1024 * 1024)),
  };
}

// ---------------------------------------------------------------------------
// Execution Engines
// ---------------------------------------------------------------------------

/**
 * Execute a shell command with timeout and output capture.
 * Returns { stdout, stderr, exitCode }.
 */
function executeShell(command, args = [], { timeoutMs = 60000, cwd, env } = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(command, args, {
      shell: true,
      cwd: cwd || AGENT_DIR,
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    const logLines = [];
    let killed = false;

    const timer = setTimeout(() => {
      killed = true;
      proc.kill("SIGKILL");
    }, timeoutMs);

    proc.stdout.on("data", (data) => {
      const chunk = data.toString();
      stdout += chunk;
      const lines = chunk.split("\n").filter(Boolean);
      logLines.push(...lines.map(l => `[stdout] ${l}`));
    });

    proc.stderr.on("data", (data) => {
      const chunk = data.toString();
      stderr += chunk;
      const lines = chunk.split("\n").filter(Boolean);
      logLines.push(...lines.map(l => `[stderr] ${l}`));
    });

    proc.on("error", (err) => {
      clearTimeout(timer);
      reject(new Error(`Spawn error: ${err.message}`));
    });

    proc.on("close", (code) => {
      clearTimeout(timer);
      if (killed) {
        reject(new Error(`Process killed: execution timed out after ${timeoutMs}ms`));
        return;
      }
      resolve({ stdout: stdout.trim(), stderr: stderr.trim(), exitCode: code || 0, logLines });
    });
  });
}

/**
 * Execute a Docker container with output capture.
 * Uses docker CLI — requires docker to be installed and accessible.
 */
async function executeDocker(image, command = [], { timeoutMs = 120000, volumes = [], envVars = {}, networkMode = "none" } = {}) {
  const args = ["run", "--rm", `--network=${networkMode}`];

  // Resource limits for safety
  args.push("--memory=512m", "--cpus=2");

  // Timeout at Docker level
  args.push(`--stop-timeout=${Math.ceil(timeoutMs / 1000)}`);

  // Volume mounts
  for (const v of volumes) {
    args.push("-v", v);
  }

  // Environment variables
  for (const [k, v] of Object.entries(envVars)) {
    args.push("-e", `${k}=${v}`);
  }

  args.push(image, ...command);

  return executeShell("docker", args, { timeoutMs: timeoutMs + 10000 });
}

/**
 * Execute a Node.js script inline.
 */
async function executeNode(script, { timeoutMs = 60000 } = {}) {
  return executeShell("node", ["-e", script], { timeoutMs });
}

/**
 * Route a task to the appropriate executor based on taskType.
 */
/**
 * Route a task to the appropriate executor.
 * Uses modular executors from ./executors/ (comfyui, workflow, etc.)
 * with inline fallback for core types (shell, docker, node).
 */
async function executeTask(task, logCallback) {
  const { taskType, payload, timeoutMs = 60000 } = task;
  const startTime = Date.now();

  // Try modular executor registry first (supports comfyui, workflow, etc.)
  try {
    const { getExecutor } = await import("./executors/index.mjs");
    const executor = getExecutor(taskType);
    if (executor) {
      const collectLines = [];
      const wrappedLog = (lines) => {
        collectLines.push(...lines);
        if (logCallback) logCallback(lines);
      };
      const result = await executor.execute(task, wrappedLog);
      return {
        data: result.data || {},
        artifacts: result.artifacts || [],
        logLines: collectLines,
        executionTimeMs: result.executionTimeMs || (Date.now() - startTime),
        exitCode: result.exitCode || 0,
      };
    }
  } catch {
    // Modular executors not available — fall through to inline
  }

  // Inline fallback for core types
  let result;
  switch (taskType) {
    case "shell": {
      const { command, args = [], cwd, env } = payload;
      if (!command) throw new Error("Shell task missing 'command' in payload");
      result = await executeShell(command, args, { timeoutMs, cwd, env });
      break;
    }

    case "docker": {
      const { image, command = [], volumes = [], envVars = {}, networkMode } = payload;
      if (!image) throw new Error("Docker task missing 'image' in payload");
      result = await executeDocker(image, command, { timeoutMs, volumes, envVars, networkMode });
      break;
    }

    case "node": {
      const { script } = payload;
      if (!script) throw new Error("Node task missing 'script' in payload");
      result = await executeNode(script, { timeoutMs });
      break;
    }

    default:
      throw new Error(`Unknown task type: ${taskType}`);
  }

  return {
    data: {
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.exitCode,
    },
    logLines: result.logLines || [],
    executionTimeMs: Date.now() - startTime,
    exitCode: result.exitCode,
  };
}

// ---------------------------------------------------------------------------
// WebSocket Connection (Hybrid: WS primary, HTTP poll fallback)
// ---------------------------------------------------------------------------

/**
 * Connect to the hub via WebSocket for real-time job dispatch.
 * Falls back to HTTP polling if WS is unavailable.
 */
class GatewayWSClient {
  constructor(config, privateKey, onJobDispatch) {
    this.config = config;
    this.privateKey = privateKey;
    this.onJobDispatch = onJobDispatch;
    this.ws = null;
    this.connected = false;
    this.reconnectTimer = null;
    this.reconnectDelay = 1000;
    this.maxReconnectDelay = 30000;
    this.heartbeatTimer = null;
    this.closing = false;
  }

  async connect() {
    if (this.closing) return;

    try {
      const WebSocket = (await import("ws")).default;
      const ts = Date.now().toString();
      const message = `WS:connect:${this.config.workerId}:${ts}`;
      const sig = sign(message, this.privateKey);

      const wsUrl = this.config.hubUrl.replace(/^http/, "ws");
      const url = `${wsUrl}/ws/gateways/${this.config.workerId}?sig=${encodeURIComponent(sig)}&ts=${ts}`;

      this.ws = new WebSocket(url);

      this.ws.on("open", () => {
        log("info", "WebSocket connected to hub");
        this.connected = true;
        this.reconnectDelay = 1000;
        this.startHeartbeat();
      });

      this.ws.on("message", (data) => {
        try {
          const msg = JSON.parse(data.toString());
          this.handleMessage(msg);
        } catch { /* ignore */ }
      });

      this.ws.on("ping", () => {
        this.ws?.pong();
      });

      this.ws.on("close", () => {
        log("info", "WebSocket disconnected from hub");
        this.connected = false;
        this.stopHeartbeat();
        if (!this.closing) this.scheduleReconnect();
      });

      this.ws.on("error", (err) => {
        log("warn", `WebSocket error: ${err.message}`);
        this.ws?.close();
      });
    } catch (err) {
      // ws module not available or connection failed — fall back to polling
      log("warn", `WebSocket connect failed: ${err.message}. Using HTTP polling.`);
      if (!this.closing) this.scheduleReconnect();
    }
  }

  handleMessage(msg) {
    if (msg.type === "job:dispatch") {
      log("info", `Job dispatched via WS: ${msg.taskId} (${msg.taskType})`);
      this.onJobDispatch(msg);
    } else if (msg.type === "drain") {
      log("info", "Drain command received via WS");
      process.emit("SIGTERM");
    } else if (msg.type === "job:cancel" && msg.taskId) {
      log("info", `Job cancel received: ${msg.taskId}`);
      this.handleJobCancel(msg.taskId);
    }
  }

  /** Handle job cancellation — kill running process and report cancelled */
  handleJobCancel(taskId) {
    // _activeTasks is set externally by the daemon after construction
    const entry = this._activeTasks?.get(taskId);
    if (!entry) {
      log("warn", `Cancel: task ${taskId} not found in active tasks`);
      // Still report cancelled in case it was queued/claimed on the hub side
      this.sendJobStatus(taskId, "cancelled");
      return;
    }

    log("info", `Cancelling task ${taskId}...`);

    // Kill the process if one is tracked (shell/docker executor)
    if (entry.proc && typeof entry.proc.kill === "function") {
      try {
        entry.proc.kill("SIGTERM");
        // Force-kill after 5s if still alive
        setTimeout(() => {
          try { entry.proc.kill("SIGKILL"); } catch { /* already dead */ }
        }, 5000);
      } catch {
        // Process may have already exited
      }
    }

    // Report cancelled status to hub
    this.sendJobStatus(taskId, "cancelled");
    this.sendJobLog(taskId, [`[system] Task cancelled by hub`]);

    // Clean up from active tasks
    this._activeTasks?.delete(taskId);

    log("info", `Task ${taskId} cancelled`);
  }

  /** Send job status update via WebSocket */
  sendJobStatus(taskId, status, data = {}) {
    if (!this.connected || !this.ws || this.ws.readyState !== 1) return false;
    try {
      this.ws.send(JSON.stringify({ type: "job:status", taskId, status, ...data }));
      return true;
    } catch { return false; }
  }

  /** Send log lines via WebSocket */
  sendJobLog(taskId, lines) {
    if (!this.connected || !this.ws || this.ws.readyState !== 1) return false;
    try {
      this.ws.send(JSON.stringify({ type: "job:log", taskId, lines, ts: Date.now() }));
      return true;
    } catch { return false; }
  }

  /** Send heartbeat via WebSocket */
  sendHeartbeat(resources) {
    if (!this.connected || !this.ws || this.ws.readyState !== 1) return false;
    try {
      this.ws.send(JSON.stringify({ type: "heartbeat", resources }));
      return true;
    } catch { return false; }
  }

  startHeartbeat() {
    this.heartbeatTimer = setInterval(() => {
      if (this.ws?.readyState === 1) this.ws.ping();
    }, 25000);
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  scheduleReconnect() {
    if (this.reconnectTimer || this.closing) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
      this.connect();
    }, this.reconnectDelay);
  }

  disconnect() {
    this.closing = true;
    this.stopHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.ws?.close();
    this.ws = null;
    this.connected = false;
  }
}

// ---------------------------------------------------------------------------
// Hub API Helpers
// ---------------------------------------------------------------------------

function serviceHeaders(config) {
  return {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${config.serviceSecret}`,
  };
}

async function apiRegisterWorker(config, workerData) {
  const url = `${config.hubUrl}/api/gateway/workers`;
  const resp = await fetchWithRetry(url, {
    method: "POST",
    headers: serviceHeaders(config),
    body: JSON.stringify(workerData),
  }, { label: "Register worker" });
  return resp;
}

/** Ed25519 self-registration — no service secret needed */
async function apiRegisterWorkerEd25519(hubUrl, workerData, publicKeyPem, privateKeyPem) {
  const ts = Date.now();
  const proofMessage = `gateway:register:${workerData.orgId}:${ts}`;
  const proofSig = sign(privateKeyPem, proofMessage);

  const url = `${hubUrl}/api/v1/gateway/register`;
  const body = {
    ...workerData,
    publicKey: publicKeyPem,
    proof: proofSig,
    ts,
  };
  const resp = await fetchWithRetry(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }, { label: "Register worker (Ed25519)" });
  return resp;
}

async function apiHeartbeat(config, workerId, resources) {
  const url = `${config.hubUrl}/api/gateway/workers/${workerId}`;
  const resp = await fetchWithRetry(url, {
    method: "POST",
    headers: serviceHeaders(config),
    body: JSON.stringify({ resources }),
  }, { label: "Heartbeat", maxRetries: 2 });
  return resp;
}

async function apiPullTask(config, workerId) {
  const url = `${config.hubUrl}/api/gateway/tasks/pull`;
  const resp = await fetchWithRetry(url, {
    method: "POST",
    headers: serviceHeaders(config),
    body: JSON.stringify({ workerId }),
  }, { label: "Pull task", maxRetries: 1 });
  return resp;
}

async function apiReportStatus(config, taskId, status, data = {}) {
  const url = `${config.hubUrl}/api/gateway/tasks/${taskId}/status`;
  const resp = await fetchWithRetry(url, {
    method: "POST",
    headers: serviceHeaders(config),
    body: JSON.stringify({ workerId: config.workerId, status, ...data }),
  }, { label: `Report ${status}`, maxRetries: 3 });
  return resp;
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async function cmdRegister() {
  const hubUrl = arg("--hub") || process.env.HUB_URL || "https://swarmprotocol.fun";
  const orgId = arg("--org") || process.env.ORG_ID;
  const name = arg("--name") || process.env.GATEWAY_WORKER_NAME;
  const region = arg("--region") || process.env.GATEWAY_REGION;
  const runtimesStr = arg("--runtimes") || process.env.GATEWAY_RUNTIMES || "shell,node";
  const tagsStr = arg("--tags") || process.env.GATEWAY_TAGS || "";
  const maxConcurrent = parseInt(arg("--max-concurrent") || process.env.GATEWAY_MAX_CONCURRENT || "4", 10);
  const serviceSecret = arg("--secret") || process.env.INTERNAL_SERVICE_SECRET;

  if (!orgId || !name) {
    console.error("Usage: gateway-agent register --hub <url> --org <orgId> --name <name> [--region <region>] [--runtimes shell,docker,node] [--tags gpu,comfyui] [--max-concurrent 4] [--secret <INTERNAL_SERVICE_SECRET>]");
    process.exit(1);
  }

  const useEd25519 = !serviceSecret;
  if (useEd25519) {
    log("info", "No service secret — using Ed25519 self-registration");
  }

  // Warn if already registered
  if (existsSync(CONFIG_PATH)) {
    const existing = JSON.parse(readFileSync(CONFIG_PATH, "utf-8"));
    log("warn", `Already registered as "${existing.workerName}" (ID: ${existing.workerId}). Re-registering.`);
  }

  // Generate or load keypair
  const { publicKey } = ensureKeypair();

  // Gather system resources
  const metrics = getSystemMetrics();
  const runtimes = runtimesStr.split(",").map(s => s.trim()).filter(Boolean);
  const tags = tagsStr ? tagsStr.split(",").map(s => s.trim()).filter(Boolean) : [];

  // Determine supported task types from runtimes
  const taskTypes = [...runtimes]; // shell, docker, node are both runtimes and task types

  const workerData = {
    orgId,
    name,
    status: "idle",
    resources: {
      maxCpuCores: metrics.maxCpuCores,
      maxMemoryMb: metrics.maxMemoryMb,
      maxConcurrent,
      cpuUsagePercent: metrics.cpuUsagePercent,
      memoryUsageMb: metrics.memoryUsageMb,
      activeTasks: 0,
    },
    capabilities: {
      taskTypes,
      runtimes,
      tags,
    },
    region: region || undefined,
    ipAddress: getPublicIPHint(),
    publicKey,
  };

  log("info", `Registering gateway "${name}" with ${hubUrl}...`);

  let resp;
  try {
    if (useEd25519) {
      const privateKeyPem = readFileSync(PRIVATE_KEY_PATH, "utf-8");
      resp = await apiRegisterWorkerEd25519(hubUrl, workerData, publicKey, privateKeyPem);
    } else {
      resp = await apiRegisterWorker({ hubUrl, serviceSecret }, workerData);
    }
  } catch (err) {
    log("error", `Registration failed after retries: ${err.message}`);
    log("info", "Entering offline bootstrap mode...");
    savePendingRegistration({ hubUrl, orgId, workerName: name, region, runtimes, tags, maxConcurrent, serviceSecret });

    const provisionalConfig = {
      hubUrl, orgId, serviceSecret,
      workerId: `provisional_${crypto.randomUUID().slice(0, 8)}`,
      workerName: name,
      region, runtimes, tags, maxConcurrent,
      registeredAt: null,
      offline: true,
    };
    saveConfig(provisionalConfig);
    log("info", `Provisional ID: ${provisionalConfig.workerId}. Will complete registration on daemon start.`);
    return;
  }

  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    log("error", `Registration failed (${resp.status}): ${err.error || "Unknown"}`);
    process.exit(1);
  }

  const data = await resp.json();
  clearPendingRegistration();

  const config = {
    hubUrl, orgId,
    ...(serviceSecret ? { serviceSecret } : {}),
    authMode: useEd25519 ? "ed25519" : "service-secret",
    workerId: data.workerId || data.id,
    workerName: name,
    region, runtimes, tags, maxConcurrent,
    registeredAt: new Date().toISOString(),
    offline: false,
  };
  saveConfig(config);

  log("info", `Registered gateway "${name}"`);
  console.log(`   Worker ID:  ${config.workerId}`);
  console.log(`   Hub:        ${hubUrl}`);
  console.log(`   Org:        ${orgId}`);
  console.log(`   Region:     ${region || "(auto)"}`);
  console.log(`   Runtimes:   ${runtimes.join(", ")}`);
  console.log(`   Tags:       ${tags.length ? tags.join(", ") : "(none)"}`);
  console.log(`   Concurrent: ${maxConcurrent}`);
  console.log(`   CPUs:       ${metrics.maxCpuCores}`);
  console.log(`   Memory:     ${metrics.maxMemoryMb} MB`);
  console.log(`\nReady. Run \`gateway-agent daemon\` to start accepting jobs.`);
}

async function cmdStatus() {
  const config = loadConfig();
  const state = loadState();
  const metrics = getSystemMetrics();

  console.log(`\n  Gateway Agent Status`);
  console.log(`  ─────────────────────`);
  console.log(`  Worker:      ${config.workerName} (${config.workerId})`);
  console.log(`  Hub:         ${config.hubUrl}`);
  console.log(`  Org:         ${config.orgId}`);
  console.log(`  Region:      ${config.region || "(auto)"}`);
  console.log(`  Offline:     ${config.offline ? "YES (pending registration)" : "no"}`);
  console.log(`  Runtimes:    ${(config.runtimes || []).join(", ")}`);
  console.log(`  Tags:        ${(config.tags || []).length ? config.tags.join(", ") : "(none)"}`);
  console.log(`  Concurrent:  ${config.maxConcurrent}`);
  console.log(`  Registered:  ${config.registeredAt || "pending"}`);
  console.log(``);
  console.log(`  System`);
  console.log(`  ──────`);
  console.log(`  CPU:         ${metrics.cpuUsagePercent}% (${metrics.maxCpuCores} cores)`);
  console.log(`  Memory:      ${metrics.memoryUsageMb} / ${metrics.maxMemoryMb} MB`);
  console.log(`  Platform:    ${os.platform()} ${os.arch()}`);
  console.log(`  Node:        ${process.version}`);
  console.log(``);
  console.log(`  Stats`);
  console.log(`  ─────`);
  console.log(`  Active:      ${state.activeTasks || 0}`);
  console.log(`  Completed:   ${state.totalCompleted || 0}`);
  console.log(`  Failed:      ${state.totalFailed || 0}`);
  console.log(`  Last HB:     ${state.lastHeartbeat ? new Date(state.lastHeartbeat).toISOString() : "never"}`);
  console.log(``);

  // Check if Docker is available
  try {
    await executeShell("docker", ["info", "--format", "{{.ServerVersion}}"], { timeoutMs: 5000 });
    console.log(`  Docker:      available`);
  } catch {
    console.log(`  Docker:      not available`);
  }
  console.log(``);
}

async function cmdDaemon() {
  const config = loadConfig();
  const intervalSec = parseInt(arg("--interval") || process.env.GATEWAY_POLL_INTERVAL_SEC || "10", 10);
  const drainTimeoutSec = parseInt(arg("--drain-timeout") || process.env.GATEWAY_DRAIN_TIMEOUT_SEC || "60", 10);
  const intervalMs = intervalSec * 1000;

  // Track active tasks in this process
  const activeTasks = new Map(); // taskId -> { promise, startedAt }
  let draining = false;
  let running = true;

  log("info", `Gateway daemon starting — polling every ${intervalSec}s, max ${config.maxConcurrent} concurrent tasks`);

  // Retry pending registration if needed
  if (config.offline) {
    log("info", "Offline mode detected — retrying registration...");
    const pending = loadPendingRegistration();
    if (pending) {
      try {
        const resp = await apiRegisterWorker(
          { hubUrl: config.hubUrl, serviceSecret: config.serviceSecret },
          {
            orgId: config.orgId,
            name: config.workerName,
            status: "idle",
            resources: {
              ...getSystemMetrics(),
              maxConcurrent: config.maxConcurrent,
              activeTasks: 0,
            },
            capabilities: {
              taskTypes: config.runtimes || ["shell", "node"],
              runtimes: config.runtimes || ["shell", "node"],
              tags: config.tags || [],
            },
            region: config.region,
            ipAddress: getPublicIPHint(),
            publicKey: ensureKeypair().publicKey,
          }
        );
        if (resp.ok) {
          const data = await resp.json();
          config.workerId = data.workerId || data.id;
          config.offline = false;
          config.registeredAt = new Date().toISOString();
          saveConfig(config);
          clearPendingRegistration();
          log("info", `Registration completed. Worker ID: ${config.workerId}`);
        }
      } catch (err) {
        log("warn", `Registration retry failed: ${err.message}. Continuing in offline mode.`);
      }
    }
  }

  if (config.offline) {
    log("error", "Cannot start daemon — still offline. Registration must succeed first.");
    process.exit(1);
  }

  // WebSocket client reference (shared with runTask for log streaming)
  let wsClient = null;

  // Handler for WS-dispatched jobs
  function handleWSJobDispatch(msg) {
    if (activeTasks.size >= config.maxConcurrent || draining) {
      log("warn", `Ignoring WS job dispatch — at capacity or draining`, { taskId: msg.taskId });
      return;
    }

    const task = {
      id: msg.taskId,
      taskType: msg.taskType,
      payload: msg.payload,
      priority: msg.priority,
      timeoutMs: msg.timeoutMs || 60000,
      resources: msg.resources || {},
    };

    log("info", `Executing WS-dispatched task ${task.id} (${task.taskType})`);
    const taskPromise = runTask(config, task, activeTasks, wsClient);
    activeTasks.set(task.id, { promise: taskPromise, startedAt: Date.now() });
    taskPromise.finally(() => activeTasks.delete(task.id));
  }

  // Try WebSocket connection (non-blocking — falls back to polling)
  const { privateKey } = ensureKeypair();
  wsClient = new GatewayWSClient(config, privateKey, handleWSJobDispatch);
  // Share the active tasks map so the WS client can cancel running tasks
  wsClient._activeTasks = activeTasks;
  wsClient.connect();

  // Graceful shutdown
  async function shutdown(signal) {
    if (draining) return;
    draining = true;
    log("info", `${signal} received — draining (timeout: ${drainTimeoutSec}s)...`);

    // Disconnect WebSocket
    if (wsClient) wsClient.disconnect();

    // Update worker status to draining
    try {
      await fetchWithRetry(
        `${config.hubUrl}/api/gateway/workers/${config.workerId}`,
        {
          method: "PATCH",
          headers: serviceHeaders(config),
          body: JSON.stringify({ status: "draining" }),
        },
        { label: "Set draining", maxRetries: 1 }
      );
    } catch { /* non-fatal */ }

    // Wait for active tasks with timeout
    if (activeTasks.size > 0) {
      log("info", `Waiting for ${activeTasks.size} active task(s)...`);
      const drainDeadline = Date.now() + drainTimeoutSec * 1000;

      while (activeTasks.size > 0 && Date.now() < drainDeadline) {
        await new Promise(r => setTimeout(r, 1000));
      }

      if (activeTasks.size > 0) {
        log("warn", `Drain timeout reached with ${activeTasks.size} task(s) still running.`);
      }
    }

    // Deregister worker
    try {
      await fetchWithRetry(
        `${config.hubUrl}/api/gateway/workers/${config.workerId}`,
        { method: "DELETE", headers: serviceHeaders(config) },
        { label: "Deregister", maxRetries: 1 }
      );
      log("info", "Worker deregistered.");
    } catch (err) {
      log("warn", `Deregister failed: ${err.message}`);
    }

    running = false;
    process.exit(0);
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  // Main loop (HTTP polling — serves as fallback when WS is down)
  while (running) {
    try {
      if (draining) break;

      const metrics = getSystemMetrics();
      const resources = {
        ...metrics,
        maxConcurrent: config.maxConcurrent,
        activeTasks: activeTasks.size,
      };

      // 1. Heartbeat — prefer WS, fallback to HTTP
      const sentViaWS = wsClient?.sendHeartbeat(resources);
      if (!sentViaWS) {
        try {
          await apiHeartbeat(config, config.workerId, resources);
        } catch (err) {
          log("warn", `Heartbeat failed: ${err.message}`);
        }
      }
      const state = loadState();
      state.lastHeartbeat = Date.now();
      state.activeTasks = activeTasks.size;
      saveState(state);

      // 2. Pull tasks if we have capacity (always poll as backup, even with WS)
      if (activeTasks.size < config.maxConcurrent && !draining) {
        try {
          const resp = await apiPullTask(config, config.workerId);
          if (resp.ok) {
            const data = await resp.json();
            const task = data.task;

            if (task) {
              log("info", `Claimed task ${task.id} (${task.taskType})`, {
                priority: task.priority,
                timeoutMs: task.timeoutMs,
              });

              const taskPromise = runTask(config, task, activeTasks, wsClient);
              activeTasks.set(task.id, { promise: taskPromise, startedAt: Date.now() });
              taskPromise.finally(() => activeTasks.delete(task.id));
            }
          }
        } catch (err) {
          log("warn", `Task pull failed: ${err.message}`);
        }
      }

      // 3. Log status periodically
      if (activeTasks.size > 0) {
        log("debug", `Active tasks: ${activeTasks.size}/${config.maxConcurrent}${wsClient?.connected ? " (WS)" : " (HTTP)"}`);
      }

    } catch (err) {
      log("error", `Daemon loop error: ${err.message}`);
    }

    await new Promise(r => setTimeout(r, intervalMs));
  }
}

/**
 * Execute a task, report status transitions, and handle results.
 */
async function runTask(config, task, activeTasks, wsClient = null) {
  const state = loadState();

  // Report running — prefer WS, fallback to HTTP
  const sentRunning = wsClient?.sendJobStatus(task.id, "running");
  if (!sentRunning) {
    try {
      await apiReportStatus(config, task.id, "running");
    } catch (err) {
      log("warn", `Failed to report running for ${task.id}: ${err.message}`);
    }
  }

  try {
    log("info", `Executing task ${task.id} (${task.taskType})...`);

    // Stream logs in real-time via WS during execution
    const allLogLines = [];
    const logCallback = (lines) => {
      allLogLines.push(...lines);
      wsClient?.sendJobLog(task.id, lines);
    };

    const result = await executeTask(task, logCallback);

    // Send any remaining buffered lines (from inline executors that don't use logCallback)
    if (result.logLines?.length > 0) {
      const unsentLines = result.logLines.filter(l => !allLogLines.includes(l));
      if (unsentLines.length > 0) {
        for (let i = 0; i < unsentLines.length; i += 50) {
          wsClient?.sendJobLog(task.id, unsentLines.slice(i, i + 50));
        }
        allLogLines.push(...unsentLines);
      }
    }

    // Log output summary locally
    const totalLines = allLogLines.length;
    if (totalLines > 0) {
      log("info", `Task ${task.id} output (${totalLines} lines, ${result.executionTimeMs}ms):`);
      const tail = allLogLines.slice(-10);
      for (const line of tail) {
        console.log(`   ${line}`);
      }
      if (totalLines > 10) {
        console.log(`   ... (${totalLines - 10} earlier lines omitted)`);
      }
    }

    // Check exit code
    if (result.exitCode !== 0) {
      throw new Error(`Process exited with code ${result.exitCode}: ${result.data?.stderr || ""}`);
    }

    // Report success — prefer WS, fallback to HTTP
    const sentComplete = wsClient?.sendJobStatus(task.id, "completed", { result: result.data });
    if (!sentComplete) {
      await apiReportStatus(config, task.id, "completed", { result: result.data });
    }
    log("info", `Task ${task.id} completed (${result.executionTimeMs}ms)`);

    state.totalCompleted = (state.totalCompleted || 0) + 1;
    saveState(state);

  } catch (err) {
    log("error", `Task ${task.id} failed: ${err.message}`);

    // Stream error log via WS
    wsClient?.sendJobLog(task.id, [`[error] ${err.message}`]);

    // Report failure — prefer WS, fallback to HTTP
    const sentFailed = wsClient?.sendJobStatus(task.id, "failed", { error: err.message });
    if (!sentFailed) {
      try {
        await apiReportStatus(config, task.id, "failed", { error: err.message });
      } catch (reportErr) {
        log("error", `Failed to report failure for ${task.id}: ${reportErr.message}`);
      }
    }

    state.totalFailed = (state.totalFailed || 0) + 1;
    saveState(state);
  }
}

async function cmdRun() {
  const taskType = process.argv[3];
  const payloadStr = process.argv[4];

  if (!taskType || !payloadStr) {
    console.error("Usage: gateway-agent run <taskType> '<payloadJson>'");
    console.error("\nExamples:");
    console.error('  gateway-agent run shell \'{"command":"echo","args":["hello world"]}\'');
    console.error('  gateway-agent run docker \'{"image":"alpine","command":["echo","hello"]}\'');
    console.error('  gateway-agent run node \'{"script":"console.log(1+1)"}\'');
    console.error('  gateway-agent run comfyui \'{"workflow":{...},"comfyuiUrl":"http://localhost:8188"}\'');
    console.error('  gateway-agent run workflow \'{"steps":[{"taskType":"shell","payload":{"command":"echo","args":["step1"]}}]}\'');
    process.exit(1);
  }

  let payload;
  try {
    payload = JSON.parse(payloadStr);
  } catch {
    console.error("Error: Invalid JSON payload.");
    process.exit(1);
  }

  const task = {
    id: `local_${crypto.randomUUID().slice(0, 8)}`,
    taskType,
    payload,
    timeoutMs: parseInt(arg("--timeout") || "60000", 10),
    priority: "normal",
  };

  log("info", `Running local task: ${taskType}`);
  try {
    const logCb = (lines) => lines.forEach(l => console.log(`  ${l}`));
    const result = await executeTask(task, logCb);
    console.log(`\n── Result ──`);
    console.log(`  Exit code:  ${result.exitCode}`);
    console.log(`  Duration:   ${result.executionTimeMs}ms`);
    if (result.artifacts?.length) {
      console.log(`  Artifacts:  ${result.artifacts.length}`);
      result.artifacts.forEach(a => console.log(`    - ${a.name} (${a.type})`));
    }
    if (result.data?.stdout) {
      console.log(`  stdout:`);
      console.log(result.data.stdout.split("\n").map(l => `    ${l}`).join("\n"));
    }
    if (result.data?.stderr) {
      console.log(`  stderr:`);
      console.log(result.data.stderr.split("\n").map(l => `    ${l}`).join("\n"));
    }
  } catch (err) {
    log("error", `Execution failed: ${err.message}`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

function getPublicIPHint() {
  const interfaces = os.networkInterfaces();
  for (const iface of Object.values(interfaces)) {
    if (!iface) continue;
    for (const info of iface) {
      if (!info.internal && info.family === "IPv4") {
        return info.address;
      }
    }
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const command = process.argv[2];

switch (command) {
  case "register":
    cmdRegister().catch(err => { log("error", err.message); process.exit(1); });
    break;
  case "status":
    cmdStatus().catch(err => { log("error", err.message); process.exit(1); });
    break;
  case "daemon":
    cmdDaemon().catch(err => { log("error", err.message); process.exit(1); });
    break;
  case "run":
    cmdRun().catch(err => { log("error", err.message); process.exit(1); });
    break;
  default:
    console.log(`
  @swarmprotocol/gateway-agent — Distributed execution node for Swarm

  Commands:
    register    Register this gateway with the Swarm hub
    status      Show gateway status and system info
    daemon      Start the daemon (polls for tasks, executes, reports)
    run         Execute a task locally (for testing)

  Examples:
    gateway-agent register --hub https://swarmprotocol.fun --org myOrg --name my-gateway --secret <secret>
    gateway-agent daemon --interval 10
    gateway-agent run shell '{"command":"echo","args":["hello"]}'
    gateway-agent status

  Environment Variables:
    HUB_URL                   Hub URL (default: https://swarmprotocol.fun)
    ORG_ID                    Organization ID
    GATEWAY_WORKER_NAME       Worker name
    GATEWAY_REGION            Region (us-east, us-west, eu-west, etc.)
    GATEWAY_RUNTIMES          Comma-separated runtimes (default: shell,node)
    GATEWAY_TAGS              Comma-separated capability tags
    GATEWAY_MAX_CONCURRENT    Max concurrent tasks (default: 4)
    GATEWAY_POLL_INTERVAL_SEC Poll interval in seconds (default: 10)
    GATEWAY_DRAIN_TIMEOUT_SEC Drain timeout in seconds (default: 60)
    INTERNAL_SERVICE_SECRET   Service secret for hub auth
`);
    break;
}
