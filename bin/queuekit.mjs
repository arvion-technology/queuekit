#!/usr/bin/env node
// QueueKit CLI, a durable queue in one SQLite file.
//
//   node bin/queuekit.mjs init
//   node bin/queuekit.mjs enqueue render_invoice_pdf --payload '{"id":42}'
//   node bin/queuekit.mjs run --limit 40 --chaos 0.3
//   node bin/queuekit.mjs stats
//   node bin/queuekit.mjs dlq list
//   node bin/queuekit.mjs dlq replay
//
// Node 22.5+ supplies node:sqlite, so there is no npm install step.

import { existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { Queue } from "../src/queue.js";
import { SqliteStorage } from "../src/sqlite.js";
import { describeSchedule, DEFAULT_BACKOFF } from "../src/backoff.js";

const argv = process.argv.slice(2);
const command = argv[0] ?? "help";

function flag(name, fallback = null) {
  const i = argv.indexOf(`--${name}`);
  if (i === -1) return fallback;
  const value = argv[i + 1];
  return value === undefined || value.startsWith("--") ? true : value;
}
function flagAll(name) {
  const values = [];
  argv.forEach((arg, i) => { if (arg === `--${name}` && argv[i + 1] && !argv[i + 1].startsWith("--")) values.push(argv[i + 1]); });
  return values;
}

const dbPath = resolve(flag("db", process.env.QUEUEKIT_DB || "queuekit.db"));

function open() {
  if (dbPath !== ":memory:") mkdirSync(dirname(dbPath), { recursive: true });
  const storage = new SqliteStorage(dbPath);
  const queue = new Queue({
    storage,
    backoff: { baseMs: 1_000, factor: 2, maxDelayMs: 60_000, jitter: "full", maxAttempts: 4 },
    concurrency: Number(flag("concurrency", 4)),
    leaseMs: 30_000,
  });
  return { storage, queue };
}

/* Handlers stand in for real work. They return a small result object, which is
 * stored on the job, useful when debugging "what did this actually do?". */
const HANDLERS = {
  send_welcome_email: (job) => ({ deliveredTo: job.payload?.email ?? "demo@example.test", template: "welcome" }),
  send_receipt: (job) => ({ receipt: `rcpt_${job.id.slice(-6)}` }),
  render_invoice_pdf: (job) => ({ pages: job.payload?.pages ?? 2, bytes: 84_320 }),
  charge_subscription: (job) => ({ provider: "sandbox", status: "captured" }),
  generate_monthly_report: (job) => ({ rows: job.payload?.rows ?? 500, format: "pdf" }),
  export_events_csv: (job) => ({ rows: 1_204, format: "csv" }),
  sync_inventory_snapshot: () => ({ products: 318, source: "warehouse" }),
  refresh_search_index: () => ({ shards: 2, documents: 41_920 }),
};

function runHandler(job, chaos) {
  if (chaos > 0 && Math.random() < chaos) {
    throw new Error("simulated upstream failure (--chaos)");
  }
  const handler = HANDLERS[job.name] ?? (() => ({ ok: true }));
  return handler(job);
}

const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
const stamp = () => new Date().toLocaleTimeString("en-US", { hour12: false });
const line = (text) => console.log(`[${stamp()}] ${text}`);

async function cmdInit() {
  const { storage, queue } = open();
  const counts = queue.counts();
  const tables = storage.db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all().map((row) => row.name);
  console.log(`database: ${dbPath}`);
  console.log(`tables:   ${tables.join(", ")}`);
  console.log(`jobs:     ${JSON.stringify(counts)}`);
  storage.close();
}

async function cmdEnqueue() {
  const name = argv[1];
  if (!name || name.startsWith("--")) {
    console.error("usage: queuekit enqueue <job-name> [--payload json] [--delay ms] [--queue name] [--priority n]");
    process.exitCode = 2;
    return;
  }
  let payload = null;
  const rawPayload = flag("payload");
  if (rawPayload && rawPayload !== true) {
    try { payload = JSON.parse(rawPayload); }
    catch { console.error("--payload must be JSON"); process.exitCode = 2; return; }
  }
  const { storage, queue } = open();
  const count = Number(flag("count", 1));
  for (let i = 0; i < count; i++) {
    const job = queue.enqueue(name, payload, {
      queue: flag("queue", "default"),
      delayMs: Number(flag("delay", 0)),
      priority: Number(flag("priority", 0)),
      maxAttempts: Number(flag("max-attempts", 4)),
    });
    console.log(`enqueued ${job.name} ${job.id}  run_at=${new Date(job.runAt).toISOString()}`);
  }
  storage.close();
}

async function cmdRun() {
  const limit = Number(flag("limit", 50));
  const chaos = Number(flag("chaos", 0));
  const tickMs = Number(flag("tick", 120));
  const { storage, queue } = open();
  let processed = 0;
  let waited = 0;

  while (processed < limit) {
    const job = queue.claim();
    if (!job) {
      const nextAt = storage.list({ state: "queued", limit: 1, order: "oldest" })[0]?.runAt;
      if (!nextAt) {
        line(`queue is empty after ${processed} job(s)`);
        break;
      }
      const waitMs = Math.min(5_000, Math.max(tickMs, nextAt - Date.now()));
      if (waited > 60_000) { line("nothing claimable for 60s, stopping"); break; }
      waited += waitMs;
      await sleep(waitMs);
      continue;
    }
    waited = 0;
    const started = Date.now();
    try {
      const result = runHandler(job, chaos);
      queue.ack(job.id, result);
      processed++;
      line(`completed ${job.name.padEnd(28)} ${job.id.slice(-8)}  attempts=${job.attempts + 1}  ${Date.now() - started}ms`);
    } catch (err) {
      const decision = queue.fail(job.id, err.message);
      const suffix = decision.deadLetter
        ? "-> dead letter"
        : `-> retry in ${Math.round(decision.delayMs / 1000)}s`;
      line(`failed    ${job.name.padEnd(28)} ${job.id.slice(-8)}  ${suffix}`);
    }
    await sleep(10);
  }

  const snapshot = queue.metricsSnapshot();
  console.log("");
  console.log(`counts    ${JSON.stringify(snapshot.counts)}`);
  console.log(`completed ${snapshot.counters.completed}  failed ${snapshot.counters.failed}  dead ${snapshot.counters.dead}`);
  storage.close();
}

async function cmdStats() {
  const { storage, queue } = open();
  const snapshot = queue.metricsSnapshot();
  const all = storage.list({ limit: 1_000_000 });
  const completed = all.filter((job) => job.state === "completed");
  const dead = all.filter((job) => job.state === "dead");
  const retried = all.filter((job) => job.attempts > 0);
  const avgAttempts = completed.length
    ? (completed.reduce((sum, job) => sum + job.attempts + 1, 0) / completed.length)
    : 0;
  const successRate = completed.length + dead.length ? completed.length / (completed.length + dead.length) : 1;
  console.log(`database ${dbPath}`);
  console.log(`counts   ${JSON.stringify(snapshot.counts)}`);
  console.log(`depth    available=${snapshot.depth.available} delayed=${snapshot.depth.delayed} running=${snapshot.depth.running}`);
  console.log(`history  ${retried.length} job(s) that failed at least once, ${dead.length} parked in the DLQ`);
  console.log(`success  ${(successRate * 100).toFixed(1)}%  ·  ${avgAttempts.toFixed(2)} attempts per completed job`);
  console.log("");
  console.log("retry schedule (jitter disabled for display):");
  for (const row of describeSchedule({ ...DEFAULT_BACKOFF, jitter: "none", maxAttempts: 4 })) {
    console.log(`  attempt ${row.attempt}: +${row.delayMs}ms  (t+${row.runAt}ms)`);
  }
  const recent = storage.list({ limit: 8 });
  if (recent.length) {
    console.log("");
    console.log("recent jobs:");
    for (const job of recent) {
      console.log(`  ${job.state.padEnd(9)} ${job.name.padEnd(28)} attempts=${job.attempts} ${job.id.slice(-8)}`);
    }
  }
  storage.close();
}

async function cmdDlq() {
  const action = argv[1] ?? "list";
  const { storage, queue } = open();
  if (action === "list") {
    const dead = queue.deadLetters({ limit: Number(flag("limit", 50)) });
    if (!dead.length) console.log("dead-letter queue is empty");
    for (const job of dead) {
      console.log(`${job.id.slice(-10)}  ${job.name.padEnd(28)} attempts=${job.attempts}  ${job.lastError}`);
    }
    storage.close();
    return;
  }
  if (action === "replay") {
    const ids = flagAll("id");
    const result = queue.replayDead({ ids: ids.length ? ids : null });
    console.log(`replayed ${result.replayed} job(s) back to queued`);
    storage.close();
    return;
  }
  console.error("usage: queuekit dlq list | queuekit dlq replay [--id job_...]");
  process.exitCode = 2;
  storage.close();
}

function help() {
  const exists = existsSync(dbPath);
  console.log(`QueueKit, durable job queue in ${exists ? dbPath : "(no database yet)"}

usage:
  queuekit init                            create the database
  queuekit enqueue <name> [options]        add a job
      --payload '{"json":true}'            JSON payload stored with the job
      --queue <name>     --priority <n>    routing and ordering
      --delay <ms>       --max-attempts <n>   --count <n>
  queuekit run [--limit 50] [--chaos 0.3]  drain jobs with the built-in handlers
  queuekit stats                           counts, rates, retry schedule
  queuekit dlq list                        parked jobs
  queuekit dlq replay [--id ...]           move parked jobs back to queued

environment:
  QUEUEKIT_DB                              database path (default ./queuekit.db)

sample:
  node bin/queuekit.mjs init
  node bin/queuekit.mjs enqueue send_welcome_email --payload '{"email":"a@b.test"}' --count 3
  node bin/queuekit.mjs enqueue render_invoice_pdf --delay 2000
  node bin/queuekit.mjs run --limit 10 --chaos 0.4
  node bin/queuekit.mjs stats
  node bin/queuekit.mjs dlq replay`);
}

const commands = { init: cmdInit, enqueue: cmdEnqueue, run: cmdRun, stats: cmdStats, dlq: cmdDlq, help };
const run = commands[command];
if (!run) { help(); process.exitCode = 2; }
else await run();
