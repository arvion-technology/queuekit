// Deterministic fleet simulation used by the browser demo.
//
// The demo could hard-code a plausible dashboard, but then it would not be a
// demo of this library. Instead it runs the real Queue, Scheduler and WorkerPool
// against MemoryStorage on a virtual clock; this module only decides *what work
// arrives* and *how long it takes*.

import { Queue } from "./queue.js";
import { Scheduler } from "./scheduler.js";
import { WorkerPool } from "./worker.js";
import { mulberry32, intBetween, weightedPick, pick } from "./prng.js";

export const JOB_TYPES = Object.freeze([
  { name: "send_welcome_email", queue: "email", weight: 5, latency: [180, 520], payload: { template: "welcome", locale: "en" } },
  { name: "send_receipt", queue: "email", weight: 3, latency: [200, 600], payload: { template: "receipt" } },
  { name: "render_invoice_pdf", queue: "billing", weight: 3, latency: [700, 1_600], payload: { pages: 2 } },
  { name: "charge_subscription", queue: "billing", weight: 2, latency: [450, 1_100], payload: { provider: "sandbox" } },
  { name: "generate_monthly_report", queue: "reports", weight: 2, latency: [1_200, 2_400], payload: { rows: 1_400 } },
  { name: "export_events_csv", queue: "reports", weight: 2, latency: [900, 1_800], payload: { days: 30 } },
  { name: "resize_avatar", queue: "media", weight: 2, latency: [250, 700], payload: { sizes: [64, 128] } },
  { name: "transcode_clip", queue: "media", weight: 1, latency: [1_500, 2_800], payload: { seconds: 30 } },
  { name: "webhook_delivery", queue: "integrations", weight: 2, latency: [120, 480], payload: { endpoint: "https://example.test/hook" } },
]);

const QUEUE_NAMES = ["email", "billing", "reports", "media", "integrations"];
const TOTAL_WEIGHT = JOB_TYPES.reduce((sum, type) => sum + type.weight, 0);

export class FleetSimulation {
  constructor({
    seed = 7,
    workers = 5,
    concurrency = 4,
    chaos = 0,
    stepMs = 100,
    startAt = Date.now(),
    warmupSteps = 160,
    backoff = { baseMs: 800, factor: 2, maxDelayMs: 20_000, jitter: "full", maxAttempts: 4 },
  } = {}) {
    this.seed = seed;
    this.random = mulberry32(seed);
    this.stepMs = stepMs;
    this.virtualNow = startAt;
    this.startedAt = startAt;
    this.chaos = chaos;
    this.paused = false;
    this.steps = 0;
    this.arrivals = 0;
    this.recurringFired = 0;

    this.queue = new Queue({
      backoff,
      concurrency,
      concurrencyByQueue: { media: 2, reports: 2 },
      rateLimit: { limit: 30, intervalMs: 4_000 },
      leaseMs: 15_000,
      clock: () => this.virtualNow,
      random: this.random,
    });

    this.scheduler = new Scheduler({ queue: this.queue, clock: () => this.virtualNow });
    this.scheduler.every("sync_inventory_snapshot", { everyMs: 30_000, queue: "integrations", payload: { source: "warehouse" }, priority: 1 });
    this.scheduler.every("refresh_search_index", { everyMs: 45_000, queue: "reports", payload: { shards: 2 } });
    this.scheduler.every("expire_stale_sessions", { everyMs: 60_000, queue: "billing", payload: { olderThanDays: 30 } });

    this.pool = new WorkerPool({
      queue: this.queue,
      queues: QUEUE_NAMES,
      workers,
      clock: () => this.virtualNow,
      random: this.random,
      handler: (job) => this.#handle(job),
      latency: (job) => {
        const type = JOB_TYPES.find((entry) => entry.name === job.name);
        return type ? intBetween(this.random, type.latency[0], type.latency[1]) : intBetween(this.random, 300, 900);
      },
    });

    this.series = [];
    this.events = [];
    this.deadLog = [];
    this.completedTotal = 0;
    if (warmupSteps > 0) this.warmup(warmupSteps);
  }

  #handle(job) {
    if (this.chaos > 0 && this.random() < this.chaos) {
      const mode = pick(this.random, ["upstream 503", "timeout after 30s", "connection reset by peer"]);
      throw new Error(`simulated ${mode}`);
    }
    return { job: job.name, handledAt: this.virtualNow };
  }

  #chooseType() {
    let roll = this.random() * TOTAL_WEIGHT;
    for (const type of JOB_TYPES) {
      roll -= type.weight;
      if (roll < 0) return type;
    }
    return JOB_TYPES[JOB_TYPES.length - 1];
  }

  #arrive(count) {
    for (let i = 0; i < count; i++) {
      const type = this.#chooseType();
      const priority = type.name === "charge_subscription" ? 5 : this.random() < 0.1 ? 2 : 0;
      this.queue.enqueue(type.name, { ...type.payload, attemptSource: "simulation" }, {
        queue: type.queue,
        priority,
        maxAttempts: 4,
      });
      this.arrivals++;
    }
  }

  #record(type, extra = {}) {
    this.events.unshift({ at: this.virtualNow, type, ...extra });
    if (this.events.length > 120) this.events.length = 120;
  }

  /** Advance the virtual clock one step. */
  step() {
    if (this.paused) return;
    this.virtualNow += this.stepMs;
    this.steps++;

    // Recurring definitions first, so a due rollup competes for workers like any
    // other job rather than jumping the claim queue.
    const due = this.scheduler.tick(this.virtualNow);
    for (const job of due) {
      this.recurringFired++;
      this.#record("recurring", { name: job.name, jobId: job.id, queue: job.queue });
    }

    const roll = this.random();
    if (roll < 0.26) this.#arrive(1);
    else if (roll < 0.30) this.#arrive(2);
    else if (roll < 0.31) this.#arrive(3);

    this.queue.releaseStale({ now: this.virtualNow });
    const { finished, claimed } = this.pool.tick(this.virtualNow);
    for (const event of claimed) {
      const job = this.queue.storage.get(event.jobId);
      this.#record("claimed", { ...event, queue: job?.queue ?? "default" });
    }
    for (const event of finished) {
      if (event.type === "completed") this.completedTotal++;
      if (event.type === "dead") this.deadLog.unshift(event);
      const job = this.queue.storage.get(event.jobId);
      this.#record(event.type, { ...event, queue: job?.queue ?? "default", name: job?.name });
    }

    if (this.steps % 5 === 0) {
      const counts = this.queue.counts();
      const queuedJobs = this.queue.listJobs({ state: "queued", limit: 5_000 });
      const ready = queuedJobs.filter((job) => job.runAt <= this.virtualNow).length;
      this.series.push({
        at: this.virtualNow,
        queued: counts.queued,
        ready,
        delayed: counts.queued - ready,
        running: counts.running,
        dead: counts.dead,
      });
      if (this.series.length > 160) this.series.shift();
    }
  }

  warmup(steps) {
    for (let i = 0; i < steps; i++) this.step();
    // Start the visible timeline clean: warmup exists to fill the gauges and the
    // DLQ, not to draw a chart of nothing.
    this.series.length = 0;
    this.events.length = 0;
    this.deadLog.length = 0;
    this.paused = false;
  }

  setChaos(value) {
    this.chaos = Math.max(0, Math.min(1, value));
    this.#record("chaos", { value: this.chaos });
  }

  setWorkers(count) { return this.pool.resize(count); }

  enqueueBurst(count = 12) {
    this.#arrive(count);
    this.#record("burst", { count });
    return count;
  }

  replayDead(ids = null) {
    const result = this.queue.replayDead({ ids });
    this.#record("replay", { count: result.replayed });
    return result;
  }

  latestJobs(limit = 14) {
    return this.queue.listJobs({ limit, order: "newest" });
  }

  deadLetters(limit = 25) {
    return this.queue.deadLetters({ limit });
  }

  snapshot() {
    const metrics = this.queue.metricsSnapshot();
    const fleet = this.pool.snapshot(this.virtualNow);
    return {
      now: this.virtualNow,
      elapsedMs: this.virtualNow - this.startedAt,
      steps: this.steps,
      paused: this.paused,
      chaos: this.chaos,
      workers: this.pool.size,
      counters: metrics.counters,
      depth: metrics.depth,
      counts: metrics.counts,
      rates: metrics.rates,
      policy: metrics.policy,
      rateLimit: metrics.rateLimit,
      fleet,
      series: this.series.slice(),
      events: this.events.slice(0, 40),
      jobs: this.latestJobs(),
      dead: this.deadLetters(),
      scheduler: this.scheduler.list(this.virtualNow).map((def) => ({
        id: def.id,
        name: def.name,
        kind: def.kind,
        everyMs: def.everyMs,
        expr: def.expr,
        dueInMs: def.dueInMs,
        enqueued: def.enqueued,
      })),
      arrivals: this.arrivals,
      completedTotal: this.completedTotal,
      recurringFired: this.recurringFired,
      queues: QUEUE_NAMES,
    };
  }
}

export { QUEUE_NAMES };
