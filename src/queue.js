// The queue itself: enqueue, atomic claim, ack, retry, dead-letter replay.
//
// What it deliberately owns:
//   - ordering (priority, then scheduled time, then insertion order)
//   - the retry decision (backoff policy) and the transition to the DLQ
//   - concurrency and rate limits, checked before touching storage
// What it delegates:
//   - durability and the atomic claim → storage adapter
//   - when to run → worker pool / simulator

import { MemoryStorage, makeJob } from "./storage.js";
import { normalisePolicy, retryPlan, DEFAULT_BACKOFF, describeSchedule } from "./backoff.js";
import { QUEUED, RUNNING, COMPLETED, DEAD, transition } from "./states.js";
import { Metrics } from "./metrics.js";

export class QueueError extends Error {}

/**
 * Token bucket. Rate limits belong to the queue that accepts work, not to each
 * worker: a worker that finishes and immediately claims again must not dodge the
 * limit.
 */
export class RateLimiter {
  constructor({ limit, intervalMs }) {
    if (!Number.isInteger(limit) || limit < 1) throw new QueueError("rateLimit.limit must be an integer >= 1");
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) throw new QueueError("rateLimit.intervalMs must be > 0");
    this.limit = limit;
    this.intervalMs = intervalMs;
    this.tokens = limit;
    this.updatedAt = 0;
  }

  #refill(now) {
    if (this.updatedAt === 0) { this.updatedAt = now; return; }
    const elapsed = Math.max(0, now - this.updatedAt);
    this.tokens = Math.min(this.limit, this.tokens + (elapsed / this.intervalMs) * this.limit);
    this.updatedAt = now;
  }

  available(now) { this.#refill(now); return this.tokens >= 1; }

  consume(now = this.updatedAt) {
    this.#refill(now);
    if (this.tokens < 1) return false;
    this.tokens -= 1;
    return true;
  }

  snapshot(now) { this.#refill(now); return { limit: this.limit, intervalMs: this.intervalMs, tokens: this.tokens }; }
}

export class Queue {
  constructor({
    storage = new MemoryStorage(),
    backoff = DEFAULT_BACKOFF,
    concurrency = Infinity,
    concurrencyByQueue = {},
    rateLimit = null,
    leaseMs = 30_000,
    clock = () => Date.now(),
    random = Math.random,
  } = {}) {
    this.storage = storage;
    this.backoff = normalisePolicy(backoff);
    this.concurrency = concurrency;
    this.concurrencyByQueue = { ...concurrencyByQueue };
    this.limiter = rateLimit ? new RateLimiter(rateLimit) : null;
    this.leaseMs = leaseMs;
    this.clock = clock;
    this.random = random;
    this.metrics = new Metrics();
    this.counters = { enqueued: 0, claimed: 0, completed: 0, failed: 0, retried: 0, dead: 0, replayed: 0, leaseExpired: 0 };
  }

  now() { return this.clock(); }

  enqueue(name, payload = null, options = {}) {
    if (!name || typeof name !== "string") throw new QueueError("enqueue needs a job name");
    const now = this.now();
    const maxAttempts = options.maxAttempts ?? this.backoff.maxAttempts;
    const runAt = options.runAt ?? (options.delayMs ? now + options.delayMs : now);
    const job = makeJob({
      id: options.id,
      name,
      payload,
      queue: options.queue ?? "default",
      priority: options.priority ?? 0,
      runAt,
      maxAttempts,
      attempts: 0,
      createdAt: now,
    });
    const stored = this.storage.insert(job);
    this.counters.enqueued++;
    this.metrics.record("enqueue", now);
    return stored;
  }

  enqueueAt(name, runAt, payload = null, options = {}) {
    return this.enqueue(name, payload, { ...options, runAt });
  }

  enqueueIn(name, delayMs, payload = null, options = {}) {
    return this.enqueue(name, payload, { ...options, delayMs });
  }

  #concurrencyFor(queue) {
    return this.concurrencyByQueue[queue] ?? this.concurrency;
  }

  #runningIn(queue) {
    return this.storage.countByState(queue)[RUNNING] ?? 0;
  }

  #runningTotal() {
    return this.storage.countByState()[RUNNING] ?? 0;
  }

  /**
   * True when another job may start right now. The global cap keeps the whole
   * process bounded; a per-queue cap keeps one noisy queue from eating it. Both
   * are checked in-process only — see the README on distributed concurrency.
   */
  canStart(queue = "default") {
    const now = this.now();
    if (this.#runningTotal() >= this.concurrency) return false;
    if (this.#runningIn(queue) >= this.#concurrencyFor(queue)) return false;
    if (this.limiter && !this.limiter.available(now)) return false;
    return true;
  }

  /**
   * Claim one job. Returns null when the queue is empty, when every job is
   * scheduled in the future, or when a limit says wait. Callers loop until null.
   */
  claim({ queue = "default", workerId = null } = {}) {
    const now = this.now();
    if (!this.canStart(queue)) return null;
    const job = this.storage.claimNext({ now, queue, workerId, leaseMs: this.leaseMs });
    if (!job) return null;
    if (this.limiter) this.limiter.consume(now);
    this.counters.claimed++;
    this.metrics.record("claim", now);
    return job;
  }

  ack(jobId, result = null) {
    const job = this.storage.get(jobId);
    if (!job) throw new QueueError(`unknown job ${jobId}`);
    if (job.state === COMPLETED) return job; // ack after a redelivery is a no-op, not an error
    transition(job.state, COMPLETED, jobId);
    const updated = this.storage.update(jobId, {
      state: COMPLETED,
      finishedAt: this.now(),
      result,
      workerId: null,
      leaseUntil: null,
    });
    this.counters.completed++;
    this.metrics.record("complete", this.now());
    return updated;
  }

  /**
   * Record a handler failure. Either schedules the next attempt or parks the job
   * in the dead-letter queue. Returns the retry decision so a worker can log it.
   */
  fail(jobId, error = "unknown error") {
    const job = this.storage.get(jobId);
    if (!job) throw new QueueError(`unknown job ${jobId}`);
    if (job.state !== RUNNING) throw new QueueError(`cannot fail job ${jobId} in state ${job.state}`);
    const now = this.now();
    const attempts = job.attempts + 1;
    const message = error instanceof Error ? error.message : String(error);
    this.metrics.record("fail", now);
    // The per-job budget can be tighter than the queue policy (one poison-message
    // treatment), so exhaustion is decided from the job, not the policy.
    const plan = attempts >= job.maxAttempts ? null : retryPlan(attempts, this.backoff, now, this.random);
    if (!plan) {
      const updated = this.storage.update(jobId, {
        state: DEAD,
        attempts,
        lastError: message,
        finishedAt: now,
        workerId: null,
        leaseUntil: null,
      });
      this.counters.failed++;
      this.counters.dead++;
      this.metrics.record("dead", now);
      return { job: updated, retried: false, deadLetter: true, delayMs: 0, nextRunAt: null };
    }
    transition(job.state, QUEUED, jobId);
    const updated = this.storage.update(jobId, {
      state: QUEUED,
      attempts,
      lastError: message,
      runAt: plan.runAt,
      startedAt: null,
      finishedAt: null,
      workerId: null,
      leaseUntil: null,
    });
    this.counters.failed++;
    this.counters.retried++;
    this.metrics.record("retry", now);
    return { job: updated, retried: true, deadLetter: false, delayMs: plan.delayMs, nextRunAt: plan.runAt };
  }

  /**
   * Return jobs whose worker disappeared to the queue. A lease is the only
   * recovery mechanism that does not need a heartbeat service, and it costs one
   * attempt: a job that reliably kills its worker must still exhaust the DLQ.
   */
  releaseStale({ now = this.now() } = {}) {
    const released = [];
    for (const job of this.storage.list({ state: RUNNING, limit: 10_000 })) {
      if (job.leaseUntil !== null && job.leaseUntil > now) continue;
      this.counters.leaseExpired++;
      const result = this.fail(job.id, "lease expired before acknowledgement");
      released.push(result.job);
    }
    return released;
  }

  deadLetters({ queue = null, limit = 100 } = {}) {
    return this.storage.list({ state: DEAD, queue, limit, order: "newest" });
  }

  /**
   * Move dead jobs back to queued. Replay resets the attempt counter by default:
   * an operator replay is a decision that the failure cause is fixed, so keeping
   * the exhausted budget would send the job straight back to the DLQ.
   */
  replayDead({ ids = null, queue = null, resetAttempts = true, limit = 1000 } = {}) {
    const wanted = ids ? new Set(ids) : null;
    const now = this.now();
    const replayed = [];
    for (const job of this.storage.list({ state: DEAD, queue, limit, order: "newest" })) {
      if (wanted && !wanted.has(job.id)) continue;
      transition(job.state, QUEUED, job.id);
      replayed.push(this.storage.update(job.id, {
        state: QUEUED,
        attempts: resetAttempts ? 0 : job.attempts,
        runAt: now,
        startedAt: null,
        finishedAt: null,
        workerId: null,
        leaseUntil: null,
        result: null,
      }));
    }
    this.counters.replayed += replayed.length;
    for (let i = 0; i < replayed.length; i++) this.metrics.record("replay", now);
    return { replayed: replayed.length, jobs: replayed };
  }

  /**
   * Move every job whose scheduled time has passed into the claim window. Not
   * needed for MemoryStorage (claim checks runAt directly), but the CLI calls it
   * after a restart so statistics about "available now" stay honest.
   */
  promoteDue({ queue = null } = {}) {
    const now = this.now();
    const due = this.storage.list({ state: QUEUED, queue, limit: 10_000 })
      .filter((job) => job.runAt > now)
      .length;
    return { delayed: due };
  }

  listJobs(options = {}) {
    return this.storage.list(options);
  }

  counts(queue = null) {
    return this.storage.countByState(queue);
  }

  metricsSnapshot() {
    const now = this.now();
    const counts = this.counts();
    const queuedJobs = this.storage.list({ state: QUEUED, limit: 10_000 });
    const available = queuedJobs.filter((job) => job.runAt <= now).length;
    const attempts = this.counters.failed + this.counters.completed;
    return {
      now,
      counts,
      depth: { total: counts[QUEUED], available, delayed: counts[QUEUED] - available, running: counts[RUNNING] },
      counters: { ...this.counters },
      rates: {
        retry: attempts ? this.counters.retried / attempts : 0,
        deadLetter: attempts ? this.counters.dead / attempts : 0,
        success: attempts ? this.counters.completed / attempts : 0,
        throughputPerMinute: this.metrics.rate("complete", now),
      },
      oldestQueuedAgeMs: oldestAge(queuedJobs, now),
      policy: {
        ...this.backoff,
        schedule: describeSchedule(this.backoff, () => 0.5),
      },
      rateLimit: this.limiter ? this.limiter.snapshot(now) : null,
    };
  }
}

function oldestAge(jobs, now) {
  if (!jobs.length) return 0;
  return Math.max(0, now - Math.min(...jobs.map((job) => job.createdAt)));
}

export { QUEUED, RUNNING, COMPLETED, DEAD };
