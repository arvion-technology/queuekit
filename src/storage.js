// Storage adapters for the queue.
//
// The interface is intentionally narrow, because "atomic claim" is the only
// operation that has to be correct under concurrency:
//
//   insert(job)                       persist a fully-formed job
//   get(id)                           one job or null
//   update(id, patch)                 read-modify-write, bumps `version`
//   claimNext({ now, queue, workerId}) atomically pick the best eligible job
//   list({ ...filters })              newest-first for the dashboard/CLI
//   countByState(queue?)              dashboard gauges
//   delete(id)
//
// MemoryStorage exists for tests and the browser demo; SQLiteStorage is the
// durable adapter used by the CLI. Both must pass test/storage-contract.test.js.

import { QUEUED, RUNNING, COMPLETED, DEAD } from "./states.js";

export class DuplicateJobError extends Error {
  constructor(id) {
    super(`job ${id} already exists`);
    this.name = "DuplicateJobError";
    this.jobId = id;
  }
}

let nextMemoryId = 1;

export function makeJob({
  id = `job_${Date.now().toString(36)}_${nextMemoryId++}`,
  name,
  payload = null,
  queue = "default",
  priority = 0,
  runAt = Date.now(),
  maxAttempts = 5,
  state = QUEUED,
  attempts = 0,
  createdAt = Date.now(),
} = {}) {
  const now = createdAt;
  return {
    id,
    name,
    payload,
    queue,
    priority,
    state,
    attempts,
    maxAttempts,
    runAt,
    createdAt: now,
    startedAt: null,
    finishedAt: null,
    leaseUntil: null,
    workerId: null,
    lastError: null,
    result: null,
    version: 0,
  };
}

/**
 * In-memory store. Ordering is the queue 's contract: highest priority first,
 * then earliest runAt, then insertion order. Keeping the sort here (and not in
 * the worker) means a SQL adapter can express it as one ORDER BY.
 */
export class MemoryStorage {
  constructor() {
    this.jobs = new Map();
    this.seq = 0;
    this.seqById = new Map();
  }

  insert(job) {
    if (this.jobs.has(job.id)) throw new DuplicateJobError(job.id);
    const stored = { ...job, version: 0, _seq: this.seq++ };
    this.jobs.set(job.id, stored);
    this.seqById.set(job.id, stored._seq);
    return this.#public(stored);
  }

  get(id) {
    const job = this.jobs.get(id);
    return job ? this.#public(job) : null;
  }

  update(id, patch) {
    const current = this.jobs.get(id);
    if (!current) return null;
    const next = { ...current, ...patch, version: current.version + 1 };
    this.jobs.set(id, next);
    return this.#public(next);
  }

  /**
   * The atomic claim. In a single-threaded runtime the method body cannot be
   * interleaved, which is exactly the guarantee SQLite gives us with one
   * UPDATE ... RETURNING statement: a job leaves `queued` exactly once.
   */
  claimNext({ now = Date.now(), queue = "default", workerId = null, leaseMs = 30_000 } = {}) {
    const candidates = [...this.jobs.values()]
      .filter((job) => job.state === QUEUED && job.runAt <= now && (!queue || job.queue === queue))
      .sort(compareJobs);
    const job = candidates[0];
    if (!job) return null;
    const claimed = {
      ...job,
      state: RUNNING,
      workerId,
      startedAt: now,
      leaseUntil: now + leaseMs,
      version: job.version + 1,
    };
    this.jobs.set(job.id, claimed);
    return this.#public(claimed);
  }

  list({ state = null, queue = null, limit = 50, offset = 0, order = "newest" } = {}) {
    let rows = [...this.jobs.values()];
    if (state) rows = rows.filter((job) => (Array.isArray(state) ? state.includes(job.state) : job.state === state));
    if (queue) rows = rows.filter((job) => job.queue === queue);
    rows.sort(order === "newest" ? (a, b) => b.createdAt - a.createdAt || b._seq - a._seq : compareJobs);
    return rows.slice(offset, offset + limit).map((job) => this.#public(job));
  }

  countByState(queue = null) {
    const counts = { [QUEUED]: 0, [RUNNING]: 0, [COMPLETED]: 0, [DEAD]: 0 };
    for (const job of this.jobs.values()) {
      if (queue && job.queue !== queue) continue;
      counts[job.state] = (counts[job.state] ?? 0) + 1;
    }
    return counts;
  }

  delete(id) {
    return this.jobs.delete(id);
  }

  clear() {
    this.jobs.clear();
    this.seqById.clear();
  }

  #public(job) {
    const { _seq, ...rest } = job;
    return rest;
  }
}

function compareJobs(a, b) {
  if (b.priority !== a.priority) return b.priority - a.priority;
  if (a.runAt !== b.runAt) return a.runAt - b.runAt;
  return (a._seq ?? a.createdAt) - (b._seq ?? b.createdAt);
}
