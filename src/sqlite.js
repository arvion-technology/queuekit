// SQLite adapter for the CLI. Node ships `node:sqlite` since 22.5, so this is
// durable storage with no npm dependency and no server process. The browser demo
// never imports this file: storage.js stays runtime-agnostic.
//
// The atomic claim is a single UPDATE ... RETURNING whose sub-select picks the
// next eligible row. Two processes racing on the same database cannot both get
// the same job because the row stops satisfying `state = 'queued'`.

import { QUEUED, RUNNING, COMPLETED, DEAD } from "./states.js";
import { DuplicateJobError } from "./storage.js";

let DatabaseSync;
try {
  ({ DatabaseSync } = await import("node:sqlite"));
} catch {
  DatabaseSync = null;
}

export const SQLITE_AVAILABLE = Boolean(DatabaseSync);

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    queue        TEXT NOT NULL DEFAULT 'default',
    payload      TEXT,
    state        TEXT NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 0,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    run_at       INTEGER NOT NULL,
    created_at   INTEGER NOT NULL,
    started_at   INTEGER,
    finished_at  INTEGER,
    lease_until  INTEGER,
    worker_id    TEXT,
    last_error   TEXT,
    result       TEXT,
    version      INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS jobs_claimable ON jobs (queue, state, run_at, priority DESC, created_at);
  CREATE INDEX IF NOT EXISTS jobs_created  ON jobs (created_at DESC);
`;

export class SqliteStorage {
  constructor(path = ":memory:") {
    if (!DatabaseSync) throw new Error("SqliteStorage needs Node 22.5+ (node:sqlite)");
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 2000;");
    this.db.exec(SCHEMA);
    this.stmts = {
      insert: this.db.prepare(`
        INSERT INTO jobs (id, name, queue, payload, state, priority, attempts, max_attempts,
                          run_at, created_at, started_at, finished_at, lease_until, worker_id,
                          last_error, result, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`),
      get: this.db.prepare("SELECT * FROM jobs WHERE id = ?"),
      update: this.db.prepare(`
        UPDATE jobs SET state = ?, attempts = ?, run_at = ?, started_at = ?, finished_at = ?,
               lease_until = ?, worker_id = ?, last_error = ?, result = ?, payload = ?,
               version = version + 1
        WHERE id = ? AND version = ?`),
      claim: this.db.prepare(`
        UPDATE jobs
           SET state = 'running', worker_id = ?, started_at = ?, lease_until = ?,
               version = version + 1
         WHERE id = (
                 SELECT id FROM jobs
                  WHERE state = 'queued' AND run_at <= ? AND queue = ?
                  ORDER BY priority DESC, run_at ASC, created_at ASC
                  LIMIT 1)
           AND state = 'queued'
        RETURNING *`),
      remove: this.db.prepare("DELETE FROM jobs WHERE id = ?"),
    };
  }

  insert(job) {
    try {
      this.stmts.insert.run(
        job.id, job.name, job.queue, JSON.stringify(job.payload), job.state,
        job.priority, job.attempts, job.maxAttempts, job.runAt, job.createdAt,
        job.startedAt, job.finishedAt, job.leaseUntil, job.workerId,
        job.lastError, jsonOrNull(job.result),
      );
    } catch (err) {
      if (String(err.message).includes("UNIQUE")) throw new DuplicateJobError(job.id);
      throw err;
    }
    return this.get(job.id);
  }

  get(id) {
    const row = this.stmts.get.get(id);
    return row ? rowToJob(row) : null;
  }

  update(id, patch) {
    const current = this.get(id);
    if (!current) return null;
    const next = { ...current, ...patch };
    this.stmts.update.run(
      next.state, next.attempts, next.runAt, next.startedAt, next.finishedAt,
      next.leaseUntil, next.workerId, next.lastError,
      jsonOrNull(next.result), JSON.stringify(next.payload), id, current.version,
    );
    return this.get(id);
  }

  claimNext({ now = Date.now(), queue = "default", workerId = null, leaseMs = 30_000 } = {}) {
    const row = this.stmts.claim.get(workerId, now, now + leaseMs, now, queue);
    return row ? rowToJob(row) : null;
  }

  list({ state = null, queue = null, limit = 50, offset = 0, order = "newest" } = {}) {
    const where = [];
    const params = [];
    if (state) {
      const states = Array.isArray(state) ? state : [state];
      where.push(`state IN (${states.map(() => "?").join(",")})`);
      params.push(...states);
    }
    if (queue) { where.push("queue = ?"); params.push(queue); }
    const orderBy = order === "newest" ? "created_at DESC" : "priority DESC, run_at ASC, created_at ASC";
    const sql = `SELECT * FROM jobs ${where.length ? "WHERE " + where.join(" AND ") : ""}
                 ORDER BY ${orderBy} LIMIT ? OFFSET ?`;
    params.push(limit, offset);
    return this.db.prepare(sql).all(...params).map(rowToJob);
  }

  countByState(queue = null) {
    const rows = queue
      ? this.db.prepare("SELECT state, COUNT(*) AS n FROM jobs WHERE queue = ? GROUP BY state").all(queue)
      : this.db.prepare("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state").all();
    const counts = { [QUEUED]: 0, [RUNNING]: 0, [COMPLETED]: 0, [DEAD]: 0 };
    for (const row of rows) counts[row.state] = Number(row.n);
    return counts;
  }

  delete(id) {
    return this.stmts.remove.run(id).changes > 0;
  }

  close() {
    this.db.close();
  }
}

function jsonOrNull(value) {
  return value === null || value === undefined ? null : JSON.stringify(value);
}

function rowToJob(row) {
  return {
    id: row.id,
    name: row.name,
    queue: row.queue,
    payload: row.payload === null ? null : JSON.parse(row.payload),
    state: row.state,
    priority: Number(row.priority),
    attempts: Number(row.attempts),
    maxAttempts: Number(row.max_attempts),
    runAt: Number(row.run_at),
    createdAt: Number(row.created_at),
    startedAt: row.started_at === null ? null : Number(row.started_at),
    finishedAt: row.finished_at === null ? null : Number(row.finished_at),
    leaseUntil: row.lease_until === null ? null : Number(row.lease_until),
    workerId: row.worker_id,
    lastError: row.last_error,
    result: row.result === null ? null : JSON.parse(row.result),
    version: Number(row.version),
  };
}
