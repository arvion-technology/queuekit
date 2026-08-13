import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorage } from "../src/storage.js";
import { Queue } from "../src/queue.js";
import { WorkerPool } from "../src/worker.js";
import { Metrics } from "../src/metrics.js";

function fleet({ concurrency = Infinity, workers = 4, latency = () => 100, handler = () => "ok", maxAttempts = 3 } = {}) {
  const clock = { now: 0 };
  const queue = new Queue({
    storage: new MemoryStorage(),
    concurrency,
    leaseMs: 10_000,
    clock: () => clock.now,
    backoff: { baseMs: 1_000, factor: 2, maxDelayMs: 10_000, jitter: "none", maxAttempts },
  });
  const pool = new WorkerPool({ queue, workers, handler, latency, clock: () => clock.now });
  return { queue, pool, clock };
}

test("the pool never exceeds the queue concurrency", () => {
  const { queue, pool, clock } = fleet({ concurrency: 2 });
  for (let i = 0; i < 4; i++) queue.enqueue(`job-${i}`);
  const first = pool.tick(clock.now);
  assert.equal(first.claimed.length, 2);
  assert.equal(pool.snapshot().busy, 2);
  clock.now += 50;
  assert.equal(pool.tick(clock.now).claimed.length, 0);
  clock.now += 50;
  const second = pool.tick(clock.now);
  assert.equal(second.finished.length, 2);
  assert.equal(second.claimed.length, 2);
});

test("a failing handler retries and eventually dead-letters", () => {
  const { queue, pool, clock } = fleet({
    workers: 1,
    maxAttempts: 2,
    handler: () => { throw new Error("downstream down"); },
  });
  const job = queue.enqueue("never-works");
  pool.tick(clock.now);
  clock.now += 100;
  const retried = pool.tick(clock.now);
  assert.equal(retried.finished[0].type, "retried");
  assert.equal(queue.counts().queued, 1);
  assert.equal(retried.finished[0].delayMs, 1_000);

  clock.now += 1_000;
  pool.tick(clock.now); // claims the retry
  clock.now += 100;
  const dead = pool.tick(clock.now);
  assert.equal(dead.finished[0].type, "dead");
  const parked = queue.deadLetters();
  assert.equal(parked.length, 1);
  assert.equal(parked[0].id, job.id);
  assert.match(parked[0].lastError, /downstream down/);
});

test("drain empties a finite workload", () => {
  const { queue, pool, clock } = fleet({ workers: 3 });
  for (let i = 0; i < 9; i++) queue.enqueue(`batch-${i}`);
  const { ticks } = pool.drain({ now: clock.now });
  assert.ok(ticks > 0);
  assert.equal(queue.counts().completed, 9);
  assert.equal(queue.counts().queued, 0);
});

test("resizing the pool keeps ids stable and can shrink", () => {
  const { queue, pool } = fleet({ workers: 2 });
  queue.enqueue("a");
  queue.enqueue("b");
  pool.tick(0);
  assert.equal(pool.size, 2);
  assert.equal(pool.resize(4), 4);
  assert.equal(pool.resize(1), 1);
  assert.equal(pool.workers[0].state, "busy");
});

test("metrics turn raw events into per-minute rates", () => {
  const metrics = new Metrics({ windowMs: 60_000, buckets: 12 });
  for (let i = 0; i < 20; i++) metrics.record("complete", 1_000 + i * 100);
  const snapshot = metrics.snapshot(6_000);
  // 20 completions spread over the last minute is 20/min, not 20 total.
  assert.equal(Math.round(snapshot.rates.complete), 20);
  assert.equal(snapshot.totals.complete, 20);
  assert.equal(metrics.series("complete", { now: 6_000, buckets: 5 }).length, 5);
  assert.equal(metrics.rate("dead", 6_000), 0);
});
