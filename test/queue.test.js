import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorage } from "../src/storage.js";
import { Queue } from "../src/queue.js";

function makeQueue(overrides = {}) {
  const clock = { now: 1_000_000 };
  const storage = overrides.storage ?? new MemoryStorage();
  const queue = new Queue({
    storage,
    backoff: { baseMs: 1_000, factor: 2, maxDelayMs: 60_000, jitter: "none", maxAttempts: 3 },
    clock: () => clock.now,
    ...overrides,
  });
  return { queue, clock, storage };
}

test("jobs are claimed in the order they were enqueued", () => {
  const { queue } = makeQueue();
  for (const name of ["a", "b", "c"]) queue.enqueue(name);
  assert.deepEqual([queue.claim().name, queue.claim().name, queue.claim().name], ["a", "b", "c"]);
  assert.equal(queue.claim(), null);
});

test("priority beats insertion order, and equal priorities keep FIFO", () => {
  const { queue } = makeQueue({ concurrency: Infinity });
  queue.enqueue("normal-1");
  queue.enqueue("urgent", null, { priority: 10 });
  queue.enqueue("normal-2");
  queue.enqueue("high", null, { priority: 5 });
  assert.deepEqual(
    [queue.claim().name, queue.claim().name, queue.claim().name, queue.claim().name],
    ["urgent", "high", "normal-1", "normal-2"],
  );
});

test("delayed jobs are invisible until their runAt", () => {
  const { queue, clock } = makeQueue();
  queue.enqueueIn("later", 5_000);
  assert.equal(queue.claim(), null);
  clock.now += 4_999;
  assert.equal(queue.claim(), null);
  clock.now += 1;
  assert.equal(queue.claim().name, "later");
});

test("a failed job retries with the documented backoff and then dies", () => {
  const { queue, clock } = makeQueue();
  const job = queue.enqueue("flaky");
  const firstClaim = queue.claim();
  assert.equal(firstClaim.id, job.id);
  const first = queue.fail(job.id, "boom");
  assert.equal(first.retried, true);
  assert.equal(first.delayMs, 1_000);
  assert.equal(first.job.attempts, 1);
  assert.equal(first.job.runAt, clock.now + 1_000);

  clock.now += 1_000;
  queue.claim();
  const second = queue.fail(job.id, "boom again");
  assert.equal(second.delayMs, 2_000);
  assert.equal(second.job.attempts, 2);

  clock.now += 2_000;
  queue.claim();
  const third = queue.fail(job.id, "still broken");
  assert.equal(third.retried, false);
  assert.equal(third.deadLetter, true);
  assert.equal(third.job.state, "dead");
  assert.equal(third.job.attempts, 3);

  assert.equal(queue.deadLetters().length, 1);
  assert.equal(queue.counts().dead, 1);
  assert.equal(queue.counts().queued, 0);
});

test("concurrency caps the number of jobs handed out at once", () => {
  const { queue } = makeQueue({ concurrency: 2 });
  for (let i = 0; i < 5; i++) queue.enqueue(`job-${i}`);
  assert.ok(queue.claim());
  assert.ok(queue.claim());
  assert.equal(queue.claim(), null);
  const [first] = queue.listJobs({ state: "running" });
  queue.ack(first.id, "done");
  assert.ok(queue.claim());
});

test("per-queue concurrency overrides the global cap", () => {
  const { queue } = makeQueue({ concurrency: 5, concurrencyByQueue: { reports: 1 } });
  queue.enqueue("a", null, { queue: "reports" });
  queue.enqueue("b", null, { queue: "reports" });
  queue.enqueue("c", null, { queue: "email" });
  assert.equal(queue.claim({ queue: "reports" }).name, "a");
  assert.equal(queue.claim({ queue: "reports" }), null);
  assert.equal(queue.claim({ queue: "email" }).name, "c");
});

test("the rate limiter throttles claims and refills over time", () => {
  const { queue, clock } = makeQueue({ rateLimit: { limit: 2, intervalMs: 1_000 } });
  for (let i = 0; i < 4; i++) queue.enqueue(`r${i}`);
  assert.ok(queue.claim());
  assert.ok(queue.claim());
  assert.equal(queue.claim(), null);
  clock.now += 400; // 0.8 of a token is not enough
  assert.equal(queue.claim(), null);
  clock.now += 200; // 1.2 tokens
  assert.ok(queue.claim());
});

test("a claim hands each job out exactly once", () => {
  const { queue, storage } = makeQueue({ concurrency: Infinity });
  for (let i = 0; i < 8; i++) queue.enqueue(`once-${i}`);
  const second = new Queue({ storage, concurrency: Infinity });
  const seen = [];
  while (true) {
    const job = (seen.length % 2 === 0 ? queue : second).claim();
    if (!job) break;
    seen.push(job.id);
  }
  assert.equal(seen.length, 8);
  assert.equal(new Set(seen).size, 8);
});

test("duplicate ids are rejected so replay cannot double-enqueue", () => {
  const { queue } = makeQueue();
  queue.enqueue("once", null, { id: "fixed-id" });
  assert.throws(() => queue.enqueue("twice", null, { id: "fixed-id" }), /already exists/);
});

test("dead-letter replay resets attempts and makes the job claimable again", () => {
  const { queue, clock } = makeQueue();
  const job = queue.enqueue("poison", null, { maxAttempts: 1 });
  queue.claim();
  queue.fail(job.id, "bad payload");
  assert.equal(queue.counts().dead, 1);

  const result = queue.replayDead();
  assert.equal(result.replayed, 1);
  assert.equal(queue.counts().dead, 0);
  assert.equal(queue.counts().queued, 1);
  const replayed = queue.claim();
  assert.equal(replayed.id, job.id);
  assert.equal(replayed.attempts, 0);
  queue.ack(job.id);
  assert.equal(queue.counts().completed, 1);

  // Replay is explicit, not automatic: a second call has nothing to do.
  assert.equal(queue.replayDead().replayed, 0);
  void clock;
});

test("an expired lease returns a job to the queue and counts as a failure", () => {
  const { queue, clock } = makeQueue({ leaseMs: 5_000 });
  const job = queue.enqueue("crashy");
  queue.claim({ workerId: "worker-1" });
  assert.equal(queue.releaseStale().length, 0);
  clock.now += 5_001;
  const released = queue.releaseStale();
  assert.equal(released.length, 1);
  assert.equal(released[0].state, "queued");
  assert.equal(released[0].attempts, 1);
  clock.now += 1_000; // the retry is scheduled one backoff step later
  const reclaimed = queue.claim();
  assert.equal(reclaimed.id, job.id);
});

test("acknowledging twice is a no-op, failing a queued job is an error", () => {
  const { queue } = makeQueue();
  const job = queue.enqueue("ack-me");
  queue.claim();
  queue.ack(job.id, { ok: true });
  assert.equal(queue.ack(job.id).state, "completed");
  assert.throws(() => queue.fail(job.id, "late"), /cannot fail/);
});

test("metrics report depth, retry rate and the configured schedule", () => {
  const { queue, clock } = makeQueue();
  queue.enqueue("one");
  queue.enqueue("two", null, { delayMs: 30_000 });
  const idle = queue.metricsSnapshot();
  assert.deepEqual(idle.depth, { total: 2, available: 1, delayed: 1, running: 0 });

  queue.claim();
  queue.fail(queue.listJobs({ state: "running" })[0].id, "nope");
  clock.now += 1_000;
  const snapshot = queue.metricsSnapshot();
  assert.equal(snapshot.counters.enqueued, 2);
  assert.equal(snapshot.counters.claimed, 1);
  assert.equal(snapshot.counters.retried, 1);
  assert.equal(snapshot.policy.schedule.length, 2);
  assert.equal(snapshot.policy.maxAttempts, 3);
});
