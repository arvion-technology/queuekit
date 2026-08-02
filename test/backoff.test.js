import test from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_BACKOFF, normalisePolicy, baseDelayMs, applyJitter, retryDelayMs,
  retryPlan, describeSchedule, BackoffPolicyError,
} from "../src/backoff.js";

test("normalisePolicy fills defaults and rejects nonsense", () => {
  const policy = normalisePolicy({ baseMs: 250 });
  assert.equal(policy.baseMs, 250);
  assert.equal(policy.factor, DEFAULT_BACKOFF.factor);
  assert.throws(() => normalisePolicy({ baseMs: 0 }), BackoffPolicyError);
  assert.throws(() => normalisePolicy({ factor: 0.5 }), BackoffPolicyError);
  assert.throws(() => normalisePolicy({ jitter: "random-ish" }), BackoffPolicyError);
  assert.throws(() => normalisePolicy({ maxAttempts: 0 }), BackoffPolicyError);
});

test("the un-jittered schedule doubles and respects the cap", () => {
  const policy = normalisePolicy({ baseMs: 1_000, factor: 2, maxDelayMs: 5_000, jitter: "none" });
  assert.deepEqual([1, 2, 3, 4, 5].map((n) => baseDelayMs(n, policy)), [1_000, 2_000, 4_000, 5_000, 5_000]);
});

test("full jitter stays inside [0, base] and is reproducible from a seeded random", () => {
  const policy = normalisePolicy({ baseMs: 1_000, factor: 2, maxDelayMs: 60_000, jitter: "full" });
  const seq = [0, 0.25, 0.5, 0.999];
  let i = 0;
  const random = () => seq[i++ % seq.length];
  assert.equal(applyJitter(1_000, "full", random), 0);
  assert.equal(applyJitter(1_000, "full", random), 250);
  assert.equal(applyJitter(1_000, "full", random), 500);
  assert.equal(applyJitter(1_000, "full", random), 999);
  // The cap is applied before jitter, so even a full-jitter draw cannot exceed it.
  assert.ok(retryDelayMs(9, policy, () => 0.999) <= 60_000);
});

test("equal jitter keeps at least half the base delay", () => {
  const policy = normalisePolicy({ jitter: "equal", baseMs: 400 });
  assert.equal(applyJitter(400, "equal", () => 0), 200);
  assert.equal(applyJitter(400, "equal", () => 0.999), 400);
});

test("retryPlan stops once the attempt budget is spent", () => {
  const policy = normalisePolicy({ baseMs: 1_000, factor: 2, maxDelayMs: 60_000, jitter: "none", maxAttempts: 3 });
  const first = retryPlan(1, policy, 10_000);
  assert.equal(first.delayMs, 1_000);
  assert.equal(first.runAt, 11_000);
  assert.equal(first.finalAttempt, false);
  const second = retryPlan(2, policy, 11_000);
  assert.equal(second.delayMs, 2_000);
  assert.equal(second.finalAttempt, true);
  assert.equal(retryPlan(3, policy, 13_000), null);
});

test("describeSchedule lists every retry with cumulative time", () => {
  const policy = normalisePolicy({ jitter: "none", baseMs: 500, factor: 3, maxDelayMs: 10_000, maxAttempts: 4 });
  assert.deepEqual(describeSchedule(policy), [
    { attempt: 1, delayMs: 500, runAt: 500 },
    { attempt: 2, delayMs: 1_500, runAt: 2_000 },
    { attempt: 3, delayMs: 4_500, runAt: 6_500 },
  ]);
});
