// Retry timing for failed jobs.
//
// The policy is data, not code: a queue can pass its own base/factor/cap/jitter
// and the tests pin the exact schedule. Jitter is injected (`random`) rather
// than read from Math.random inside the maths, so a failing retry test can be
// reproduced from a seed.

export const JITTER_MODES = Object.freeze(["full", "equal", "none"]);

export const DEFAULT_BACKOFF = Object.freeze({
  baseMs: 1_000,
  factor: 2,
  maxDelayMs: 300_000,
  jitter: "full",
  maxAttempts: 5,
});

export class BackoffPolicyError extends Error {}

function positiveInt(value, name, { allowZero = false } = {}) {
  if (!Number.isFinite(value) || !Number.isInteger(value) || value < (allowZero ? 0 : 1)) {
    throw new BackoffPolicyError(`${name} must be an integer ${allowZero ? ">= 0" : ">= 1"}, got ${value}`);
  }
  return value;
}

/**
 * Merge a partial policy over the defaults and validate it. Throws early so a
 * typo in config shows up at boot rather than after the first production failure.
 */
export function normalisePolicy(partial = {}) {
  const merged = { ...DEFAULT_BACKOFF, ...partial };
  positiveInt(merged.baseMs, "baseMs");
  positiveInt(merged.maxDelayMs, "maxDelayMs");
  positiveInt(merged.maxAttempts, "maxAttempts");
  if (!Number.isFinite(merged.factor) || merged.factor < 1) {
    throw new BackoffPolicyError(`factor must be a number >= 1, got ${merged.factor}`);
  }
  if (!JITTER_MODES.includes(merged.jitter)) {
    throw new BackoffPolicyError(`jitter must be one of ${JITTER_MODES.join(", ")}, got ${merged.jitter}`);
  }
  return Object.freeze(merged);
}

/**
 * Un-jittered delay for a given failure count (1 = first failure). Capped before
 * jitter so the cap is a real upper bound: a full-jitter delay never exceeds
 * maxDelayMs.
 */
export function baseDelayMs(attempt, policy = DEFAULT_BACKOFF) {
  const p = normalisePolicy(policy);
  const n = positiveInt(attempt, "attempt");
  const raw = p.baseMs * Math.pow(p.factor, n - 1);
  return Math.min(p.maxDelayMs, Math.round(raw));
}

function unit(random) {
  const value = random();
  if (!(value >= 0 && value < 1)) throw new BackoffPolicyError(`random() must return [0, 1), got ${value}`);
  return value;
}

/**
 * full  : delay * u, spreads a retry storm over the whole window
 * equal : delay/2 + (delay/2)*u, keeps a floor, still decorrelates workers
 * none  : delay, deterministic, for tests and fixed schedules
 */
export function applyJitter(delayMs, jitter, random = Math.random) {
  if (jitter === "none") return delayMs;
  if (jitter === "equal") return Math.round(delayMs / 2 + (delayMs / 2) * unit(random));
  if (jitter === "full") return Math.round(delayMs * unit(random));
  throw new BackoffPolicyError(`unknown jitter mode ${jitter}`);
}

/** Retry delay after the nth failure. */
export function retryDelayMs(attempt, policy = DEFAULT_BACKOFF, random = Math.random) {
  const p = normalisePolicy(policy);
  return applyJitter(baseDelayMs(attempt, p), p.jitter, random);
}

/**
 * Everything a caller needs to persist: which attempt is next and when it runs.
 * Returns `null` when the attempt budget is already spent, that caller should
 * move the job to the dead-letter queue instead.
 */
export function retryPlan(failedAttempt, policy = DEFAULT_BACKOFF, now = Date.now(), random = Math.random) {
  const p = normalisePolicy(policy);
  const attempts = positiveInt(failedAttempt, "failedAttempt");
  if (attempts >= p.maxAttempts) return null;
  const delayMs = applyJitter(baseDelayMs(attempts, p), p.jitter, random);
  return { attempt: attempts, delayMs, runAt: now + delayMs, finalAttempt: attempts + 1 === p.maxAttempts };
}

/**
 * Human-readable schedule, used by the README, the CLI and the demo's policy
 * panel. `random` is fixed by the caller when the output must be stable.
 */
export function describeSchedule(policy = DEFAULT_BACKOFF, random = () => 0.5) {
  const p = normalisePolicy(policy);
  const rows = [];
  let total = 0;
  for (let attempt = 1; attempt < p.maxAttempts; attempt++) {
    const delayMs = applyJitter(baseDelayMs(attempt, p), p.jitter, random);
    total += delayMs;
    rows.push({ attempt, delayMs, runAt: total });
  }
  return rows;
}
