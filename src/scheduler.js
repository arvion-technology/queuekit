// Delayed and recurring jobs.
//
// Delayed jobs need nothing beyond `runAt` in storage — the claim query already
// filters on it. Recurring jobs need a definition with a next-fire time, so this
// module owns a small cron parser and a Scheduler that turns definitions into
// ordinary queued jobs. Cron fields are evaluated in UTC; that is a deliberate
// limitation, documented in the README, because "server local time" has burned
// every team that has ever scheduled a nightly job.

export class CronError extends Error {}

const FIELDS = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day of month", min: 1, max: 31 },
  { name: "month", min: 1, max: 12 },
  { name: "day of week", min: 0, max: 7 },
];

function expandField(text, field) {
  const values = new Set();
  for (const part of text.split(",")) {
    const [rangePart, stepPart] = part.split("/");
    const step = stepPart === undefined ? 1 : Number(stepPart);
    if (!Number.isInteger(step) || step < 1) throw new CronError(`bad step in "${part}"`);
    let lo;
    let hi;
    if (rangePart === "*") {
      lo = field.min;
      hi = field.max;
    } else if (rangePart.includes("-")) {
      const [a, b] = rangePart.split("-").map(Number);
      lo = a;
      hi = b;
    } else {
      lo = hi = Number(rangePart);
    }
    if (!Number.isInteger(lo) || !Number.isInteger(hi) || lo < field.min || hi > field.max || lo > hi) {
      throw new CronError(`bad value in "${part}" for ${field.name} (${field.min}-${field.max})`);
    }
    for (let v = lo; v <= hi; v += step) values.add(field.name === "day of week" && v === 7 ? 0 : v);
  }
  return values;
}

/** Parse a five-field cron expression. Throws CronError on bad input. */
export function parseCron(expression) {
  const parts = String(expression).trim().split(/\s+/);
  if (parts.length !== 5) throw new CronError(`cron needs 5 fields, got ${parts.length}: "${expression}"`);
  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts.map((text, i) => expandField(text, FIELDS[i]));
  if (!minute.size || !hour.size || !dayOfMonth.size || !month.size || !dayOfWeek.size) {
    throw new CronError(`cron field has no possible value: "${expression}"`);
  }
  return {
    expression: parts.join(" "),
    minute,
    hour,
    dayOfMonth,
    month,
    dayOfWeek,
    // POSIX: when both day fields are restricted, either may match.
    domRestricted: parts[2] !== "*",
    dowRestricted: parts[4] !== "*",
  };
}

function matchesCron(cron, date) {
  if (!cron.minute.has(date.getUTCMinutes())) return false;
  if (!cron.hour.has(date.getUTCHours())) return false;
  if (!cron.month.has(date.getUTCMonth() + 1)) return false;
  const dom = cron.dayOfMonth.has(date.getUTCDate());
  const dow = cron.dayOfWeek.has(date.getUTCDay());
  if (cron.domRestricted && cron.dowRestricted) return dom || dow;
  return dom && dow;
}

/**
 * Next UTC minute strictly after `from` that matches. Minute granularity: cron
 * has no seconds field, and pretending otherwise would make `next` ambiguous.
 */
export function nextCronTime(expression, from = Date.now()) {
  const cron = typeof expression === "string" ? parseCron(expression) : expression;
  const cursor = new Date(from);
  cursor.setUTCSeconds(0, 0);
  cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);
  const limit = 366 * 24 * 60;
  for (let i = 0; i < limit; i++) {
    if (matchesCron(cron, cursor)) return cursor.getTime();
    cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);
  }
  throw new CronError(`no occurrence of "${cron.expression}" within a year`);
}

export function describeCron(expression, from = Date.now(), count = 3) {
  let cursor = from;
  const times = [];
  for (let i = 0; i < count; i++) {
    cursor = nextCronTime(expression, cursor);
    times.push(cursor);
  }
  return times;
}

/**
 * Holds recurring definitions and enqueues a normal job when one is due.
 *
 * At-least-once: if a previous occurrence is still queued, the next one is
 * enqueued anyway. That is the right default for "every five minutes" jobs such
 * as metrics rollups, but it means handlers must be idempotent.
 */
export class Scheduler {
  constructor({ queue, clock = () => Date.now() } = {}) {
    if (!queue) throw new CronError("Scheduler needs a queue");
    this.queue = queue;
    this.clock = clock;
    this.definitions = new Map();
    this.seq = 0;
  }

  #add(definition) {
    const id = definition.id ?? `rec_${++this.seq}`;
    if (this.definitions.has(id)) throw new CronError(`duplicate recurring id ${id}`);
    const entry = { ...definition, id, lastEnqueuedAt: null, enqueued: 0 };
    this.definitions.set(id, entry);
    return entry;
  }

  every(name, { everyMs, runAt = null, payload = null, ...options } = {}) {
    if (!Number.isInteger(everyMs) || everyMs < 1_000) throw new CronError("everyMs must be an integer >= 1000");
    const now = this.clock();
    return this.#add({
      kind: "interval", id: options.id, name, everyMs, payload, options,
      nextRunAt: runAt ?? now + everyMs,
    });
  }

  cron(name, { expr, payload = null, ...options } = {}) {
    const parsed = parseCron(expr);
    const now = this.clock();
    return this.#add({
      kind: "cron", id: options.id, name, expr: parsed.expression, payload, options,
      nextRunAt: nextCronTime(parsed, now),
    });
  }

  remove(id) { return this.definitions.delete(id); }

  list(now = this.clock()) {
    return [...this.definitions.values()].map((def) => ({
      ...def,
      dueInMs: Math.max(0, def.nextRunAt - now),
    }));
  }

  /** Enqueue every definition that is due and advance its next fire time. */
  tick(now = this.clock()) {
    const enqueued = [];
    for (const def of this.definitions.values()) {
      if (def.nextRunAt > now) continue;
      const job = this.queue.enqueue(def.name, def.payload, {
        ...def.options,
        // Give the occurrence a distinct identifier so a duplicate enqueue in a
        // retry loop cannot silently replace yesterday's job.
        id: def.options.id ? `${def.options.id}-${now}` : undefined,
      });
      def.lastEnqueuedAt = now;
      def.enqueued++;
      if (def.kind === "interval") {
        let next = def.nextRunAt + def.everyMs;
        if (next <= now) next = now + def.everyMs; // a sleep/suspend should not replay a burst
        def.nextRunAt = next;
      } else {
        def.nextRunAt = nextCronTime(def.expr, now);
      }
      enqueued.push(job);
    }
    return enqueued;
  }
}
