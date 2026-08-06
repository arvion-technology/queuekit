import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorage } from "../src/storage.js";
import { Queue } from "../src/queue.js";
import { Scheduler, parseCron, nextCronTime, CronError } from "../src/scheduler.js";

const iso = (ms) => new Date(ms).toISOString();

test("parseCron expands steps, ranges and lists", () => {
  const cron = parseCron("*/15 9-11 1,15 * 1");
  assert.deepEqual([...cron.minute].sort((a, b) => a - b), [0, 15, 30, 45]);
  assert.deepEqual([...cron.hour].sort((a, b) => a - b), [9, 10, 11]);
  assert.deepEqual([...cron.dayOfMonth].sort((a, b) => a - b), [1, 15]);
  assert.equal(cron.domRestricted, true);
  assert.throws(() => parseCron("* * * *"), CronError);
  assert.throws(() => parseCron("60 * * * *"), CronError);
  assert.throws(() => parseCron("* * * * 9"), CronError);
});

test("nextCronTime returns the next UTC match strictly after the cursor", () => {
  assert.equal(iso(nextCronTime("*/15 * * * *", Date.parse("2026-01-01T00:07:00Z"))), "2026-01-01T00:15:00.000Z");
  assert.equal(iso(nextCronTime("*/15 * * * *", Date.parse("2026-01-01T00:15:00Z"))), "2026-01-01T00:30:00.000Z");
  // 2026-06-01 is a Monday.
  assert.equal(iso(nextCronTime("30 2 * * 1", Date.parse("2026-06-01T00:00:00Z"))), "2026-06-01T02:30:00.000Z");
  assert.equal(iso(nextCronTime("0 0 1 * *", Date.parse("2026-06-15T00:00:00Z"))), "2026-07-01T00:00:00.000Z");
});

test("day-of-month and day-of-week use POSIX OR semantics", () => {
  // Both restricted: the 1st of the month OR any Monday.
  const from = Date.parse("2026-06-02T00:00:00Z"); // Tuesday
  assert.equal(iso(nextCronTime("0 0 1 * 1", from)), "2026-06-08T00:00:00.000Z"); // next Monday
});

test("interval recurrences enqueue one job per period and skip missed periods", () => {
  const clock = { now: 0 };
  const queue = new Queue({ storage: new MemoryStorage(), clock: () => clock.now });
  const scheduler = new Scheduler({ queue, clock: () => clock.now });
  const def = scheduler.every("rollup", { everyMs: 1_000, payload: { window: "5m" } });
  assert.equal(def.nextRunAt, 1_000);

  assert.equal(scheduler.tick(500).length, 0);
  const fired = scheduler.tick(1_000);
  assert.equal(fired.length, 1);
  assert.equal(fired[0].name, "rollup");
  assert.deepEqual(fired[0].payload, { window: "5m" });
  assert.equal(scheduler.tick(1_999).length, 0);
  assert.equal(scheduler.tick(2_000).length, 1);

  // A suspended process should not replay a backlog of occurrences.
  clock.now = 10_000;
  assert.equal(scheduler.tick(10_000).length, 1);
  assert.equal(scheduler.list(10_000)[0].nextRunAt, 11_000);
});

test("cron recurrences use UTC fire times and requeue themselves", () => {
  const clock = { now: Date.parse("2026-01-01T00:00:00Z") };
  const queue = new Queue({ storage: new MemoryStorage(), clock: () => clock.now });
  const scheduler = new Scheduler({ queue, clock: () => clock.now });
  const def = scheduler.cron("usage_rollup", { expr: "*/5 * * * *" });
  assert.equal(iso(def.nextRunAt), "2026-01-01T00:05:00.000Z");

  clock.now = Date.parse("2026-01-01T00:05:00Z");
  assert.equal(scheduler.tick(clock.now).length, 1);
  assert.equal(iso(scheduler.list(clock.now)[0].nextRunAt), "2026-01-01T00:10:00.000Z");
  assert.equal(queue.counts().queued, 1);
});

test("duplicate recurring ids are rejected", () => {
  const queue = new Queue({ storage: new MemoryStorage(), clock: () => 0 });
  const scheduler = new Scheduler({ queue, clock: () => 0 });
  scheduler.every("sync", { everyMs: 5_000, id: "sync" });
  assert.throws(() => scheduler.every("sync again", { everyMs: 5_000, id: "sync" }), CronError);
  assert.equal(scheduler.remove("sync"), true);
});
