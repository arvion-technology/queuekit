import test from "node:test";
import assert from "node:assert/strict";
import { FleetSimulation } from "../src/simulator.js";

test("the simulation is deterministic for a seed", () => {
  const a = new FleetSimulation({ seed: 11, warmupSteps: 60 });
  const b = new FleetSimulation({ seed: 11, warmupSteps: 60 });
  const snapA = a.snapshot();
  const snapB = b.snapshot();
  assert.equal(snapA.arrivals, snapB.arrivals);
  assert.equal(snapA.counters.enqueued, snapB.counters.enqueued);
  assert.deepEqual(snapA.counts, snapB.counts);
  assert.deepEqual(snapA.series, snapB.series);
});

test("chaos mode produces dead letters and replay moves them back", () => {
  const sim = new FleetSimulation({ seed: 3, chaos: 1, warmupSteps: 900, workers: 4 });
  assert.ok(sim.snapshot().dead.length > 0, "all jobs fail under 100% chaos");
  const before = sim.queue.counts().dead;
  const result = sim.replayDead();
  assert.equal(result.replayed, before);
  assert.equal(sim.queue.counts().dead, 0);
  assert.ok(sim.queue.counts().queued > 0);
});

test("pausing stops the virtual clock and worker progress", () => {
  const sim = new FleetSimulation({ seed: 5, warmupSteps: 20 });
  sim.paused = true;
  const before = sim.virtualNow;
  for (let i = 0; i < 10; i++) sim.step();
  assert.equal(sim.virtualNow, before);
});

test("recurring definitions are registered and eventually fire", () => {
  const sim = new FleetSimulation({ seed: 9, warmupSteps: 0 });
  assert.equal(sim.snapshot().scheduler.length, 3);
  for (let i = 0; i < 700; i++) sim.step(); // 70s of virtual time
  assert.ok(sim.snapshot().recurringFired > 0);
});

test("snapshot shape is what the demo renders", () => {
  const sim = new FleetSimulation({ seed: 21, warmupSteps: 80 });
  const snap = sim.snapshot();
  for (const key of ["now", "depth", "counts", "rates", "policy", "fleet", "series", "events", "jobs", "dead", "scheduler"]) {
    assert.ok(key in snap, `missing ${key}`);
  }
  assert.equal(snap.fleet.workers, 5);
  assert.ok(snap.jobs.length > 0);
  for (let i = 0; i < 10; i++) sim.step();
  assert.ok(sim.snapshot().series.length > 0);
});
