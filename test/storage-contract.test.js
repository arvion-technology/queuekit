import test from "node:test";
import assert from "node:assert/strict";
import { MemoryStorage, makeJob, DuplicateJobError } from "../src/storage.js";
import { SqliteStorage, SQLITE_AVAILABLE } from "../src/sqlite.js";

// Both adapters must satisfy the same narrow contract. SQLite is skipped (not
// failed) on a Node version that predates node:sqlite.
const adapters = [
  ["MemoryStorage", () => new MemoryStorage()],
  ...(SQLITE_AVAILABLE ? [["SqliteStorage", () => new SqliteStorage(":memory:")]] : []),
];

for (const [name, open] of adapters) {
  test(`${name}: insert, get and versioned update`, () => {
    const store = open();
    const job = makeJob({ id: "j1", name: "test", payload: { n: 1 }, runAt: 10 });
    store.insert(job);
    assert.equal(store.get("j1").payload.n, 1);
    assert.equal(store.get("missing"), null);
    const updated = store.update("j1", { state: "running" });
    assert.equal(updated.version, 1);
    assert.equal(store.update("missing", { state: "running" }), null);
    assert.throws(() => store.insert(job), DuplicateJobError);
    store.delete?.("j1");
    store.close?.();
  });

  test(`${name}: atomic claim ordering and single ownership`, () => {
    const store = open();
    store.insert(makeJob({ id: "low", name: "low", runAt: 0, createdAt: 1 }));
    store.insert(makeJob({ id: "high", name: "high", runAt: 0, priority: 9, createdAt: 2 }));
    store.insert(makeJob({ id: "future", name: "future", runAt: 5_000, createdAt: 3 }));

    const first = store.claimNext({ now: 100, workerId: "w1" });
    assert.equal(first.id, "high");
    assert.equal(first.state, "running");
    const second = store.claimNext({ now: 100, workerId: "w2" });
    assert.equal(second.id, "low");
    assert.equal(store.claimNext({ now: 100 }), null, "future job is not eligible");
    assert.equal(store.claimNext({ now: 6_000 }).id, "future");

    store.close?.();
  });

  test(`${name}: state counts and listing filters`, () => {
    const store = open();
    store.insert(makeJob({ id: "a", name: "a", queue: "email", runAt: 0 }));
    store.insert(makeJob({ id: "b", name: "b", queue: "email", runAt: 0 }));
    store.insert(makeJob({ id: "c", name: "c", queue: "reports", runAt: 0 }));
    store.claimNext({ now: 10, queue: "email" });
    assert.deepEqual(store.countByState(), { queued: 2, running: 1, completed: 0, dead: 0 });
    assert.equal(store.countByState("email").running, 1);
    assert.deepEqual(store.list({ state: "queued", queue: "email" }).map((job) => job.id), ["b"]);
    assert.equal(store.list({ limit: 1 }).length, 1);
    store.close?.();
  });
}
