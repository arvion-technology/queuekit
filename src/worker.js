// A small worker fleet. Tests drive it with a virtual clock; the demo drives it
// from a timer. A worker is modelled as: claim → busy for `latency` → ack or
// fail. Keeping the handler synchronous makes the simulation deterministic, and
// the point of the project is the queue semantics, not concurrency primitives
// inside one process.

export class WorkerPool {
  constructor({
    queue,
    workers = 4,
    handler,
    latency = () => 500,
    clock = () => Date.now(),
    random = Math.random,
    queues = ["default"],
  } = {}) {
    if (!queue) throw new Error("WorkerPool needs a queue");
    if (!Array.isArray(queues) || queues.length === 0) throw new Error("WorkerPool needs at least one queue to poll");
    this.queue = queue;
    this.queues = [...queues];
    this.queueCursor = 0;
    this.handler = handler ?? (() => null);
    this.latency = latency;
    this.clock = clock;
    this.random = random;
    this.events = [];
    this.workers = Array.from({ length: workers }, (_, i) => ({
      id: `worker-${i + 1}`,
      state: "idle",
      currentJobId: null,
      currentJobName: null,
      busyUntil: 0,
      outcome: null,
      startedAt: null,
      processed: 0,
      failed: 0,
      claimed: 0,
    }));
  }

  get size() { return this.workers.length; }

  resize(count) {
    if (!Number.isInteger(count) || count < 1) throw new Error("worker count must be >= 1");
    while (this.workers.length < count) {
      this.workers.push({
        id: `worker-${this.workers.length + 1}`, state: "idle", currentJobId: null,
        currentJobName: null, busyUntil: 0, outcome: null, startedAt: null,
        processed: 0, failed: 0, claimed: 0,
      });
    }
    while (this.workers.length > count) {
      const worker = this.workers.pop();
      if (worker.currentJobId) this.queue.fail(worker.currentJobId, "worker removed");
    }
    return this.workers.length;
  }

  #emit(type, payload) {
    const event = { type, at: this.clock(), ...payload };
    this.events.push(event);
    if (this.events.length > 500) this.events.shift();
    return event;
  }

  /**
   * One scheduling step: finish jobs whose latency elapsed, then let idle
   * workers claim. The handler runs at claim time so failure and retry timing
   * are decided by queue.fail at completion time.
   */
  tick(now = this.clock()) {
    const finished = [];
    for (const worker of this.workers) {
      if (worker.state !== "busy" || now < worker.busyUntil) continue;
      const jobId = worker.currentJobId;
      const outcome = worker.outcome;
      worker.state = "idle";
      worker.currentJobId = null;
      worker.currentJobName = null;
      worker.outcome = null;
      if (outcome.ok) {
        this.queue.ack(jobId, outcome.value);
        worker.processed++;
        finished.push(this.#emit("completed", { workerId: worker.id, jobId }));
      } else {
        const decision = this.queue.fail(jobId, outcome.error);
        worker.failed++;
        finished.push(this.#emit(decision.deadLetter ? "dead" : "retried", {
          workerId: worker.id, jobId, error: outcome.error,
          delayMs: decision.delayMs, nextRunAt: decision.nextRunAt,
        }));
      }
    }

    const claimed = [];
    for (const worker of this.workers) {
      if (worker.state !== "idle") continue;
      const job = this.#claimAnyQueue(worker);
      if (!job) break; // every polled queue is empty or limited, nothing to hand out
      let outcome;
      try {
        const value = this.handler(job, { worker, random: this.random, now });
        outcome = { ok: true, value: value ?? null };
      } catch (err) {
        outcome = { ok: false, error: err instanceof Error ? err.message : String(err) };
      }
      worker.state = "busy";
      worker.currentJobId = job.id;
      worker.currentJobName = job.name;
      worker.startedAt = now;
      worker.busyUntil = now + Math.max(1, this.latency(job, this.random));
      worker.outcome = outcome;
      worker.claimed++;
      claimed.push(this.#emit("claimed", { workerId: worker.id, jobId: job.id, name: job.name }));
    }
    return { finished, claimed };
  }

  /** Try each polled queue once, starting after the last queue that produced work. */
  #claimAnyQueue(worker) {
    for (let attempt = 0; attempt < this.queues.length; attempt++) {
      const index = (this.queueCursor + attempt) % this.queues.length;
      const job = this.queue.claim({ workerId: worker.id, queue: this.queues[index] });
      if (job) {
        this.queueCursor = (index + 1) % this.queues.length;
        return job;
      }
    }
    return null;
  }

  /** Run ticks until every worker is idle and the queue has nothing claimable. */
  drain({ now = this.clock(), maxTicks = 10_000, stepMs = 100 } = {}) {
    let ticks = 0;
    while (ticks++ < maxTicks) {
      this.tick(now);
      const busy = this.workers.some((worker) => worker.state === "busy");
      const depth = this.queue.metricsSnapshot().depth;
      if (!busy && depth.available === 0 && depth.delayed === 0) break;
      now += stepMs;
    }
    return { ticks, now };
  }

  snapshot(now = this.clock()) {
    return {
      workers: this.size,
      busy: this.workers.filter((worker) => worker.state === "busy").length,
      idle: this.workers.filter((worker) => worker.state === "idle").length,
      processed: this.workers.reduce((sum, worker) => sum + worker.processed, 0),
      failed: this.workers.reduce((sum, worker) => sum + worker.failed, 0),
      detail: this.workers.map((worker) => ({
        id: worker.id,
        state: worker.state,
        jobId: worker.currentJobId,
        jobName: worker.currentJobName,
        busyForMs: worker.state === "busy" ? Math.max(0, worker.busyUntil - now) : 0,
        processed: worker.processed,
        failed: worker.failed,
      })),
    };
  }
}
