// Rolling counters for the dashboard and the CLI.
//
// A queue's interesting numbers are rates, not totals: "412 completed" says
// nothing, "412 completed in the last minute" does. The recorder keeps one
// minute-granularity bucket per event type and sums the window.

const EVENT_TYPES = ["enqueue", "claim", "complete", "fail", "retry", "dead", "replay"];

export class Metrics {
  constructor({ windowMs = 60_000, buckets = 30 } = {}) {
    this.windowMs = windowMs;
    this.bucketMs = Math.max(1_000, Math.round(windowMs / buckets));
    this.buckets = new Map(); // bucketStart -> Map(type -> count)
    this.totals = Object.fromEntries(EVENT_TYPES.map((type) => [type, 0]));
  }

  record(type, at = Date.now(), by = 1) {
    const start = Math.floor(at / this.bucketMs) * this.bucketMs;
    let bucket = this.buckets.get(start);
    if (!bucket) { bucket = new Map(); this.buckets.set(start, bucket); }
    bucket.set(type, (bucket.get(type) ?? 0) + by);
    if (type in this.totals) this.totals[type] += by;
    this.#prune(at);
  }

  /** Events of one type per minute, averaged over the window. */
  rate(type, now = Date.now()) {
    const from = now - this.windowMs;
    let total = 0;
    for (const [start, bucket] of this.buckets) {
      if (start + this.bucketMs <= from) continue;
      total += bucket.get(type) ?? 0;
    }
    return (total / this.windowMs) * 60_000;
  }

  count(type, now = Date.now()) {
    return this.rate(type, now) * (this.windowMs / 60_000);
  }

  /** Per-bucket series for the demo chart; oldest first. */
  series(type, { now = Date.now(), buckets = 20 } = {}) {
    const out = [];
    const lastStart = Math.floor(now / this.bucketMs) * this.bucketMs;
    for (let i = buckets - 1; i >= 0; i--) {
      const start = lastStart - i * this.bucketMs;
      out.push({ at: start, count: this.buckets.get(start)?.get(type) ?? 0 });
    }
    return out;
  }

  snapshot(now = Date.now()) {
    return {
      windowMs: this.windowMs,
      bucketMs: this.bucketMs,
      totals: { ...this.totals },
      rates: Object.fromEntries(EVENT_TYPES.map((type) => [type, this.rate(type, now)])),
      series: Object.fromEntries(EVENT_TYPES.map((type) => [type, this.series(type, { now })])),
    };
  }

  #prune(now) {
    const horizon = now - this.windowMs * 2;
    for (const start of this.buckets.keys()) {
      if (start < horizon) this.buckets.delete(start);
    }
  }
}
