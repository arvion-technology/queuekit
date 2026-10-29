// QueueKit demo. The only thing simulated here is work arriving and handlers
// failing; ordering, retries, concurrency, leases and the DLQ are the real
// modules under ../src. Rendering is intentionally dumb: rebuild the handful of
// panels each frame rather than diffing the DOM.
import { FleetSimulation } from "../src/simulator.js";

const $ = A.$, $$ = A.$$, el = A.el, fmt = A.fmt;

const sim = new FleetSimulation({
  seed: 11,
  workers: 5,
  concurrency: 4,
  chaos: 0.3,
  warmupSteps: 2400,
});
for (let i = 0; i < 30; i++) sim.step(); // give the depth chart a first line

const state = { view: "fleet", chaos: true, paused: false };
let frame = 0;

/* ------------------------------------------------------------- formatting */

const shortClock = (at) => new Date(at).toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
const elapsed = (ms) => {
  if (ms < 300) return "now";
  if (ms < 1_000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1_000)}s`;
};
const title = (name) => (name || ", ").replace(/_/g, " ");

function stateBadge(value) {
  return el("span", { class: `badge state-badge ${value}`, text: value });
}

function statCard({ label, value, sub, tone = "gold", spark }) {
  const sparkColor = tone === "red" ? "#f87171" : tone === "blue" ? "#3a7bff" : "#f5c26b";
  return el("div", { class: "card" },
    el("div", { class: "stat" },
      el("div", { class: "stat-label", text: label }),
      el("div", { class: "row", style: "align-items:baseline;gap:10px;margin:2px 0 3px" },
        el("div", { class: `stat-value ${tone}`, text: value }),
        spark && spark.length > 1 ? A.sparkline(spark, { width: 74, height: 24, color: sparkColor }) : null),
      el("div", { class: "small muted", text: sub })));
}

/* ------------------------------------------------------------------ fleet */

function renderKpis(snap) {
  const queuedSpark = snap.series.slice(-24).map((point) => point.queued);
  const throughput = Math.round(snap.rates.throughputPerMinute);
  $("#kpis").replaceChildren(
    statCard({
      label: "Queued",
      value: fmt.num(snap.depth.total),
      sub: `${snap.depth.available} ready · ${snap.depth.delayed} waiting on retry`,
      spark: queuedSpark,
    }),
    statCard({
      label: "Running",
      value: `${snap.counts.running} / ${snap.fleet.workers}`,
      sub: `${snap.fleet.idle} idle worker${snap.fleet.idle === 1 ? "" : "s"}`,
      tone: "blue",
    }),
    statCard({
      label: "Throughput",
      value: `${fmt.num(throughput)}/min`,
      sub: `${fmt.num(snap.completedTotal)} completed this run`,
    }),
    statCard({
      label: "Dead letters",
      value: fmt.num(snap.counts.dead),
      sub: `${fmt.num(snap.counters.retried)} retries · ${(snap.rates.retry * 100).toFixed(0)}% retry rate`,
      tone: "red",
    }),
  );
}

function renderDepthChart(snap) {
  const series = snap.series.slice(-90);
  if (series.length < 2) return;
  $("#depth-chart").replaceChildren(A.lineChart({
    series: [{ name: "Queued", data: series.map((point) => point.queued), color: "#f5c26b" }],
    labels: series.map((point) => shortClock(point.at).slice(3)),
    width: 760,
    height: 116,
  }));
}

function renderPolicy(snap) {
  const schedule = snap.policy.schedule;
  $("#policy-badge").textContent = `max ${snap.policy.maxAttempts}`;
  const maxDelay = Math.max(...schedule.map((row) => row.delayMs), 1);
  $("#policy-schedule").replaceChildren(...schedule.map((row) =>
    el("div", { class: "policy-row" },
      el("div", { class: "row", style: "font-size:12px" },
        el("span", { class: "muted", text: `Attempt ${row.attempt}` }),
        el("span", { class: "spacer" }),
        el("span", { class: "mono", text: `+${elapsed(row.delayMs)}` })),
      el("div", { class: `progress thin ${row.attempt === schedule.length ? "danger" : ""}`, style: "margin-top:5px" },
        el("i", { style: `width:${(row.delayMs / maxDelay) * 100}%` })))));
  const limiter = snap.rateLimit;
  if (limiter) {
    $("#rate-limit-label").textContent = `${limiter.tokens.toFixed(1)} / ${limiter.limit} tokens · refill ${limiter.intervalMs / 1000}s`;
    $("#rate-limit-bar").style.width = `${Math.max(4, (limiter.tokens / limiter.limit) * 100)}%`;
  }
  $("#concurrency-label").textContent = "4 concurrent";
  $("#lease-label").textContent = "15s lease";
}

function renderWorkers(snap) {
  $("#worker-summary").textContent = `${snap.fleet.busy} busy · ${snap.fleet.processed} completed · ${snap.fleet.failed} failed`;
  $("#worker-count").textContent = String(snap.fleet.workers);
  $("#worker-list").replaceChildren(...snap.fleet.detail.map((worker) =>
    el("div", { class: `worker-row ${worker.state}` },
      el("div", { class: "worker-avatar", text: worker.id.replace("worker-", "W") }),
      el("div", { style: "min-width:0" },
        el("div", { class: "worker-job", text: worker.state === "busy" ? title(worker.jobName) : "Waiting for work" }),
        el("div", { class: "worker-meta" },
          el("span", { text: worker.state === "busy" ? (worker.busyForMs < 300 ? "finishing" : `${elapsed(worker.busyForMs)} left`) : "idle" }),
          el("span", { class: "mono", text: `${worker.processed} ok · ${worker.failed} failed` }))),
      el("span", { class: `dot ${worker.state === "busy" ? "pulse" : ""}`, style: `color:${worker.state === "busy" ? "var(--gold)" : "var(--muted)"}`, title: worker.state }))));
}

function renderJobs(snap) {
  $("#arrivals-badge").textContent = `${fmt.num(snap.counters.enqueued)} enqueued`;
  $("#jobs-table").replaceChildren(A.dataTable({
    columns: [
      {
        label: "Job",
        render: (job) => el("span", {},
          el("span", { style: "font-weight:600;font-size:12.5px", text: title(job.name) }),
          el("span", { class: "small muted", text: ` · ${job.queue}` })),
      },
      { label: "State", width: "92px", render: (job) => stateBadge(job.state) },
      {
        label: "Attempts", width: "84px", align: "right",
        render: (job) => el("span", { class: "mono", style: "font-size:11.5px", text: `${job.attempts + 1}/${job.maxAttempts}` }),
      },
      {
        label: "Age", width: "74px", align: "right",
        render: (job) => el("span", { class: "mono muted", style: "font-size:11.5px", text: elapsed(snap.now - job.createdAt) }),
      },
    ],
    rows: snap.jobs.slice(0, 5),
    onRowClick: (job) => A.modal({
      title: `Job ${job.id.slice(-8)}`,
      body: el("div", { class: "col", style: "gap:12px" },
        el("div", { class: "row wrap" },
          stateBadge(job.state),
          el("span", { class: "badge", text: `${job.attempts}/${job.maxAttempts} attempts` }),
          el("span", { class: "badge", text: job.queue }),
          el("span", { class: "badge mono", text: job.id })),
        job.lastError
          ? el("div", { class: "dlq-note" }, el("span", { class: "badge red", text: "last error" }), el("p", { text: job.lastError }))
          : null,
        el("pre", { class: "code", text: JSON.stringify({ payload: job.payload, result: job.result }, null, 2) })),
      actions: [{ label: "Close" }],
    }),
  }));
}

function eventRow(event) {
  let meta = "";
  if (event.type === "claimed" || event.type === "completed") meta = `${title(event.name)} · ${event.queue}`;
  else if (event.type === "retried") meta = `${title(event.name)} · retried, next in +${Math.round(event.delayMs)}ms`;
  else if (event.type === "dead") meta = `${title(event.name)} · ${event.error}`;
  else if (event.type === "recurring") meta = `${title(event.name)} · scheduler`;
  else if (event.type === "replay") meta = `${event.count} replayed`;
  else if (event.type === "burst") meta = `${event.count} jobs enqueued`;
  else if (event.type === "chaos") meta = `chaos ${(event.value * 100).toFixed(0)}%`;
  return el("div", { class: "event-row" },
    el("span", { class: "event-time mono", text: shortClock(event.at) }),
    el("span", { class: `event-type ${event.type}`, text: event.type }),
    el("span", { class: "worker-job", text: meta }),
    el("span", { class: "mono muted", style: "font-size:10px", text: event.type === "retried" ? `t+${Math.round(event.delayMs)}ms` : "" }));
}

function renderEvents(snap) {
  const rows = snap.events.slice(0, 8).map((event) => eventRow(event));
  const empty = el("div", { class: "small muted", text: "Waiting for events…" });
  $("#event-log").replaceChildren(...(rows.length ? rows : [empty]));
  $("#dlq-event-log").replaceChildren(...(rows.length ? rows : [el("div", { class: "small muted", text: "Waiting for events…" })]));
}

function renderRecurring(snap) {
  $("#recurring-badge").textContent = `${snap.scheduler.length} definitions`;
  $("#recurring-list").replaceChildren(...snap.scheduler.map((def) =>
    el("div", { class: "recurring-row" },
      el("span", { class: "badge", text: def.kind === "cron" ? "cron" : "interval" }),
      el("div", { style: "min-width:0" },
        el("div", { class: "worker-job", text: title(def.name) }),
        el("div", { class: "worker-meta" },
          el("span", { class: "mono", text: def.expr || `every ${elapsed(def.everyMs)}` }),
          el("span", { text: `${def.enqueued} fired` }))),
      el("span", { class: "small muted mono", style: "margin-left:auto", text: `in ${elapsed(def.dueInMs)}` }))));
}

/* -------------------------------------------------------------------- dlq */

function renderDlq(snap) {
  const dead = snap.dead;
  $("#dlq-tab-count").textContent = String(snap.counts.dead);
  $("#dlq-count-badge").textContent = `${dead.length} parked`;
  const attempts = dead.reduce((sum, job) => sum + job.attempts, 0);
  $("#dlq-summary").replaceChildren(
    statCard({ label: "Parked jobs", value: fmt.num(dead.length), sub: "waiting for an operator", tone: "red" }),
    statCard({ label: "Failed attempts", value: fmt.num(attempts), sub: "spent before being parked" }),
    statCard({
      label: "Oldest",
      value: dead.length ? elapsed(snap.now - dead[dead.length - 1].createdAt) : ", ",
      sub: "age of the oldest parked job",
      tone: "blue",
    }),
  );

  $("#dlq-table").replaceChildren(A.dataTable({
    columns: [
      {
        label: "Job",
        render: (job) => el("span", {},
          el("span", { style: "font-weight:600;font-size:12.5px", text: title(job.name) }),
          el("span", { class: "small muted", text: ` · ${job.queue}` })),
      },
      { label: "Error", render: (job) => el("span", { class: "small", style: "color:var(--red)", text: job.lastError || ", " }) },
      { label: "Attempts", width: "84px", align: "right", render: (job) => el("span", { class: "mono", text: `${job.attempts}/${job.maxAttempts}` }) },
      { label: "Age", width: "70px", align: "right", render: (job) => el("span", { class: "mono muted", text: elapsed(snap.now - job.createdAt) }) },
      {
        label: "", width: "82px", align: "right",
        render: (job) => el("span", { class: "replay-cell" },
          el("button", {
            class: "btn sm", text: "Replay",
            onclick: (event) => {
              event.stopPropagation();
              const result = sim.replayDead([job.id]);
              A.toast(`Replayed ${result.replayed} job`, "success", 1800);
              render();
            },
          })),
      },
    ],
    rows: dead.slice(0, 12),
    empty: "No dead letters right now. Turn on chaos mode or wait for a poison message.",
    onRowClick: (job) => A.modal({
      title: `Dead letter ${job.id.slice(-8)}`,
      body: el("div", { class: "col", style: "gap:12px" },
        el("div", { class: "row wrap" },
          el("span", { class: "badge red", text: `${job.attempts} attempts` }),
          el("span", { class: "badge", text: job.queue }),
          el("span", { class: "badge mono", text: job.id })),
        el("pre", { class: "code", text: JSON.stringify({ lastError: job.lastError, payload: job.payload }, null, 2) })),
      actions: [{ label: "Close" }],
    }),
  }));
}

/* ------------------------------------------------------------------- loop */

function render() {
  const snap = sim.snapshot();
  $("#run-clock .mono").textContent = elapsed(snap.elapsedMs);
  $("#pause-btn").textContent = state.paused ? "Resume" : "Pause";
  $("#chaos-switch").setAttribute("aria-checked", String(state.chaos));
  $("#dlq-tab-count").textContent = String(snap.counts.dead);

  if (state.view === "fleet") {
    renderKpis(snap);
    renderPolicy(snap);
    renderWorkers(snap);
    renderJobs(snap);
    renderEvents(snap);
    renderRecurring(snap);
    $("#depth-now").textContent = `${snap.depth.total} queued`;
    if (frame % 3 === 0) renderDepthChart(snap);
  } else {
    renderDlq(snap);
    renderEvents(snap);
  }
}

function tick() {
  if (!state.paused) sim.step();
  frame++;
  render();
}

/* ---------------------------------------------------------------- routing */

function route() {
  const requested = (location.hash || "#fleet").slice(1);
  state.view = requested === "dlq" ? "dlq" : "fleet";
  $$("[data-view-panel]").forEach((panel) => { panel.hidden = panel.dataset.viewPanel !== state.view; });
  $$("[data-view]").forEach((link) => link.classList.toggle("active", link.dataset.view === state.view));
  render();
  window.scrollTo({ top: 0 });
}

/* --------------------------------------------------------------- controls */

function wire() {
  $("#chaos-switch").addEventListener("click", () => {
    state.chaos = !state.chaos;
    sim.setChaos(state.chaos ? 0.3 : 0);
    A.toast(state.chaos ? "Chaos on: 30% of handler runs fail" : "Chaos off: handlers succeed", state.chaos ? "info" : "success", 2000);
    render();
  });

  $("#pause-btn").addEventListener("click", () => {
    state.paused = !state.paused;
    sim.paused = state.paused;
    render();
  });

  $("#workers-up").addEventListener("click", () => { sim.setWorkers(Math.min(8, sim.pool.size + 1)); render(); });
  $("#workers-down").addEventListener("click", () => { sim.setWorkers(Math.max(1, sim.pool.size - 1)); render(); });

  $("#burst-btn").addEventListener("click", () => {
    sim.enqueueBurst(12);
    A.toast("Enqueued 12 jobs across the queues", "success", 1800);
    render();
  });

  const replay = () => {
    if (!sim.snapshot().counts.dead) { A.toast("Dead-letter queue is empty", "info", 1800); return; }
    const result = sim.replayDead();
    A.toast(`Replayed ${result.replayed} job${result.replayed === 1 ? "" : "s"} back to queued`, "success");
    render();
  };
  $("#replay-btn").addEventListener("click", replay);
  $("#replay-all-btn").addEventListener("click", replay);

  window.addEventListener("hashchange", route);
}

wire();
route();
setInterval(tick, 110);
