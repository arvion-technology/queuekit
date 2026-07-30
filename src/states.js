// The job lifecycle is deliberately small: four states and five transitions.
// Every transition goes through `transition()` so an illegal move fails loudly
// in tests instead of quietly corrupting the dashboard.

export const QUEUED = "queued";
export const RUNNING = "running";
export const COMPLETED = "completed";
export const DEAD = "dead";

export const JOB_STATES = Object.freeze([QUEUED, RUNNING, COMPLETED, DEAD]);

// queued -> running          a worker claimed the job
// running -> completed       handler returned without throwing
// running -> queued          handler threw and the job still has attempts left
// running -> dead            handler threw and attempts are exhausted (DLQ)
// dead -> queued             manual replay from the dead-letter queue
const TRANSITIONS = Object.freeze({
  [QUEUED]: Object.freeze([RUNNING]),
  [RUNNING]: Object.freeze([COMPLETED, QUEUED, DEAD]),
  [COMPLETED]: Object.freeze([]),
  [DEAD]: Object.freeze([QUEUED]),
});

export function canTransition(from, to) {
  return Boolean(TRANSITIONS[from]?.includes(to));
}

export function isTerminal(state) {
  return state === COMPLETED || state === DEAD;
}

export function isClaimable(state) {
  return state === QUEUED;
}

export class InvalidTransitionError extends Error {
  constructor(from, to, jobId) {
    super(`illegal transition ${from} -> ${to}${jobId ? ` for job ${jobId}` : ""}`);
    this.name = "InvalidTransitionError";
    this.from = from;
    this.to = to;
    this.jobId = jobId;
  }
}

/**
 * Validate and return the next state. Throwing here is cheap and catches the
 * one bug class that makes queue dashboards lie: a job counted in two states.
 */
export function transition(from, to, jobId) {
  if (!canTransition(from, to)) throw new InvalidTransitionError(from, to, jobId);
  return to;
}

export function allowedFrom(from) {
  return TRANSITIONS[from] ?? [];
}
