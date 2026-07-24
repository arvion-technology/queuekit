// Small seeded PRNG (mulberry32). The demo uses it so a reload produces the
// same fleet run, and the backoff tests can reproduce a jitter sequence exactly.

export function mulberry32(seed) {
  let state = seed >>> 0;
  return function random() {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

/** Integer in [min, max], inclusive. */
export function intBetween(random, min, max) {
  return min + Math.floor(random() * (max - min + 1));
}

export function pick(random, items) {
  return items[Math.floor(random() * items.length)];
}

/** Weighted pick: `entries` is [[value, weight], ...]. */
export function weightedPick(random, entries) {
  const total = entries.reduce((sum, [, weight]) => sum + weight, 0);
  let roll = random() * total;
  for (const [value, weight] of entries) {
    roll -= weight;
    if (roll < 0) return value;
  }
  return entries[entries.length - 1][0];
}
