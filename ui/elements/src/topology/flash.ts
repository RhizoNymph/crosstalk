/**
 * Flashing new traffic. Pure: no DOM, no WebGL.
 *
 * A live refetch compares each drawn edge's count (transmissions, or
 * accesses for an access edge) with the previous payload's; an edge whose
 * count rose, or that is new, pulses for `FLASH_MS`. `pulse` is the
 * envelope: a quick rise, then an ease-out back to nothing.
 */

import type { GraphEdge } from './model.ts';

export const FLASH_MS = 1200;
/** The share of the pulse spent rising. */
const RISE = 0.15;

/** Each edge's traffic count, by edge key. */
export function edgeCounts(edges: readonly GraphEdge[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const edge of edges) {
    const count =
      edge.kind === 'transmission'
        ? edge.members.reduce((sum, m) => sum + m.transmissions, 0)
        : edge.members.reduce((sum, m) => sum + m.accesses, 0);
    counts.set(edge.key, count);
  }
  return counts;
}

/** The edges of `next` whose count is higher than in `previous` (new edges included). */
export function risenEdges(
  previous: ReadonlyMap<string, number>,
  next: ReadonlyMap<string, number>,
): Set<string> {
  const risen = new Set<string>();
  for (const [key, count] of next) {
    if (count > (previous.get(key) ?? 0)) risen.add(key);
  }
  return risen;
}

/** The pulse's strength in [0, 1], `elapsed` ms after it started. */
export function pulse(elapsed: number, duration = FLASH_MS): number {
  if (!(elapsed >= 0) || elapsed >= duration) return 0;
  const t = elapsed / duration;
  if (t < RISE) return t / RISE;
  const fall = (t - RISE) / (1 - RISE);
  return (1 - fall) * (1 - fall);
}

/** Pulses in flight: key → start time (ms). */
export type Flashes = Map<string, number>;

/** Drops finished pulses; true while any is still running at `now`. */
export function prune(flashes: Flashes, now: number, duration = FLASH_MS): boolean {
  for (const [key, start] of flashes) {
    if (now - start >= duration) flashes.delete(key);
  }
  return flashes.size > 0;
}

/** The strength of `key`'s pulse at `now`, 0 when it has none. */
export function strength(flashes: Flashes, key: string, now: number): number {
  const start = flashes.get(key);
  return start === undefined ? 0 : pulse(now - start);
}
