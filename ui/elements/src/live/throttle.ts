/**
 * Rate-limiting live refreshes: a page refreshes at most once per
 * `minIntervalMs`, however often the feed matches its watch tokens.
 */

/**
 * How long to wait before the next refresh: at least `settleMs` (to gather
 * a burst), and until `minIntervalMs` has passed since the last refresh
 * started (`lastStartedAt`, `null` for none).
 */
export function refreshDelay(
  now: number,
  lastStartedAt: number | null,
  settleMs: number,
  minIntervalMs: number,
): number {
  if (lastStartedAt === null) return settleMs;
  return Math.max(settleMs, lastStartedAt + minIntervalMs - now);
}
