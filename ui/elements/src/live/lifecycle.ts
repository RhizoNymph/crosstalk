/**
 * What the page lifecycle asks of `<ct-live>`'s feed connection.
 *
 * A page leaving for the back/forward cache keeps running its open
 * connections unless it closes them, and browsers allow only a few
 * connections per host (six over HTTP/1.1). Every page holds one feed
 * stream, so a handful of cached pages would starve the page in front of
 * them: its payload fetches and even the next navigation would queue until a
 * cached page is evicted. So the stream closes on `pagehide`, and a page
 * restored from the cache (`pageshow` with `persisted`) opens a new one and
 * refreshes, since it missed the events of its time in the cache.
 */

/** `close` the stream, `reopen` it and refresh, or leave it (`none`). */
export type LifecycleStep = 'close' | 'reopen' | 'none';

/** The step a `pagehide` or `pageshow` event of the window asks for. */
export function lifecycleStep(type: string, persisted: boolean): LifecycleStep {
  if (type === 'pagehide') return 'close';
  if (type === 'pageshow' && persisted) return 'reopen';
  return 'none';
}
