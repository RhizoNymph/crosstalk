/**
 * Refetching an element's payload when the live feed's watermark advances.
 *
 * An element with a `data-live` input (the feed URL, `/data/live`) opens
 * its own `EventSource` and calls `tick` on `watermark` events, at most
 * once every `minIntervalMs`. The element then refetches its `data-src`
 * and merges the result into what it draws, instead of the page being
 * re-rendered. Like `<ct-live>`, the stream closes on `pagehide` and
 * reopens (with a tick, for the events it missed) when the page comes back
 * from the back/forward cache (`live/lifecycle.ts`).
 */

import { lifecycleStep } from '../live/lifecycle.ts';
import { refreshDelay } from '../live/throttle.ts';

const SETTLE_MS = 250;
export const LIVE_REFETCH_MS = 3500;

export class LiveRefetch {
  readonly #tick: () => void;
  readonly #minIntervalMs: number;
  #url = '';
  #source: EventSource | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #last: number | null = null;
  readonly #lifecycle = (event: Event): void => {
    const persisted = event instanceof PageTransitionEvent && event.persisted;
    switch (lifecycleStep(event.type, persisted)) {
      case 'close':
        this.#close();
        break;
      case 'reopen':
        this.#open();
        this.#schedule();
        break;
      case 'none':
        break;
    }
  };

  constructor(tick: () => void, minIntervalMs = LIVE_REFETCH_MS) {
    this.#tick = tick;
    this.#minIntervalMs = minIntervalMs;
  }

  /** Follows the feed at `url` (empty: none), replacing any earlier stream. */
  start(url: string): void {
    this.stop();
    this.#url = url.trim();
    if (this.#url === '') return;
    window.addEventListener('pagehide', this.#lifecycle);
    window.addEventListener('pageshow', this.#lifecycle);
    this.#open();
  }

  stop(): void {
    window.removeEventListener('pagehide', this.#lifecycle);
    window.removeEventListener('pageshow', this.#lifecycle);
    this.#close();
    this.#url = '';
  }

  #open(): void {
    this.#close();
    if (this.#url === '') return;
    const source = new EventSource(this.#url);
    this.#source = source;
    source.addEventListener('watermark', () => this.#schedule());
    source.addEventListener('resync', () => this.#schedule());
  }

  #close(): void {
    this.#source?.close();
    this.#source = null;
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
  }

  #schedule(): void {
    if (this.#timer !== null) return;
    const delay = refreshDelay(Date.now(), this.#last, SETTLE_MS, this.#minIntervalMs);
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#last = Date.now();
      this.#tick();
    }, delay);
  }
}
