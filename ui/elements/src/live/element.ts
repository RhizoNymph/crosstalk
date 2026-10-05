/**
 * `<ct-live>`: keeps a page current with the live feed.
 *
 * - Input: `data-src`, the feed URL (`/data/live`). Changing it reconnects.
 * - Output: `value`, the id of the last feed event received (a
 *   `LiveCursor`), announced with `change`.
 * - Opens an `EventSource` (which resumes with `Last-Event-ID` after a
 *   drop). On an entity event that a `[data-live-watch]` token of the page
 *   matches, or on `resync`, it refreshes the page's live regions
 *   (`refresh.ts`), coalescing bursts. When a refresh is not possible it
 *   shows a notice with a reload button instead.
 * - Closes the stream on `pagehide` and, when the page comes back from the
 *   back/forward cache, opens a new one and refreshes (`lifecycle.ts`).
 */

import { lifecycleStep } from './lifecycle.ts';
import { type RefreshOutcome, refreshRegions } from './refresh.ts';
import { refreshDelay } from './throttle.ts';
import { isLiveKind, LIVE_KINDS, parseNotice, parseWatch, watches } from './watch.ts';

/** How long a burst of events is gathered before one refresh. */
const SETTLE_MS = 250;
/** The least time between two refreshes of the page. */
const MIN_INTERVAL_MS = 4000;

const STYLE = `
:host { display: block; }
:host([hidden]) { display: none; }
.notice {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
  padding: 6px 10px;
  border: 1px solid #fcd34d;
  border-radius: 4px;
  background: #fffbeb;
  color: #78350f;
  font-size: 13px;
}
.notice[hidden] { display: none; }
button {
  font: inherit;
  padding: 2px 8px;
  border: 1px solid currentColor;
  border-radius: 3px;
  background: transparent;
  color: inherit;
  cursor: pointer;
}
@media (prefers-color-scheme: dark) {
  .notice { border-color: #92400e; background: #451a03; color: #fde68a; }
}
`;

/** Every watch token the document declares now. */
function declaredTokens(): ReturnType<typeof parseWatch> {
  return [...document.querySelectorAll('[data-live-watch]')].flatMap((marker) =>
    parseWatch(marker.getAttribute('data-live-watch') ?? ''),
  );
}

export class LiveElement extends HTMLElement {
  static readonly observedAttributes = ['data-src'];

  readonly #notice: HTMLDivElement;
  readonly #message: HTMLSpanElement;
  #source: EventSource | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #refresh: AbortController | null = null;
  #lastRefreshAt: number | null = null;
  #value = '';
  readonly #lifecycle = (event: Event): void => {
    const persisted = event instanceof PageTransitionEvent && event.persisted;
    switch (lifecycleStep(event.type, persisted)) {
      case 'close':
        this.#disconnect();
        break;
      case 'reopen':
        this.#connect();
        // Events sent while the page sat in the cache were missed.
        if (declaredTokens().length > 0) this.#schedule();
        break;
      case 'none':
        break;
    }
  };

  constructor() {
    super();
    const shadow = this.attachShadow({ mode: 'open' });
    const style = document.createElement('style');
    style.textContent = STYLE;
    this.#notice = document.createElement('div');
    this.#notice.className = 'notice';
    this.#notice.setAttribute('role', 'status');
    this.#notice.hidden = true;
    this.#message = document.createElement('span');
    const reload = document.createElement('button');
    reload.type = 'button';
    reload.textContent = 'Reload';
    reload.addEventListener('click', () => location.reload());
    this.#notice.append(this.#message, reload);
    shadow.append(style, this.#notice);
  }

  /** The id of the last feed event received. */
  get value(): string {
    return this.#value;
  }

  set value(next: string) {
    this.#value = String(next);
  }

  connectedCallback(): void {
    window.addEventListener('pagehide', this.#lifecycle);
    window.addEventListener('pageshow', this.#lifecycle);
    this.#connect();
  }

  disconnectedCallback(): void {
    window.removeEventListener('pagehide', this.#lifecycle);
    window.removeEventListener('pageshow', this.#lifecycle);
    this.#disconnect();
  }

  attributeChangedCallback(_name: string, previous: string | null, next: string | null): void {
    if (this.isConnected && previous !== next) this.#connect();
  }

  #disconnect(): void {
    this.#source?.close();
    this.#source = null;
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    this.#refresh?.abort();
    this.#refresh = null;
  }

  #connect(): void {
    this.#disconnect();
    const src = this.dataset.src?.trim() ?? '';
    if (src === '') return;
    const source = new EventSource(src);
    this.#source = source;
    for (const kind of LIVE_KINDS) {
      source.addEventListener(kind, (event) => this.#entity(kind, event));
    }
    source.addEventListener('resync', (event) => {
      this.#received(event);
      if (declaredTokens().length > 0) this.#schedule();
    });
    source.addEventListener('heartbeat', (event) => this.#received(event));
    source.addEventListener('error', () => {
      if (source.readyState === EventSource.CLOSED) {
        this.#show('Live updates stopped. Reload to see the latest data.');
      }
    });
  }

  #received(event: Event): void {
    if (!(event instanceof MessageEvent) || event.lastEventId === '') return;
    if (event.lastEventId === this.#value) return;
    this.#value = event.lastEventId;
    this.dispatchEvent(new Event('change'));
  }

  #entity(kind: string, event: Event): void {
    this.#received(event);
    if (!(event instanceof MessageEvent) || !isLiveKind(kind)) return;
    const notice = parseNotice(kind, String(event.data));
    if (!notice.ok) {
      console.warn(`ct-live: ignoring a ${kind} event: ${notice.error}`);
      return;
    }
    if (watches(declaredTokens(), notice.value)) this.#schedule();
  }

  #schedule(): void {
    if (this.#timer !== null) return;
    const delay = refreshDelay(Date.now(), this.#lastRefreshAt, SETTLE_MS, MIN_INTERVAL_MS);
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#lastRefreshAt = Date.now();
      void this.#run();
    }, delay);
  }

  async #run(): Promise<void> {
    this.#refresh?.abort();
    const controller = new AbortController();
    this.#refresh = controller;
    const outcome: RefreshOutcome = await refreshRegions(controller.signal);
    if (this.#refresh !== controller) return;
    this.#refresh = null;
    if (outcome.kind === 'needs-reload') {
      this.#show(`This page's data changed (${outcome.why}).`);
    } else if (outcome.kind === 'refreshed') {
      this.#notice.hidden = true;
    }
  }

  #show(message: string): void {
    this.#message.textContent = message;
    this.#notice.hidden = false;
  }
}
