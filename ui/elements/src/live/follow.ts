/**
 * Pacing the refreshes of a page that follows the present.
 *
 * A followed page (`/` and `/topology` with `follow=<span>`; the server
 * marks it with `[data-live-follow]`) resolves its window again on every
 * render, so it must re-render even when the feed is quiet: the pacer
 * asks for a refresh every `FOLLOW_INTERVAL_MS` while the page follows and
 * the tab is visible. While the tab is hidden it neither ticks nor admits
 * the feed's refreshes, and becoming visible again asks for one refresh,
 * which catches up on whatever was skipped.
 *
 * A pinned page is never paced: no timer, and every feed refresh is
 * admitted whatever the visibility, exactly as before follow mode.
 */

/** How often a followed, visible page refreshes when nothing else asks. */
export const FOLLOW_INTERVAL_MS = 30_000;

export class FollowPacer {
  readonly #refresh: () => void;
  readonly #intervalMs: number;
  #following = false;
  #visible: boolean;
  #timer: ReturnType<typeof setInterval> | null = null;

  constructor(refresh: () => void, visible: boolean, intervalMs = FOLLOW_INTERVAL_MS) {
    this.#refresh = refresh;
    this.#visible = visible;
    this.#intervalMs = intervalMs;
  }

  /** Whether the page currently follows. */
  get following(): boolean {
    return this.#following;
  }

  /** What the page now declares: following or pinned. */
  follow(following: boolean): void {
    if (following === this.#following) return;
    this.#following = following;
    this.#arm();
  }

  /** The tab became visible or hidden. */
  visibility(visible: boolean): void {
    if (visible === this.#visible) return;
    this.#visible = visible;
    this.#arm();
    if (visible && this.#following) this.#refresh();
  }

  /** Whether a refresh the feed asks for may run now. */
  admits(): boolean {
    return !this.#following || this.#visible;
  }

  /** Stops ticking and forgets the page (the stream closed). */
  stop(): void {
    this.#following = false;
    this.#arm();
  }

  /** Runs the timer exactly while following a visible page, from now. */
  #arm(): void {
    if (this.#timer !== null) clearInterval(this.#timer);
    this.#timer = null;
    if (this.#following && this.#visible) {
      this.#timer = setInterval(() => this.#refresh(), this.#intervalMs);
    }
  }
}
