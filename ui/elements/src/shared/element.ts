/**
 * The element ↔ page contract shared by every element:
 *
 * - Inputs are `data-*` attributes, observed with `attributeChangedCallback`.
 *   `data-src` is the payload URL on the UI's own `/data/` routes; changing
 *   it aborts the in-flight request and loads again.
 * - Output is the string `value` property, announced with a plain `change`
 *   event (not a `CustomEvent`: Topcoat's runtime wraps events and only
 *   `e.target.value` survives).
 * - Loading, error and empty states are shown in the element, never as a
 *   blank canvas. Subclasses free WebGL contexts in `unmount`.
 */

import { describeLoadError, type LoadError } from './fetch.ts';
import type { Result } from './result.ts';
import { readTheme, type Theme, watchColorScheme } from './theme.ts';

const BASE_CSS = `
:host {
  display: block;
  position: relative;
  box-sizing: border-box;
  color: inherit;
  font-size: 12px;
  line-height: 1.4;
  contain: content;
}
:host([hidden]) { display: none; }
*, *::before, *::after { box-sizing: border-box; }
.frame { position: absolute; inset: 0; }
.stage { position: absolute; inset: 0; }
.status {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
  text-align: center;
  pointer-events: none;
  color: var(--ct-muted);
  font-size: 12px;
}
.status[hidden] { display: none; }
.status[data-kind='loading'] { background: var(--ct-veil); }
.status[data-kind='error'] { pointer-events: auto; }
.status .panel {
  max-width: 36em;
  border-radius: 4px;
  padding: 8px 12px;
  white-space: pre-line;
}
.status[data-kind='error'] .panel {
  border: 1px solid #fca5a5;
  background: #fef2f2;
  color: #991b1b;
  text-align: left;
}
.status[data-kind='empty'] .panel { border: 1px dashed var(--ct-faint); }
@media (prefers-color-scheme: dark) {
  .status[data-kind='error'] .panel { border-color: #7f1d1d; background: #450a0a; color: #fecaca; }
}
.tooltip {
  position: absolute;
  z-index: 3;
  max-width: 320px;
  padding: 6px 8px;
  border-radius: 4px;
  border: 1px solid var(--ct-faint);
  background: var(--ct-surface);
  color: var(--ct-text);
  box-shadow: 0 2px 8px rgb(0 0 0 / 0.15);
  pointer-events: none;
  font-size: 11px;
  line-height: 1.45;
  white-space: nowrap;
}
.tooltip[hidden] { display: none; }
.tooltip .title { font-weight: 600; }
.tooltip .dim { color: var(--ct-muted); }
.tooltip .claim {
  display: inline-block;
  margin-top: 2px;
  padding: 0 4px;
  border: 1px dashed var(--ct-muted);
  border-radius: 3px;
}
.num { font-variant-numeric: tabular-nums; }
`;

export type StatusKind = 'loading' | 'error' | 'empty';

export abstract class PayloadElement<T> extends HTMLElement {
  protected readonly shadow: ShadowRoot;
  /** Where the subclass draws. */
  protected readonly stage: HTMLDivElement;
  /** Holds the stage, status and any overlays; carries the theme variables. */
  protected readonly frame: HTMLDivElement;
  readonly #status: HTMLDivElement;
  #controller: AbortController | null = null;
  #unwatch: (() => void) | null = null;
  #value = '';
  #theme: Theme | null = null;

  constructor(styles: string) {
    super();
    this.shadow = this.attachShadow({ mode: 'open' });
    const style = document.createElement('style');
    style.textContent = BASE_CSS + styles;
    this.stage = document.createElement('div');
    this.stage.className = 'stage';
    this.stage.part.add('stage');
    this.#status = document.createElement('div');
    this.#status.className = 'status';
    this.#status.part.add('status');
    this.#status.hidden = true;
    this.frame = document.createElement('div');
    this.frame.className = 'frame';
    this.frame.append(this.stage, this.#status);
    this.shadow.append(style, this.frame);
  }

  /** The current selection, in the grammar of `shared/selection.ts`. */
  get value(): string {
    return this.#value;
  }

  /** Sets the selection without announcing it, like an input's `value`. */
  set value(next: string) {
    this.#value = String(next);
    this.valueChanged(this.#value);
  }

  /** The theme read at the last mount or colour-scheme change. */
  protected get theme(): Theme {
    if (this.#theme === null) this.#theme = this.#readTheme();
    return this.#theme;
  }

  /** Sets `value` and fires `change`, if it differs. */
  protected select(next: string): void {
    if (next === this.#value) return;
    this.#value = next;
    this.valueChanged(next);
    this.dispatchEvent(new Event('change'));
  }

  connectedCallback(): void {
    this.#unwatch = watchColorScheme(() => {
      this.#theme = this.#readTheme();
      this.themeChanged();
    });
    void this.#reload();
  }

  disconnectedCallback(): void {
    this.#controller?.abort();
    this.#controller = null;
    this.#unwatch?.();
    this.#unwatch = null;
    this.unmount();
  }

  attributeChangedCallback(name: string, previous: string | null, next: string | null): void {
    if (!this.isConnected || previous === next) return;
    if (name === 'data-src') {
      void this.#reload();
    } else {
      this.inputChanged(name);
    }
  }

  /** Fetches and validates the payload at `url`. */
  protected abstract load(url: string, signal: AbortSignal): Promise<Result<T, LoadError>>;
  /** The message for an empty payload, or `null` when it has something to draw. */
  protected abstract emptyMessage(payload: T): string | null;
  /** Draws a payload, replacing anything drawn before. */
  protected abstract mount(payload: T): void;
  /** Frees whatever `mount` created (WebGL contexts, listeners). */
  protected abstract unmount(): void;
  /** A `data-*` input other than `data-src` changed. */
  protected abstract inputChanged(name: string): void;
  /** The page set `value`. */
  protected valueChanged(_value: string): void {}
  /** The colour scheme changed; `theme` is already updated. */
  protected abstract themeChanged(): void;

  protected showStatus(kind: StatusKind, message: string): void {
    const panel = document.createElement('div');
    panel.className = 'panel';
    panel.textContent = message;
    this.#status.replaceChildren(panel);
    this.#status.dataset.kind = kind;
    this.#status.setAttribute('role', kind === 'error' ? 'alert' : 'status');
    this.#status.hidden = false;
  }

  protected hideStatus(): void {
    this.#status.hidden = true;
    this.#status.replaceChildren();
  }

  #readTheme(): Theme {
    const theme = readTheme(this);
    const css = (c: readonly number[]) =>
      `rgba(${c.slice(0, 3).map(Math.round).join(', ')}, ${c[3]})`;
    // On the shadow frame, not the host: the page owns the host's attributes.
    const vars = this.frame.style;
    vars.setProperty('--ct-text', css(theme.text));
    vars.setProperty('--ct-muted', css(theme.muted));
    vars.setProperty('--ct-faint', css(theme.faint));
    vars.setProperty('--ct-surface', css(theme.surface));
    vars.setProperty('--ct-veil', css([...theme.surface.slice(0, 3), 0.6]));
    vars.setProperty('font-family', theme.font);
    return theme;
  }

  async #reload(): Promise<void> {
    this.#controller?.abort();
    const src = this.dataset.src?.trim() ?? '';
    if (src === '') {
      this.#controller = null;
      this.unmount();
      this.showStatus('empty', 'No data source.');
      return;
    }
    const controller = new AbortController();
    this.#controller = controller;
    this.#theme = this.#readTheme();
    this.showStatus('loading', 'Loading…');
    const result = await this.load(src, controller.signal);
    if (this.#controller !== controller) return;
    this.#controller = null;
    if (!result.ok) {
      if (result.error.kind === 'aborted') return;
      this.unmount();
      this.showStatus('error', describeLoadError(result.error));
      return;
    }
    const empty = this.emptyMessage(result.value);
    if (empty !== null) {
      this.unmount();
      this.showStatus('empty', empty);
      return;
    }
    this.hideStatus();
    try {
      this.mount(result.value);
    } catch (error) {
      // WebGL libraries throw plain Errors (no context, lost context).
      if (!(error instanceof Error)) throw error;
      this.unmount();
      this.showStatus('error', `Could not draw: ${error.message}`);
    }
  }
}

/** Defines `name` once, so loading a bundle twice is harmless. */
export function define(name: string, element: CustomElementConstructor): void {
  if (customElements.get(name) === undefined) customElements.define(name, element);
}
