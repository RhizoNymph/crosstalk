/**
 * `<ct-timebrush>`: transmissions per bucket as bars, with a brush for the
 * view's time window. Plain SVG, no dependencies.
 *
 * Inputs: `data-src` (a `/data/timeline?…` URL), `data-from` and `data-to`
 * (RFC 3339; the current window, drawn as the brush). Output: `value` =
 * `<from>/<to>`, snapped to bucket edges, set on pointer-up.
 */

import { type TimelinePayload, timelinePayload } from '../payloads/timeline.ts';
import { mix, toCss, withAlpha } from '../shared/color.ts';
import { PayloadElement } from '../shared/element.ts';
import { type LoadError, loadJson } from '../shared/fetch.ts';
import { formatBytes, formatCount, formatUtc, formatUtcShort } from '../shared/format.ts';
import type { Result } from '../shared/result.ts';
import { encodeBrushSelection } from '../shared/selection.ts';
import {
  type Axis,
  axisOf,
  bars,
  bucketAt,
  type EdgeRange,
  nearestEdge,
  rangeTimes,
  shiftRange,
  snapDrag,
  ticks,
  toX,
  windowSpan,
} from './model.ts';

const SVG = 'http://www.w3.org/2000/svg';
const PAD_X = 8;
const TOP = 18;
const AXIS = 16;
const HANDLE_HIT = 6;

const STYLES = `
:host { height: 96px; }
svg { position: absolute; inset: 0; width: 100%; height: 100%; display: block; touch-action: none; user-select: none; }
svg text { font-size: 10px; fill: var(--ct-muted); }
svg .label { font-size: 11px; }
svg .strong { fill: var(--ct-text); }
`;

type Drag =
  | { readonly mode: 'new'; readonly startX: number }
  | { readonly mode: 'move'; readonly startX: number; readonly origin: EdgeRange }
  | { readonly mode: 'from' | 'to'; readonly origin: EdgeRange };

function el<K extends keyof SVGElementTagNameMap>(
  name: K,
  attributes: Record<string, string | number>,
): SVGElementTagNameMap[K] {
  const node = document.createElementNS(SVG, name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, String(value));
  return node;
}

export class TimebrushElement extends PayloadElement<TimelinePayload> {
  static observedAttributes = ['data-src', 'data-from', 'data-to'];

  #payload: TimelinePayload | null = null;
  #axis: Axis | null = null;
  #svg: SVGSVGElement | null = null;
  #resize: ResizeObserver | null = null;
  /** The brush while dragging, or after a selection until the page catches up. */
  #pending: EdgeRange | null = null;
  #drag: Drag | null = null;
  #hover: number | null = null;
  #size = { width: 0, height: 0 };

  constructor() {
    super(STYLES);
  }

  protected load(url: string, signal: AbortSignal): Promise<Result<TimelinePayload, LoadError>> {
    return loadJson(url, signal, timelinePayload);
  }

  protected emptyMessage(payload: TimelinePayload): string | null {
    return payload.buckets.length > 0 ? null : 'No buckets in this window.';
  }

  protected mount(payload: TimelinePayload): void {
    this.#teardown();
    this.#payload = payload;
    const svg = el('svg', { role: 'img', tabindex: 0 });
    svg.setAttribute('aria-label', 'Transmissions over time; drag to choose a window');
    this.stage.replaceChildren(svg);
    this.#svg = svg;
    svg.addEventListener('pointerdown', this.#onDown);
    svg.addEventListener('pointermove', this.#onMove);
    svg.addEventListener('pointerup', this.#onUp);
    svg.addEventListener('pointercancel', this.#onCancel);
    svg.addEventListener('pointerleave', this.#onLeave);
    this.#resize = new ResizeObserver(([entry]) => {
      if (entry === undefined) return;
      this.#size = { width: entry.contentRect.width, height: entry.contentRect.height };
      this.#render();
    });
    this.#resize.observe(this.stage);
    const box = this.stage.getBoundingClientRect();
    this.#size = { width: box.width, height: box.height };
    this.#render();
  }

  protected unmount(): void {
    this.#teardown();
    this.#payload = null;
  }

  protected inputChanged(): void {
    // The page caught up with (or overrode) our selection.
    this.#pending = null;
    this.#render();
  }

  protected themeChanged(): void {
    this.#render();
  }

  #teardown(): void {
    this.#resize?.disconnect();
    this.#resize = null;
    this.#svg = null;
    this.#drag = null;
    this.#pending = null;
    this.stage.replaceChildren();
  }

  #localX(event: PointerEvent): number {
    return event.clientX - (this.#svg?.getBoundingClientRect().left ?? 0);
  }

  /** The brush as drawn: pending, else the page's window. */
  #brushSpan(): [number, number] | null {
    const axis = this.#axis;
    if (axis === null) return null;
    if (this.#pending !== null) {
      return [
        toX(axis, axis.edgeMs[this.#pending.from] ?? 0),
        toX(axis, axis.edgeMs[this.#pending.to] ?? 0),
      ];
    }
    const from = this.dataset.from;
    const to = this.dataset.to;
    return from === undefined || to === undefined ? null : windowSpan(axis, from, to);
  }

  #currentRange(): EdgeRange | null {
    const axis = this.#axis;
    const span = this.#brushSpan();
    if (axis === null || span === null) return null;
    const from = nearestEdge(axis, span[0]);
    const to = nearestEdge(axis, span[1]);
    return to > from ? { from, to } : null;
  }

  readonly #onDown = (event: PointerEvent): void => {
    const axis = this.#axis;
    if (axis === null || event.button !== 0) return;
    const x = this.#localX(event);
    const span = this.#brushSpan();
    const range = this.#currentRange();
    if (span !== null && range !== null && Math.abs(x - span[0]) <= HANDLE_HIT) {
      this.#drag = { mode: 'from', origin: range };
    } else if (span !== null && range !== null && Math.abs(x - span[1]) <= HANDLE_HIT) {
      this.#drag = { mode: 'to', origin: range };
    } else if (span !== null && range !== null && x > span[0] && x < span[1]) {
      this.#drag = { mode: 'move', startX: x, origin: range };
    } else {
      this.#drag = { mode: 'new', startX: x };
      this.#pending = snapDrag(axis, x, x);
    }
    this.#svg?.setPointerCapture(event.pointerId);
    this.#render();
  };

  readonly #onMove = (event: PointerEvent): void => {
    const axis = this.#axis;
    if (axis === null) return;
    const x = this.#localX(event);
    const drag = this.#drag;
    if (drag === null) {
      const bucket = x >= axis.x0 && x <= axis.x1 ? bucketAt(axis, x) : null;
      const span = this.#brushSpan();
      const nearHandle =
        span !== null &&
        (Math.abs(x - span[0]) <= HANDLE_HIT || Math.abs(x - span[1]) <= HANDLE_HIT);
      if (this.#svg !== null) {
        this.#svg.style.cursor = nearHandle
          ? 'ew-resize'
          : span !== null && x > span[0] && x < span[1]
            ? 'grab'
            : 'crosshair';
      }
      if (bucket !== this.#hover) {
        this.#hover = bucket;
        this.#render();
      }
      return;
    }
    const last = axis.edges.length - 1;
    switch (drag.mode) {
      case 'new':
        this.#pending = snapDrag(axis, drag.startX, x);
        break;
      case 'move':
        this.#pending = shiftRange(axis, drag.origin, x - drag.startX);
        break;
      case 'from': {
        const edge = Math.min(nearestEdge(axis, x), drag.origin.to - 1);
        this.#pending = { from: Math.max(0, edge), to: drag.origin.to };
        break;
      }
      case 'to': {
        const edge = Math.max(nearestEdge(axis, x), drag.origin.from + 1);
        this.#pending = { from: drag.origin.from, to: Math.min(last, edge) };
        break;
      }
    }
    this.#render();
  };

  readonly #onUp = (event: PointerEvent): void => {
    const axis = this.#axis;
    const drag = this.#drag;
    this.#drag = null;
    this.#svg?.releasePointerCapture(event.pointerId);
    if (axis === null || drag === null || this.#pending === null) return;
    const times = rangeTimes(axis, this.#pending);
    if (times !== null) this.select(encodeBrushSelection({ kind: 'window', ...times }));
    this.#render();
  };

  readonly #onCancel = (): void => {
    this.#drag = null;
    this.#pending = null;
    this.#render();
  };

  readonly #onLeave = (): void => {
    if (this.#hover !== null && this.#drag === null) {
      this.#hover = null;
      this.#render();
    }
  };

  #render(): void {
    const svg = this.#svg;
    const payload = this.#payload;
    if (svg === null || payload === null) return;
    const { width, height } = this.#size;
    if (width <= 0 || height <= 0) return;
    const theme = this.theme;
    const axis = axisOf(payload.buckets, PAD_X, width - PAD_X);
    this.#axis = axis;
    const plotBottom = height - AXIS;
    const plotHeight = Math.max(4, plotBottom - TOP);
    const children: SVGElement[] = [];

    const stripes = el('pattern', {
      id: 'nonfinal',
      patternUnits: 'userSpaceOnUse',
      width: 4,
      height: 4,
      patternTransform: 'rotate(45)',
    });
    stripes.append(
      el('rect', { width: 4, height: 4, fill: toCss(withAlpha(theme.agent, 0.18)) }),
      el('rect', { width: 1.5, height: 4, fill: toCss(withAlpha(theme.agent, 0.75)) }),
    );
    const defs = el('defs', {});
    defs.append(stripes);
    children.push(defs);

    children.push(
      el('line', {
        x1: axis.x0,
        x2: axis.x1,
        y1: plotBottom + 0.5,
        y2: plotBottom + 0.5,
        stroke: toCss(theme.faint),
      }),
    );

    const barColor = toCss(theme.agent);
    const hoverColor = toCss(theme.text);
    for (const bar of bars(axis, payload, plotHeight, width / payload.buckets.length > 4 ? 1 : 0)) {
      if (bar.height === 0) continue;
      children.push(
        el('rect', {
          x: bar.x,
          y: plotBottom - bar.height,
          width: bar.width,
          height: bar.height,
          rx: Math.min(1.5, bar.width / 3),
          fill: bar.index === this.#hover ? hoverColor : bar.final ? barColor : 'url(#nonfinal)',
        }),
      );
    }

    const watermarkMs = Date.parse(payload.watermark);
    const [start, end] = [axis.edgeMs[0] ?? 0, axis.edgeMs[axis.edgeMs.length - 1] ?? 0];
    if (watermarkMs > start && watermarkMs < end) {
      const x = Math.round(toX(axis, watermarkMs)) + 0.5;
      children.push(
        el('line', {
          x1: x,
          x2: x,
          y1: TOP - 2,
          y2: plotBottom,
          stroke: toCss(theme.muted),
          'stroke-dasharray': '2 2',
        }),
      );
    }

    for (const tick of ticks(axis)) {
      const x = Math.round(tick.x) + 0.5;
      children.push(
        el('line', {
          x1: x,
          x2: x,
          y1: plotBottom,
          y2: plotBottom + 3,
          stroke: toCss(theme.faint),
        }),
      );
      const label = el('text', {
        x,
        y: height - 4,
        'text-anchor': tick.x < axis.x0 + 20 ? 'start' : tick.x > axis.x1 - 20 ? 'end' : 'middle',
      });
      label.textContent = formatUtcShort(tick.ms, tick.day);
      children.push(label);
    }

    const span = this.#brushSpan();
    if (span !== null) {
      const [x0, x1] = span;
      const veil = toCss(withAlpha(theme.surface, theme.dark ? 0.62 : 0.55));
      children.push(
        el('rect', {
          x: axis.x0,
          y: TOP - 2,
          width: Math.max(0, x0 - axis.x0),
          height: plotHeight + 2,
          fill: veil,
        }),
        el('rect', {
          x: x1,
          y: TOP - 2,
          width: Math.max(0, axis.x1 - x1),
          height: plotHeight + 2,
          fill: veil,
        }),
        el('rect', {
          x: x0,
          y: TOP - 2,
          width: Math.max(1, x1 - x0),
          height: plotHeight + 2,
          fill: toCss(withAlpha(theme.route.channel, 0.07)),
          stroke: toCss(mix(theme.route.channel, theme.surface, 0.1)),
          'stroke-width': 1,
          rx: 2,
        }),
      );
      for (const x of [x0, x1]) {
        children.push(
          el('rect', {
            x: x - 1.5,
            y: TOP + plotHeight / 2 - 8,
            width: 3,
            height: 16,
            rx: 1.5,
            fill: toCss(theme.route.channel),
          }),
        );
      }
    }

    children.push(...this.#labels(payload, axis, width));
    svg.replaceChildren(...children);
  }

  /** The header line: what the bars count and how final they are, or the hovered bucket. */
  #labels(payload: TimelinePayload, axis: Axis, width: number): SVGElement[] {
    const left = el('text', { x: axis.x0, y: 12, class: 'label' });
    const hovered = this.#hover === null ? undefined : payload.buckets[this.#hover];
    if (hovered !== undefined) {
      left.classList.add('strong');
      left.textContent = `${formatUtc(hovered.from)} – ${formatUtc(hovered.to).slice(11)} UTC · ${formatCount(hovered.transmissions)} transmissions · ${formatBytes(hovered.matchedBytes)}${hovered.final ? '' : ' · not final'}`;
    } else {
      const minutes = Math.round(payload.bucketMs / 60_000);
      const per = minutes % 60 === 0 ? `${minutes / 60} h` : `${minutes} min`;
      left.textContent = `transmissions per ${per} · final up to ${formatUtc(payload.watermark)} UTC`;
    }
    const range = this.#pending ?? this.#currentRange();
    const times = range === null ? null : rangeTimes(axis, range);
    if (times === null || width < 360) return [left];
    const right = el('text', { x: axis.x1, y: 12, 'text-anchor': 'end', class: 'label strong' });
    right.textContent = `${formatUtc(times.from)} → ${formatUtc(times.to)}`;
    return [left, right];
  }
}
