/**
 * The time brush's geometry and snapping. Pure: times in epoch milliseconds,
 * positions in CSS pixels.
 *
 * A brush always spans whole buckets: each end snaps to the nearest bucket
 * edge, and a click (or a drag shorter than one bucket) selects the bucket
 * under the pointer. The emitted times are the payload's own edge strings.
 */

import type { TimelineBucket, TimelinePayload } from '../payloads/timeline.ts';
import { formatUtcShort } from '../shared/format.ts';

export interface Axis {
  /** Bucket edges: `from` of every bucket, then `to` of the last. */
  readonly edges: readonly string[];
  readonly edgeMs: readonly number[];
  readonly x0: number;
  readonly x1: number;
}

export function axisOf(buckets: readonly TimelineBucket[], x0: number, x1: number): Axis {
  const edges = buckets.map((b) => b.from);
  const last = buckets[buckets.length - 1];
  if (last !== undefined) edges.push(last.to);
  return { edges, edgeMs: edges.map((e) => Date.parse(e)), x0, x1 };
}

function domain(axis: Axis): [number, number] {
  return [axis.edgeMs[0] ?? 0, axis.edgeMs[axis.edgeMs.length - 1] ?? 1];
}

export function toX(axis: Axis, ms: number): number {
  const [t0, t1] = domain(axis);
  if (t1 <= t0) return axis.x0;
  return axis.x0 + ((ms - t0) / (t1 - t0)) * (axis.x1 - axis.x0);
}

export function toMs(axis: Axis, x: number): number {
  const [t0, t1] = domain(axis);
  if (axis.x1 <= axis.x0) return t0;
  return t0 + ((x - axis.x0) / (axis.x1 - axis.x0)) * (t1 - t0);
}

/** The bucket under `x`, clamped to the axis. */
export function bucketAt(axis: Axis, x: number): number {
  const ms = toMs(axis, x);
  const count = axis.edges.length - 1;
  for (let i = 0; i < count; i++) {
    if (ms < (axis.edgeMs[i + 1] ?? Number.POSITIVE_INFINITY)) return Math.max(0, i);
  }
  return Math.max(0, count - 1);
}

/** The edge index nearest `x`. */
export function nearestEdge(axis: Axis, x: number): number {
  const ms = toMs(axis, x);
  let best = 0;
  let distance = Number.POSITIVE_INFINITY;
  axis.edgeMs.forEach((edge, i) => {
    const d = Math.abs(edge - ms);
    if (d < distance) {
      distance = d;
      best = i;
    }
  });
  return best;
}

/** A brush as edge indexes, `from < to`. */
export interface EdgeRange {
  readonly from: number;
  readonly to: number;
}

/** Snaps a drag from `xa` to `xb` to whole buckets. */
export function snapDrag(axis: Axis, xa: number, xb: number): EdgeRange | null {
  const buckets = axis.edges.length - 1;
  if (buckets < 1) return null;
  let a = nearestEdge(axis, Math.min(xa, xb));
  let b = nearestEdge(axis, Math.max(xa, xb));
  if (a === b) {
    const bucket = bucketAt(axis, xb);
    a = bucket;
    b = bucket + 1;
  }
  return { from: a, to: b };
}

/** Moves a range by `dx` pixels, snapped to edges and kept inside the axis. */
export function shiftRange(axis: Axis, range: EdgeRange, dx: number): EdgeRange {
  const width = range.to - range.from;
  const startX = toX(axis, axis.edgeMs[range.from] ?? 0) + dx;
  const maxFrom = axis.edges.length - 1 - width;
  const from = Math.min(Math.max(0, nearestEdge(axis, startX)), Math.max(0, maxFrom));
  return { from, to: from + width };
}

/** The value a range encodes to: `<from>/<to>`. */
export function rangeTimes(axis: Axis, range: EdgeRange): { from: string; to: string } | null {
  const from = axis.edges[range.from];
  const to = axis.edges[range.to];
  return from === undefined || to === undefined ? null : { from, to };
}

/**
 * The pixel span of a window (`data-from`/`data-to`), clamped to the axis;
 * `null` if it does not overlap it.
 */
export function windowSpan(axis: Axis, from: string, to: string): [number, number] | null {
  const a = Date.parse(from);
  const b = Date.parse(to);
  const [t0, t1] = domain(axis);
  if (Number.isNaN(a) || Number.isNaN(b) || b <= t0 || a >= t1 || a >= b) return null;
  return [toX(axis, Math.max(a, t0)), toX(axis, Math.min(b, t1))];
}

export interface Bar {
  readonly index: number;
  readonly x: number;
  readonly width: number;
  readonly height: number;
  readonly final: boolean;
}

/** Bars scaled to `plotHeight` by transmissions, with `gap` px between. */
export function bars(axis: Axis, payload: TimelinePayload, plotHeight: number, gap: number): Bar[] {
  const max = Math.max(1, ...payload.buckets.map((b) => b.transmissions));
  return payload.buckets.map((b, index) => {
    const left = toX(axis, Date.parse(b.from));
    const right = toX(axis, Date.parse(b.to));
    const width = Math.max(1, right - left - gap);
    const height = b.transmissions === 0 ? 0 : Math.max(1, (b.transmissions / max) * plotHeight);
    return { index, x: left + Math.min(gap, right - left - 1) / 2, width, height, final: b.final };
  });
}

const MINUTE = 60_000;
const DAY = 1440 * MINUTE;
export const TICK_STEPS = [5, 15, 30, 60, 120, 180, 360, 720, 1440, 2880, 10080].map(
  (m) => m * MINUTE,
);

export interface Tick {
  readonly ms: number;
  readonly x: number;
  /** Midnight UTC: labelled with the date. */
  readonly day: boolean;
}

function ticksAt(axis: Axis, step: number): Tick[] {
  const [t0, t1] = domain(axis);
  const out: Tick[] = [];
  for (let ms = Math.ceil(t0 / step) * step; ms <= t1; ms += step) {
    out.push({ ms, x: toX(axis, ms), day: ms % DAY === 0 });
  }
  return out;
}

/** Ticks at a round UTC step, at most one per `minSpacing` pixels. */
export function ticks(axis: Axis, minSpacing = 64): Tick[] {
  const [t0, t1] = domain(axis);
  const span = t1 - t0;
  const width = axis.x1 - axis.x0;
  if (span <= 0 || width <= 0) return [];
  const maxTicks = Math.max(1, Math.floor(width / minSpacing));
  const step =
    TICK_STEPS.find((s) => span / s <= maxTicks) ?? TICK_STEPS[TICK_STEPS.length - 1] ?? span;
  return ticksAt(axis, step);
}

export type LabelAnchor = 'start' | 'middle' | 'end';

/** A tick with its label, placed so it stays inside the axis. */
export interface LabelledTick extends Tick {
  readonly text: string;
  readonly anchor: LabelAnchor;
  /** The label's horizontal extent in pixels. */
  readonly left: number;
  readonly right: number;
}

/**
 * A tick's label: `HH:MM`, with the date at midnight (`MM-DD HH:MM`); only
 * the date when the step is whole days.
 */
export function tickLabel(tick: Tick, step: number): string {
  if (step >= DAY && step % DAY === 0) return new Date(tick.ms).toISOString().slice(5, 10);
  return formatUtcShort(tick.ms, tick.day);
}

/** Centred on the tick, or pinned to an end of the axis it would overflow. */
function place(axis: Axis, tick: Tick, text: string, width: number): LabelledTick {
  let anchor: LabelAnchor = 'middle';
  let left = tick.x - width / 2;
  if (left < axis.x0) {
    anchor = 'start';
    left = tick.x;
  } else if (tick.x + width / 2 > axis.x1) {
    anchor = 'end';
    left = tick.x - width;
  }
  return { ...tick, text, anchor, left, right: left + width };
}

/** Whether consecutive labels keep `gap` pixels apart. */
export function labelsFit(labels: readonly LabelledTick[], gap: number): boolean {
  return labels.every((label, i) => {
    const previous = labels[i - 1];
    return previous === undefined || label.left - previous.right >= gap;
  });
}

/**
 * Labelled ticks at the finest round UTC step whose labels, measured with
 * `measure` (text → pixels), keep at least `gap` pixels apart. When even the
 * coarsest step crowds, labels that would overlap the one before are
 * dropped.
 */
export function labelledTicks(
  axis: Axis,
  measure: (text: string) => number,
  gap = 10,
): LabelledTick[] {
  const [t0, t1] = domain(axis);
  const width = axis.x1 - axis.x0;
  if (t1 <= t0 || width <= 0) return [];
  const labelled = (step: number) =>
    ticksAt(axis, step).map((tick) => {
      const text = tickLabel(tick, step);
      return place(axis, tick, text, measure(text));
    });
  for (const step of TICK_STEPS) {
    // A label is never narrower than a few pixels; skip hopeless steps early.
    if ((t1 - t0) / step > width / 4) continue;
    const labels = labelled(step);
    if (labelsFit(labels, gap)) return labels;
  }
  const kept: LabelledTick[] = [];
  for (const label of labelled(TICK_STEPS[TICK_STEPS.length - 1] ?? DAY)) {
    const previous = kept[kept.length - 1];
    if (previous === undefined || label.left - previous.right >= gap) kept.push(label);
  }
  return kept;
}
