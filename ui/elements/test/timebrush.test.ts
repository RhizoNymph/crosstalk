import { describe, expect, it } from 'vitest';
import { type TimelinePayload, timelinePayload } from '../src/payloads/timeline.ts';
import {
  type Axis,
  axisOf,
  bars,
  bucketAt,
  type LabelledTick,
  labelledTicks,
  labelsFit,
  nearestEdge,
  rangeTimes,
  shiftRange,
  snapDrag,
  tickLabel,
  ticks,
  toMs,
  toX,
  windowSpan,
} from '../src/timebrush/model.ts';
import { fixtureJson } from './fixtures.ts';

const payload = (): TimelinePayload => timelinePayload.parse(fixtureJson('timeline.json'));
// 96 buckets over 960 px: 10 px per 15-minute bucket.
const axis = () => axisOf(payload().buckets, 0, 960);

describe('axis', () => {
  it('maps time to pixels and back', () => {
    const a = axis();
    expect(a.edges.length).toBe(97);
    expect(toX(a, Date.parse('2026-10-02T12:00:00Z'))).toBeCloseTo(480);
    expect(new Date(toMs(a, 480)).toISOString()).toBe('2026-10-02T12:00:00.000Z');
  });

  it('finds the bucket under a pixel, clamped', () => {
    const a = axis();
    expect(bucketAt(a, 5)).toBe(0);
    expect(bucketAt(a, 15)).toBe(1);
    expect(bucketAt(a, -50)).toBe(0);
    expect(bucketAt(a, 5000)).toBe(95);
    expect(nearestEdge(a, 14)).toBe(1);
    expect(nearestEdge(a, 16)).toBe(2);
  });
});

describe('snapping', () => {
  it('snaps a drag to the nearest bucket edges, in either direction', () => {
    const a = axis();
    const forward = snapDrag(a, 92, 247);
    const backward = snapDrag(a, 247, 92);
    expect(forward).toEqual({ from: 9, to: 25 });
    expect(backward).toEqual(forward);
    expect(rangeTimes(a, forward ?? { from: 0, to: 0 })).toEqual({
      from: '2026-10-02T02:15:00Z',
      to: '2026-10-02T06:15:00Z',
    });
  });

  it('selects the bucket under a click', () => {
    const a = axis();
    expect(snapDrag(a, 123, 123)).toEqual({ from: 12, to: 13 });
    expect(snapDrag(a, 121, 124)).toEqual({ from: 12, to: 13 });
  });

  it('moves a range by whole buckets and keeps it inside the axis', () => {
    const a = axis();
    expect(shiftRange(a, { from: 10, to: 20 }, 31)).toEqual({ from: 13, to: 23 });
    expect(shiftRange(a, { from: 10, to: 20 }, -500)).toEqual({ from: 0, to: 10 });
    expect(shiftRange(a, { from: 10, to: 20 }, 5000)).toEqual({ from: 86, to: 96 });
  });

  it('emits the payload’s own edge strings', () => {
    const a = axis();
    const times = rangeTimes(a, { from: 0, to: 96 });
    expect(times).toEqual({ from: '2026-10-02T00:00:00Z', to: '2026-10-03T00:00:00Z' });
  });
});

describe('window span', () => {
  it('draws the current window, clamped to the axis', () => {
    const a = axis();
    expect(windowSpan(a, '2026-10-02T09:00:00Z', '2026-10-02T17:00:00Z')).toEqual([360, 680]);
    expect(windowSpan(a, '2026-10-01T09:00:00Z', '2026-10-02T01:00:00Z')).toEqual([0, 40]);
    expect(windowSpan(a, '2026-10-05T00:00:00Z', '2026-10-06T00:00:00Z')).toBeNull();
    expect(windowSpan(a, 'garbage', '2026-10-02T01:00:00Z')).toBeNull();
  });
});

describe('bars and ticks', () => {
  it('scales bars to the tallest bucket and keeps the final flag', () => {
    const p = payload();
    const a = axis();
    const drawn = bars(a, p, 50, 1);
    expect(drawn.length).toBe(96);
    expect(Math.max(...drawn.map((b) => b.height))).toBe(50);
    expect(drawn.filter((b) => !b.final).map((b) => b.index)).toEqual([93, 94, 95]);
    expect(drawn[0]?.width).toBe(9);
  });

  it('places round UTC ticks, labelling midnight with the date', () => {
    const a = axis();
    const marks = ticks(a, 64);
    expect(marks.length).toBeLessThanOrEqual(15);
    const step = (marks[1]?.ms ?? 0) - (marks[0]?.ms ?? 0);
    expect(step).toBe(2 * 3600_000);
    expect(marks[0]?.day).toBe(true);
    expect(marks.filter((t) => t.day).length).toBe(2);
  });
});

describe('axis labels', () => {
  // About what 10 px system-ui measures: 6 px a character.
  const measure = (text: string) => text.length * 6;
  const HOUR = 3_600_000;
  /** A week of hourly buckets (the topology brush) over `width` pixels. */
  const week = (width: number): Axis => {
    const start = Date.parse('2026-09-26T00:00:00Z');
    const edgeMs = Array.from({ length: 169 }, (_, i) => start + i * HOUR);
    return {
      edges: edgeMs.map((ms) => new Date(ms).toISOString().replace('.000Z', 'Z')),
      edgeMs,
      x0: 8,
      x1: 8 + width,
    };
  };
  const stepOf = (labels: readonly LabelledTick[]) => (labels[1]?.ms ?? 0) - (labels[0]?.ms ?? 0);
  const inside = (a: Axis, labels: readonly LabelledTick[]) =>
    labels.every((l) => l.left >= a.x0 - 1e-9 && l.right <= a.x1 + 1e-9);

  it('never overlaps at the widths the topology page draws', () => {
    for (const width of [300, 420, 560, 700, 760, 900, 1200]) {
      const a = week(width);
      const labels = labelledTicks(a, measure, 10);
      expect(labels.length, `${width}px`).toBeGreaterThanOrEqual(2);
      expect(labelsFit(labels, 10), `${width}px`).toBe(true);
      expect(inside(a, labels), `${width}px`).toBe(true);
    }
  });

  it('labels whole-day steps with the date only and pins edge labels inside', () => {
    const a = week(560);
    const labels = labelledTicks(a, measure, 10);
    expect(stepOf(labels)).toBe(24 * HOUR);
    expect(labels.map((l) => l.text)).toEqual([
      '09-26',
      '09-27',
      '09-28',
      '09-29',
      '09-30',
      '10-01',
      '10-02',
      '10-03',
    ]);
    expect(labels[0]?.anchor).toBe('start');
    expect(labels[labels.length - 1]?.anchor).toBe('end');
    expect(labels[3]?.anchor).toBe('middle');
  });

  it('gets finer as the axis widens', () => {
    const narrow = stepOf(labelledTicks(week(400), measure));
    const wide = stepOf(labelledTicks(week(2400), measure));
    expect(wide).toBeLessThan(narrow);
    const day = labelledTicks(axis(), measure);
    expect(labelsFit(day, 10)).toBe(true);
    expect(stepOf(day)).toBeLessThanOrEqual(3 * HOUR);
    expect(day.find((l) => l.day)?.text).toBe('10-02 00:00');
  });

  it('drops crowded labels when even the coarsest step does not fit', () => {
    const labels = labelledTicks(week(60), measure, 10);
    expect(labelsFit(labels, 10)).toBe(true);
    expect(labels.length).toBeGreaterThanOrEqual(1);
  });

  it('formats tick labels by step', () => {
    const midnight = { ms: Date.parse('2026-10-02T00:00:00Z'), x: 0, day: true };
    const noon = { ms: Date.parse('2026-10-02T12:00:00Z'), x: 0, day: false };
    expect(tickLabel(midnight, 24 * HOUR)).toBe('10-02');
    expect(tickLabel(midnight, 6 * HOUR)).toBe('10-02 00:00');
    expect(tickLabel(noon, 6 * HOUR)).toBe('12:00');
  });
});
