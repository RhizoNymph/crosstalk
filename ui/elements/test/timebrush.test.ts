import { describe, expect, it } from 'vitest';
import { type TimelinePayload, timelinePayload } from '../src/payloads/timeline.ts';
import {
  axisOf,
  bars,
  bucketAt,
  nearestEdge,
  rangeTimes,
  shiftRange,
  snapDrag,
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
