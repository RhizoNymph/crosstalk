import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FOLLOW_INTERVAL_MS, FollowPacer } from '../src/live/follow.ts';
import { topologyPayload } from '../src/payloads/topology.ts';
import { attributeChanges, isSlide } from '../src/shared/slide.ts';
import { planMerge } from '../src/topology/merge.ts';
import { buildModel } from '../src/topology/model.ts';
import { fixtureJson } from './fixtures.ts';

describe('follow pacer', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('refreshes every 30 s while following a visible page', () => {
    const refresh = vi.fn();
    const pacer = new FollowPacer(refresh, true);
    pacer.follow(true);
    expect(FOLLOW_INTERVAL_MS).toBe(30_000);
    vi.advanceTimersByTime(29_999);
    expect(refresh).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(refresh).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(60_000);
    expect(refresh).toHaveBeenCalledTimes(3);
    pacer.stop();
  });

  it('never ticks on a pinned page, and admits every feed refresh there', () => {
    const refresh = vi.fn();
    const pacer = new FollowPacer(refresh, true);
    pacer.follow(false);
    vi.advanceTimersByTime(10 * FOLLOW_INTERVAL_MS);
    expect(refresh).not.toHaveBeenCalled();
    pacer.visibility(false);
    expect(pacer.admits()).toBe(true);
    pacer.visibility(true);
    expect(refresh).not.toHaveBeenCalled();
  });

  it('pauses while the tab is hidden and refreshes once on becoming visible', () => {
    const refresh = vi.fn();
    const pacer = new FollowPacer(refresh, true);
    pacer.follow(true);
    vi.advanceTimersByTime(FOLLOW_INTERVAL_MS);
    expect(refresh).toHaveBeenCalledTimes(1);
    pacer.visibility(false);
    expect(pacer.admits()).toBe(false);
    vi.advanceTimersByTime(10 * FOLLOW_INTERVAL_MS);
    expect(refresh).toHaveBeenCalledTimes(1);
    pacer.visibility(true);
    expect(refresh).toHaveBeenCalledTimes(2);
    expect(pacer.admits()).toBe(true);
    // The timer starts over from the visible refresh.
    vi.advanceTimersByTime(FOLLOW_INTERVAL_MS - 1);
    expect(refresh).toHaveBeenCalledTimes(2);
    vi.advanceTimersByTime(1);
    expect(refresh).toHaveBeenCalledTimes(3);
    pacer.stop();
  });

  it('starts paused on a hidden tab', () => {
    const refresh = vi.fn();
    const pacer = new FollowPacer(refresh, false);
    pacer.follow(true);
    vi.advanceTimersByTime(5 * FOLLOW_INTERVAL_MS);
    expect(refresh).not.toHaveBeenCalled();
    pacer.visibility(true);
    expect(refresh).toHaveBeenCalledTimes(1);
    pacer.stop();
  });

  it('declaring the same thing again does not restart the timer', () => {
    const refresh = vi.fn();
    const pacer = new FollowPacer(refresh, true);
    pacer.follow(true);
    vi.advanceTimersByTime(20_000);
    pacer.follow(true);
    pacer.visibility(true);
    vi.advanceTimersByTime(10_000);
    expect(refresh).toHaveBeenCalledTimes(1);
    pacer.stop();
  });

  it('stops ticking when the page stops following or the pacer stops', () => {
    const refresh = vi.fn();
    const pacer = new FollowPacer(refresh, true);
    pacer.follow(true);
    pacer.follow(false);
    vi.advanceTimersByTime(5 * FOLLOW_INTERVAL_MS);
    expect(refresh).not.toHaveBeenCalled();
    pacer.follow(true);
    pacer.stop();
    vi.advanceTimersByTime(5 * FOLLOW_INTERVAL_MS);
    expect(refresh).not.toHaveBeenCalled();
    expect(pacer.following).toBe(false);
  });
});

describe('slides', () => {
  const base = '/data/topology?from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z&v=2&w=tx&g=agents';
  const slid = '/data/topology?from=2026-10-02T00:05:00Z&to=2026-10-03T00:05:00Z&v=2&w=tx&g=agents';

  it('a change of the window keys alone is a slide', () => {
    expect(isSlide(base, slid, ['from', 'to'])).toBe(true);
  });

  it('any other change is not', () => {
    expect(isSlide(base, slid.replace('w=tx', 'w=bytes'), ['from', 'to'])).toBe(false);
    expect(isSlide(base, slid.replace('/data/topology', '/data/timeline'), ['from', 'to'])).toBe(
      false,
    );
    expect(isSlide(base, `${slid}&a=x`, ['from', 'to'])).toBe(false);
    expect(isSlide(base, slid, [])).toBe(false);
  });

  it('the same URL is not a slide', () => {
    expect(isSlide(base, base, ['from', 'to'])).toBe(false);
  });

  it('extra keys the element names may change too', () => {
    const brush = `${base}&buckets=168`;
    const next = `${slid}&buckets=169`;
    expect(isSlide(brush, next, ['from', 'to'])).toBe(false);
    expect(isSlide(brush, next, ['from', 'to', 'buckets'])).toBe(true);
  });

  it('a slid payload merges with every kept node in place', () => {
    const payload = topologyPayload.parse(fixtureJson('topology-agents.json'));
    const before = buildModel(payload, false);
    const drawn = new Map(before.nodes.map((n, i) => [n.id as string, { x: i, y: -i }]));
    // The window moved on: the first node's traffic left it.
    const gone = before.nodes[0]?.id;
    if (gone === undefined) throw new Error('the fixture has nodes');
    const after = buildModel(
      {
        ...payload,
        nodes: payload.nodes.filter((n) => n.id !== gone),
        edges: payload.edges.filter(
          (e) => e.kind !== 'transmission' || (e.from !== gone && e.to !== gone),
        ),
      },
      false,
    );
    const plan = planMerge(
      drawn,
      before.edges.map((e) => e.key),
      after,
    );
    expect(plan.removedNodes).toEqual([gone]);
    expect(plan.addedNodes.size).toBe(0);
    for (const node of after.nodes) {
      expect(plan.positions.get(node.id)).toEqual(drawn.get(node.id));
    }
  });
});

describe('attribute changes for a kept element', () => {
  it('sets what is new or changed and removes what is gone', () => {
    expect(
      attributeChanges(
        [
          ['id', 'graph'],
          ['data-src', '/a?from=1'],
          ['data-collapse', 'true'],
          ['data-topcoat-on:change', 'old'],
        ],
        [
          ['id', 'graph'],
          ['data-src', '/a?from=2'],
          ['data-topcoat-on:change', 'new'],
          ['data-live', '/data/live'],
        ],
      ),
    ).toEqual({
      set: [
        ['data-src', '/a?from=2'],
        ['data-topcoat-on:change', 'new'],
        ['data-live', '/data/live'],
      ],
      remove: ['data-collapse'],
    });
  });

  it('is empty when nothing changed', () => {
    expect(attributeChanges([['a', '1']], [['a', '1']])).toEqual({ set: [], remove: [] });
  });
});
