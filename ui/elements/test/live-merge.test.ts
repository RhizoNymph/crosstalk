import { describe, expect, it } from 'vitest';
import { type TopologyPayload, topologyPayload } from '../src/payloads/topology.ts';
import {
  edgeCounts,
  FLASH_MS,
  type Flashes,
  prune,
  pulse,
  risenEdges,
  strength,
} from '../src/topology/flash.ts';
import { layout, type Position } from '../src/topology/layout.ts';
import { planMerge } from '../src/topology/merge.ts';
import { buildModel } from '../src/topology/model.ts';
import { fixtureJson } from './fixtures.ts';

const agents = (): TopologyPayload => topologyPayload.parse(fixtureJson('topology-agents.json'));

/** The payload with only the first `n` transmission edges and the agents they touch. */
function earlier(payload: TopologyPayload, n: number): TopologyPayload {
  const edges = payload.edges.slice(0, n);
  const ids = new Set(edges.flatMap((e) => (e.kind === 'transmission' ? [e.from, e.to] : [])));
  return { ...payload, edges, nodes: payload.nodes.filter((node) => ids.has(node.id)) };
}

/** The payload with every edge's transmissions bumped where `bump` says. */
function bumped(payload: TopologyPayload, bump: (index: number) => number): TopologyPayload {
  return {
    ...payload,
    edges: payload.edges.map((e, i) =>
      e.kind === 'transmission' ? { ...e, transmissions: e.transmissions + bump(i) } : e,
    ),
  };
}

describe('edge diff', () => {
  it('detects edges whose count rose, and only those', () => {
    const before = buildModel(agents(), false);
    const after = buildModel(
      bumped(agents(), (i) => (i === 0 ? 3 : 0)),
      false,
    );
    const risen = risenEdges(edgeCounts(before.edges), edgeCounts(after.edges));
    expect(risen.size).toBe(1);
    const first = agents().edges[0];
    if (first?.kind !== 'transmission') throw new Error('fixture starts with a transmission');
    const [key] = [...risen];
    expect(key).toContain(first.from);
    expect(key).toContain(first.to);
  });

  it('counts new edges as risen and ignores unchanged and fallen ones', () => {
    const previous = new Map([
      ['a', 3],
      ['b', 5],
      ['c', 2],
    ]);
    const next = new Map([
      ['a', 3],
      ['b', 4],
      ['c', 7],
      ['d', 1],
    ]);
    expect([...risenEdges(previous, next)].sort()).toEqual(['c', 'd']);
  });

  it('sums member transmissions per drawn edge', () => {
    const model = buildModel(agents(), false);
    const counts = edgeCounts(model.edges);
    const total = agents().edges.reduce(
      (sum, e) => sum + (e.kind === 'transmission' ? e.transmissions : 0),
      0,
    );
    expect([...counts.values()].reduce((a, b) => a + b, 0)).toBe(total);
  });
});

describe('pulse', () => {
  it('rises, peaks and fades back to nothing', () => {
    expect(pulse(-1)).toBe(0);
    expect(pulse(0)).toBe(0);
    expect(pulse(FLASH_MS * 0.15)).toBeCloseTo(1);
    expect(pulse(FLASH_MS * 0.5)).toBeGreaterThan(0);
    expect(pulse(FLASH_MS * 0.5)).toBeLessThan(1);
    expect(pulse(FLASH_MS)).toBe(0);
    expect(pulse(FLASH_MS * 2)).toBe(0);
  });

  it('prunes finished flashes', () => {
    const flashes: Flashes = new Map([
      ['old', 0],
      ['new', 1000],
    ]);
    expect(prune(flashes, 1500)).toBe(true);
    expect([...flashes.keys()]).toEqual(['new']);
    expect(strength(flashes, 'new', 1100)).toBeGreaterThan(0);
    expect(strength(flashes, 'missing', 1100)).toBe(0);
    expect(prune(flashes, 1000 + FLASH_MS)).toBe(false);
  });
});

describe('merge', () => {
  const positionsOf = (payload: TopologyPayload): Map<string, Position> =>
    layout(buildModel(payload, false));
  const edgeKeys = (payload: TopologyPayload): string[] =>
    buildModel(payload, false).edges.map((e) => e.key);

  it('keeps the positions of nodes already drawn', () => {
    const before = earlier(agents(), 3);
    const drawn = positionsOf(before);
    const plan = planMerge(drawn, edgeKeys(before), buildModel(agents(), false));
    for (const [id, at] of drawn) expect(plan.positions.get(id)).toEqual(at);
  });

  it('places new nodes near their neighbours', () => {
    const before = earlier(agents(), 3);
    const drawn = positionsOf(before);
    const model = buildModel(agents(), false);
    const plan = planMerge(drawn, edgeKeys(before), model);
    expect(plan.addedNodes.size).toBe(model.nodes.length - drawn.size);
    expect(plan.addedNodes.size).toBeGreaterThan(0);
    const xs = [...drawn.values()].map((p) => p.x);
    const ys = [...drawn.values()].map((p) => p.y);
    const span = Math.max(Math.max(...xs) - Math.min(...xs), Math.max(...ys) - Math.min(...ys));
    for (const id of plan.addedNodes) {
      const at = plan.positions.get(id);
      expect(at).toBeDefined();
      expect(Number.isFinite(at?.x)).toBe(true);
      const placed = model.edges
        .filter((e) => e.source === id || e.target === id)
        .map((e) => (e.source === id ? e.target : e.source))
        .flatMap((n) => {
          const p = drawn.get(n);
          return p === undefined ? [] : [p];
        });
      if (placed.length === 0 || at === undefined) continue;
      const cx = placed.reduce((s, p) => s + p.x, 0) / placed.length;
      const cy = placed.reduce((s, p) => s + p.y, 0) / placed.length;
      expect(Math.hypot(at.x - cx, at.y - cy)).toBeLessThanOrEqual(span * 0.06 + 1e-9);
    }
    expect(plan.positions.size).toBe(model.nodes.length);
  });

  it('is deterministic', () => {
    const before = earlier(agents(), 3);
    const drawn = positionsOf(before);
    const model = buildModel(agents(), false);
    const a = planMerge(drawn, edgeKeys(before), model);
    const b = planMerge(drawn, edgeKeys(before), model);
    expect([...a.positions]).toEqual([...b.positions]);
  });

  it('removes edges and nodes the new model lacks', () => {
    const full = agents();
    const drawn = positionsOf(full);
    const smaller = earlier(full, 2);
    const plan = planMerge(drawn, edgeKeys(full), buildModel(smaller, false));
    const kept = new Set(edgeKeys(smaller));
    expect(plan.removedEdges.length).toBe(edgeKeys(full).length - kept.size);
    for (const key of plan.removedEdges) expect(kept.has(key)).toBe(false);
    const nodes = new Set(smaller.nodes.map((n) => n.id as string));
    expect(plan.removedNodes.length).toBe(drawn.size - nodes.size);
    for (const id of plan.removedNodes) expect(nodes.has(id)).toBe(false);
    expect(plan.addedNodes.size).toBe(0);
  });

  it('places a node with no drawn neighbour inside the drawing', () => {
    const drawn = new Map<string, Position>([
      ['a', { x: 0, y: 0 }],
      ['b', { x: 100, y: 100 }],
    ]);
    const model = {
      nodes: [{ id: 'a' }, { id: 'b' }, { id: 'z' }],
      edges: [],
      drawnAs: new Map(),
    } as unknown as Parameters<typeof planMerge>[2];
    const plan = planMerge(drawn, [], model);
    const at = plan.positions.get('z');
    expect(at).toBeDefined();
    expect(Math.hypot((at?.x ?? 0) - 50, (at?.y ?? 0) - 50)).toBeLessThanOrEqual(25 + 1e-9);
  });
});
