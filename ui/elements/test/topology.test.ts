import { describe, expect, it } from 'vitest';
import { type TopologyPayload, topologyPayload } from '../src/payloads/topology.ts';
import type { Ulid } from '../src/shared/ulid.ts';
import { iterationsFor, layout, seedPosition } from '../src/topology/layout.ts';
import {
  buildModel,
  EDGE_WIDTH,
  edgeSelection,
  highlightOf,
  NODE_SIZE,
  nodeSelection,
  rootOf,
} from '../src/topology/model.ts';
import { shortLabel } from '../src/topology/style.ts';
import { edgeTooltip, nodeTooltip } from '../src/topology/tooltip.ts';
import { fixtureJson } from './fixtures.ts';

const agents = (): TopologyPayload => topologyPayload.parse(fixtureJson('topology-agents.json'));
const channels = (): TopologyPayload =>
  topologyPayload.parse(fixtureJson('topology-channels.json'));
const id = (n: number) =>
  `01K6HB7H0002GG002YXM0000${n.toString(32).toUpperCase().padStart(2, '0')}` as Ulid;
const PLANNER = id(1);
const RESEARCHER = id(2);
const REVIEWER = id(4);
const DOCS_WRITER = id(6);
const channelId = (n: number) =>
  `01K6HB7H000320002YXM0000${n.toString(32).toUpperCase().padStart(2, '0')}` as Ulid;

describe('model', () => {
  it('sizes nodes by volume and edges by share', () => {
    const model = buildModel(agents(), false);
    const planner = model.nodes.find((n) => n.id === PLANNER);
    expect(planner?.size).toBe(NODE_SIZE.max);
    for (const node of model.nodes) {
      expect(node.size).toBeGreaterThanOrEqual(NODE_SIZE.min);
      expect(node.size).toBeLessThanOrEqual(NODE_SIZE.max);
    }
    const widest = Math.max(...model.edges.map((e) => e.width));
    expect(widest).toBe(EDGE_WIDTH.max);
    const widths = [...model.edges].sort((a, b) => a.share - b.share).map((e) => e.width);
    expect(widths).toEqual([...widths].sort((a, b) => a - b));
  });

  it('follows parents to the top-most present ancestor', () => {
    const payload = agents();
    const byId = new Map(
      payload.nodes.flatMap((n) => (n.kind === 'agent' ? [[n.id, n] as const] : [])),
    );
    expect(rootOf(RESEARCHER, byId)).toBe(PLANNER);
    expect(rootOf(PLANNER, byId)).toBe(PLANNER);
  });

  it('collapses sub-agents into their parent', () => {
    const model = buildModel(agents(), true);
    const planner = model.nodes.find((n) => n.id === PLANNER);
    expect(planner?.kind === 'agent' && planner.members.length).toBe(3);
    expect(planner?.label).toBe('planner +2');
    expect(model.nodes.some((n) => n.id === RESEARCHER)).toBe(false);
    expect(model.drawnAs.get(RESEARCHER)).toBe(PLANNER);
    // planner <-> researcher and planner <-> …000003 are internal now.
    expect(model.edges.some((e) => e.source === e.target)).toBe(false);
    const volume = agents()
      .nodes.filter((n) => n.kind === 'agent' && [PLANNER, RESEARCHER, id(3)].includes(n.id))
      .reduce((sum, n) => sum + n.volume, 0);
    expect(planner?.volume).toBe(volume);
  });

  it('merges parallel edges after collapsing and selects the heaviest', () => {
    const model = buildModel(agents(), true);
    for (const edge of model.edges) {
      if (edge.kind !== 'transmission') continue;
      const shares = edge.members.map((m) => m.share);
      expect(shares).toEqual([...shares].sort((a, b) => b - a));
      const selection = edgeSelection(edge);
      expect(selection.kind === 'edge' && selection.route).toBe(edge.members[0]?.route);
    }
  });

  it('directs access edges by operation', () => {
    const model = buildModel(channels(), false);
    const channelIds = new Set(model.nodes.filter((n) => n.kind === 'channel').map((n) => n.id));
    for (const edge of model.edges) {
      if (edge.kind !== 'access') continue;
      const head = edge.members[0];
      if (head?.op === 'write') expect(channelIds.has(edge.target)).toBe(true);
      else expect(channelIds.has(edge.source)).toBe(true);
      expect(edgeSelection(edge)).toEqual({ kind: 'channel', id: head?.channel });
    }
  });

  it('highlights a selected edge with its endpoints', () => {
    const model = buildModel(agents(), false);
    const edge = model.edges.find((e) => e.source === PLANNER && e.target === REVIEWER);
    if (edge === undefined) throw new Error('missing edge');
    const highlight = highlightOf(model, edgeSelection(edge));
    expect(highlight?.edges).toEqual(new Set([edge.key]));
    expect(highlight?.nodes).toEqual(new Set([PLANNER, REVIEWER]));
  });

  it('highlights an agent with its neighbourhood, through collapse', () => {
    const collapsed = buildModel(agents(), true);
    const highlight = highlightOf(collapsed, { kind: 'agent', id: RESEARCHER });
    expect(highlight?.nodes.has(PLANNER)).toBe(true);
    expect(highlight?.edges.size).toBeGreaterThan(0);
    expect(highlightOf(collapsed, { kind: 'none' })).toBeNull();
    expect(
      highlightOf(collapsed, { kind: 'agent', id: '7ZZZZZZZZZZZZZZZZZZZZZZZZZ' as Ulid }),
    ).toBeNull();
  });

  it('highlights a channel in agents mode through the edges routed over it', () => {
    const model = buildModel(agents(), false);
    const handoff = channelId(1);
    const highlight = highlightOf(model, { kind: 'channel', id: handoff });
    const routed = model.edges.filter(
      (e) => e.kind === 'transmission' && e.members.some((m) => m.route === `ch.${handoff}`),
    );
    expect(routed.length).toBe(2);
    expect(highlight?.edges).toEqual(new Set(routed.map((e) => e.key)));
    // planner → reviewer and docs-writer → reviewer.
    expect(highlight?.nodes).toEqual(new Set([PLANNER, REVIEWER, DOCS_WRITER]));
  });

  it('highlights a channel in agents mode after collapsing', () => {
    const collapsed = buildModel(agents(), true);
    // ci-bot → planner and reviewer → deploy, both over the pastebin channel.
    const highlight = highlightOf(collapsed, { kind: 'channel', id: channelId(2) });
    expect(highlight?.edges.size).toBe(2);
    expect(highlight?.nodes).toEqual(new Set([id(5), PLANNER, REVIEWER, id(10)]));
  });

  it('highlights nothing for a channel no drawn edge goes through', () => {
    const model = buildModel(agents(), false);
    expect(highlightOf(model, { kind: 'channel', id: channelId(9) })).toBeNull();
    expect(highlightOf(model, { kind: 'channel', id: PLANNER })).toBeNull();
  });

  it('highlights a channel node with its accesses in channels mode', () => {
    const model = buildModel(channels(), false);
    const handoff = channelId(1);
    const highlight = highlightOf(model, { kind: 'channel', id: handoff });
    expect(highlight?.nodes).toEqual(new Set([handoff, PLANNER, REVIEWER, DOCS_WRITER]));
    expect(highlight?.edges.size).toBe(3);
    for (const key of highlight?.edges ?? []) {
      expect(model.edges.find((e) => e.key === key)?.kind).toBe('access');
    }
  });

  it('selects nodes by kind', () => {
    const model = buildModel(channels(), false);
    for (const node of model.nodes) {
      expect(nodeSelection(node)).toEqual({ kind: node.kind, id: node.id });
    }
  });
});

describe('layout', () => {
  it('seeds positions from ids alone', () => {
    expect(seedPosition(PLANNER)).toEqual(seedPosition(PLANNER));
    expect(seedPosition(PLANNER)).not.toEqual(seedPosition(RESEARCHER));
    const { x, y } = seedPosition(REVIEWER, 50);
    expect(Math.hypot(x, y)).toBeLessThanOrEqual(50);
  });

  it('gives the same picture for the same payload', () => {
    const first = layout(buildModel(agents(), false));
    const second = layout(buildModel(agents(), false));
    expect([...second]).toEqual([...first]);
  });

  it('does not depend on payload order', () => {
    const payload = agents();
    const shuffled: TopologyPayload = {
      ...payload,
      nodes: [...payload.nodes].reverse(),
      edges: [...payload.edges].reverse(),
    };
    const a = layout(buildModel(payload, false));
    const b = layout(buildModel(shuffled, false));
    for (const [key, position] of a) expect(b.get(key)).toEqual(position);
  });

  it('spreads nodes out', () => {
    const positions = [...layout(buildModel(channels(), false)).values()];
    positions.forEach((a, i) => {
      for (const b of positions.slice(i + 1)) {
        expect(Math.hypot(a.x - b.x, a.y - b.y)).toBeGreaterThan(0.01);
      }
    });
  });

  it('bounds iterations by graph size', () => {
    expect(iterationsFor(1)).toBe(800);
    expect(iterationsFor(500)).toBe(240);
    expect(iterationsFor(100_000)).toBe(120);
  });
});

describe('tooltips and labels', () => {
  it('shows harness claims as claims', () => {
    const model = buildModel(agents(), false);
    const planner = model.nodes.find((n) => n.id === PLANNER);
    if (planner === undefined) throw new Error('missing planner');
    const lines = nodeTooltip(planner);
    const claims = lines.filter((l) => l.style === 'claim');
    expect(claims.length).toBe(1);
    expect(claims[0]?.text.startsWith('claims Claude Code 2.1.3')).toBe(true);
    expect(claims[0]?.detail).toBe('claude-cli/2.1.3 (external, cli)');
    expect(lines[0]).toEqual({ text: 'planner', style: 'title' });
  });

  it('describes an edge with names and its route', () => {
    const model = buildModel(agents(), false);
    const names = new Map(model.nodes.map((n) => [n.id, n.label]));
    const edge = model.edges.find((e) => e.source === PLANNER && e.target === RESEARCHER);
    if (edge === undefined) throw new Error('missing edge');
    const lines = edgeTooltip(edge, names);
    expect(lines[0]?.text).toBe('planner → researcher');
    expect(lines[1]?.text).toBe('delegation: parent → child');
  });

  it('shortens long labels in the middle', () => {
    expect(shortLabel('short')).toBe('short');
    const long = shortLabel('mcp:linear/get_issue:ENG-4411-and-more', 20);
    expect(long.length).toBe(20);
    expect(long).toContain('…');
    expect(long.startsWith('mcp:linear')).toBe(true);
  });
});
