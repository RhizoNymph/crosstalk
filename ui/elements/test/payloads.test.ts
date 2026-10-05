import { describe, expect, it } from 'vitest';
import { timelinePayload } from '../src/payloads/timeline.ts';
import { topologyPayload } from '../src/payloads/topology.ts';
import { clone, fixtureJson } from './fixtures.ts';

type Json = Record<string, unknown> & { nodes: Json[]; edges: Json[]; buckets: Json[] };

const agents = () => clone(fixtureJson('topology-agents.json')) as Json;
const channels = () => clone(fixtureJson('topology-channels.json')) as Json;
const timeline = () => clone(fixtureJson('timeline.json')) as Json;

describe('topology payload', () => {
  it('parses what Rust emits in both modes', () => {
    const a = topologyPayload.parse(agents());
    expect(a.mode).toBe('agents');
    expect(a.nodes.length).toBe(12);
    expect(a.nodes.every((n) => n.kind === 'agent')).toBe(true);
    expect(a.edges.every((e) => e.kind === 'transmission')).toBe(true);
    const c = topologyPayload.parse(channels());
    expect(c.mode).toBe('channels');
    // One channel node per channel an access touches or a transmission is
    // routed through: an unused channel is not drawn.
    expect(c.nodes.filter((n) => n.kind === 'channel').length).toBe(4);
    expect(c.edges.some((e) => e.kind === 'access')).toBe(true);
  });

  it('marks unconfirmed channels and refuses an unknown confirmation', () => {
    const c = topologyPayload.parse(channels());
    const unconfirmed = c.nodes.filter(
      (n) => n.kind === 'channel' && n.confirmation === 'unconfirmed',
    );
    expect(unconfirmed.length).toBe(1);
    const payload = channels();
    (payload.nodes.find((n) => n.kind === 'channel') as Json).confirmation = 'maybe';
    expect(topologyPayload.safeParse(payload).success).toBe(false);
  });

  it('parses the empty payload', () => {
    const empty = topologyPayload.parse(fixtureJson('topology-empty.json'));
    expect(empty.nodes).toEqual([]);
  });

  it('keeps claims as claims', () => {
    const planner = topologyPayload.parse(agents()).nodes[0];
    expect(planner?.kind === 'agent' && planner.claims[0]?.harness).toBe('Claude Code');
  });

  it('rejects unknown keys', () => {
    const payload = agents();
    payload.extra = 1;
    expect(topologyPayload.safeParse(payload).success).toBe(false);
  });

  it('rejects a malformed ULID', () => {
    const payload = agents();
    (payload.nodes[0] as Json).id = 'not-a-ulid';
    expect(topologyPayload.safeParse(payload).success).toBe(false);
  });

  it('rejects a route kind that does not match the route code', () => {
    const payload = agents();
    const edge = payload.edges.find((e) => e.route === 'dl.p2c') as Json;
    edge.routeKind = 'direct';
    expect(topologyPayload.safeParse(payload).success).toBe(false);
  });

  it('rejects an unknown route code', () => {
    const payload = agents();
    (payload.edges[0] as Json).route = 'teleport';
    expect(topologyPayload.safeParse(payload).success).toBe(false);
  });

  it('rejects an edge whose endpoint has no node', () => {
    const payload = agents();
    payload.nodes = payload.nodes.slice(1);
    const result = topologyPayload.safeParse(payload);
    expect(result.success).toBe(false);
    expect(result.error?.issues.some((i) => i.message.includes('no agent node'))).toBe(true);
  });

  it('rejects access edges in agents mode', () => {
    const payload = channels();
    payload.mode = 'agents';
    expect(topologyPayload.safeParse(payload).success).toBe(false);
  });

  it('rejects shares outside [0, 1] and negative counts', () => {
    const share = agents();
    (share.edges[0] as Json).share = 1.5;
    expect(topologyPayload.safeParse(share).success).toBe(false);
    const count = agents();
    (count.nodes[0] as Json).volume = -1;
    expect(topologyPayload.safeParse(count).success).toBe(false);
  });

  it('rejects an inverted window', () => {
    const payload = agents();
    payload.window = { from: '2026-10-03T00:00:00Z', to: '2026-10-02T00:00:00Z' };
    expect(topologyPayload.safeParse(payload).success).toBe(false);
  });
});

describe('timeline payload', () => {
  it('parses what Rust emits', () => {
    const t = timelinePayload.parse(timeline());
    expect(t.buckets.length).toBe(96);
    expect(t.bucketMs).toBe(15 * 60 * 1000);
    const finals = t.buckets.filter((b) => b.final).length;
    expect(finals).toBe(93);
    expect(t.buckets.slice(0, finals).every((b) => b.final)).toBe(true);
  });

  it('rejects overlapping buckets', () => {
    const payload = timeline();
    (payload.buckets[1] as Json).from = (payload.buckets[0] as Json).from;
    expect(timelinePayload.safeParse(payload).success).toBe(false);
  });

  it('rejects a missing final flag', () => {
    const payload = timeline();
    delete (payload.buckets[0] as Json).final;
    expect(timelinePayload.safeParse(payload).success).toBe(false);
  });

  it('rejects times with an offset', () => {
    const payload = timeline();
    payload.watermark = '2026-10-02T23:22:30+02:00';
    expect(timelinePayload.safeParse(payload).success).toBe(false);
  });
});
