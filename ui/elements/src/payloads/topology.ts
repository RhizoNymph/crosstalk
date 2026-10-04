/**
 * The `<ct-topology>` payload: `GET /data/topology?<view state>`, JSON.
 * Mirrors `TopologyPayload` in `ui/src/data/topology/mod.rs`; see the doc
 * comment there and "Element payloads" in `docs/features/ui.md`.
 */

import * as z from 'zod';
import { parseRouteCode, type RouteCode, routeKindOf } from '../shared/route.ts';
import { count, routeKind, share, timestamp, ulid, window } from './common.ts';

const routeCode = z
  .string()
  .refine((text) => parseRouteCode(text) !== null, 'expected a route code')
  .transform((text) => text as RouteCode);

const claim = z.strictObject({
  harness: z.string(),
  version: z.string().nullable(),
  userAgent: z.string(),
  lastSeen: timestamp,
});

export const AGENT_STATES = ['registered', 'provisional', 'established'] as const;
export const POLICIES = ['unreviewed', 'sanctioned', 'unsanctioned'] as const;
export type Policy = (typeof POLICIES)[number];

const agentNode = z.strictObject({
  kind: z.literal('agent'),
  id: ulid,
  name: z.string(),
  state: z.enum(AGENT_STATES),
  parent: ulid.nullable(),
  volume: count,
  transmissionsIn: count,
  transmissionsOut: count,
  claims: z.array(claim),
});

const channelNode = z.strictObject({
  kind: z.literal('channel'),
  id: ulid,
  name: z.string(),
  origin: z.enum(['declared', 'discovered']),
  detection: z.enum(['awaitingTraffic', 'unused', 'observed', 'candidate', 'active', 'dormant']),
  policy: z.enum(POLICIES),
  volume: count,
});

const transmissionEdge = z
  .strictObject({
    kind: z.literal('transmission'),
    from: ulid,
    to: ulid,
    route: routeCode,
    routeKind,
    share,
    transmissions: count,
    matchedBytes: count,
  })
  .refine((e) => routeKindOf(e.route) === e.routeKind, 'routeKind does not match route');

const accessEdge = z.strictObject({
  kind: z.literal('access'),
  agent: ulid,
  channel: ulid,
  op: z.enum(['read', 'write']),
  accesses: count,
  share,
});

export const topologyPayload = z
  .strictObject({
    mode: z.enum(['agents', 'channels']),
    window,
    weighting: z.enum(['tx', 'bytes']),
    topicVersion: count,
    watermark: timestamp,
    nodes: z.array(z.discriminatedUnion('kind', [agentNode, channelNode])),
    edges: z.array(z.union([transmissionEdge, accessEdge])),
  })
  .superRefine((payload, ctx) => {
    const kinds = new Map<string, 'agent' | 'channel'>();
    payload.nodes.forEach((node, i) => {
      if (kinds.has(node.id)) {
        ctx.addIssue({ code: 'custom', path: ['nodes', i, 'id'], message: 'duplicate node' });
      }
      kinds.set(node.id, node.kind);
      if (payload.mode === 'agents' && node.kind === 'channel') {
        ctx.addIssue({
          code: 'custom',
          path: ['nodes', i],
          message: 'channel node in agents mode',
        });
      }
    });
    const expect = (id: string, kind: 'agent' | 'channel', path: (string | number)[]) => {
      if (kinds.get(id) !== kind) {
        ctx.addIssue({ code: 'custom', path, message: `endpoint has no ${kind} node` });
      }
    };
    payload.edges.forEach((edge, i) => {
      if (edge.kind === 'transmission') {
        expect(edge.from, 'agent', ['edges', i, 'from']);
        expect(edge.to, 'agent', ['edges', i, 'to']);
      } else {
        if (payload.mode === 'agents') {
          ctx.addIssue({
            code: 'custom',
            path: ['edges', i],
            message: 'access edge in agents mode',
          });
        }
        expect(edge.agent, 'agent', ['edges', i, 'agent']);
        expect(edge.channel, 'channel', ['edges', i, 'channel']);
      }
    });
  });

export type TopologyPayload = z.output<typeof topologyPayload>;
export type TopologyNode = TopologyPayload['nodes'][number];
export type AgentNode = Extract<TopologyNode, { kind: 'agent' }>;
export type ChannelNode = Extract<TopologyNode, { kind: 'channel' }>;
export type TopologyEdge = TopologyPayload['edges'][number];
export type TransmissionEdge = Extract<TopologyEdge, { kind: 'transmission' }>;
export type AccessEdge = Extract<TopologyEdge, { kind: 'access' }>;
