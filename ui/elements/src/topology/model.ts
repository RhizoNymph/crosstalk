/**
 * The drawn graph, derived from a topology payload. Pure: no DOM, no WebGL.
 *
 * - Agent nodes are sized by volume (transmissions in and out); channel
 *   nodes by accesses, on their own scale.
 * - Edge width is proportional to share; transmission and access shares are
 *   normalised separately, as the server does.
 * - With `collapse`, every sub-agent is drawn as its top-most ancestor that
 *   is in the payload; edges inside a collapsed node disappear and parallel
 *   edges merge (by route code, or by channel and operation).
 */

import type {
  AccessEdge,
  AgentNode,
  ChannelNode,
  Policy,
  TopologyPayload,
  TransmissionEdge,
} from '../payloads/topology.ts';
import type { RouteKind } from '../shared/route.ts';
import type { TopologySelection } from '../shared/selection.ts';
import type { Ulid } from '../shared/ulid.ts';

export const NODE_SIZE = { min: 3.5, max: 15 } as const;
export const EDGE_WIDTH = { min: 0.75, max: 7 } as const;

export interface AgentGraphNode {
  readonly kind: 'agent';
  readonly id: Ulid;
  readonly label: string;
  readonly volume: number;
  readonly size: number;
  /** The agent this node draws first, then any collapsed sub-agents. */
  readonly members: readonly AgentNode[];
  readonly provisional: boolean;
}

export interface ChannelGraphNode {
  readonly kind: 'channel';
  readonly id: Ulid;
  readonly label: string;
  readonly volume: number;
  readonly size: number;
  readonly policy: Policy;
  readonly channel: ChannelNode;
}

export type GraphNode = AgentGraphNode | ChannelGraphNode;

export interface TransmissionGraphEdge {
  readonly kind: 'transmission';
  readonly key: string;
  readonly source: Ulid;
  readonly target: Ulid;
  readonly routeKind: RouteKind;
  readonly share: number;
  readonly width: number;
  /** The payload edges drawn here, heaviest first. */
  readonly members: readonly TransmissionEdge[];
}

export interface AccessGraphEdge {
  readonly kind: 'access';
  readonly key: string;
  /** Writes go agent → channel, reads channel → agent. */
  readonly source: Ulid;
  readonly target: Ulid;
  readonly routeKind: 'channel';
  readonly share: number;
  readonly width: number;
  readonly members: readonly AccessEdge[];
}

export type GraphEdge = TransmissionGraphEdge | AccessGraphEdge;

export interface GraphModel {
  readonly nodes: readonly GraphNode[];
  readonly edges: readonly GraphEdge[];
  /** Every agent id to the node that draws it. */
  readonly drawnAs: ReadonlyMap<Ulid, Ulid>;
}

/** Follows parents to the top-most ancestor present in `agents`. */
export function rootOf(id: Ulid, agents: ReadonlyMap<Ulid, AgentNode>): Ulid {
  let current = id;
  const seen = new Set<Ulid>([id]);
  for (;;) {
    const parent = agents.get(current)?.parent ?? null;
    if (parent === null || !agents.has(parent) || seen.has(parent)) return current;
    seen.add(parent);
    current = parent;
  }
}

function scaled(value: number, max: number, range: { min: number; max: number }): number {
  if (max <= 0) return range.min;
  return range.min + (range.max - range.min) * Math.sqrt(Math.max(0, value) / max);
}

function widthOf(share: number, maxShare: number): number {
  if (maxShare <= 0) return EDGE_WIDTH.min;
  return EDGE_WIDTH.min + (EDGE_WIDTH.max - EDGE_WIDTH.min) * (share / maxShare);
}

const byShareDesc = <E extends { share: number }>(a: E, b: E) => b.share - a.share;

export function buildModel(payload: TopologyPayload, collapse: boolean): GraphModel {
  const agents = new Map<Ulid, AgentNode>();
  const channels: ChannelNode[] = [];
  for (const node of payload.nodes) {
    if (node.kind === 'agent') agents.set(node.id, node);
    else channels.push(node);
  }

  const drawnAs = new Map<Ulid, Ulid>();
  for (const id of agents.keys()) drawnAs.set(id, collapse ? rootOf(id, agents) : id);

  const groups = new Map<Ulid, AgentNode[]>();
  for (const [id, root] of drawnAs) {
    const group = groups.get(root) ?? [];
    const agent = agents.get(id);
    if (agent === undefined) continue;
    if (id === root) group.unshift(agent);
    else group.push(agent);
    groups.set(root, group);
  }

  const agentVolumes = [...groups.values()].map((members) =>
    members.reduce((sum, m) => sum + m.volume, 0),
  );
  const maxAgentVolume = Math.max(0, ...agentVolumes);
  const maxChannelVolume = Math.max(0, ...channels.map((c) => c.volume));

  const nodes: GraphNode[] = [];
  for (const [root, members] of groups) {
    const head = members[0];
    if (head === undefined) continue;
    const volume = members.reduce((sum, m) => sum + m.volume, 0);
    const extra = members.length - 1;
    nodes.push({
      kind: 'agent',
      id: root,
      label: extra > 0 ? `${head.name} +${extra}` : head.name,
      volume,
      size: scaled(volume, maxAgentVolume, NODE_SIZE),
      members,
      provisional: head.state === 'provisional',
    });
  }
  for (const channel of channels) {
    nodes.push({
      kind: 'channel',
      id: channel.id,
      label: channel.name,
      volume: channel.volume,
      size: scaled(channel.volume, maxChannelVolume, NODE_SIZE),
      policy: channel.policy,
      channel,
    });
  }

  const transmissions = new Map<
    string,
    { source: Ulid; target: Ulid; members: TransmissionEdge[] }
  >();
  const accesses = new Map<string, { source: Ulid; target: Ulid; members: AccessEdge[] }>();
  for (const edge of payload.edges) {
    if (edge.kind === 'transmission') {
      const source = drawnAs.get(edge.from) ?? edge.from;
      const target = drawnAs.get(edge.to) ?? edge.to;
      if (source === target && collapse && edge.from !== edge.to) continue;
      const key = `t:${source}>${target}:${edge.route}`;
      const group = transmissions.get(key) ?? { source, target, members: [] };
      group.members.push(edge);
      transmissions.set(key, group);
    } else {
      const agent = drawnAs.get(edge.agent) ?? edge.agent;
      const [source, target] = edge.op === 'write' ? [agent, edge.channel] : [edge.channel, agent];
      const key = `a:${source}>${target}`;
      const group = accesses.get(key) ?? { source, target, members: [] };
      group.members.push(edge);
      accesses.set(key, group);
    }
  }

  const sumShares = (members: readonly { share: number }[]) =>
    members.reduce((sum, m) => sum + m.share, 0);
  const maxTransmissionShare = Math.max(
    0,
    ...[...transmissions.values()].map((g) => sumShares(g.members)),
  );
  const maxAccessShare = Math.max(0, ...[...accesses.values()].map((g) => sumShares(g.members)));

  const edges: GraphEdge[] = [];
  for (const [key, group] of transmissions) {
    const members = [...group.members].sort(byShareDesc);
    const head = members[0];
    if (head === undefined) continue;
    const share = sumShares(members);
    edges.push({
      kind: 'transmission',
      key,
      source: group.source,
      target: group.target,
      routeKind: head.routeKind,
      share,
      width: widthOf(share, maxTransmissionShare),
      members,
    });
  }
  for (const [key, group] of accesses) {
    const share = sumShares(group.members);
    edges.push({
      kind: 'access',
      key,
      source: group.source,
      target: group.target,
      routeKind: 'channel',
      share,
      width: widthOf(share, maxAccessShare),
      members: [...group.members].sort(byShareDesc),
    });
  }

  return { nodes, edges, drawnAs };
}

/**
 * What clicking a drawn edge selects. A transmission edge selects its
 * heaviest payload edge (exact unless sub-agents were collapsed into it); an
 * access edge selects its channel.
 */
export function edgeSelection(edge: GraphEdge): TopologySelection {
  if (edge.kind === 'access') {
    const channel = edge.members[0]?.channel;
    return channel === undefined ? { kind: 'none' } : { kind: 'channel', id: channel };
  }
  const head = edge.members[0];
  return head === undefined
    ? { kind: 'none' }
    : { kind: 'edge', from: head.from, to: head.to, route: head.route };
}

export function nodeSelection(node: GraphNode): TopologySelection {
  return node.kind === 'agent' ? { kind: 'agent', id: node.id } : { kind: 'channel', id: node.id };
}

export interface Highlight {
  readonly nodes: ReadonlySet<Ulid>;
  readonly edges: ReadonlySet<string>;
}

/** What a selection lights up, or `null` when nothing is selected or found. */
export function highlightOf(model: GraphModel, selection: TopologySelection): Highlight | null {
  const nodes = new Set<Ulid>();
  const edges = new Set<string>();
  switch (selection.kind) {
    case 'none':
      return null;
    case 'edge': {
      for (const edge of model.edges) {
        if (
          edge.kind === 'transmission' &&
          edge.members.some(
            (m) =>
              m.from === selection.from && m.to === selection.to && m.route === selection.route,
          )
        ) {
          edges.add(edge.key);
          nodes.add(edge.source);
          nodes.add(edge.target);
        }
      }
      break;
    }
    case 'agent':
    case 'channel': {
      const id =
        selection.kind === 'agent'
          ? (model.drawnAs.get(selection.id) ?? selection.id)
          : selection.id;
      if (!model.nodes.some((n) => n.id === id)) return null;
      nodes.add(id);
      for (const edge of model.edges) {
        if (edge.source === id || edge.target === id) {
          edges.add(edge.key);
          nodes.add(edge.source);
          nodes.add(edge.target);
        }
      }
      break;
    }
  }
  return edges.size === 0 && nodes.size === 0 ? null : { nodes, edges };
}
