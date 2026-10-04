/**
 * Tooltip content for nodes and edges, as lines of text. Harness claims are
 * shown as "claims X", never as the agent's identity.
 */

import type { AgentNode } from '../payloads/topology.ts';
import { formatBytes, formatCount, formatShare, formatUtc } from '../shared/format.ts';
import { describeRoute } from '../shared/route.ts';
import { shortUlid, type Ulid } from '../shared/ulid.ts';
import type { GraphEdge, GraphNode } from './model.ts';

export type LineStyle = 'title' | 'plain' | 'dim' | 'claim';
export interface TooltipLine {
  readonly text: string;
  readonly style: LineStyle;
  /** Shown on hover of the line, e.g. a claim's full user agent. */
  readonly detail?: string;
}

const line = (text: string, style: LineStyle = 'plain'): TooltipLine => ({ text, style });

const DETECTION_NAMES: Readonly<Record<string, string>> = {
  awaitingTraffic: 'awaiting traffic',
};

function claimLines(agent: AgentNode): TooltipLine[] {
  return agent.claims.map((c) => ({
    text: `claims ${c.harness}${c.version === null ? '' : ` ${c.version}`} · seen ${formatUtc(c.lastSeen)}`,
    style: 'claim',
    detail: c.userAgent,
  }));
}

export function nodeTooltip(node: GraphNode): TooltipLine[] {
  if (node.kind === 'channel') {
    const c = node.channel;
    return [
      line(c.name, 'title'),
      line(`channel · ${c.origin} · ${DETECTION_NAMES[c.detection] ?? c.detection}`, 'dim'),
      line(`policy ${c.policy} · ${formatCount(c.volume)} accesses`),
      line(c.id, 'dim'),
    ];
  }
  const [head, ...collapsed] = node.members;
  if (head === undefined) return [line(node.label, 'title')];
  const lines = [
    line(head.name, 'title'),
    line(
      `${head.state} agent · in ${formatCount(head.transmissionsIn)} · out ${formatCount(head.transmissionsOut)}`,
    ),
    ...claimLines(head),
  ];
  if (collapsed.length > 0) {
    const names = collapsed.map((m) => m.name);
    const shown = names.slice(0, 4).join(', ') + (names.length > 4 ? ', …' : '');
    lines.push(line(`+${collapsed.length} sub-agents: ${shown}`, 'dim'));
    lines.push(line(`combined volume ${formatCount(node.volume)}`, 'dim'));
  }
  lines.push(line(head.id, 'dim'));
  return lines;
}

export function edgeTooltip(edge: GraphEdge, names: ReadonlyMap<Ulid, string>): TooltipLine[] {
  const name = (id: Ulid) => names.get(id) ?? shortUlid(id);
  if (edge.kind === 'access') {
    const head = edge.members[0];
    if (head === undefined) return [];
    const accesses = edge.members.reduce((sum, m) => sum + m.accesses, 0);
    return [
      line(
        `${name(head.agent)} ${head.op === 'write' ? 'writes' : 'reads'} ${name(head.channel)}`,
        'title',
      ),
      line(`${formatCount(accesses)} accesses · ${formatShare(edge.share)} of accesses`),
    ];
  }
  const head = edge.members[0];
  if (head === undefined) return [];
  const transmissions = edge.members.reduce((sum, m) => sum + m.transmissions, 0);
  const bytes = edge.members.reduce((sum, m) => sum + m.matchedBytes, 0);
  const lines = [
    line(`${name(edge.source)} → ${name(edge.target)}`, 'title'),
    line(describeRoute(head.route), 'dim'),
    line(
      `${formatShare(edge.share)} · ${formatCount(transmissions)} transmissions · ${formatBytes(bytes)} matched`,
    ),
  ];
  if (head.routeKind === 'channel')
    lines.push(line(`via ${name(head.route.slice(3) as Ulid)}`, 'dim'));
  if (edge.members.length > 1) {
    lines.push(line(`${edge.members.length} edges merged; click selects the largest`, 'dim'));
  }
  return lines;
}
