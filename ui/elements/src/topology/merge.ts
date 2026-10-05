/**
 * Merging a refetched topology into the drawn graph. Pure: no DOM, no WebGL.
 *
 * Nodes already drawn keep their positions. A new node is placed at the
 * centroid of its placed neighbours, with a small jitter from its id (so the
 * same data always gives the same picture); a node with no placed neighbour
 * goes near the middle of the drawing. Nodes and edges missing from the new
 * model are removed.
 */

import { unitHash } from '../shared/hash.ts';
import type { Position } from './layout.ts';
import type { GraphModel } from './model.ts';

export interface MergePlan {
  /** Where every node of the new model goes. */
  readonly positions: ReadonlyMap<string, Position>;
  /** Nodes that were not drawn before. */
  readonly addedNodes: ReadonlySet<string>;
  /** Drawn nodes the new model no longer has. */
  readonly removedNodes: readonly string[];
  /** Drawn edges the new model no longer has. */
  readonly removedEdges: readonly string[];
}

interface Bounds {
  readonly cx: number;
  readonly cy: number;
  readonly span: number;
}

function boundsOf(positions: Iterable<Position>): Bounds {
  let [minX, minY, maxX, maxY] = [
    Number.POSITIVE_INFINITY,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
  ];
  for (const p of positions) {
    minX = Math.min(minX, p.x);
    minY = Math.min(minY, p.y);
    maxX = Math.max(maxX, p.x);
    maxY = Math.max(maxY, p.y);
  }
  if (!Number.isFinite(minX)) return { cx: 0, cy: 0, span: 100 };
  return {
    cx: (minX + maxX) / 2,
    cy: (minY + maxY) / 2,
    span: Math.max(1, maxX - minX, maxY - minY),
  };
}

function jitter(id: string, radius: number): Position {
  const angle = 2 * Math.PI * unitHash(`${id}#jitter`);
  const distance = radius * (0.5 + 0.5 * unitHash(`${id}#jitter-r`));
  return { x: distance * Math.cos(angle), y: distance * Math.sin(angle) };
}

export function planMerge(
  drawnNodes: ReadonlyMap<string, Position>,
  drawnEdges: Iterable<string>,
  model: GraphModel,
): MergePlan {
  const bounds = boundsOf(drawnNodes.values());
  const positions = new Map<string, Position>();
  const pending: string[] = [];
  for (const node of model.nodes) {
    const kept = drawnNodes.get(node.id);
    if (kept === undefined) pending.push(node.id);
    else positions.set(node.id, kept);
  }
  const addedNodes = new Set(pending);

  const neighbours = new Map<string, string[]>();
  for (const edge of model.edges) {
    if (edge.source === edge.target) continue;
    neighbours.set(edge.source, [...(neighbours.get(edge.source) ?? []), edge.target]);
    neighbours.set(edge.target, [...(neighbours.get(edge.target) ?? []), edge.source]);
  }

  // Place nodes next to placed neighbours first; a chain of new nodes is
  // placed outward from the drawing, one link per round.
  let remaining = [...pending].sort();
  while (remaining.length > 0) {
    const next: string[] = [];
    const placedThisRound = new Map<string, Position>();
    for (const id of remaining) {
      const near = (neighbours.get(id) ?? []).flatMap((n) => {
        const at = positions.get(n);
        return at === undefined ? [] : [at];
      });
      if (near.length === 0) {
        next.push(id);
        continue;
      }
      const cx = near.reduce((sum, p) => sum + p.x, 0) / near.length;
      const cy = near.reduce((sum, p) => sum + p.y, 0) / near.length;
      const offset = jitter(id, bounds.span * 0.06);
      placedThisRound.set(id, { x: cx + offset.x, y: cy + offset.y });
    }
    for (const [id, at] of placedThisRound) positions.set(id, at);
    if (placedThisRound.size === 0) {
      for (const id of next) {
        const offset = jitter(id, bounds.span * 0.25);
        positions.set(id, { x: bounds.cx + offset.x, y: bounds.cy + offset.y });
      }
      break;
    }
    remaining = next;
  }

  const nodeIds = new Set(model.nodes.map((n) => n.id as string));
  const edgeKeys = new Set(model.edges.map((e) => e.key));
  return {
    positions,
    addedNodes,
    removedNodes: [...drawnNodes.keys()].filter((id) => !nodeIds.has(id)),
    removedEdges: [...drawnEdges].filter((key) => !edgeKeys.has(key)),
  };
}
