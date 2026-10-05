/**
 * Deterministic layout: the same payload always gives the same picture.
 * Initial positions come from hashing node ids (no randomness), nodes and
 * edges are inserted in sorted order, and ForceAtlas2 runs a fixed number
 * of synchronous iterations that depends only on the node count.
 */

import Graph from 'graphology';
import forceAtlas2 from 'graphology-layout-forceatlas2';
import { unitHash } from '../shared/hash.ts';
import type { GraphModel } from './model.ts';

export interface Position {
  readonly x: number;
  readonly y: number;
}

/** A position on a disc of radius `radius`, from the id alone. */
export function seedPosition(id: string, radius = 100): Position {
  const angle = 2 * Math.PI * unitHash(id);
  const distance = radius * Math.sqrt(unitHash(`${id}#r`));
  return { x: distance * Math.cos(angle), y: distance * Math.sin(angle) };
}

/** Fewer iterations for bigger graphs, so large layouts stay interactive. */
export function iterationsFor(nodeCount: number): number {
  return Math.round(Math.min(800, Math.max(120, 120_000 / Math.max(1, nodeCount))));
}

export function layout(model: GraphModel): Map<string, Position> {
  const graph = new Graph({ type: 'directed', multi: true, allowSelfLoops: false });
  const nodes = [...model.nodes].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  for (const node of nodes) {
    graph.addNode(node.id, { ...seedPosition(node.id), size: node.size });
  }
  const maxShare = Math.max(0, ...model.edges.map((e) => e.share));
  const edges = [...model.edges].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  for (const edge of edges) {
    if (edge.source === edge.target) continue;
    if (!graph.hasNode(edge.source) || !graph.hasNode(edge.target)) continue;
    graph.addEdgeWithKey(edge.key, edge.source, edge.target, {
      weight: maxShare > 0 ? 0.2 + (0.8 * edge.share) / maxShare : 1,
    });
  }
  const positions = forceAtlas2(graph, {
    iterations: iterationsFor(graph.order),
    getEdgeWeight: 'weight',
    settings: {
      ...forceAtlas2.inferSettings(graph),
      linLogMode: true,
      outboundAttractionDistribution: false,
      adjustSizes: false,
      scalingRatio: 6,
      gravity: 1.5,
      strongGravityMode: true,
      slowDown: 2,
    },
  });
  return new Map(Object.entries(positions));
}
