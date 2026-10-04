/** Colours and labels of the drawn graph. Pure, given a theme. */

import { mix, type Rgba, withAlpha } from '../shared/color.ts';
import type { Theme } from '../shared/theme.ts';
import type { GraphEdge, GraphNode } from './model.ts';

/** Diamonds cover half a disc's area at the same size; enlarge to balance. */
export const DIAMOND_SCALE = 1.35;
export const MAX_LABEL_CHARS = 28;

export function nodeColor(node: GraphNode, theme: Theme): Rgba {
  if (node.kind === 'channel') return theme.policy[node.policy];
  return node.provisional ? mix(theme.agent, theme.surface, 0.35) : theme.agent;
}

// Every colour handed to sigma is opaque, pre-mixed with the surface: sigma
// blends as if colours were premultiplied, so translucent ones wash out.

export function edgeColor(edge: GraphEdge, theme: Theme): Rgba {
  const route = theme.route[edge.routeKind];
  return withAlpha(mix(route, theme.surface, edge.kind === 'access' ? 0.35 : 0.12), 1);
}

/** A node colour pushed back towards the surface, for unselected nodes. */
export function dimmedNode(color: Rgba, theme: Theme): Rgba {
  return withAlpha(mix(color, theme.surface, 0.72), 1);
}

/** An edge colour pushed back towards the surface, for unselected edges. */
export function dimmedEdge(color: Rgba, theme: Theme): Rgba {
  return withAlpha(mix(color, theme.surface, 0.8), 1);
}

/** Shortens long names in the middle, keeping both ends readable. */
export function shortLabel(text: string, max = MAX_LABEL_CHARS): string {
  if (text.length <= max) return text;
  const head = Math.ceil((max - 1) * 0.6);
  const tail = max - 1 - head;
  return `${text.slice(0, head)}…${text.slice(text.length - tail)}`;
}
