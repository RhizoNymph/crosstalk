/**
 * Curvature for edges that share a pair of nodes, so none is drawn on top
 * of another.
 *
 * A curved edge bends to the left of its direction of travel, so `a → b`
 * and `b → a` with the same curvature bend to opposite sides. Edges in the
 * same direction (several routes between one pair) get increasing
 * curvature. An edge with no other edge between its two nodes stays
 * straight (curvature 0).
 */

export const CURVATURE_STEP = 0.25;

export type EdgeEnds = { key: string; source: string; target: string };

export function curvatures(edges: readonly EdgeEnds[]): Map<string, number> {
  const byDirection = new Map<string, EdgeEnds[]>();
  for (const edge of edges) {
    const direction = `${edge.source}\u0000${edge.target}`;
    const group = byDirection.get(direction);
    if (group) group.push(edge);
    else byDirection.set(direction, [edge]);
  }

  const result = new Map<string, number>();
  for (const [direction, group] of byDirection) {
    const [source, target] = direction.split('\u0000');
    const reverse = byDirection.has(`${target}\u0000${source}`);
    const shared = reverse || group.length > 1;
    // Sort by key so the same payload always gives the same picture.
    const ordered = [...group].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    ordered.forEach((edge, index) => {
      result.set(edge.key, shared ? CURVATURE_STEP * (index + 1) : 0);
    });
  }
  return result;
}
