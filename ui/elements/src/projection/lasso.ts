/**
 * Lasso polygons in projection (data) coordinates. The polygon a page puts
 * in its URL is simplified to at most `MAX_LASSO_VERTICES` vertices and
 * rounded to `LASSO_DECIMALS`; membership is even-odd point-in-polygon over
 * that rounded polygon, which is what the server resolves too.
 */

import { formatCoordinate, MAX_LASSO_VERTICES, type Vertex } from '../shared/selection.ts';
import { type Transform, toData } from './transform.ts';

/** Even-odd ray casting. Points exactly on an edge may fall either way. */
export function pointInPolygon(x: number, y: number, polygon: readonly Vertex[]): boolean {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const a = polygon[i];
    const b = polygon[j];
    if (a === undefined || b === undefined) continue;
    const [xi, yi] = a;
    const [xj, yj] = b;
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}

/** Indexes of the points inside `polygon`. */
export function pointsInPolygon(
  xs: ArrayLike<number>,
  ys: ArrayLike<number>,
  polygon: readonly Vertex[],
): number[] {
  if (polygon.length < 3) return [];
  let minX = Number.POSITIVE_INFINITY;
  let maxX = Number.NEGATIVE_INFINITY;
  let minY = Number.POSITIVE_INFINITY;
  let maxY = Number.NEGATIVE_INFINITY;
  for (const [x, y] of polygon) {
    minX = Math.min(minX, x);
    maxX = Math.max(maxX, x);
    minY = Math.min(minY, y);
    maxY = Math.max(maxY, y);
  }
  const hits: number[] = [];
  for (let i = 0; i < xs.length; i++) {
    const x = xs[i] ?? Number.NaN;
    const y = ys[i] ?? Number.NaN;
    if (x < minX || x > maxX || y < minY || y > maxY) continue;
    if (pointInPolygon(x, y, polygon)) hits.push(i);
  }
  return hits;
}

function distanceToSegment(p: Vertex, a: Vertex, b: Vertex): number {
  const dx = b[0] - a[0];
  const dy = b[1] - a[1];
  const length2 = dx * dx + dy * dy;
  const t =
    length2 === 0
      ? 0
      : Math.max(0, Math.min(1, ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / length2));
  return Math.hypot(p[0] - (a[0] + t * dx), p[1] - (a[1] + t * dy));
}

/** Ramer–Douglas–Peucker over an open polyline. */
function rdp(points: readonly Vertex[], epsilon: number): Vertex[] {
  const first = points[0];
  const last = points[points.length - 1];
  if (points.length < 3 || first === undefined || last === undefined) return [...points];
  let index = 0;
  let max = 0;
  for (let i = 1; i < points.length - 1; i++) {
    const p = points[i];
    if (p === undefined) continue;
    const d = distanceToSegment(p, first, last);
    if (d > max) {
      max = d;
      index = i;
    }
  }
  if (max <= epsilon) return [first, last];
  const left = rdp(points.slice(0, index + 1), epsilon);
  const right = rdp(points.slice(index), epsilon);
  return [...left.slice(0, -1), ...right];
}

/**
 * Simplifies a closed polygon to at most `maxVertices` vertices: RDP with a
 * tolerance growing from 0.2% of the bounding diagonal, then even
 * decimation if still too long. Never returns fewer than three vertices
 * when given three or more.
 */
export function simplifyPolygon(
  polygon: readonly Vertex[],
  maxVertices = MAX_LASSO_VERTICES,
): Vertex[] {
  const open = [...polygon];
  const head = open[0];
  const tail = open[open.length - 1];
  if (
    head !== undefined &&
    tail !== undefined &&
    open.length > 1 &&
    head[0] === tail[0] &&
    head[1] === tail[1]
  ) {
    open.pop();
  }
  if (open.length <= 3) return open;
  const xs = open.map((p) => p[0]);
  const ys = open.map((p) => p[1]);
  const diagonal = Math.hypot(Math.max(...xs) - Math.min(...xs), Math.max(...ys) - Math.min(...ys));
  let epsilon = diagonal * 0.002;
  let simplified = open;
  for (let round = 0; round < 12; round++) {
    // Close the ring for RDP so the seam is simplified like any other part.
    const ring = rdp([...open, open[0] as Vertex], epsilon).slice(0, -1);
    simplified = ring.length >= 3 ? ring : simplified;
    if (simplified.length <= maxVertices) break;
    epsilon *= 1.8;
  }
  if (simplified.length > maxVertices) {
    const step = simplified.length / maxVertices;
    simplified = Array.from(
      { length: maxVertices },
      (_, i) => simplified[Math.floor(i * step)] as Vertex,
    );
  }
  return simplified;
}

/** Rounds a vertex as the value carries it. */
export function roundVertex(vertex: Vertex): Vertex {
  return [Number(formatCoordinate(vertex[0])), Number(formatCoordinate(vertex[1]))];
}

/**
 * The polygon a lasso drawn in normalised space selects, in data
 * coordinates: mapped back, simplified, rounded, with duplicate vertices
 * (after rounding) removed. `null` when it has fewer than three vertices.
 */
export function lassoPolygon(normalized: readonly Vertex[], transform: Transform): Vertex[] | null {
  const data = normalized.map(([x, y]) => toData(transform, x, y) as Vertex);
  const same = (a: Vertex, b: Vertex) => a[0] === b[0] && a[1] === b[1];
  const unique: Vertex[] = [];
  for (const vertex of simplifyPolygon(data).map(roundVertex)) {
    const previous = unique[unique.length - 1];
    if (previous === undefined || !same(previous, vertex)) unique.push(vertex);
  }
  const first = unique[0];
  const last = unique[unique.length - 1];
  if (unique.length > 1 && first !== undefined && last !== undefined && same(first, last)) {
    unique.pop();
  }
  return unique.length >= 3 ? unique : null;
}

/**
 * Lasso coordinates as regl-scatterplot reports them: pairs, or a flat list.
 */
export function vertexList(coordinates: unknown): Vertex[] {
  if (!Array.isArray(coordinates)) return [];
  const out: Vertex[] = [];
  if (coordinates.every((c) => typeof c === 'number')) {
    for (let i = 0; i + 1 < coordinates.length; i += 2) {
      out.push([coordinates[i] as number, coordinates[i + 1] as number]);
    }
    return out;
  }
  for (const c of coordinates) {
    if (Array.isArray(c) && typeof c[0] === 'number' && typeof c[1] === 'number') {
      out.push([c[0], c[1]]);
    }
  }
  return out;
}
