import { describe, expect, it } from 'vitest';
import {
  lassoPolygon,
  pointInPolygon,
  pointsInPolygon,
  simplifyPolygon,
  vertexList,
} from '../src/projection/lasso.ts';
import { fitTransform, toData, toNormalized } from '../src/projection/transform.ts';
import { MAX_LASSO_VERTICES, type Vertex } from '../src/shared/selection.ts';

const square: Vertex[] = [
  [0, 0],
  [10, 0],
  [10, 10],
  [0, 10],
];
// A "U": the notch between x 3..7 above y 3 is outside.
const u: Vertex[] = [
  [0, 0],
  [10, 0],
  [10, 10],
  [7, 10],
  [7, 3],
  [3, 3],
  [3, 10],
  [0, 10],
];

describe('point in polygon', () => {
  it('handles a convex polygon', () => {
    expect(pointInPolygon(5, 5, square)).toBe(true);
    expect(pointInPolygon(-1, 5, square)).toBe(false);
    expect(pointInPolygon(5, 11, square)).toBe(false);
  });

  it('handles a concave polygon', () => {
    expect(pointInPolygon(1.5, 8, u)).toBe(true);
    expect(pointInPolygon(5, 8, u)).toBe(false);
    expect(pointInPolygon(5, 1, u)).toBe(true);
  });

  it('works in data coordinates, negative ones included', () => {
    const polygon: Vertex[] = [
      [-4.5, 2.1],
      [-3.9, 3.4],
      [-4.8, 3.1],
    ];
    expect(pointInPolygon(-4.4, 2.9, polygon)).toBe(true);
    expect(pointInPolygon(-3.95, 2.2, polygon)).toBe(false);
  });

  it('collects the indexes inside', () => {
    const xs = new Float32Array([1, 5, 9, 11, 5]);
    const ys = new Float32Array([1, 8, 9, 5, 1]);
    expect(pointsInPolygon(xs, ys, u)).toEqual([0, 2, 4]);
    expect(pointsInPolygon(xs, ys, square.slice(0, 2))).toEqual([]);
  });
});

describe('simplification', () => {
  const circle = (n: number): Vertex[] =>
    Array.from({ length: n }, (_, i) => {
      const a = (i / n) * 2 * Math.PI;
      return [Math.cos(a) * 3, Math.sin(a) * 3] as Vertex;
    });

  it('bounds the vertex count and keeps at least three', () => {
    const simplified = simplifyPolygon(circle(500));
    expect(simplified.length).toBeLessThanOrEqual(MAX_LASSO_VERTICES);
    expect(simplified.length).toBeGreaterThanOrEqual(3);
    expect(simplifyPolygon(circle(500), 5).length).toBeLessThanOrEqual(5);
  });

  it('drops a closing duplicate and collinear points', () => {
    const line: Vertex[] = [
      [0, 0],
      [5, 0],
      [10, 0],
      [10, 10],
      [0, 10],
      [0, 0],
    ];
    expect(simplifyPolygon(line)).toEqual([
      [0, 0],
      [10, 0],
      [10, 10],
      [0, 10],
    ]);
  });

  it('keeps the selection of a simplified circle close to the original', () => {
    const points: [number, number][] = [];
    for (let x = -3; x <= 3; x += 0.25) for (let y = -3; y <= 3; y += 0.25) points.push([x, y]);
    const original = circle(400);
    const simplified = simplifyPolygon(original);
    const disagree = points.filter(
      ([x, y]) => pointInPolygon(x, y, original) !== pointInPolygon(x, y, simplified),
    );
    expect(disagree.length / points.length).toBeLessThan(0.03);
  });
});

describe('lasso polygon', () => {
  it('maps normalised lasso vertices back to data coordinates, rounded', () => {
    const transform = fitTransform([-5, 5], [-2, 2]);
    const data: Vertex[] = [
      [-1.23456, -1],
      [2.5, -1.5],
      [0.333333, 1.75],
    ];
    const normalized = data.map(([x, y]) => toNormalized(transform, x, y) as Vertex);
    expect(lassoPolygon(normalized, transform)).toEqual([
      [-1.2346, -1],
      [2.5, -1.5],
      [0.3333, 1.75],
    ]);
  });

  it('is null when rounding leaves fewer than three vertices', () => {
    const transform = fitTransform([0, 1], [0, 1]);
    expect(
      lassoPolygon(
        [
          [0.1, 0.1],
          [0.1000001, 0.1],
          [0.1, 0.1000001],
        ],
        transform,
      ),
    ).toBeNull();
  });

  it('accepts flat and paired coordinate lists', () => {
    expect(vertexList([1, 2, 3, 4])).toEqual([
      [1, 2],
      [3, 4],
    ]);
    expect(
      vertexList([
        [1, 2],
        [3, 4],
      ]),
    ).toEqual([
      [1, 2],
      [3, 4],
    ]);
    expect(vertexList('nope')).toEqual([]);
  });
});

describe('transform', () => {
  it('fits the longer axis into [-margin, margin] and inverts', () => {
    const t = fitTransform([-5, 5, 0], [-1, 1, 0], 0.9);
    expect(toNormalized(t, 5, 0)[0]).toBeCloseTo(0.9);
    expect(toNormalized(t, 0, 1)[1]).toBeCloseTo(0.18);
    const [x, y] = toData(t, ...toNormalized(t, 3.2, -0.7));
    expect(x).toBeCloseTo(3.2);
    expect(y).toBeCloseTo(-0.7);
  });

  it('handles a single point', () => {
    const t = fitTransform([2], [3]);
    expect(toNormalized(t, 2, 3)).toEqual([0, 0]);
  });
});
