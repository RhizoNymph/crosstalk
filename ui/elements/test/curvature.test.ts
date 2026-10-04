import { describe, expect, it } from 'vitest';
import { CURVATURE_STEP, curvatures } from '../src/topology/curvature.ts';

const edge = (key: string, source: string, target: string) => ({ key, source, target });

describe('curvatures', () => {
  it('keeps a lone edge straight', () => {
    const result = curvatures([edge('e1', 'a', 'b'), edge('e2', 'b', 'c')]);
    expect(result.get('e1')).toBe(0);
    expect(result.get('e2')).toBe(0);
  });

  it('curves both directions of a reciprocal pair by the same amount', () => {
    const result = curvatures([edge('ab', 'a', 'b'), edge('ba', 'b', 'a')]);
    expect(result.get('ab')).toBe(CURVATURE_STEP);
    expect(result.get('ba')).toBe(CURVATURE_STEP);
  });

  it('fans out edges in the same direction', () => {
    const result = curvatures([edge('x2', 'a', 'b'), edge('x1', 'a', 'b'), edge('x3', 'a', 'b')]);
    expect([result.get('x1'), result.get('x2'), result.get('x3')]).toEqual([
      CURVATURE_STEP,
      CURVATURE_STEP * 2,
      CURVATURE_STEP * 3,
    ]);
  });

  it('does not depend on input order', () => {
    const edges = [edge('p', 'a', 'b'), edge('q', 'a', 'b'), edge('r', 'b', 'a')];
    const forward = curvatures(edges);
    const backward = curvatures([...edges].reverse());
    expect([...forward.entries()].sort()).toEqual([...backward.entries()].sort());
  });
});
