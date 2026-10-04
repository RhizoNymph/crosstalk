import { describe, expect, it } from 'vitest';
import type { RouteCode } from '../src/shared/route.ts';
import { parseRouteCode, routeKindOf } from '../src/shared/route.ts';
import {
  decodeBrushSelection,
  decodeProjectionSelection,
  decodeTopologySelection,
  encodeBrushSelection,
  encodeProjectionSelection,
  encodeTopologySelection,
  formatCoordinate,
  type TopologySelection,
} from '../src/shared/selection.ts';
import type { Ulid } from '../src/shared/ulid.ts';

const A = '01K6HB7H0002GG002YXM000001' as Ulid;
const B = '01K6HB7H0002GG002YXM000002' as Ulid;
const CH = '01K6HB7H000320002YXM000001';

describe('topology selection', () => {
  const cases: [TopologySelection, string][] = [
    [{ kind: 'none' }, ''],
    [{ kind: 'agent', id: A }, `agent:${A}`],
    [{ kind: 'channel', id: B }, `channel:${B}`],
    [{ kind: 'edge', from: A, to: B, route: 'dl.p2c' as RouteCode }, `edge:${A}:${B}:dl.p2c`],
    [{ kind: 'edge', from: A, to: B, route: `ch.${CH}` as RouteCode }, `edge:${A}:${B}:ch.${CH}`],
  ];

  it('encodes and decodes every kind', () => {
    for (const [selection, text] of cases) {
      expect(encodeTopologySelection(selection)).toBe(text);
      const decoded = decodeTopologySelection(text);
      expect(decoded.ok && decoded.value).toEqual(selection);
    }
  });

  it('keeps colons inside tool names', () => {
    const text = `edge:${A}:${B}:dr.tool.mcp:wiki:read`;
    const decoded = decodeTopologySelection(text);
    expect(decoded.ok && decoded.value.kind === 'edge' && decoded.value.route).toBe(
      'dr.tool.mcp:wiki:read',
    );
    expect(decoded.ok && encodeTopologySelection(decoded.value)).toBe(text);
  });

  it('rejects malformed values', () => {
    for (const text of [
      'agent:',
      'agent:nope',
      `agent:${A}:extra`,
      `edge:${A}:${B}`,
      `edge:${A}:${B}:teleport`,
      `edge:${A}:short:un`,
      'node:x',
      `ch.${CH}`,
    ]) {
      expect(decodeTopologySelection(text).ok, text).toBe(false);
    }
  });
});

describe('route codes', () => {
  it('match ui/src/url/route.rs', () => {
    expect(parseRouteCode('dl.c2p')).not.toBeNull();
    expect(parseRouteCode('dr.tool.')).toBeNull();
    expect(parseRouteCode('ch.nope')).toBeNull();
    expect(routeKindOf(parseRouteCode(`ch.${CH}`) as RouteCode)).toBe('channel');
    expect(routeKindOf(parseRouteCode('dr.sys') as RouteCode)).toBe('direct');
    expect(routeKindOf(parseRouteCode('un') as RouteCode)).toBe('unobserved');
  });
});

describe('projection selection', () => {
  it('rounds lasso coordinates to four decimals without trailing zeros', () => {
    expect(formatCoordinate(1.23456789)).toBe('1.2346');
    expect(formatCoordinate(2)).toBe('2');
    expect(formatCoordinate(-0.00001)).toBe('0');
    expect(formatCoordinate(-3.1)).toBe('-3.1');
    const text = encodeProjectionSelection({
      kind: 'lasso',
      polygon: [
        [0, 0],
        [1.00004, 0],
        [0.5, -2.123449],
      ],
    });
    expect(text).toBe('lasso:0,0;1,0;0.5,-2.1234');
  });

  it('round-trips lasso and point values', () => {
    for (const text of ['lasso:0,0;1,0;0.5,-2.1234', `point:${A}`, '']) {
      const decoded = decodeProjectionSelection(text);
      expect(decoded.ok && encodeProjectionSelection(decoded.value)).toBe(text);
    }
  });

  it('rejects malformed lassos', () => {
    for (const text of [
      'lasso:0,0;1,1',
      'lasso:0,0;1,x;2,2',
      'lasso:0,0,0;1,1;2,2',
      'lasso:1e3,0;1,1;2,2',
      `lasso:${Array.from({ length: 49 }, (_, i) => `${i},${i % 2}`).join(';')}`,
      'point:abc',
      'circle:1',
    ]) {
      expect(decodeProjectionSelection(text).ok, text).toBe(false);
    }
  });
});

describe('brush selection', () => {
  it('round-trips a window', () => {
    const text = '2026-10-02T09:00:00Z/2026-10-02T17:00:00Z';
    const decoded = decodeBrushSelection(text);
    expect(decoded.ok && decoded.value).toEqual({
      kind: 'window',
      from: '2026-10-02T09:00:00Z',
      to: '2026-10-02T17:00:00Z',
    });
    expect(decoded.ok && encodeBrushSelection(decoded.value)).toBe(text);
  });

  it('rejects inverted, offset and malformed windows', () => {
    for (const text of [
      '2026-10-02T17:00:00Z/2026-10-02T09:00:00Z',
      '2026-10-02T09:00:00Z/2026-10-02T09:00:00Z',
      '2026-10-02T09:00:00+01:00/2026-10-02T17:00:00Z',
      '2026-10-02T09:00:00Z',
      'a/b/c',
    ]) {
      expect(decodeBrushSelection(text).ok, text).toBe(false);
    }
  });
});
