import { describe, expect, it } from 'vitest';
import { decodeProjection, type Projection } from '../src/payloads/projection.ts';
import { topologyPayload } from '../src/payloads/topology.ts';
import { colorPoints, highlightMask, parseColorBy } from '../src/projection/colors.ts';
import { luminance, mix, parseColor, type Rgba, toCss, toHex } from '../src/shared/color.ts';
import { type Theme, themeFrom } from '../src/shared/theme.ts';
import { buildModel } from '../src/topology/model.ts';
import { dimmedNode, edgeColor, nodeColor } from '../src/topology/style.ts';
import { fixtureBuffer, fixtureJson } from './fixtures.ts';

const WHITE: Rgba = [255, 255, 255, 1];
const INK: Rgba = [24, 24, 27, 1];

function theme(
  tokens: Record<string, string> = {},
  surface: Rgba = WHITE,
  text: Rgba = INK,
): Theme {
  return themeFrom({ token: (name) => tokens[name] ?? '', text, surface, font: 'system-ui' });
}

function projection(): Projection {
  const result = decodeProjection(fixtureBuffer('projection.bin'));
  if (!result.ok) throw new Error(result.error.message);
  return result.value;
}

describe('colour parsing', () => {
  it('parses hex and rgb forms', () => {
    expect(parseColor('#2563eb')).toEqual([37, 99, 235, 1]);
    expect(parseColor(' #fff ')).toEqual([255, 255, 255, 1]);
    expect(parseColor('#00000080')?.[3]).toBeCloseTo(0.5, 2);
    expect(parseColor('rgb(1, 2, 3)')).toEqual([1, 2, 3, 1]);
    expect(parseColor('rgba(1, 2, 3, 0.25)')).toEqual([1, 2, 3, 0.25]);
    expect(parseColor('rgb(1 2 3 / 50%)')).toEqual([1, 2, 3, 0.5]);
    expect(parseColor('oklch(0.5 0.1 200)')).toBeNull();
  });

  it('mixes and formats', () => {
    expect(mix([0, 0, 0, 1], [255, 255, 255, 1], 0.5)).toEqual([127.5, 127.5, 127.5, 1]);
    expect(toCss([1.4, 2.6, 3, 0.5])).toBe('rgba(1, 3, 3, 0.5)');
    expect(toHex([37, 99, 235, 1])).toBe('#2563eb');
    expect(luminance([255, 255, 255, 1])).toBeCloseTo(1);
  });
});

describe('theme', () => {
  it('reads route colours from the page tokens', () => {
    const t = theme({ '--color-route-channel': '#112233' });
    expect(t.route.channel).toEqual([17, 34, 51, 1]);
  });

  it('falls back to the app.css values when a token is missing or invalid', () => {
    const t = theme({ '--color-route-direct': 'not a colour' });
    expect(toHex(t.route.direct)).toBe('#059669');
    expect(toHex(t.route.delegation)).toBe('#7c3aed');
    expect(toHex(t.policy.unreviewed)).toBe('#d97706');
  });

  it('detects dark mode from the surface and picks the dark series', () => {
    const light = theme();
    const dark = theme({}, [9, 9, 11, 1], [244, 244, 245, 1]);
    expect(light.dark).toBe(false);
    expect(dark.dark).toBe(true);
    expect(toHex(light.series[0] ?? WHITE)).toBe('#2a78d6');
    expect(toHex(dark.series[0] ?? WHITE)).toBe('#3987e5');
    expect(luminance(dark.agent)).toBeGreaterThan(luminance(light.agent));
  });
});

describe('topology colours', () => {
  const model = buildModel(topologyPayload.parse(fixtureJson('topology-channels.json')), false);
  const t = theme();

  it('colours edges by route kind and channels by policy', () => {
    for (const edge of model.edges) {
      const route = t.route[edge.routeKind];
      const color = edgeColor(edge, t);
      expect(color[3]).toBe(1);
      // Pre-mixed towards the surface, so closer to the route colour than to white.
      const distance = (a: Rgba, b: Rgba) => Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
      expect(distance(color, route)).toBeLessThan(distance(color, WHITE));
    }
    for (const node of model.nodes) {
      if (node.kind === 'channel') expect(nodeColor(node, t)).toEqual(t.policy[node.policy]);
      else expect(nodeColor(node, t)[3]).toBe(1);
    }
  });

  it('dims towards the surface, opaquely', () => {
    const node = model.nodes[0];
    if (node === undefined) throw new Error('no node');
    const dim = dimmedNode(nodeColor(node, t), t);
    expect(dim[3]).toBe(1);
    expect(luminance(dim)).toBeGreaterThan(luminance(nodeColor(node, t)));
  });
});

describe('projection colours', () => {
  const t = theme();

  it('parses colour-by with a topic default', () => {
    expect(parseColorBy('sender')).toBe('sender');
    expect(parseColorBy('nonsense')).toBe('topic');
    expect(parseColorBy(undefined)).toBe('topic');
  });

  it('colours by topic with fixed slots and an outlier category', () => {
    const p = projection();
    const coloring = colorPoints(p, 'topic', t);
    expect(coloring.palette.length).toBe(t.series.length + 2);
    for (let i = 0; i < p.ids.length; i++) {
      const topic = p.topic[i];
      const category = coloring.categories[i];
      // Six topics fit in eight slots: slot = table index, outliers last.
      expect(category).toBe(topic === 0xffffffff ? t.series.length + 1 : topic);
    }
    expect(coloring.legend.at(-1)?.label).toBe('outlier');
    const counts = coloring.legend.map((e) => e.count);
    expect(counts.reduce((a, b) => a + b, 0)).toBe(p.ids.length);
  });

  it('keeps an entity on its colour whatever its rank', () => {
    const p = projection();
    const bySender = colorPoints(p, 'sender', t);
    const planner = bySender.legend.find((e) => e.label === 'planner');
    expect(planner?.color).toEqual(t.series[0]);
  });

  it('folds categories past the slots into "other"', () => {
    const p = projection();
    const narrow = { ...t, series: t.series.slice(0, 3) };
    const coloring = colorPoints(p, 'sender', narrow);
    const other = coloring.legend.find((e) => e.label.startsWith('other'));
    expect(other?.label).toBe('other (5)');
    expect(coloring.legend.length).toBe(4);
  });

  it('colours by route with the route tokens', () => {
    const p = projection();
    const coloring = colorPoints(p, 'route', t);
    expect(coloring.palette).toEqual([
      t.route.channel,
      t.route.delegation,
      t.route.direct,
      t.route.unobserved,
    ]);
    expect(Array.from(coloring.categories)).toEqual(Array.from(p.route));
  });

  it('marks hidden topic labels', () => {
    const p = projection();
    const hidden: Projection = {
      ...p,
      header: { ...p.header, topics: p.header.topics.map((topic) => ({ ...topic, label: null })) },
    };
    const coloring = colorPoints(hidden, 'topic', t);
    expect(coloring.legend.filter((e) => e.hidden === true).length).toBe(6);
  });

  it('builds a highlight mask from a ULID list', () => {
    const ids = ['A', 'B', 'C'];
    expect(highlightMask(ids, '')).toBeNull();
    expect(highlightMask(ids, ' , ')).toBeNull();
    expect(Array.from(highlightMask(ids, 'C, A') ?? [])).toEqual([1, 0, 1]);
  });
});
