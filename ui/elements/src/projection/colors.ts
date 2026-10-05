/**
 * Colouring projection points by a category. Route kinds use the theme's
 * route colours. Other categories use the eight categorical slots in fixed
 * order: the eight most frequent entries get a slot (in table order, so a
 * colour follows its entity, not its rank), the rest fold into "other", and
 * absent values (outliers, not channel-routed) are "none".
 */

import { NONE, type Projection } from '../payloads/projection.ts';
import { mix, type Rgba } from '../shared/color.ts';
import { ROUTE_KINDS } from '../shared/route.ts';
import type { Theme } from '../shared/theme.ts';
import { shortUlid } from '../shared/ulid.ts';

export const COLOR_BY = ['topic', 'sender', 'reader', 'route', 'channel'] as const;
export type ColorBy = (typeof COLOR_BY)[number];

export function parseColorBy(text: string | undefined): ColorBy {
  return COLOR_BY.find((c) => c === text) ?? 'topic';
}

export interface LegendEntry {
  readonly label: string;
  readonly color: Rgba;
  readonly count: number;
  /** The label is a placeholder because content is hidden. */
  readonly hidden?: boolean;
}

export interface Coloring {
  /** Per point, an index into `palette`. */
  readonly categories: Uint32Array;
  readonly palette: readonly Rgba[];
  /** Most frequent first; "other" and "none" last. */
  readonly legend: readonly LegendEntry[];
}

interface Table {
  readonly column: Uint32Array | Uint8Array;
  readonly labels: readonly { readonly label: string; readonly hidden: boolean }[];
  readonly noneLabel: string;
}

function table(projection: Projection, colorBy: Exclude<ColorBy, 'route'>): Table {
  const { header } = projection;
  switch (colorBy) {
    case 'topic':
      return {
        column: projection.topic,
        labels: header.topics.map((t) =>
          t.label === null
            ? { label: `topic ${shortUlid(t.id)}`, hidden: true }
            : { label: t.label, hidden: false },
        ),
        noneLabel: 'outlier',
      };
    case 'sender':
    case 'reader':
      return {
        column: colorBy === 'sender' ? projection.sender : projection.reader,
        labels: header.agents.map((a) => ({ label: a.name, hidden: false })),
        noneLabel: 'unknown',
      };
    case 'channel':
      return {
        column: projection.channel,
        labels: header.channels.map((c) => ({ label: c.name, hidden: false })),
        noneLabel: 'not channel-routed',
      };
  }
}

export function colorPoints(projection: Projection, colorBy: ColorBy, theme: Theme): Coloring {
  const n = projection.ids.length;
  const categories = new Uint32Array(n);
  if (colorBy === 'route') {
    const counts = new Array<number>(ROUTE_KINDS.length).fill(0);
    for (let i = 0; i < n; i++) {
      const kind = projection.route[i] ?? 0;
      categories[i] = kind;
      counts[kind] = (counts[kind] ?? 0) + 1;
    }
    const palette = ROUTE_KINDS.map((k) => theme.route[k]);
    const legend = ROUTE_KINDS.map((kind, i) => ({
      label: kind,
      color: theme.route[kind],
      count: counts[i] ?? 0,
    }))
      .filter((e) => e.count > 0)
      .sort((a, b) => b.count - a.count);
    return { categories, palette, legend };
  }

  const { column, labels, noneLabel } = table(projection, colorBy);
  const counts = new Array<number>(labels.length).fill(0);
  let noneCount = 0;
  for (let i = 0; i < n; i++) {
    const value = column[i] ?? NONE;
    if (value === NONE || value >= labels.length) noneCount++;
    else counts[value] = (counts[value] ?? 0) + 1;
  }
  const slots = theme.series.length;
  const chosen = counts
    .map((count, index) => ({ count, index }))
    .filter((e) => e.count > 0)
    .sort((a, b) => b.count - a.count || a.index - b.index)
    .slice(0, slots)
    .sort((a, b) => a.index - b.index);
  const slotOf = new Map(chosen.map((e, slot) => [e.index, slot]));
  const otherIndex = slots;
  const noneIndex = slots + 1;
  const none = mix(theme.other, theme.surface, 0.45);
  const palette = [...theme.series, theme.other, none];

  let otherCount = 0;
  for (let i = 0; i < n; i++) {
    const value = column[i] ?? NONE;
    if (value === NONE || value >= labels.length) {
      categories[i] = noneIndex;
      continue;
    }
    const slot = slotOf.get(value);
    if (slot === undefined) {
      categories[i] = otherIndex;
      otherCount++;
    } else {
      categories[i] = slot;
    }
  }

  const legend: LegendEntry[] = chosen
    .map((e) => {
      const entry = labels[e.index];
      const color = theme.series[slotOf.get(e.index) ?? 0] ?? theme.other;
      return { label: entry?.label ?? '?', color, count: e.count, hidden: entry?.hidden ?? false };
    })
    .sort((a, b) => b.count - a.count);
  const others = counts.filter((c) => c > 0).length - chosen.length;
  if (otherCount > 0) {
    legend.push({ label: `other (${others})`, color: theme.other, count: otherCount });
  }
  if (noneCount > 0) legend.push({ label: noneLabel, color: none, count: noneCount });
  return { categories, palette, legend };
}

/**
 * Per point, 1 when its transmission is in `highlight` (a comma-separated
 * ULID list), else 0. `null` when the list is empty: nothing is dimmed.
 */
export function highlightMask(ids: readonly string[], highlight: string): Uint8Array | null {
  const wanted = new Set(
    highlight
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0),
  );
  if (wanted.size === 0) return null;
  const mask = new Uint8Array(ids.length);
  ids.forEach((id, i) => {
    if (wanted.has(id)) mask[i] = 1;
  });
  return mask;
}
