/**
 * The `value` grammar of every element. A page stores these strings in a
 * signal and the URL, so they are short, stable and parseable back.
 *
 * - `<ct-topology>`: `edge:<fromUlid>:<toUlid>:<routeCode>` | `agent:<ulid>` |
 *   `channel:<ulid>` | `` (cleared). The route code is the rest of the text
 *   after the third colon (tool names may contain colons).
 * - `<ct-projection>`: `lasso:<x1>,<y1>;<x2>,<y2>;…` (at least three
 *   vertices in projection coordinates, at most four decimals) |
 *   `point:<transmissionUlid>` | ``.
 * - `<ct-timebrush>`: `<fromRfc3339>/<toRfc3339>` with `from < to` | ``.
 */

import { err, ok, type Result } from './result.ts';
import { parseRouteCode, type RouteCode } from './route.ts';
import { parseUlid, type Ulid } from './ulid.ts';

export type SelectionError = { readonly kind: 'selection'; readonly message: string };

function fail(message: string): Result<never, SelectionError> {
  return err({ kind: 'selection', message });
}

export type TopologySelection =
  | { readonly kind: 'none' }
  | { readonly kind: 'edge'; readonly from: Ulid; readonly to: Ulid; readonly route: RouteCode }
  | { readonly kind: 'agent'; readonly id: Ulid }
  | { readonly kind: 'channel'; readonly id: Ulid };

export const NO_TOPOLOGY_SELECTION: TopologySelection = { kind: 'none' };

export function encodeTopologySelection(selection: TopologySelection): string {
  switch (selection.kind) {
    case 'none':
      return '';
    case 'edge':
      return `edge:${selection.from}:${selection.to}:${selection.route}`;
    case 'agent':
      return `agent:${selection.id}`;
    case 'channel':
      return `channel:${selection.id}`;
  }
}

export function decodeTopologySelection(text: string): Result<TopologySelection, SelectionError> {
  if (text === '') return ok(NO_TOPOLOGY_SELECTION);
  const [kind, ...rest] = text.split(':');
  if (kind === 'agent' || kind === 'channel') {
    const id = rest.length === 1 ? parseUlid(rest[0] ?? '') : null;
    if (id === null) return fail(`${kind}: expected one ULID`);
    return ok(kind === 'agent' ? { kind: 'agent', id } : { kind: 'channel', id });
  }
  if (kind === 'edge') {
    const from = parseUlid(rest[0] ?? '');
    const to = parseUlid(rest[1] ?? '');
    const route = parseRouteCode(rest.slice(2).join(':'));
    if (from === null || to === null) return fail('edge: expected two ULIDs');
    if (route === null) return fail('edge: unknown route code');
    return ok({ kind: 'edge', from, to, route });
  }
  return fail(`unknown selection kind ${JSON.stringify(kind)}`);
}

export type Vertex = readonly [x: number, y: number];

export type ProjectionSelection =
  | { readonly kind: 'none' }
  | { readonly kind: 'lasso'; readonly polygon: readonly Vertex[] }
  | { readonly kind: 'point'; readonly id: Ulid };

/** The most vertices a lasso value carries; longer lassos are simplified first. */
export const MAX_LASSO_VERTICES = 48;
export const LASSO_DECIMALS = 4;

/** A coordinate rounded to [`LASSO_DECIMALS`], without trailing zeros or `-0`. */
export function formatCoordinate(value: number): string {
  const fixed = value.toFixed(LASSO_DECIMALS).replace(/\.?0+$/, '');
  return fixed === '-0' ? '0' : fixed;
}

export function encodeProjectionSelection(selection: ProjectionSelection): string {
  switch (selection.kind) {
    case 'none':
      return '';
    case 'point':
      return `point:${selection.id}`;
    case 'lasso':
      return `lasso:${selection.polygon
        .map(([x, y]) => `${formatCoordinate(x)},${formatCoordinate(y)}`)
        .join(';')}`;
  }
}

const NUMBER = /^-?\d+(\.\d+)?$/;

export function decodeProjectionSelection(
  text: string,
): Result<ProjectionSelection, SelectionError> {
  if (text === '') return ok({ kind: 'none' });
  if (text.startsWith('point:')) {
    const id = parseUlid(text.slice('point:'.length));
    return id === null ? fail('point: expected a ULID') : ok({ kind: 'point', id });
  }
  if (text.startsWith('lasso:')) {
    const polygon: Vertex[] = [];
    for (const pair of text.slice('lasso:'.length).split(';')) {
      const [x, y, extra] = pair.split(',');
      if (x === undefined || y === undefined || extra !== undefined) {
        return fail('lasso: expected x,y pairs');
      }
      if (!NUMBER.test(x) || !NUMBER.test(y)) return fail('lasso: coordinates must be decimals');
      polygon.push([Number(x), Number(y)]);
    }
    if (polygon.length < 3) return fail('lasso: needs at least three vertices');
    if (polygon.length > MAX_LASSO_VERTICES) {
      return fail(`lasso: at most ${MAX_LASSO_VERTICES} vertices`);
    }
    return ok({ kind: 'lasso', polygon });
  }
  return fail('expected lasso: or point:');
}

export type BrushSelection =
  | { readonly kind: 'none' }
  | { readonly kind: 'window'; readonly from: string; readonly to: string };

const RFC3339_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;

export function encodeBrushSelection(selection: BrushSelection): string {
  return selection.kind === 'none' ? '' : `${selection.from}/${selection.to}`;
}

export function decodeBrushSelection(text: string): Result<BrushSelection, SelectionError> {
  if (text === '') return ok({ kind: 'none' });
  const [from, to, extra] = text.split('/');
  if (from === undefined || to === undefined || extra !== undefined) {
    return fail('window: expected from/to');
  }
  if (!RFC3339_UTC.test(from) || !RFC3339_UTC.test(to)) {
    return fail('window: expected RFC 3339 UTC times');
  }
  if (Date.parse(from) >= Date.parse(to)) return fail('window: from must be before to');
  return ok({ kind: 'window', from, to });
}
