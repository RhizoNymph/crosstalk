/**
 * Route codes, as `ui/src/url/route.rs` writes them: `ch.<channel ulid>`,
 * `dl.p2c`, `dl.c2p`, `dr.user`, `dr.sys`, `dr.tool.<tool name>`, `un`.
 */

export const ROUTE_KINDS = ['channel', 'delegation', 'direct', 'unobserved'] as const;
export type RouteKind = (typeof ROUTE_KINDS)[number];

declare const routeBrand: unique symbol;
/** A validated route code. */
export type RouteCode = string & { readonly [routeBrand]: true };

const ROUTE_PATTERN =
  /^(ch\.[0-7][0-9A-HJKMNP-TV-Z]{25}|dl\.p2c|dl\.c2p|dr\.user|dr\.sys|dr\.tool\..+|un)$/;

export function parseRouteCode(text: string): RouteCode | null {
  return ROUTE_PATTERN.test(text) ? (text as RouteCode) : null;
}

/** The kind a route code belongs to. */
export function routeKindOf(code: RouteCode): RouteKind {
  if (code.startsWith('ch.')) return 'channel';
  if (code.startsWith('dl.')) return 'delegation';
  if (code.startsWith('dr.')) return 'direct';
  return 'unobserved';
}

/** A short human description of a route code, for tooltips. */
export function describeRoute(code: RouteCode): string {
  if (code.startsWith('ch.')) return 'channel';
  if (code.startsWith('dr.tool.')) return `tool result: ${code.slice('dr.tool.'.length)}`;
  switch (code) {
    case 'dl.p2c':
      return 'delegation: parent → child';
    case 'dl.c2p':
      return 'delegation: child → parent';
    case 'dr.user':
      return 'direct: user turn';
    case 'dr.sys':
      return 'direct: system prompt';
    default:
      return 'unobserved';
  }
}
