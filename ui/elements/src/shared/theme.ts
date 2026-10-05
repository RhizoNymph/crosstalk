/**
 * The colours an element draws with. Route-kind colours come from the page's
 * CSS custom properties (`--color-route-*`, the Tailwind theme tokens in
 * `ui/styles/app.css`), so the theme drives the graph; text and surface
 * colours come from the element's computed style, so light and dark mode
 * follow the page. The categorical series are the validated reference
 * palette (eight slots, light and dark steps).
 */

import type { Policy } from '../payloads/topology.ts';
import { luminance, mix, parseColor, type Rgba, withAlpha } from './color.ts';
import { ROUTE_KINDS, type RouteKind } from './route.ts';

export const ROUTE_TOKENS: Readonly<Record<RouteKind, string>> = {
  channel: '--color-route-channel',
  delegation: '--color-route-delegation',
  direct: '--color-route-direct',
  unobserved: '--color-route-unobserved',
};

/** The values of `ui/styles/app.css`, used when a token is not defined. */
export const ROUTE_FALLBACKS: Readonly<Record<RouteKind, string>> = {
  channel: '#2563eb',
  delegation: '#7c3aed',
  direct: '#059669',
  unobserved: '#dc2626',
};

export const POLICY_TOKENS: Readonly<Record<Policy, string>> = {
  unreviewed: '--color-policy-unreviewed',
  sanctioned: '--color-policy-sanctioned',
  unsanctioned: '--color-policy-unsanctioned',
};

export const POLICY_FALLBACKS: Readonly<Record<Policy, string>> = {
  unreviewed: '#d97706',
  sanctioned: '#64748b',
  unsanctioned: '#be123c',
};

const SERIES_LIGHT = [
  '#2a78d6',
  '#eb6834',
  '#1baf7a',
  '#eda100',
  '#e87ba4',
  '#008300',
  '#4a3aa7',
  '#e34948',
];
const SERIES_DARK = [
  '#3987e5',
  '#d95926',
  '#199e70',
  '#c98500',
  '#d55181',
  '#008300',
  '#9085e9',
  '#e66767',
];

export interface Theme {
  readonly dark: boolean;
  readonly surface: Rgba;
  readonly text: Rgba;
  readonly muted: Rgba;
  /** Gridlines, axes and idle marks. */
  readonly faint: Rgba;
  readonly agent: Rgba;
  readonly route: Readonly<Record<RouteKind, Rgba>>;
  readonly policy: Readonly<Record<Policy, Rgba>>;
  /** Categorical slots in fixed order. */
  readonly series: readonly Rgba[];
  /** "Other" and "none" categories. */
  readonly other: Rgba;
  readonly font: string;
}

export interface ThemeInputs {
  /** A custom property's value, or `''` when undefined. */
  readonly token: (name: string) => string;
  readonly text: Rgba;
  readonly surface: Rgba;
  readonly font: string;
}

const BLACK: Rgba = [0, 0, 0, 1];

function color(text: string, fallback: string): Rgba {
  return parseColor(text) ?? parseColor(fallback) ?? BLACK;
}

/** Derives the theme from resolved inputs. Pure, so colour mapping is testable. */
export function themeFrom(inputs: ThemeInputs): Theme {
  const dark = luminance(inputs.surface) < 0.4;
  const route = Object.fromEntries(
    ROUTE_KINDS.map((kind) => [
      kind,
      color(inputs.token(ROUTE_TOKENS[kind]), ROUTE_FALLBACKS[kind]),
    ]),
  ) as Record<RouteKind, Rgba>;
  const policy = {
    unreviewed: color(inputs.token(POLICY_TOKENS.unreviewed), POLICY_FALLBACKS.unreviewed),
    sanctioned: color(inputs.token(POLICY_TOKENS.sanctioned), POLICY_FALLBACKS.sanctioned),
    unsanctioned: color(inputs.token(POLICY_TOKENS.unsanctioned), POLICY_FALLBACKS.unsanctioned),
  };
  return {
    dark,
    surface: inputs.surface,
    text: inputs.text,
    muted: mix(inputs.text, inputs.surface, 0.4),
    faint: mix(inputs.text, inputs.surface, 0.85),
    agent: mix(inputs.text, inputs.surface, dark ? 0.3 : 0.25),
    route,
    policy,
    series: (dark ? SERIES_DARK : SERIES_LIGHT).map((hex) => color(hex, hex)),
    other: mix(inputs.text, inputs.surface, 0.62),
    font: inputs.font,
  };
}

let probe: CanvasRenderingContext2D | null = null;

/**
 * Resolves any CSS colour (including `oklch()`, which Tailwind's palette
 * computes to) to sRGB bytes by painting one pixel.
 */
export function resolveCssColor(text: string): Rgba | null {
  const parsed = parseColor(text);
  if (parsed !== null) return parsed;
  if (probe === null) {
    const canvas = document.createElement('canvas');
    canvas.width = 1;
    canvas.height = 1;
    probe = canvas.getContext('2d', { willReadFrequently: true });
  }
  if (probe === null || text.trim() === '') return null;
  probe.clearRect(0, 0, 1, 1);
  probe.fillStyle = '#000';
  probe.fillStyle = text;
  probe.fillRect(0, 0, 1, 1);
  const [r = 0, g = 0, b = 0, a = 0] = probe.getImageData(0, 0, 1, 1).data;
  return a === 0 ? null : [r, g, b, a / 255];
}

function prefersDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

/** The first opaque background at or above `element`, across shadow roots. */
function effectiveSurface(element: Element): Rgba {
  let node: Element | null = element;
  while (node !== null) {
    const background = resolveCssColor(getComputedStyle(node).backgroundColor);
    if (background !== null && background[3] > 0.5) return withAlpha(background, 1);
    const parent: Element | null = node.parentElement;
    const root = node.getRootNode();
    node = parent ?? (root instanceof ShadowRoot ? root.host : null);
  }
  return prefersDark() ? [9, 9, 11, 1] : [255, 255, 255, 1];
}

/** Reads the theme for `host` from the page. */
export function readTheme(host: HTMLElement): Theme {
  const style = getComputedStyle(host);
  const surface = effectiveSurface(host);
  const fallbackText: Rgba = luminance(surface) < 0.4 ? [244, 244, 245, 1] : [24, 24, 27, 1];
  return themeFrom({
    token: (name) => resolveToken(style.getPropertyValue(name)),
    text: resolveCssColor(style.color) ?? fallbackText,
    surface,
    font: style.fontFamily || 'ui-sans-serif, system-ui, sans-serif',
  });
}

function resolveToken(value: string): string {
  const trimmed = value.trim();
  if (trimmed === '' || parseColor(trimmed) !== null) return trimmed;
  const resolved = resolveCssColor(trimmed);
  return resolved === null ? '' : `rgba(${resolved.join(', ')})`;
}

/** Calls `onChange` when the colour scheme flips; returns the unsubscribe. */
export function watchColorScheme(onChange: () => void): () => void {
  const query = window.matchMedia('(prefers-color-scheme: dark)');
  query.addEventListener('change', onChange);
  return () => query.removeEventListener('change', onChange);
}
