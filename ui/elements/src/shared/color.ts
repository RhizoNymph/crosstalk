/** Pure colour arithmetic over sRGB bytes. */

export type Rgba = readonly [r: number, g: number, b: number, a: number];

const HEX = /^#([0-9a-f]{3,4}|[0-9a-f]{6}|[0-9a-f]{8})$/i;
const RGB = /^rgba?\(\s*([\d.]+)[\s,]+([\d.]+)[\s,]+([\d.]+)(?:[\s,/]+([\d.]+%?))?\s*\)$/i;

/** Parses `#rgb`, `#rgba`, `#rrggbb`, `#rrggbbaa`, `rgb()` and `rgba()`. */
export function parseColor(text: string): Rgba | null {
  const value = text.trim();
  const hex = HEX.exec(value)?.[1];
  if (hex !== undefined) {
    const full = hex.length <= 4 ? [...hex].map((c) => c + c).join('') : hex;
    const byte = (i: number) => Number.parseInt(full.slice(i * 2, i * 2 + 2), 16);
    return [byte(0), byte(1), byte(2), full.length === 8 ? byte(3) / 255 : 1];
  }
  const rgb = RGB.exec(value);
  if (rgb !== null) {
    const alpha = rgb[4];
    const a =
      alpha === undefined
        ? 1
        : alpha.endsWith('%')
          ? Number(alpha.slice(0, -1)) / 100
          : Number(alpha);
    return [Number(rgb[1]), Number(rgb[2]), Number(rgb[3]), Math.min(1, Math.max(0, a))];
  }
  return null;
}

/** `a` towards `b` by `t` (0 = a, 1 = b), alpha included. */
export function mix(a: Rgba, b: Rgba, t: number): Rgba {
  const lerp = (x: number, y: number) => x + (y - x) * t;
  return [lerp(a[0], b[0]), lerp(a[1], b[1]), lerp(a[2], b[2]), lerp(a[3], b[3])];
}

export function withAlpha(color: Rgba, alpha: number): Rgba {
  return [color[0], color[1], color[2], alpha];
}

/** WCAG relative luminance in [0, 1]. */
export function luminance(color: Rgba): number {
  const channel = (v: number) => {
    const s = v / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * channel(color[0]) + 0.7152 * channel(color[1]) + 0.0722 * channel(color[2]);
}

/** `rgba(r, g, b, a)` with rounded channels, for canvas, SVG and WebGL libraries. */
export function toCss(color: Rgba): string {
  const [r, g, b, a] = color.map((v, i) => (i < 3 ? Math.round(v) : Math.round(v * 1000) / 1000));
  return `rgba(${r}, ${g}, ${b}, ${a})`;
}

/** `#rrggbb` (alpha dropped). */
export function toHex(color: Rgba): string {
  return `#${color
    .slice(0, 3)
    .map((v) => Math.round(v).toString(16).padStart(2, '0'))
    .join('')}`;
}
