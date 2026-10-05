/**
 * The map between projection (data) coordinates and the scatterplot's
 * normalised space ([-1, 1] on the longer axis), preserving aspect ratio.
 */

export interface Transform {
  readonly cx: number;
  readonly cy: number;
  /** Normalised units per data unit. */
  readonly scale: number;
}

/** Fits the points into `[-margin, margin]`. */
export function fitTransform(
  xs: ArrayLike<number>,
  ys: ArrayLike<number>,
  margin = 0.92,
): Transform {
  let minX = Number.POSITIVE_INFINITY;
  let maxX = Number.NEGATIVE_INFINITY;
  let minY = Number.POSITIVE_INFINITY;
  let maxY = Number.NEGATIVE_INFINITY;
  for (let i = 0; i < xs.length; i++) {
    const x = xs[i] ?? 0;
    const y = ys[i] ?? 0;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }
  if (xs.length === 0) return { cx: 0, cy: 0, scale: 1 };
  const span = Math.max(maxX - minX, maxY - minY);
  return {
    cx: (minX + maxX) / 2,
    cy: (minY + maxY) / 2,
    scale: span > 0 ? (2 * margin) / span : 1,
  };
}

export function toNormalized(t: Transform, x: number, y: number): [number, number] {
  return [(x - t.cx) * t.scale, (y - t.cy) * t.scale];
}

export function toData(t: Transform, nx: number, ny: number): [number, number] {
  return [nx / t.scale + t.cx, ny / t.scale + t.cy];
}

/** Both columns normalised, as the scatterplot draws them. */
export function normalizeColumns(
  t: Transform,
  xs: ArrayLike<number>,
  ys: ArrayLike<number>,
): { x: Float32Array; y: Float32Array } {
  const x = new Float32Array(xs.length);
  const y = new Float32Array(ys.length);
  for (let i = 0; i < xs.length; i++) {
    x[i] = ((xs[i] ?? 0) - t.cx) * t.scale;
    y[i] = ((ys[i] ?? 0) - t.cy) * t.scale;
  }
  return { x, y };
}
