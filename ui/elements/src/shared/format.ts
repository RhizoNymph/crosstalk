/** Number and time formatting for labels and tooltips. */

export function formatCount(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}k`;
  return `${(n / 1_000_000).toFixed(n < 10_000_000 ? 1 : 0)}M`;
}

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}

export function formatShare(share: number): string {
  const percent = share * 100;
  return `${percent < 10 ? percent.toFixed(1) : percent.toFixed(0)}%`;
}

/** `2026-10-02 14:05` in UTC; seconds only when non-zero. */
export function formatUtc(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  const text = date.toISOString();
  const seconds = text.slice(17, 19);
  return `${text.slice(0, 10)} ${text.slice(11, 16)}${seconds === '00' ? '' : `:${seconds}`}`;
}

/** `14:05` (UTC), or the date too when `withDate`. */
export function formatUtcShort(ms: number, withDate: boolean): string {
  const text = new Date(ms).toISOString();
  return withDate ? `${text.slice(5, 10)} ${text.slice(11, 16)}` : text.slice(11, 16);
}
