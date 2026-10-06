/**
 * A followed window sliding under an element that survives the refresh.
 *
 * A followed page's refresh keeps the nodes of its `[data-live-keep]`
 * elements (`live/refresh.ts`) and applies the fresh render's attributes
 * to them. The fresh `data-src` names the newly resolved window, so a
 * slide arrives as a `data-src` change in which only the window keys
 * differ. Such an element refetches quietly and merges the payload into
 * what it draws instead of reloading.
 */

/** Query pairs of `url` except `ignored`, sorted, with its path first. */
function shape(url: string, ignored: readonly string[]): string[] | null {
  let parsed: URL;
  try {
    parsed = new URL(url, 'http://element.invalid');
  } catch (error) {
    if (error instanceof TypeError) return null;
    throw error;
  }
  const pairs = [...parsed.searchParams]
    .filter(([key]) => !ignored.includes(key))
    .map(([key, value]) => `${key}=${value}`)
    .sort();
  return [parsed.pathname, ...pairs];
}

/**
 * Whether `next` differs from `previous` only in the keys `window` (`from`
 * and `to`, and whatever else the element derives from the window).
 */
export function isSlide(previous: string, next: string, window: readonly string[]): boolean {
  if (window.length === 0 || previous === next) return false;
  const before = shape(previous, window);
  const after = shape(next, window);
  if (before === null || after === null || before.length !== after.length) return false;
  return before.every((part, i) => part === after[i]);
}

/** What applying `next`'s attributes to an element holding `current` takes. */
export interface AttributeChanges {
  readonly set: readonly (readonly [string, string])[];
  readonly remove: readonly string[];
}

export function attributeChanges(
  current: Iterable<readonly [string, string]>,
  next: Iterable<readonly [string, string]>,
): AttributeChanges {
  const now = new Map(current);
  const wanted = new Map(next);
  return {
    set: [...wanted].filter(([name, value]) => now.get(name) !== value),
    remove: [...now.keys()].filter((name) => !wanted.has(name)),
  };
}
