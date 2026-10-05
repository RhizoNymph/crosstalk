/**
 * The live feed as `<ct-live>` reads it: the SSE events of `GET /data/live`
 * (mirrors `ui/src/data/live.rs`) and the tokens a page declares in
 * `data-live-watch` (`ui/src/components/live.rs`).
 *
 * A token is a feed event kind alone (any id of that kind) or `kind:key`
 * for one entity: a ULID, or a version number for `topic-version`.
 */

// No zod here: the element loads on every page, and these shapes are
// three small objects (zod would add about 85 KB to the bundle).
import { err, ok, type Result } from '../shared/result.ts';
import { isUlid } from '../shared/ulid.ts';

/** The `event:` names of entity events. */
export const LIVE_KINDS = [
  'alert',
  'channel',
  'agent',
  'rule',
  'verdict',
  'projection',
  'topic-version',
  'watermark',
] as const;
export type LiveKind = (typeof LIVE_KINDS)[number];

export function isLiveKind(text: string): text is LiveKind {
  return (LIVE_KINDS as readonly string[]).includes(text);
}

/** What one entity event names: its kind and the key of the entity. */
export interface LiveNotice {
  readonly kind: LiveKind;
  /** The ULID, the version as decimal text, or `null` for the watermark. */
  readonly key: string | null;
}

export interface WatchToken {
  readonly kind: LiveKind;
  /** `null` watches every entity of the kind. */
  readonly key: string | null;
}

const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;

function json(text: string): Result<unknown, string> {
  try {
    return ok(JSON.parse(text) as unknown);
  } catch (error) {
    if (error instanceof SyntaxError) return err(`not JSON: ${error.message}`);
    throw error;
  }
}

/** `value` as an object with exactly the one key `key`, and that key's value. */
function only(value: unknown, key: string): Result<unknown, string> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return err('expected an object');
  }
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== key) return err(`expected exactly {"${key}": …}`);
  return ok((value as Record<string, unknown>)[key]);
}

/** Reads an entity event's `data:` for its `event:` kind. */
export function parseNotice(kind: LiveKind, data: string): Result<LiveNotice, string> {
  const parsed = json(data);
  if (!parsed.ok) return parsed;
  if (kind === 'topic-version') {
    const version = only(parsed.value, 'version');
    if (!version.ok) return version;
    return Number.isSafeInteger(version.value) && (version.value as number) >= 0
      ? ok({ kind, key: String(version.value) })
      : err('version: expected a non-negative integer');
  }
  if (kind === 'watermark') {
    const at = only(parsed.value, 'at');
    if (!at.ok) return at;
    return typeof at.value === 'string' && TIMESTAMP.test(at.value)
      ? ok({ kind, key: null })
      : err('at: expected an RFC 3339 UTC time');
  }
  const id = only(parsed.value, 'id');
  if (!id.ok) return id;
  return typeof id.value === 'string' && isUlid(id.value)
    ? ok({ kind, key: id.value })
    : err('id: expected a ULID');
}

/** The tokens of one `data-live-watch` value; unknown kinds are skipped. */
export function parseWatch(text: string): WatchToken[] {
  const tokens: WatchToken[] = [];
  for (const word of text.split(/\s+/)) {
    if (word === '') continue;
    const colon = word.indexOf(':');
    const kind = colon < 0 ? word : word.slice(0, colon);
    const key = colon < 0 ? null : word.slice(colon + 1);
    if (!isLiveKind(kind) || key === '') continue;
    tokens.push({ kind, key });
  }
  return tokens;
}

/** Whether any token watches what `notice` names. */
export function watches(tokens: readonly WatchToken[], notice: LiveNotice): boolean {
  return tokens.some(
    (token) => token.kind === notice.kind && (token.key === null || token.key === notice.key),
  );
}
