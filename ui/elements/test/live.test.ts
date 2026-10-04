import { describe, expect, it } from 'vitest';
import { parseNotice, parseWatch, watches } from '../src/live/watch.ts';

const ALERT = '01K6HB7H0002GG002YXM000001';
const OTHER = '01K6HB7H0002GG002YXM000002';

describe('watch tokens', () => {
  it('parses kinds alone and kinds with a key, skipping unknown kinds', () => {
    expect(parseWatch(`  alert channel:${ALERT}\ttopic-version:2 nonsense other:x rule: `)).toEqual(
      [
        { kind: 'alert', key: null },
        { kind: 'channel', key: ALERT },
        { kind: 'topic-version', key: '2' },
      ],
    );
    expect(parseWatch('')).toEqual([]);
  });

  it('matches a kind alone against every id, a key against its own', () => {
    const tokens = parseWatch(`alert channel:${ALERT}`);
    expect(watches(tokens, { kind: 'alert', key: OTHER })).toBe(true);
    expect(watches(tokens, { kind: 'channel', key: ALERT })).toBe(true);
    expect(watches(tokens, { kind: 'channel', key: OTHER })).toBe(false);
    expect(watches(tokens, { kind: 'agent', key: ALERT })).toBe(false);
    expect(watches(parseWatch('watermark'), { kind: 'watermark', key: null })).toBe(true);
  });
});

describe('feed events', () => {
  it('reads the id, version or time each kind carries', () => {
    expect(parseNotice('alert', JSON.stringify({ id: ALERT }))).toEqual({
      ok: true,
      value: { kind: 'alert', key: ALERT },
    });
    expect(parseNotice('topic-version', '{"version":2}')).toEqual({
      ok: true,
      value: { kind: 'topic-version', key: '2' },
    });
    expect(parseNotice('watermark', '{"at":"2026-10-02T23:50:00Z"}')).toEqual({
      ok: true,
      value: { kind: 'watermark', key: null },
    });
  });

  it('refuses data that is not the kind shape', () => {
    expect(parseNotice('alert', '{"id":"not a ulid"}').ok).toBe(false);
    expect(parseNotice('alert', 'not json').ok).toBe(false);
    expect(parseNotice('topic-version', JSON.stringify({ id: ALERT })).ok).toBe(false);
    expect(parseNotice('agent', JSON.stringify({ id: ALERT, extra: 1 })).ok).toBe(false);
  });
});
