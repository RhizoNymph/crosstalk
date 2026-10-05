import { describe, expect, it } from 'vitest';
import { END_REASONS, endStep, parseEnd, UNREACHABLE_MESSAGE } from '../src/live/end.ts';
import { lifecycleStep } from '../src/live/lifecycle.ts';
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

describe('page lifecycle', () => {
  it('closes the stream whenever the page hides, cached or not', () => {
    expect(lifecycleStep('pagehide', true)).toBe('close');
    expect(lifecycleStep('pagehide', false)).toBe('close');
  });

  it('reopens only for a page restored from the back/forward cache', () => {
    expect(lifecycleStep('pageshow', true)).toBe('reopen');
    expect(lifecycleStep('pageshow', false)).toBe('none');
    expect(lifecycleStep('visibilitychange', true)).toBe('none');
  });
});

describe('stream end', () => {
  it('parses the reason /data/live sends', () => {
    for (const reason of END_REASONS) {
      expect(parseEnd(JSON.stringify({ reason }))).toEqual({ ok: true, value: reason });
    }
  });

  it('refuses an end it cannot read', () => {
    expect(parseEnd('nope').ok).toBe(false);
    expect(parseEnd('{}').ok).toBe(false);
    expect(parseEnd('{"reason":"gone"}').ok).toBe(false);
    expect(parseEnd('{"reason":7}').ok).toBe(false);
  });

  it('stops and says the gateway is unreachable', () => {
    expect(endStep('unreachable')).toEqual({ kind: 'stop', message: UNREACHABLE_MESSAGE });
    expect(UNREACHABLE_MESSAGE).toContain('Live updates lost: gateway unreachable');
  });

  it('lets every other end reconnect and resume', () => {
    for (const reason of ['lagged', 'session-ended', 'shutting-down'] as const) {
      expect(endStep(reason)).toEqual({ kind: 'resume' });
    }
  });
});
