/**
 * The `<ct-projection>` payload: `GET /data/projection/{id}`, binary.
 * Mirrors `ui/src/data/projection/format.rs`; the layout table is there and
 * in "Element payloads" in `docs/features/ui.md`. Little-endian, 4-byte
 * aligned columns:
 *
 * `CTPJ | u32 version | u32 h | header JSON (h bytes) | f32 xs | f32 ys |
 * u32 sender | u32 reader | u8 route (padded to 4) | u32 channel | u32 topic |
 * 16-byte big-endian transmission ids`.
 */

import * as z from 'zod';
import { err, ok, type Result } from '../shared/result.ts';
import { ROUTE_KINDS } from '../shared/route.ts';
import { type Ulid, ulidFromBytes } from '../shared/ulid.ts';
import { count, describeIssues, routeKind, timestamp, ulid, window } from './common.ts';

export const MAGIC = 'CTPJ';
export const VERSION = 1;
/** The index of an absent channel or topic. */
export const NONE = 0xffff_ffff;

const named = z.strictObject({ id: ulid, name: z.string() });

export const projectionHeader = z.strictObject({
  id: ulid,
  count,
  window,
  topicVersion: count,
  fittedAt: timestamp,
  embeddingModel: z.strictObject({ name: z.string(), dimension: count }),
  params: z.strictObject({
    neighbors: count,
    minDist: z.number().min(0).max(1),
    seed: z.string().regex(/^\d+$/),
    sampleLimit: count,
  }),
  routeKinds: z
    .array(routeKind)
    .refine(
      (kinds) => kinds.length === ROUTE_KINDS.length && kinds.every((k, i) => k === ROUTE_KINDS[i]),
      'unexpected route kind table',
    ),
  agents: z.array(named),
  channels: z.array(named),
  topics: z.array(z.strictObject({ id: ulid, label: z.string().nullable() })),
});

export type ProjectionHeader = z.output<typeof projectionHeader>;

/** A decoded projection: the header and one typed array per column. */
export interface Projection {
  readonly header: ProjectionHeader;
  readonly xs: Float32Array;
  readonly ys: Float32Array;
  readonly sender: Uint32Array;
  readonly reader: Uint32Array;
  /** Index into `ROUTE_KINDS`. */
  readonly route: Uint8Array;
  /** Index into `header.channels`, or [`NONE`]. */
  readonly channel: Uint32Array;
  /** Index into `header.topics`, or [`NONE`]. */
  readonly topic: Uint32Array;
  readonly ids: readonly Ulid[];
}

export type DecodeError = { readonly kind: 'decode'; readonly message: string };

function fail(message: string): Result<never, DecodeError> {
  return err({ kind: 'decode', message });
}

const pad4 = (n: number): number => (4 - (n % 4)) % 4;

/** UTF-8 JSON text; a `TypeError` is invalid UTF-8, a `SyntaxError` bad JSON. */
function readHeaderJson(bytes: Uint8Array): Result<unknown, DecodeError> {
  try {
    return ok(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)));
  } catch (error) {
    if (error instanceof SyntaxError || error instanceof TypeError) {
      return fail(`header: ${error.message}`);
    }
    throw error;
  }
}

/** Decodes and validates a projection payload. Never throws. */
export function decodeProjection(buffer: ArrayBuffer): Result<Projection, DecodeError> {
  const view = new DataView(buffer);
  if (buffer.byteLength < 12) return fail('truncated preamble');
  const magic = String.fromCharCode(
    view.getUint8(0),
    view.getUint8(1),
    view.getUint8(2),
    view.getUint8(3),
  );
  if (magic !== MAGIC) return fail('not a projection payload');
  const version = view.getUint32(4, true);
  if (version !== VERSION) return fail(`unsupported format version ${version}`);
  const headerLength = view.getUint32(8, true);
  if (headerLength % 4 !== 0) return fail('header length is not 4-byte aligned');
  if (12 + headerLength > buffer.byteLength) return fail('truncated header');

  const raw = readHeaderJson(new Uint8Array(buffer, 12, headerLength));
  if (!raw.ok) return raw;
  const parsed = projectionHeader.safeParse(raw.value);
  if (!parsed.success) return fail(`header: ${describeIssues(parsed.error)}`);
  const header = parsed.data;

  const n = header.count;
  const columns = 12 + headerLength;
  const expected = columns + 41 * n + pad4(n);
  if (buffer.byteLength !== expected) {
    return fail(`expected ${expected} bytes for ${n} points, got ${buffer.byteLength}`);
  }
  // Typed arrays view the buffer in place; LE matches every platform that
  // runs a browser, and the columns are 4-byte aligned.
  let at = columns;
  const take32 = <T>(make: (b: ArrayBuffer, o: number, l: number) => T): T => {
    const array = make(buffer, at, n);
    at += 4 * n;
    return array;
  };
  const xs = take32((b, o, l) => new Float32Array(b, o, l));
  const ys = take32((b, o, l) => new Float32Array(b, o, l));
  const sender = take32((b, o, l) => new Uint32Array(b, o, l));
  const reader = take32((b, o, l) => new Uint32Array(b, o, l));
  const route = new Uint8Array(buffer, at, n);
  at += n + pad4(n);
  const channel = take32((b, o, l) => new Uint32Array(b, o, l));
  const topic = take32((b, o, l) => new Uint32Array(b, o, l));
  const idBytes = new Uint8Array(buffer, at, 16 * n);

  const ids: Ulid[] = new Array(n);
  for (let i = 0; i < n; i++) {
    const s = sender[i] ?? NONE;
    const r = reader[i] ?? NONE;
    const c = channel[i] ?? NONE;
    const t = topic[i] ?? NONE;
    if (s >= header.agents.length || r >= header.agents.length) {
      return fail(`point ${i}: agent index out of range`);
    }
    if ((route[i] ?? 255) >= ROUTE_KINDS.length) return fail(`point ${i}: unknown route kind`);
    if (c !== NONE && c >= header.channels.length) return fail(`point ${i}: channel out of range`);
    if (t !== NONE && t >= header.topics.length) return fail(`point ${i}: topic out of range`);
    if (!Number.isFinite(xs[i] ?? Number.NaN) || !Number.isFinite(ys[i] ?? Number.NaN)) {
      return fail(`point ${i}: non-finite coordinate`);
    }
    ids[i] = ulidFromBytes(idBytes, 16 * i);
  }
  return ok({ header, xs, ys, sender, reader, route, channel, topic, ids });
}
