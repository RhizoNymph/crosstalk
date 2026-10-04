import { describe, expect, it } from 'vitest';
import { decodeProjection, NONE } from '../src/payloads/projection.ts';
import { isUlid, type Ulid, ulidFromBytes, ulidToBytes } from '../src/shared/ulid.ts';
import { fixtureBuffer } from './fixtures.ts';

const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

/** A slow, obviously correct reference: BigInt and repeated division. */
function referenceUlid(bytes: Uint8Array, offset: number): string {
  let value = 0n;
  for (let i = 0; i < 16; i++) value = (value << 8n) | BigInt(bytes[offset + i] ?? 0);
  let out = '';
  for (let i = 0; i < 26; i++) {
    out = ALPHABET[Number(value & 31n)] + out;
    value >>= 5n;
  }
  return out;
}

function decoded() {
  const result = decodeProjection(fixtureBuffer('projection.bin'));
  if (!result.ok) throw new Error(result.error.message);
  return result.value;
}

/** Parts of a payload, to re-encode with one thing broken. */
function split(buffer: ArrayBuffer) {
  const view = new DataView(buffer);
  const headerLength = view.getUint32(8, true);
  const header = JSON.parse(new TextDecoder().decode(new Uint8Array(buffer, 12, headerLength)));
  const columns = new Uint8Array(buffer, 12 + headerLength);
  return { header, columns };
}

function encode(header: unknown, columns: Uint8Array): ArrayBuffer {
  let json = new TextEncoder().encode(JSON.stringify(header));
  const padded = new Uint8Array(Math.ceil(json.length / 4) * 4).fill(0x20);
  padded.set(json);
  json = padded;
  const out = new Uint8Array(12 + json.length + columns.length);
  out.set(new TextEncoder().encode('CTPJ'), 0);
  const view = new DataView(out.buffer);
  view.setUint32(4, 1, true);
  view.setUint32(8, json.length, true);
  out.set(json, 12);
  out.set(columns, 12 + json.length);
  return out.buffer;
}

describe('projection payload', () => {
  it('decodes the fixture Rust writes', () => {
    const p = decoded();
    expect(p.ids.length).toBe(640);
    expect(p.header.count).toBe(640);
    expect(p.xs.length).toBe(640);
    expect(p.header.agents[0]?.name).toBe('planner');
    expect(p.header.routeKinds).toEqual(['channel', 'delegation', 'direct', 'unobserved']);
    expect(p.header.params.seed).toBe('42');
    expect(p.header.topics.map((t) => t.label)).toContain('API rate limits');
  });

  it('decodes every id like the reference encoder', () => {
    const buffer = fixtureBuffer('projection.bin');
    const p = decoded();
    const idStart = buffer.byteLength - 16 * p.ids.length;
    const bytes = new Uint8Array(buffer);
    p.ids.forEach((id, i) => {
      expect(isUlid(id)).toBe(true);
      expect(id).toBe(referenceUlid(bytes, idStart + 16 * i));
    });
    expect(new Set(p.ids).size).toBe(p.ids.length);
  });

  it('marks absent channels and topics with NONE, and only those', () => {
    const p = decoded();
    let outliers = 0;
    for (let i = 0; i < p.ids.length; i++) {
      const channel = p.channel[i] ?? NONE;
      const isChannelRoute = p.route[i] === 0;
      expect(channel !== NONE).toBe(isChannelRoute);
      if (p.topic[i] === NONE) outliers++;
    }
    expect(outliers).toBeGreaterThan(0);
  });

  it('decodes an empty projection', () => {
    const { header } = split(fixtureBuffer('projection.bin'));
    const empty = { ...header, count: 0 };
    const result = decodeProjection(encode(empty, new Uint8Array(0)));
    expect(result.ok && result.value.ids.length).toBe(0);
  });

  it('rejects a bad magic, a truncation and trailing bytes', () => {
    const buffer = fixtureBuffer('projection.bin');
    const magic = new Uint8Array(buffer.slice(0));
    magic[0] = 0x58;
    expect(decodeProjection(magic.buffer).ok).toBe(false);
    expect(decodeProjection(buffer.slice(0, buffer.byteLength - 1)).ok).toBe(false);
    const longer = new Uint8Array(buffer.byteLength + 4);
    longer.set(new Uint8Array(buffer));
    expect(decodeProjection(longer.buffer).ok).toBe(false);
    expect(decodeProjection(new ArrayBuffer(4)).ok).toBe(false);
  });

  it('rejects an index outside its table', () => {
    const buffer = fixtureBuffer('projection.bin');
    const { header, columns } = split(buffer);
    const broken = { ...header, agents: header.agents.slice(0, 1) };
    const result = decodeProjection(encode(broken, columns));
    expect(result.ok).toBe(false);
    expect(!result.ok && result.error.message).toMatch(/agent index out of range/);
  });

  it('rejects a header that does not match the schema', () => {
    const { header, columns } = split(fixtureBuffer('projection.bin'));
    const result = decodeProjection(encode({ ...header, routeKinds: ['direct'] }, columns));
    expect(result.ok).toBe(false);
  });

  it('rejects an unsupported version', () => {
    const buffer = fixtureBuffer('projection.bin').slice(0);
    new DataView(buffer).setUint32(4, 2, true);
    const result = decodeProjection(buffer);
    expect(!result.ok && result.error.message).toMatch(/version 2/);
  });
});

describe('ulid bytes', () => {
  it('encodes the extremes like ui/src/url/ulid.rs', () => {
    expect(ulidFromBytes(new Uint8Array(16), 0)).toBe('00000000000000000000000000');
    expect(ulidFromBytes(new Uint8Array(16).fill(0xff), 0)).toBe('7ZZZZZZZZZZZZZZZZZZZZZZZZZ');
  });

  it('round-trips through bytes', () => {
    for (const id of [
      '01K6HB7H0002GG002YXM000001',
      '7ZZZZZZZZZZZZZZZZZZZZZZZZZ',
      '00000000000000000000000001',
    ]) {
      expect(ulidFromBytes(ulidToBytes(id as Ulid), 0)).toBe(id);
    }
  });
});
