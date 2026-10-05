/**
 * ULID text for entity ids: 26 characters of Crockford base32, the same
 * encoding as `ui/src/url/ulid.rs`. Payloads carry canonical (upper-case)
 * text, so a ULID is validated, never normalised, on the client.
 */

declare const ulidBrand: unique symbol;
/** A validated, canonical ULID string. */
export type Ulid = string & { readonly [ulidBrand]: true };

const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
/** 26 digits; the first carries only 3 bits, so it is 0-7. */
export const ULID_PATTERN = /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/;

export function isUlid(text: string): text is Ulid {
  return ULID_PATTERN.test(text);
}

export function parseUlid(text: string): Ulid | null {
  return isUlid(text) ? text : null;
}

/**
 * Encodes the 16 big-endian bytes at `offset` as ULID text. Reads 5 bits per
 * digit with two virtual leading zero bits (26 digits carry 130 bits).
 */
export function ulidFromBytes(bytes: Uint8Array, offset: number): Ulid {
  let out = '';
  for (let digit = 0; digit < 26; digit++) {
    const start = digit * 5 - 2;
    let value = 0;
    for (let bit = start; bit < start + 5; bit++) {
      value <<= 1;
      if (bit >= 0) {
        const byte = bytes[offset + (bit >> 3)] ?? 0;
        value |= (byte >> (7 - (bit & 7))) & 1;
      }
    }
    out += ALPHABET[value];
  }
  return out as Ulid;
}

/** The 16 big-endian bytes of a ULID. */
export function ulidToBytes(ulid: Ulid): Uint8Array {
  let value = 0n;
  for (const ch of ulid) {
    value = (value << 5n) | BigInt(ALPHABET.indexOf(ch));
  }
  const bytes = new Uint8Array(16);
  for (let i = 15; i >= 0; i--) {
    bytes[i] = Number(value & 0xffn);
    value >>= 8n;
  }
  return bytes;
}

/** The last six characters, as `components::short_id` shows an unlabelled id. */
export function shortUlid(ulid: Ulid): string {
  return `…${ulid.slice(-6)}`;
}
