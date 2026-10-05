/** The payloads `ui/src/data/fixtures/` writes, read as the elements fetch them. */

import { readFileSync } from 'node:fs';

const dir = new URL('./fixtures/', import.meta.url);

export function fixtureJson(name: string): unknown {
  return JSON.parse(readFileSync(new URL(name, dir), 'utf8'));
}

export function fixtureBuffer(name: string): ArrayBuffer {
  const bytes = readFileSync(new URL(name, dir));
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

/** A deep copy to mutate in negative tests. */
export function clone<T>(value: T): T {
  return structuredClone(value);
}
