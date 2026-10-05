/**
 * Fetching element payloads from the UI's `/data/` routes. Every failure is
 * a typed [`LoadError`]; an aborted request (its `data-src` changed or the
 * element left the page) is `aborted` and never shown.
 */

import type * as z from 'zod';
import { describeIssues } from '../payloads/common.ts';
import { err, ok, type Result } from './result.ts';

export type LoadError =
  | { readonly kind: 'aborted' }
  | { readonly kind: 'network'; readonly message: string }
  | { readonly kind: 'http'; readonly status: number; readonly message: string }
  | { readonly kind: 'invalid'; readonly message: string };

/** One line for the error panel. */
export function describeLoadError(error: LoadError): string {
  switch (error.kind) {
    case 'aborted':
      return 'request cancelled';
    case 'network':
      return `could not reach the server: ${error.message}`;
    case 'http':
      return `HTTP ${error.status}: ${error.message}`;
    case 'invalid':
      return `unexpected payload: ${error.message}`;
  }
}

const HTTP_MEANINGS: Readonly<Record<number, string>> = {
  400: 'bad request',
  403: 'you do not have permission to see this',
  404: 'not found',
  500: 'server error',
};

async function request(url: string, signal: AbortSignal): Promise<Result<Response, LoadError>> {
  let response: Response;
  try {
    response = await fetch(url, { signal, credentials: 'same-origin' });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      return err({ kind: 'aborted' });
    }
    if (error instanceof TypeError) return err({ kind: 'network', message: error.message });
    throw error;
  }
  if (!response.ok) {
    const body = (await readText(response, signal)).trim();
    const message = body.length > 0 ? body : (HTTP_MEANINGS[response.status] ?? 'request failed');
    return err({ kind: 'http', status: response.status, message: message.slice(0, 300) });
  }
  return ok(response);
}

async function readText(response: Response, signal: AbortSignal): Promise<string> {
  const body = await readBody(() => response.text(), signal);
  return body.ok ? body.value : '';
}

async function readBody<T>(
  read: () => Promise<T>,
  signal: AbortSignal,
): Promise<Result<T, LoadError>> {
  try {
    return ok(await read());
  } catch (error) {
    if (signal.aborted) return err({ kind: 'aborted' });
    if (error instanceof TypeError || error instanceof SyntaxError) {
      return err({ kind: 'invalid', message: error.message });
    }
    throw error;
  }
}

/** Fetches JSON and validates it against `schema`. */
export async function loadJson<S extends z.ZodType>(
  url: string,
  signal: AbortSignal,
  schema: S,
): Promise<Result<z.output<S>, LoadError>> {
  const response = await request(url, signal);
  if (!response.ok) return response;
  const body = await readBody(() => response.value.json() as Promise<unknown>, signal);
  if (!body.ok) return body;
  const parsed = schema.safeParse(body.value);
  if (!parsed.success) return err({ kind: 'invalid', message: describeIssues(parsed.error) });
  return ok(parsed.data);
}

/** Fetches bytes and decodes them with `decode`. */
export async function loadBinary<T>(
  url: string,
  signal: AbortSignal,
  decode: (buffer: ArrayBuffer) => Result<T, { readonly message: string }>,
): Promise<Result<T, LoadError>> {
  const response = await request(url, signal);
  if (!response.ok) return response;
  const body = await readBody(() => response.value.arrayBuffer(), signal);
  if (!body.ok) return body;
  const decoded = decode(body.value);
  if (!decoded.ok) return err({ kind: 'invalid', message: decoded.error.message });
  return ok(decoded.value);
}
