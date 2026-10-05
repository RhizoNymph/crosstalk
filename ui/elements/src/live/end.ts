/**
 * The last event of a `GET /data/live` stream: `event: end` with
 * `{"reason": ...}` and no id (mirrors `ended` in `ui/src/data/live.rs`).
 *
 * After it the response closes and the browser's `EventSource` reconnects
 * with its last id, which suits every reason but one: `unreachable` means
 * the UI's backend gave up reaching the gateway, so a reconnect would only
 * fail again. For it `<ct-live>` closes the stream and says why.
 */

import { err, ok, type Result } from '../shared/result.ts';

/** Why a stream ended. */
export const END_REASONS = ['lagged', 'session-ended', 'shutting-down', 'unreachable'] as const;
export type EndReason = (typeof END_REASONS)[number];

/** What `<ct-live>` does at an end: reconnect, or stop and show `message`. */
export type EndStep =
  | { readonly kind: 'resume' }
  | { readonly kind: 'stop'; readonly message: string };

/** The notice of a stream the gateway can no longer feed. */
export const UNREACHABLE_MESSAGE = 'Live updates lost: gateway unreachable. Reload to try again.';

/** The reason an end event's `data` names. */
export function parseEnd(data: string): Result<EndReason, string> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return err('not JSON');
  }
  if (typeof parsed !== 'object' || parsed === null || !('reason' in parsed)) {
    return err('no reason');
  }
  const reason = (parsed as { reason: unknown }).reason;
  if (typeof reason !== 'string' || !(END_REASONS as readonly string[]).includes(reason)) {
    return err(`unknown reason ${JSON.stringify(reason)}`);
  }
  return ok(reason as EndReason);
}

/** The step an end of `reason` asks for. */
export function endStep(reason: EndReason): EndStep {
  switch (reason) {
    case 'unreachable':
      return { kind: 'stop', message: UNREACHABLE_MESSAGE };
    case 'lagged':
    case 'session-ended':
    case 'shutting-down':
      return { kind: 'resume' };
  }
}
