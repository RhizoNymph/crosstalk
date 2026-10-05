/** Schema pieces shared by the payloads, mirroring `ui/src/data/topology/mod.rs`. */

// A namespace import, not `{ z }`: it lets the bundler drop zod's locales
// and JSON Schema tooling (about 85 KB instead of 450 KB minified).
import * as z from 'zod';
import { ROUTE_KINDS } from '../shared/route.ts';
import { ULID_PATTERN, type Ulid } from '../shared/ulid.ts';

export const ulid = z
  .string()
  .regex(ULID_PATTERN, 'expected a ULID')
  .transform((text) => text as Ulid);

/** RFC 3339 in UTC, as `format_time` writes it. */
export const timestamp = z.iso.datetime({ offset: false });

/** A `u64` counter. Counts stay far below 2^53. */
export const count = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);

export const share = z.number().min(0).max(1);

export const routeKind = z.enum(ROUTE_KINDS);

/** A half-open `[from, to)` window. */
export const window = z
  .strictObject({ from: timestamp, to: timestamp })
  .refine((w) => Date.parse(w.from) < Date.parse(w.to), 'window: from must be before to');

/** The first few issues of a failed parse, one per line. */
export function describeIssues(error: z.ZodError): string {
  const lines = error.issues
    .slice(0, 3)
    .map((issue) => `${issue.path.length > 0 ? issue.path.join('.') : '(root)'}: ${issue.message}`);
  const more = error.issues.length > 3 ? `\n… and ${error.issues.length - 3} more` : '';
  return lines.join('\n') + more;
}
