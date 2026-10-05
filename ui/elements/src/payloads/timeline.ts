/**
 * The `<ct-timebrush>` payload: `GET /data/timeline?<view state>&buckets=<n>`,
 * JSON. Mirrors `TimelinePayload` in `ui/src/data/timeline.rs`.
 */

import * as z from 'zod';
import { count, timestamp, window } from './common.ts';

const bucket = z
  .strictObject({
    from: timestamp,
    to: timestamp,
    transmissions: count,
    matchedBytes: count,
    final: z.boolean(),
  })
  .refine((b) => Date.parse(b.from) < Date.parse(b.to), 'bucket: from must be before to');

export const timelinePayload = z
  .strictObject({
    window,
    bucketMs: count,
    watermark: timestamp,
    buckets: z.array(bucket),
  })
  .superRefine((payload, ctx) => {
    payload.buckets.forEach((b, i) => {
      const previous = payload.buckets[i - 1];
      if (previous !== undefined && Date.parse(previous.to) > Date.parse(b.from)) {
        ctx.addIssue({ code: 'custom', path: ['buckets', i], message: 'buckets overlap' });
      }
    });
  });

export type TimelinePayload = z.output<typeof timelinePayload>;
export type TimelineBucket = TimelinePayload['buckets'][number];
