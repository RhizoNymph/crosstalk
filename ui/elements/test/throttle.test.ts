import { describe, expect, it } from 'vitest';
import { refreshDelay } from '../src/live/throttle.ts';

describe('refresh throttle', () => {
  it('waits only the settle delay before the first refresh', () => {
    expect(refreshDelay(10_000, null, 250, 4000)).toBe(250);
  });

  it('waits out the minimum interval after a recent refresh', () => {
    expect(refreshDelay(11_000, 10_000, 250, 4000)).toBe(3000);
    expect(refreshDelay(13_900, 10_000, 250, 4000)).toBe(250);
  });

  it('waits only the settle delay once the interval has passed', () => {
    expect(refreshDelay(20_000, 10_000, 250, 4000)).toBe(250);
  });
});
