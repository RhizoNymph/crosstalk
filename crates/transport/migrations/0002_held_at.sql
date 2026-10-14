-- When a delivery was taken (bus-clock micros), so a hold no process
-- tracks any more (its taker's reaper message lost, its commit landing
-- after its provisional deadline, a process gone without `recover_held`
-- seeing it) is still taken back: the reaper sweeps holds older than the
-- longest a tracked hold can live (`transport.restart.held-redelivered`,
-- `transport.hold.always-reaped`). NULL for rows held before this column.
ALTER TABLE deliveries ADD COLUMN held_at BIGINT;

CREATE INDEX deliveries_held ON deliveries (held_at) WHERE state = 'held';
