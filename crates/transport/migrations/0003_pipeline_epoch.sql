-- The pipeline epoch: one row, the number of times the pipeline lock has
-- been taken. Each new holder of the lock bumps it (autocommit, on the
-- lock's own session) before it waits for the fence, and every connection
-- of a pipeline incarnation reads it after joining the fence shared: a
-- connection whose incarnation's epoch is no longer this one refuses
-- itself, so an older incarnation can never write once a newer one runs
-- (`transport.pipeline-lock.no-write-from-an-older-epoch`,
-- crates/gateway/src/store/fence). 0 until the first take.
CREATE TABLE pipeline_epoch (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch     bigint NOT NULL CHECK (epoch >= 0)
);

INSERT INTO pipeline_epoch (singleton, epoch) VALUES (true, 0);
