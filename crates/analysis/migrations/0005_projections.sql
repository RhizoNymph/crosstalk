-- L6 projection jobs and frames (crosstalk_analysis::projections,
-- PgProjectionStore). A job moves only through the spec's transitions
-- (ProjectionInfo::start, requeue, complete, fail, expire); its record is
-- ProjectionInfo's wire JSON, with the columns claims, lease lapses and
-- expiry select on beside it. Leases and expiry compare times passed in,
-- never now().

CREATE TABLE projection_jobs (
    id text COLLATE "C" PRIMARY KEY,
    -- ProjectionInfo's wire JSON.
    info text NOT NULL,
    state text NOT NULL CHECK (state IN ('queued', 'fitting', 'ready', 'failed', 'expired')),
    requested_at bigint NOT NULL,
    -- While fitting: when the claim's lease lapses.
    lease_until bigint,
    -- While ready: when the fit returned (frame retention runs from it).
    fitted_at bigint,
    CHECK ((state = 'fitting') = (lease_until IS NOT NULL)),
    CHECK ((state = 'ready') = (fitted_at IS NOT NULL))
);
CREATE INDEX projection_jobs_queued ON projection_jobs (requested_at, id) WHERE state = 'queued';
CREATE INDEX projection_jobs_leased ON projection_jobs (lease_until) WHERE state = 'fitting';
CREATE INDEX projection_jobs_ready ON projection_jobs (fitted_at) WHERE state = 'ready';

-- A ready job's frame, in the binary layout of ProjectionFrame::encode.
-- Deleted when the frame expires.
CREATE TABLE projection_frames (
    job text COLLATE "C" PRIMARY KEY REFERENCES projection_jobs (id),
    frame bytea NOT NULL
);
