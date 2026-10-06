-- L1's exchange store, schema "canonical" (the runner sets the search
-- path): every captured exchange record without its bodies, which stay in
-- the blob store.

CREATE TABLE exchanges (
    -- The exchange id, ULID text.
    id         text COLLATE "C" PRIMARY KEY CHECK (length(id) = 26),
    -- ExchangeMeta::started_at, microseconds since the epoch.
    started_at bigint NOT NULL,
    -- The StoredExchange, as its wire JSON.
    record     text NOT NULL
);

-- ExchangeReads::list: newest first by (started_at, id).
CREATE INDEX exchanges_newest ON exchanges (started_at DESC, id DESC);
