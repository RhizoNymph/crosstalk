-- TransmissionStore::holding: every content match a stored transmission
-- holds, keyed as MatchKey (origin span, reader exchange, read location),
-- rewritten with the transmission on every save. A key is held by at most
-- one transmission (identity is reader exchange, sender and route).
CREATE TABLE transmission_matches (
    origin          TEXT COLLATE "C" NOT NULL,
    reader_exchange TEXT COLLATE "C" NOT NULL,
    -- The read part's message hash, lower-case hex.
    message         TEXT COLLATE "C" NOT NULL,
    part            INTEGER NOT NULL CHECK (part BETWEEN 0 AND 65535),
    range_start     BIGINT NOT NULL CHECK (range_start >= 0),
    range_end       BIGINT NOT NULL CHECK (range_end > range_start),
    transmission_id TEXT COLLATE "C" NOT NULL REFERENCES transmissions (id) ON DELETE CASCADE,
    PRIMARY KEY (origin, reader_exchange, message, part, range_start, range_end)
);

CREATE INDEX transmission_matches_by_transmission ON transmission_matches (transmission_id);

-- Transmissions saved before this table existed.
INSERT INTO transmission_matches
    (origin, reader_exchange, message, part, range_start, range_end, transmission_id)
SELECT m.v->>'origin',
       m.v->>'reader_exchange',
       m.v->'read_at'->'part'->>'message',
       (m.v->'read_at'->'part'->>'index')::integer,
       (m.v->'read_at'->'range'->>'start')::bigint,
       (m.v->'read_at'->'range'->>'end')::bigint,
       t.id
FROM transmissions t,
     LATERAL jsonb_array_elements(
         CASE t.state
             WHEN 'confirmed' THEN t.transmission::jsonb->'state'->'data'->'content'
             WHEN 'classified' THEN t.transmission::jsonb->'state'->'data'->'confirmed'->'content'
             WHEN 'aggregated' THEN t.transmission::jsonb->'state'->'data'->'confirmed'->'content'
             ELSE '[]'::jsonb
         END
     ) AS m(v)
ON CONFLICT DO NOTHING;
