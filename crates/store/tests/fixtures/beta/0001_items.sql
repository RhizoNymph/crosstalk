-- Fixture layer migration (beta): the same version and table name as alpha,
-- with a different shape.
CREATE TABLE items (
    id bigint PRIMARY KEY,
    weight integer NOT NULL CHECK (weight > 0)
);
