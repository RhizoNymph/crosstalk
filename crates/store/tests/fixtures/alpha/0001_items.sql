-- Fixture layer migration (alpha): version 1 creates `items`. The beta
-- fixture also has a version 1 creating `items`; per-layer schemas and
-- migrations tables keep them apart.
CREATE TABLE items (
    id bigint PRIMARY KEY,
    label text NOT NULL
);
