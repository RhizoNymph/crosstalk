-- Fixture layer migration (beta): a second version, unqualified, so it lands
-- in the layer's schema through the runner's search_path.
CREATE TABLE notes (
    item_id bigint NOT NULL REFERENCES items (id),
    body text NOT NULL
);
