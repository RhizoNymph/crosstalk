//! Postgres-gated integration tests of the flow layer's restart
//! durability. Each test skips (and says so) without `TEST_DATABASE_URL`.

mod checkpoint;
mod outbox;
