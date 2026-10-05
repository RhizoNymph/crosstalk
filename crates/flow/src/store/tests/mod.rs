//! Tests of the flow stores. The Postgres tests are gated on
//! `TEST_DATABASE_URL` (or `.env.test`): without it they print
//! `skipping <name>` and pass.

mod concurrency;
mod detection;
mod model;
mod registry;
mod support;
mod verdicts;
