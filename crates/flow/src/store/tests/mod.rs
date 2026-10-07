//! Tests of the flow stores. The Postgres tests are gated on
//! `TEST_DATABASE_URL` (or `.env.test`): without it they print
//! `skipping <name>` and pass.

mod concurrency;
mod detection;
mod holding;
mod list;
mod model;
mod registry;
pub(crate) mod support;
mod verdicts;
