# Store

`crosstalk-store` (`crates/store`), roadmap item P1.5: the Postgres
infrastructure every layer crate's Postgres implementation builds on. It
holds the connection pool and its config, the per-layer schema and
migration runner, the required extensions, typed errors with a
classification layer crates map into their spec errors, a serializable
transaction retry helper, and the test database harness.

It is infrastructure: layer crates may depend on it (the architecture test
classifies it `Open`), and it depends on no layer crate, only on
`crosstalk-spec` and third-party crates.

## Scope

- `StoreConfig`: the database URL from `DATABASE_URL` (a secret, so
  environment only) and the pool sizing (`PoolSettings`) from structured
  config.
- `Store::connect`: a `sqlx` `PgPool` that has proven one connection.
- `Layer` and the per-layer schema convention; `migrate(pool, layer,
  migrations)`, which runs one layer's migrations in its own schema with its
  own migrations table.
- `ensure_extensions`: `vector` (pgvector) and `pg_trgm`.
- `StoreError`, `DbFailure` and `classify(&sqlx::Error)`.
- `retry_serializable`: a `SERIALIZABLE` transaction with bounded retries.
- `TestDb`: a fresh database per test on the `TEST_DATABASE_URL` server, and
  `scripts/test-db.sh`, which starts that server.

## Non-scope

- Any layer's tables, queries or migrations. Each layer crate owns
  `crates/<layer>/migrations/` and its Postgres implementations of the spec
  store traits; this crate only runs and supports them.
- Mapping `DbFailure` into spec errors. That mapping is per layer, because
  each layer's spec error enum is different.
- TimescaleDB. Decision D3 proposes plain Postgres with partitioned bucket
  tables first, so TimescaleDB is neither required nor created. If D3 lands
  on hypertables, it becomes an `Extension` variant, which is an optional one
  since not every server ships it.
- TLS. No TLS feature is compiled in; see [TLS](#tls).
- Compile-time-checked queries (`sqlx::query!`); see [D2](#decision-d2-sqlx-with-runtime-queries).

## Decision D2: sqlx with runtime queries

**Chosen: sqlx `=0.9.0`** (released 2026-05-21) with runtime-checked queries
(`sqlx::query`, `query_as`, `query_scalar` with `.bind`), not the
`query!` macros, and not tokio-postgres.

Why sqlx over tokio-postgres:

- **Pool included.** tokio-postgres has no pool, so it needs deadpool or bb8
  as a second dependency, with its own config and error types.
- **Migrations included, per layer.** sqlx's migrator embeds a directory at
  compile time (`sqlx::migrate!`), checksums applied migrations, holds an
  advisory lock, and since 0.9 takes a custom migrations table name and
  schemas to create (`dangerous_set_table_name`, `create_schema`). That is
  exactly the per-layer table this item needs. With tokio-postgres we would
  write and maintain all of that ourselves (or add refinery).
- **Typed errors.** `sqlx::Error::Database` exposes the SQLSTATE and
  constraint name, which is all `classify` needs.
- **Convention.** The user's Rust guide names sqlx for SQL.
- What tokio-postgres would offer (explicit pipelining, a lower-level
  protocol API) is not needed now. sqlx also covers the bulk path
  (`COPY ... FROM STDIN` through `PgConnection::copy_in_raw`).

Why runtime queries rather than the checked macros: `query!` checks SQL
against a live database at compile time, or against a `.sqlx/` offline
cache that has to be regenerated against a live database whenever a query
changes. CI here has no database, and a stale cache fails the build in a
confusing way. Runtime queries are instead covered by the gated integration
tests and, per roadmap principle 3, by the model-based property tests that
compare each Postgres store with its `crosstalk-memory` reference. The
offline macros can be adopted later per crate without changing this crate.

Features: `default-features = false` with `postgres`, `runtime-tokio`,
`migrate` and `macros`. `macros` is only for `sqlx::migrate!`, which reads
files and never connects to a database. Leaving out the defaults drops
`any` and `json`. sqlx 0.9 needs Rust 1.94, which the pinned nightly
satisfies. It resolves against the workspace's `tokio = "=1.53.1"`.

### TLS

No TLS backend is compiled in, because the local and test servers speak
plaintext on 127.0.0.1. A URL with `sslmode=require` fails to connect. When
a deployment needs TLS, add sqlx's `tls-rustls-ring-webpki` (or
`-native-roots`) feature. That matches the UI's rustls 0.23.45 on ring, and
needs no other change here.

## Data and control flow

### Configuration and connect

1. The gateway's structured config (JSON or YAML) carries a `PoolSettings`:
   `{"max_connections": 10, "min_connections": 0, "acquire_timeout_ms": 5000}`.
   - `max_connections` is required and at least 1.
   - `min_connections` defaults to 0 and is at most `max_connections`.
   - `acquire_timeout_ms` defaults to 5000 and is non-zero.
   - Unknown fields are refused, and deserializing goes through
     `PoolSettings::new`.
2. `StoreConfig::from_env(pool)` reads `DATABASE_URL` and parses it into a
   `DatabaseUrl`. The scheme must be `postgres://` or `postgresql://`, then
   `PgConnectOptions` parses it. Failures are `ConfigError`:
   - `MissingVar`, `NotUnicode`;
   - `InvalidUrl { reason: Scheme | Unparsable }`. The reason never repeats
     the URL.

   `from_lookup` takes the variable lookup as a closure, which is how the
   unit tests avoid touching the process environment (`set_var` is unsafe
   in edition 2024, and unsafe is forbidden).
3. `Store::connect(&config)` builds a `PgPool` with the settings and opens
   one connection. Failure is `StoreError::Connect { target, failure,
   source }`:
   - `target` is `host:port/database`, never the user or password;
   - `failure` is usually `ConnectionLost` or `PoolTimedOut`.

   On success it logs `info` with `target_db`, `max_connections` and
   `min_connections`.

### Per-layer migrations

Each layer crate owns a schema named after the layer (`Layer::schema`:
`ingress`, `canonical`, `transport`, `reconstruct`, `provenance`, `flow`,
`analysis`, `topology`, `surface`) and a `migrations/` directory beside its
`Cargo.toml`. Every layer crate embeds its own migrations, because
`sqlx::migrate!` takes a path relative to the calling crate's manifest:

```rust
// crates/flow/src/store.rs (a layer crate; depends on sqlx and crosstalk-store)
static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

crosstalk_store::migrate(pool, Layer::Flow, Migrations::Embedded(&MIGRATIONS)).await?;
```

The macro expands to `::sqlx::...` paths, so a layer crate that embeds
migrations also lists `sqlx.workspace = true`. `crosstalk_store::sqlx` is
re-exported for naming types through the same pin.

`migrate(pool, layer, Migrations::Embedded(&m) | Migrations::Directory(path))`:

1. Resolves the migrations: it copies the embedded list, or reads the
   directory at run time.
2. Builds a fresh `Migrator` for the layer:
   - the migrations table is `"<layer>"."_sqlx_migrations"`
     (`Layer::migrations_table`);
   - the schema to create is `"<layer>"`;
   - locking stays on.
3. Acquires a pool connection and marks it `close_on_drop`, so it never
   returns to the pool. Then it runs `SET search_path TO "<layer>", public`
   on it. Unqualified DDL lands in the layer's schema, and the extensions in
   `public` (the `vector` type, `gin_trgm_ops`) still resolve.
4. Runs the migrator (`run_direct`; see below) on that connection. sqlx:
   1. takes the per-database advisory lock;
   2. creates the schema and the table;
   3. refuses a dirty version, or an applied migration whose checksum
      changed (`VersionMismatch`) or that is missing (`VersionMissing`);
   4. applies each pending migration and its bookkeeping row in one
      transaction;
   5. unlocks.
5. Drops the connection, which closes the session. That also releases the
   advisory lock if a migration failed before sqlx unlocked. On success it
   logs `info` with `layer`, `migrations` and `table`.

Failures are `StoreError::Migrate { layer, source: MigrateError }`.

Two layers can therefore both have a version `1`, and both can name a table
`items`. Their histories live in different tables and their objects in
different schemas. Concurrent runners for different layers (start-up wiring,
several nodes) serialize on the advisory lock.

`run_direct` is used instead of `run` because `run`'s `A: Acquire<'a>`
bound makes the future fail the `Send + 'static` check `tokio::spawn` needs
(rustc #100013). sqlx exposes `run_direct` for this. It is `#[doc(hidden)]`,
which the exact pin makes safe, and `migrate_future_is_spawnable` guards it.

Runtime queries from layer crates qualify tables with the schema
(`flow.channels`). The pool keeps the server's default `search_path`, which
the runner never changes.

### Extensions

`ensure_extensions(pool)` runs `ensure_extension` for each of
`Extension::REQUIRED = [Vector, PgTrgm]`:

1. If `pg_available_extensions` does not list the extension, it fails with
   `StoreError::ExtensionMissing { extension, problem: NotAvailable }`.
2. It runs `CREATE EXTENSION IF NOT EXISTS <name> WITH SCHEMA public`.
3. It reads `extversion` from `pg_extension`, whether or not the create
   failed:
   - Present: the extension is ready. This also covers losing a concurrent
     create race in the same database, which fails on a catalog unique index
     after the winner commits.
   - Absent after a failed create: `ExtensionMissing { problem:
     CreateFailed(DbFailure) }`, typically a privilege failure.

It returns `Vec<InstalledExtension { extension, version }>` and logs `info`
per extension.

### Errors and classification

`classify(&sqlx::Error) -> DbFailure`:

| Source | `DbFailure` |
| --- | --- |
| SQLSTATE `23505` | `UniqueViolation { constraint }` |
| SQLSTATE `23503` | `ForeignKeyViolation { constraint }` |
| SQLSTATE `23514` | `CheckViolation { constraint }` |
| SQLSTATE `40001` | `SerializationFailure` (retryable) |
| SQLSTATE `40P01` | `DeadlockDetected` (retryable) |
| SQLSTATE class `08`, `57P01`, `57P02`, `57P03`; `Io`, `Tls`, `PoolClosed`, `WorkerCrashed` | `ConnectionLost` |
| `PoolTimedOut` | `PoolTimedOut` |
| `RowNotFound` | `RowNotFound` |
| `Migrate(Execute(e) \| ExecuteMigration(e, _))` | `classify(e)` |
| any other server error | `Server { sqlstate }` |
| anything else (decode, encode, protocol, config, column) | `Client` |

`is_retryable()` is true for serialization failures and deadlocks only.
`is_unavailable()` is true for `ConnectionLost` and `PoolTimedOut`.

A layer maps these into its spec error, for example:

- `UniqueViolation` becomes a conflict;
- `is_unavailable()` becomes its unavailability variant;
- `RowNotFound` becomes not found.

`StoreError` variants:

- `Config`;
- `Connect { target, failure, source }`;
- `Migrate { layer, source }`;
- `ExtensionMissing { extension, problem }`;
- `Query { failure, source }`, which `From<sqlx::Error>` builds, classified;
- `RetriesExhausted { attempts, failure, source }`.

`StoreError::failure()` returns the classified failure when there is one.

### Serializable retries

```rust
retry_serializable(pool, &SerializableRetry::default(), |conn| Box::pin(async move {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM flow.channels").fetch_one(&mut *conn).await?;
    if n > LIMIT { return Err(TxError::Abort(MyError::Full)); }
    sqlx::query("INSERT INTO flow.channels ...").execute(&mut *conn).await?;
    Ok(n)
})).await
```

1. `pool.begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE")`.
2. Run the body with the transaction's connection. `?` on a `sqlx::Error`
   gives `TxError::Db`, and the body refuses with `TxError::Abort(E)`.
3. `Ok` commits. An `Err` rolls back; a failed rollback is logged at `debug`,
   and the body's error is the one reported.
4. A `Db` error from the body or the commit is classified:
   - Retryable, with budget left: log `debug` (`attempt`, `max_attempts`,
     `failure`, `backoff_ms`), sleep `backoff_after(attempt)`, and go to 1.
   - Retryable, budget spent: `SerializableError::Store(RetriesExhausted)`,
     logged at `warn`.
   - Not retryable: `SerializableError::Store(Query { failure, .. })`.
5. `Abort(e)` returns `SerializableError::Aborted(e)` at once, without a
   retry.

`SerializableRetry::new(max_attempts: NonZeroU32, initial, max)` refuses
`initial > max`. The backoff doubles from `initial` and is capped at `max`.
The default is 5 attempts, 5 ms initial and 200 ms max. The body is
`for<'c> FnMut(&'c mut PgConnection) -> TxFuture<'c, T, E> + Send`, and
`TxFuture` is a boxed `Send` future, so the whole call is `Send` like every
spec trait future (`retry_future_is_send`). The body can run several times,
so it must have no side effects outside the transaction.

### Test databases

`TestDb::new().await`:

1. Requires a multi-threaded tokio runtime. Otherwise it fails with
   `RuntimeProblem::{NoRuntime, CurrentThread}`; the reason is in teardown
   below.
2. Reads `TEST_DATABASE_URL`, an admin-capable URL. Unset gives
   `TestDbError::NotConfigured`.
3. On a short-lived admin connection, runs `CREATE DATABASE
   "crosstalk_test_<pid>_<nanos>_<seq>" TEMPLATE template0`. The database
   starts empty: no extensions, even if `template1` has some.
4. Opens a pool on it with `TestDb::default_pool_settings()`: 4 connections
   and a 30 s acquire timeout, small enough for many tests at once.
   `with_pool_settings` overrides this.

A whole database per test, not a schema per test, because the layer
schemas have fixed names: two tests migrating `flow` into one database would
collide. Databases are independent, so tests run in parallel, both within
a test binary and across processes (the pid is in the name).

Helpers:

- `pool()`, `store()`, `url()` and `name()`;
- `migrate(layer, migrations)`, the layer runner against this database;
- `ensure_extensions()`.

Teardown (`close().await`, or `Drop` if the test did not close):

1. Close the pool, waiting at most `CLOSE_GRACE` (1 s) for checked-out
   connections.
2. On a new admin connection, run `DROP DATABASE IF EXISTS "<name>" WITH
   (FORCE)`, which also terminates any session still attached.

`Drop` cannot await, so it runs teardown through
`tokio::task::block_in_place(|| handle.block_on(...))`. This is tokio's own
way to block from sync code inside its runtime, and it only works on the
multi-threaded runtime, which is why `new` insists on one. A failed drop is
logged at `warn` and printed. A database left behind (only if teardown fails)
is named `crosstalk_test_*` and dies with the disposable container.

`TestDb::new_or_skip(test)` is the gate: when `TEST_DATABASE_URL` is unset it
writes `skipping <test>: TEST_DATABASE_URL is not set (run scripts/test-db.sh
and export the URL it prints)` straight to the stderr handle, which libtest
does not capture, so the line shows in a plain `cargo test`. It then returns
`Ok(None)`, and the test returns early as a pass:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn my_store_test() -> Result<(), MyFailure> {
    let Some(db) = TestDb::new_or_skip("my_store_test").await? else { return Ok(()) };
    db.ensure_extensions().await?;
    db.migrate(Layer::Flow, Migrations::Embedded(&MIGRATIONS)).await?;
    // ...
    db.close().await?;
    Ok(())
}
```

## Running the database tests

```sh
eval "$(bash scripts/test-db.sh)"     # start Postgres 18 + pgvector, export TEST_DATABASE_URL
cargo test -p crosstalk-store         # unit tests + the gated integration tests
bash scripts/test-db.sh stop          # remove the container
```

`scripts/test-db.sh`:

- starts `pgvector/pgvector:pg18` (Postgres 18 with pgvector; `pg_trgm`
  ships in contrib) as container `crosstalk-test-db`, published on
  `127.0.0.1:55432` only;
- puts the data directory on a tmpfs, turns fsync, synchronous commit and
  full-page writes off, and sets `max_connections=500` for parallel tests;
- waits up to 60 s for `pg_isready` over TCP. The entrypoint's init-time
  server is socket-only, so TCP readiness means the real server is up;
- checks that both extensions are available;
- prints `export TEST_DATABASE_URL=postgres://postgres:crosstalk@127.0.0.1:55432/postgres`.

`url` reprints the URL and `stop` removes the container. The image,
container name, port and password are overridable through
`CROSSTALK_TEST_DB_*`. The password guards a throwaway, loopback-only test
server and is not a secret.

Without `TEST_DATABASE_URL`, `cargo test` (and `scripts/check.sh`) still
passes. Every gated test prints its skip line, and the unit tests,
`unreachable_server_is_a_typed_connect_error` and the compile-time `Send`
checks still run.

## Invariants and constraints

- `crosstalk-store` depends on no layer crate. The architecture test treats
  it as `Open`.
- Each layer's objects and migration history live only in its own schema.
  Nothing is created in `public` except the extensions.
- The migration runner never changes the `search_path` of a pooled
  connection. Its connection is closed, not returned.
- `DatabaseUrl`'s `Debug`, `target()` and every error omit the password.
- `PoolSettings` and `SerializableRetry` exist only in valid states (checked
  constructors; deserialization goes through them).
- Identifiers spliced into SQL are fixed lowercase layer names or
  `TestDbName`s (prefix, digits, underscores; at most 63 bytes). No input
  string is ever spliced.
- `retry_serializable` retries only `SerializationFailure` and
  `DeadlockDetected`, never more than `max_attempts` times in total, and
  never retries an `Abort`.
- `TestDb` exists only on a multi-threaded runtime, so its `Drop` can always
  run teardown.
- No `unwrap` or `expect` outside tests; `unsafe` is forbidden workspace-wide.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/store/Cargo.toml` | Manifest: `crosstalk-spec`, serde, sqlx, thiserror, tokio (`rt-multi-thread`, `time`), tracing; dev: serde_json, sqlx, thiserror, tokio (+`macros`, `sync`) | — |
| `crates/store/src/lib.rs` | Crate doc and re-exports | everything below, `sqlx` |
| `crates/store/src/config.rs` | URL from the environment, pool sizing | `StoreConfig`, `DatabaseUrl`, `PoolSettings`, `ConfigError`, `UrlProblem`, `DATABASE_URL_VAR`, `TEST_DATABASE_URL_VAR` |
| `crates/store/src/pool.rs` | The pool | `Store` (`connect`, `from_pool`, `pool`, `migrate`, `ensure_extensions`, `close`) |
| `crates/store/src/layer.rs` | Layers and their schemas | `Layer` (`ALL`, `schema`, `quoted_schema`, `migrations_table`, `search_path`), `MIGRATIONS_TABLE` |
| `crates/store/src/migrate.rs` | Per-layer migration runner | `migrate`, `Migrations` |
| `crates/store/src/extensions.rs` | Required extensions | `Extension`, `InstalledExtension`, `ensure_extensions`, `ensure_extension` |
| `crates/store/src/error.rs` | Errors and classification | `StoreError`, `DbFailure`, `ExtensionProblem`, `classify` |
| `crates/store/src/retry.rs` | Serializable retries | `retry_serializable`, `SerializableRetry`, `InvalidSerializableRetry`, `TxError`, `TxFuture`, `SerializableError` |
| `crates/store/src/test_db.rs` | Test database harness | `TestDb`, `TestDbName`, `TestDbError`, `RuntimeProblem`, `TEST_DB_PREFIX` |
| `crates/store/tests/postgres/main.rs` | Gated integration tests: shared helpers and the `Failure` type | — |
| `crates/store/tests/postgres/{harness,migrations,extensions,retry,connect}.rs` | Isolation and teardown; per-layer migrations; extensions; retries; connect | — |
| `crates/store/tests/fixtures/{alpha,beta,search}/` | Fixture migration directories. alpha and beta share version 1 and table `items`; search uses `vector` and `gin_trgm_ops` | — |
| `scripts/test-db.sh` | Disposable Postgres 18 + pgvector for the gated tests | — |

## Tests

Unit tests (no database), 46 of them:

- `config`: URL parsing and scheme checks, missing and non-unicode
  variables, no password in `Debug` or errors, `PoolSettings` JSON shapes
  and refusals, the checked constructor.
- `error`: every `classify` row, through a fake `DatabaseError`;
  `is_retryable` and `is_unavailable`; `StoreError::failure` and the
  `From<sqlx::Error>` conversion.
- `layer`: distinct, lowercase schemas and tables.
- `migrate`: the layer migrator's table, schema and lock; the future is
  `Send + 'static`.
- `extensions`: the required set, idempotent DDL in `public`, no TimescaleDB.
- `retry`: backoff doubling, cap and overflow; the policy constructor; the
  future is `Send`.
- `test_db`: names are unique, safe and at most 63 bytes; the runtime checks;
  `block_in_place` teardown inside a multi-threaded `#[tokio::test]`.

Integration tests (`tests/postgres`), gated on `TEST_DATABASE_URL` except
the first:

- connect: `unreachable_server_is_a_typed_connect_error` (runs without a
  server) and `connect_opens_a_working_pool`.
- harness: databases are isolated; `close` and `Drop` both drop the database,
  `Drop` even with a connection still checked out; 8 parallel databases each
  migrate the same layer.
- migrations:
  - same versions in two layers don't collide, with nothing in `public` and
    the pool's `search_path` untouched;
  - re-running is a no-op;
  - all nine layers migrate concurrently;
  - an edited applied migration is refused with `VersionMismatch`, and is
    fine under another layer;
  - a failing migration is typed and leaves nothing behind;
  - unique, check, foreign-key and undefined-table errors classify.
- extensions: created in `public`, idempotent, safe under concurrent calls,
  and usable unqualified from a layer migration (a `vector(3)` nearest
  neighbour and a `%` trigram match).
- retry:
  - write skew between two transactions is retried to success, and the
    retried one sees the first one's row;
  - an always-conflicting body runs exactly `max_attempts` times, then gives
    `RetriesExhausted`;
  - an abort rolls back without a retry;
  - a unique violation is not retried;
  - the isolation level is `serializable`.
