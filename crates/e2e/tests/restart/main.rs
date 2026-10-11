//! Restart durability end to end: the wiki relay fed through Postgres-mode
//! `Live` processes (`Live::<PgSet>::start_pg`) that are killed or stopped
//! mid-stream and restarted over the same database, filesystem blob store
//! and spool directory, as `serve` restarts on its volume. Each run's
//! final answers (agents, the topology graph, the channel edge's
//! transmissions, every stored transmission with its summary and evidence
//! page, the channels, the watermark; `crosstalk_e2e::answers`) must equal
//! an uninterrupted run's once minted ids are renamed: a restarted process
//! draws new ids, as `serve` draws them from OS entropy.
//!
//! - kills: after each exchange while feeding, in the middle of a settle,
//!   right after the flow consumer wrote a checkpoint, and while store
//!   outbox rows wait for their relay;
//! - a graceful restart mid-stream;
//! - `surface.cursor.survives-restart` (INV-1219): page cursors issued
//!   before a restart resolve to the same pages after it;
//! - `surface.ids.unique-across-restart` (INV-1220): with `serve`'s id
//!   policy, ids minted after a restart at the very same instant never
//!   repeat a persisted one.
//!
//! The kill points are aimed by watching the database (a checkpoint row
//! rewritten, outbox rows staged) and the kill lands a moment later, so
//! the exact point varies from run to run; what each test asserts holds
//! wherever it lands. Each test logs what the database held right after
//! its kill.
//!
//! Postgres tests: they need the test database (`TEST_DATABASE_URL`,
//! `TestDb::new_or_skip`) and skip, passing, without one. The renaming
//! itself is checked without a database
//! (`renamed_answers_agree_across_id_seeds`).
//! Evidence paths: `crosstalk_e2e::restart::*`.

#[path = "../common/mod.rs"]
mod common;

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use crosstalk_api::PgIds;
use crosstalk_e2e::answers::answers;
use crosstalk_e2e::options;
use crosstalk_e2e::read;
use crosstalk_e2e::scenario::{DEFAULT_START, Scenario};
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_gateway::live::pg::PgSet;
use crosstalk_gateway::live::{BlobConfig, Live, LiveClock, LiveConfig, Ticking};
use crosstalk_gateway::pipeline::Settings;
use crosstalk_memory::support::ManualClock;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_reconstruct::thread::ThreadConfig;
use crosstalk_spec::ids::{AgentId, AuditId, MergeId};
use crosstalk_spec::interfaces::l8_surface::actions::{ActionOutcome, OperatorAction};
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;
use crosstalk_spec::interfaces::l8_surface::conversation::ConversationFilter;
use crosstalk_spec::interfaces::l8_surface::lists::AgentFilter;
use crosstalk_spec::interfaces::l8_surface::operators::RequestIdentity;
use crosstalk_spec::interfaces::l8_surface::{Caller, OperatorActions, QueryApi};
use crosstalk_spec::observed::agent::AgentLabel;
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::TestDb;
use crosstalk_store::sqlx;
use crosstalk_transport::BusConfig;

use common::{
    OUTBOX_ROWS, Volume, caller, feed, finish_and_compare, kill_pg, migrated, part, passes,
    read as read_answers, settle, start_generation, start_pg, stop_pg, uninterrupted, within,
};

fn scenario() -> Scenario {
    Scenario::wiki_relay(DEFAULT_START)
}

/// A memory-mode live process whose id generators draw from `seed`.
async fn memory_live(scenario: &Scenario, seed: u64) -> (Live, ManualClock) {
    let clock = ManualClock::at(scenario.start);
    let live = Live::start(LiveConfig {
        surface: crosstalk_api::InProcessOptions {
            seed,
            ..options::in_process(clock.clone()).expect("surface options")
        },
        clock: LiveClock::Manual(clock.clone()),
        blobs: BlobConfig::Memory,
        bus: BusConfig::default(),
        pipeline: Settings::default(),
        flow: options::flow().expect("flow options"),
        provenance: ProvenanceConfig::default(),
        extract: ExtractConfig::default(),
        threading: ThreadConfig::default(),
        ticking: Ticking::OnSettle,
        seed,
        capture: None,
        exchange_log: None,
    })
    .await
    .expect("the memory live process");
    (live, clock)
}

/// The comparison the restart tests rest on, without a database: two
/// memory-mode runs whose id generators draw from different seeds mint
/// different agent ids, and their answers agree exactly once renamed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renamed_answers_agree_across_id_seeds() {
    let scenario = scenario();
    let [_, after] = passes(&scenario);
    let mut runs = Vec::new();
    for seed in [0xE2E, 0x005E_ED0F_7E57] {
        let (live, clock) = memory_live(&scenario, seed).await;
        crosstalk_e2e::feed(&scenario, live.pipeline(), |at| clock.set(at))
            .await
            .expect("fed");
        live.settle(after).await.expect("settles");
        let caller = live
            .caller(RequestIdentity::Anonymous)
            .await
            .expect("a caller");
        runs.push(
            answers(
                live.surface().as_ref(),
                &caller,
                &live.stores().transmissions,
                &scenario,
                live.watermark(),
            )
            .await
            .expect("answers"),
        );
        live.shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
    }
    assert!(runs[0].detected(), "{}", runs[0].transmissions);
    assert_ne!(
        runs[0].agents, runs[1].agents,
        "the seeds draw different agent ids"
    );
    assert_eq!(runs[0].renamed(), runs[1].renamed());
}

/// Kill after each of the first three exchanges, restarting each time and
/// feeding the next: the fourth process finishes the stream and answers
/// as the uninterrupted run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_while_feeding_finishes_as_uninterrupted() {
    let Some(db) = migrated("killed_while_feeding_finishes_as_uninterrupted").await else {
        return;
    };
    let scenario = scenario();
    let expected = uninterrupted(&scenario).await;
    let volume = Volume::new(scenario.start);
    let exchanges = scenario.exchanges.len();
    let mut live = start_generation(&db, &volume, 0).await;
    for killed_after in 1..exchanges {
        feed(
            &live,
            &volume,
            &part(&scenario, killed_after - 1..killed_after),
        )
        .await;
        let after_kill = kill_pg(live, &db).await;
        eprintln!("killed after exchange {killed_after}: {after_kill}");
        live = start_generation(&db, &volume, u64::try_from(killed_after).unwrap_or(0)).await;
    }
    feed(&live, &volume, &part(&scenario, exchanges - 1..exchanges)).await;
    finish_and_compare(&live, &scenario, &expected).await;
    stop_pg(live).await;
    db.close().await.expect("drops");
}

/// `settle` until `trigger` resolves first, bounded; whether the settle
/// finished before the trigger.
async fn settle_until(
    live: &Live<PgSet>,
    until: Timestamp,
    trigger: impl Future<Output = ()>,
) -> bool {
    within(live, "a settle raced against its kill trigger", async {
        tokio::select! {
            biased;
            () = trigger => false,
            settled = live.settle(until) => {
                settled.expect("settles");
                true
            }
        }
    })
    .await
}

/// Wait until `sql` (one integer or text column) answers differently from
/// its first answer.
async fn changed(db: &TestDb, sql: &'static str) {
    let first: Option<String> = sqlx::query_scalar(sql)
        .fetch_one(db.pool())
        .await
        .expect("reads");
    loop {
        let now: Option<String> = sqlx::query_scalar(sql)
            .fetch_one(db.pool())
            .await
            .expect("reads");
        if now != first {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Wait until the store outboxes hold a staged row.
async fn outbox_staged(db: &TestDb) {
    loop {
        let (rows,): (i64,) = sqlx::query_as(OUTBOX_ROWS)
            .fetch_one(db.pool())
            .await
            .expect("reads");
        if rows > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Feed the whole stream, kill once `trigger` fires during the first
/// settle, restart, finish, and compare with the uninterrupted run.
async fn kill_during_settle<T, F>(db: &TestDb, what: &str, trigger: T)
where
    T: FnOnce() -> F,
    F: Future<Output = ()>,
{
    let scenario = scenario();
    let expected = uninterrupted(&scenario).await;
    let [ends, _] = passes(&scenario);
    let volume = Volume::new(scenario.start);
    let live = start_generation(db, &volume, 0).await;
    feed(&live, &volume, &scenario).await;
    let finished = settle_until(&live, ends, trigger()).await;
    let after_kill = kill_pg(live, db).await;
    eprintln!("{what}: killed (the settle had finished: {finished}); {after_kill}");
    let restarted = start_generation(db, &volume, 1).await;
    let report = restarted.layers().status.snapshot().report;
    eprintln!("{what}: recovery {report:?}");
    finish_and_compare(&restarted, &scenario, &expected).await;
    stop_pg(restarted).await;
}

/// Killed in the middle of the first settle, the stages mid-delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_mid_settle_finishes_as_uninterrupted() {
    let Some(db) = migrated("killed_mid_settle_finishes_as_uninterrupted").await else {
        return;
    };
    kill_during_settle(&db, "mid-settle", || {
        tokio::time::sleep(Duration::from_millis(30))
    })
    .await;
    db.close().await.expect("drops");
}

/// Killed right after the flow consumer rewrote a checkpoint: between the
/// checkpoint's commit and the acks it covers (INV-1215's restart).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_right_after_a_checkpoint_finishes_as_uninterrupted() {
    let Some(db) = migrated("killed_right_after_a_checkpoint_finishes_as_uninterrupted").await
    else {
        return;
    };
    kill_during_settle(&db, "after a checkpoint", || {
        changed(
            &db,
            "SELECT coalesce(string_agg(xmin::text, ',' ORDER BY shard), '') FROM flow.checkpoints",
        )
    })
    .await;
    db.close().await.expect("drops");
}

/// Killed while a store outbox holds rows its relay has not published:
/// the restart's recovery relays them under their stamped ids.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_with_outbox_rows_pending_finishes_as_uninterrupted() {
    let Some(db) = migrated("killed_with_outbox_rows_pending_finishes_as_uninterrupted").await
    else {
        return;
    };
    kill_during_settle(&db, "outbox rows pending", || outbox_staged(&db)).await;
    db.close().await.expect("drops");
}

/// Stopped gracefully after A's two exchanges, restarted, B's fed: the
/// same answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_restart_mid_stream_finishes_as_uninterrupted() {
    let Some(db) = migrated("graceful_restart_mid_stream_finishes_as_uninterrupted").await else {
        return;
    };
    let scenario = scenario();
    let expected = uninterrupted(&scenario).await;
    let volume = Volume::new(scenario.start);
    let live = start_generation(&db, &volume, 0).await;
    feed(&live, &volume, &part(&scenario, 0..2)).await;
    stop_pg(live).await;
    let restarted = start_generation(&db, &volume, 1).await;
    feed(
        &restarted,
        &volume,
        &part(&scenario, 2..scenario.exchanges.len()),
    )
    .await;
    finish_and_compare(&restarted, &scenario, &expected).await;
    stop_pg(restarted).await;
    db.close().await.expect("drops");
}

fn page_of_one<L>(after: Option<crosstalk_spec::paging::Cursor<L>>) -> PageRequest<L> {
    PageRequest {
        size: PageSize::new(1).expect("a page size"),
        after,
    }
}

/// The scenario's agents, renamed twice (two audit entries naming A).
async fn rename_twice(live: &Live<PgSet>, caller: &Caller, agent: AgentId, tag: &str) {
    for n in 0..2 {
        let rename = OperatorAction::RenameAgent {
            agent,
            label: AgentLabel::new(&format!("{tag} {n}")).ok(),
        };
        within(live, "a rename", live.surface().act(caller, rename))
            .await
            .expect("renamed");
    }
}

/// What the cursor test pages through: the second page of each list.
#[derive(Debug, PartialEq)]
struct SecondPages {
    agents: String,
    conversations: Option<String>,
    audit: String,
}

/// `surface.cursor.survives-restart` (INV-1219): the second page of the
/// agents, the conversations and the audit log, each asked for with the
/// cursor the first page issued before a restart, is the same page after
/// it, read by a new process with the same deployment secret.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursors_resolve_after_restart() {
    let Some(db) = migrated("cursors_resolve_after_restart").await else {
        return;
    };
    let scenario = scenario();
    let [_, after] = passes(&scenario);
    let window = read::window(&scenario, options::BUCKET).expect("a window");
    let volume = Volume::new(scenario.start);
    let live = start_generation(&db, &volume, 0).await;
    feed(&live, &volume, &scenario).await;
    settle(&live, after).await;
    let lead = caller(&live).await;
    let agents = within(
        &live,
        "the agents",
        read::agents(live.surface().as_ref(), &lead, &scenario, window),
    )
    .await
    .expect("agents")
    .agents
    .expect("A and B");
    rename_twice(&live, &lead, agents.a, "before").await;

    let surface = live.surface();
    let first_agents = within(
        &live,
        "the first agents page",
        surface.agents(&lead, &AgentFilter::default(), window, &page_of_one(None)),
    )
    .await
    .expect("agents page");
    let agents_cursor = first_agents
        .value
        .next()
        .cloned()
        .expect("two agents: a cursor");
    let first_conversations = within(
        &live,
        "the first conversations page",
        surface.conversations(&lead, &ConversationFilter::default(), &page_of_one(None)),
    )
    .await
    .expect("conversations page");
    let conversations_cursor = first_conversations.next().cloned();
    let audit_filter = AuditFilter {
        by: Vec::new(),
        subject: None,
        window: None,
    };
    let first_audit = within(
        &live,
        "the first audit page",
        surface.audit(&lead, &audit_filter, &page_of_one(None)),
    )
    .await
    .expect("audit page");
    let audit_cursor = first_audit
        .next()
        .cloned()
        .expect("two audit entries: a cursor");

    let second = |live: &Live<PgSet>| {
        let lead = lead.clone();
        let agents_cursor = agents_cursor.clone();
        let conversations_cursor = conversations_cursor.clone();
        let audit_cursor = audit_cursor.clone();
        let audit_filter = audit_filter.clone();
        let surface = std::sync::Arc::clone(live.surface());
        async move {
            let agents = surface
                .agents(
                    &lead,
                    &AgentFilter::default(),
                    window,
                    &page_of_one(Some(agents_cursor)),
                )
                .await
                .expect("the agents cursor resolves");
            let conversations = match conversations_cursor {
                Some(cursor) => Some(format!(
                    "{:?}",
                    surface
                        .conversations(
                            &lead,
                            &ConversationFilter::default(),
                            &page_of_one(Some(cursor))
                        )
                        .await
                        .expect("the conversations cursor resolves")
                )),
                None => None,
            };
            let audit = surface
                .audit(&lead, &audit_filter, &page_of_one(Some(audit_cursor)))
                .await
                .expect("the audit cursor resolves");
            SecondPages {
                agents: format!("{:?}", agents.value),
                conversations,
                audit: format!("{audit:?}"),
            }
        }
    };
    let before = within(&live, "the second pages", second(&live)).await;
    stop_pg(live).await;

    // A new process, later: its own config entries are newer than every
    // page above.
    volume
        .clock
        .set(Timestamp::from_micros(after.as_micros() + 1_000_000));
    let restarted = start_generation(&db, &volume, 1).await;
    let after_restart = within(
        &restarted,
        "the second pages after the restart",
        second(&restarted),
    )
    .await;
    assert_eq!(after_restart, before);
    stop_pg(restarted).await;
    db.close().await.expect("drops");
}

/// What one process minted: a merge of B into A (undone), two renames.
async fn mint(live: &Live<PgSet>, caller: &Caller, a: AgentId, b: AgentId, tag: &str) -> MergeId {
    let merge = OperatorAction::merge_agents(caller, b, a).expect("two agents");
    let outcome = within(live, "a merge", live.surface().act(caller, merge))
        .await
        .expect("merged");
    let ActionOutcome::Merged(merge) = outcome else {
        panic!("a merge answers Merged: {outcome:?}");
    };
    within(
        live,
        "an unmerge",
        live.surface()
            .act(caller, OperatorAction::Unmerge { merge }),
    )
    .await
    .expect("unmerged");
    rename_twice(live, caller, a, tag).await;
    merge
}

/// Every audit entry's id, newest first.
async fn audit_ids(live: &Live<PgSet>, caller: &Caller) -> Vec<AuditId> {
    let filter = AuditFilter {
        by: Vec::new(),
        subject: None,
        window: None,
    };
    let mut request = PageRequest {
        size: PageSize::new(100).expect("a page size"),
        after: None,
    };
    let mut ids = Vec::new();
    loop {
        let page = within(
            live,
            "an audit page",
            live.surface().audit(caller, &filter, &request),
        )
        .await
        .expect("audit page");
        let (items, next) = page.into_parts();
        ids.extend(items.iter().map(|entry| entry.id));
        match next {
            Some(next) => request.after = Some(next),
            None => return ids,
        }
    }
}

/// `surface.ids.unique-across-restart` (INV-1220): processes minting as
/// `serve` does (`PgIds::Entropy`, `gateway::postgres::SERVE_IDS`), each
/// started after the last was killed or stopped, at the very same instant
/// on the clock: every merge record and audit entry a restarted process
/// mints is a new id, none equal to one persisted before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minted_ids_never_collide_after_restart() {
    let Some(db) = migrated("minted_ids_never_collide_after_restart").await else {
        return;
    };
    assert_eq!(
        crosstalk_gateway::gateway::postgres::SERVE_IDS,
        PgIds::Entropy,
        "the policy under test is serve's"
    );
    let scenario = scenario();
    let [ends, after] = passes(&scenario);
    let window = read::window(&scenario, options::BUCKET).expect("a window");
    let volume = Volume::new(scenario.start);

    // First process: the whole stream, then at `ends` a merge of B into A,
    // its unmerge and two renames. Killed.
    let first = start_pg(&db, &volume, PgIds::Entropy).await;
    feed(&first, &volume, &scenario).await;
    settle(&first, ends).await;
    let lead = caller(&first).await;
    let agents = within(
        &first,
        "the agents",
        read::agents(first.surface().as_ref(), &lead, &scenario, window),
    )
    .await
    .expect("agents")
    .agents
    .expect("A and B");
    let mut merges = vec![mint(&first, &lead, agents.a, agents.b, "first").await];
    let mut persisted = audit_ids(&first, &lead).await;
    let killed = kill_pg(first, &db).await;
    eprintln!("first process killed: {killed}");

    // Two more processes on the clock where the first left it: the same
    // actions at the same instant. The second is killed, the third
    // stopped gracefully.
    for (generation, tag) in [(1, "second"), (2, "third")] {
        let live = start_pg(&db, &volume, PgIds::Entropy).await;
        assert_eq!(live.clock().now(), ends, "the same instant");
        let lead = caller(&live).await;
        let merge = mint(&live, &lead, agents.a, agents.b, tag).await;
        assert!(
            !merges.contains(&merge),
            "process {generation} minted a persisted merge id"
        );
        merges.push(merge);
        let ids = audit_ids(&live, &lead).await;
        let distinct: BTreeSet<AuditId> = ids.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            ids.len(),
            "process {generation}: an audit id repeats"
        );
        assert!(
            persisted.iter().all(|id| distinct.contains(id)),
            "process {generation}: every persisted entry is still there"
        );
        assert!(
            ids.len() >= persisted.len() + 4,
            "process {generation}: a merge, an unmerge and two renames recorded after the restart: {} then {}",
            persisted.len(),
            ids.len()
        );
        persisted = ids;
        match generation {
            1 => {
                let killed = kill_pg(live, &db).await;
                eprintln!("process {generation} killed: {killed}");
            }
            _ => {
                settle(&live, after).await;
                let answers = read_answers(&live, &scenario).await;
                assert!(answers.detected(), "{}", answers.transmissions);
                stop_pg(live).await;
            }
        }
    }
    db.close().await.expect("drops");
}

/// A start that finds no free connection (every one of its pool's taken,
/// as when the server's are held by other sessions or a killed process's
/// are not gone yet) is retried, as `serve`'s pipeline task retries it,
/// instead of failing the run: the restart e2e once failed opening the
/// sinks store on "pool timed out".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_survives_a_pool_exhausted_at_open() {
    let Some(db) = migrated("start_survives_a_pool_exhausted_at_open").await else {
        return;
    };
    let settings = crosstalk_store::PoolSettings::new(
        std::num::NonZeroU32::new(4).expect("non-zero"),
        0,
        Duration::from_secs(2),
    )
    .expect("pool settings");
    let pool = crosstalk_gateway::store::lazy_pool_named(
        &crosstalk_store::StoreConfig::new(db.url().clone(), settings),
        "crosstalk-e2e-exhausted",
    );
    // Every connection taken for a while, then given back.
    let mut taken = Vec::new();
    for _ in 0..4 {
        taken.push(pool.acquire().await.expect("a connection"));
    }
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(6)).await;
        drop(taken);
    });
    let volume = Volume::new(DEFAULT_START);
    let live = common::start_pg_on(&db, pool, &volume, PgIds::Seeded(common::SEED)).await;
    release.await.expect("released");
    assert_eq!(
        live.layers().status.snapshot().pipeline_text(),
        "running",
        "started once connections were free"
    );
    stop_pg(live).await;
    db.close().await.expect("drops");
}
