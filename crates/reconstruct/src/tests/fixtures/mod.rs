//! Threading stress tests on public agent datasets, read in place from
//! `~/Data/ai/agents` (never copied into the repository). They depend on
//! local data, so they are `#[ignore]`d: run them with
//! `cargo test -p crosstalk-reconstruct -- --ignored fixtures`.
//!
//! - [`ai_village`]: AI Village's Claude Code stream, one resumed SDK
//!   session with 940 compact boundaries, replayed call by call through the
//!   whole consumer (attribution and threading) as replayed exchanges.
//! - [`lmcache`]: lmcache's agentic traces, where two re-runs of one task
//!   interleave under one session id.
//!
//! The synthetic regression cases they revealed are in
//! [`super::threading`].

mod ai_village;
mod lmcache;

use std::path::PathBuf;

use crosstalk_spec::ids::{CredentialHash, SecretVersion};
use crosstalk_spec::observed::client::{
    ClientContext, CredentialRef, CredentialScheme, HarnessClaim, HarnessFamily, HarnessIds,
    IngressMode, RequestClass, RouteName,
};
use crosstalk_spec::support::{Blake3, Timestamp};
use crosstalk_testkit::build::exchange::anthropic_api;

/// `~/Data/ai/agents/<path>`, or `None` (the test is skipped) when absent.
pub(crate) fn dataset(path: &str) -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let full = PathBuf::from(home).join("Data/ai/agents").join(path);
    if full.exists() {
        Some(full)
    } else {
        eprintln!("skipping: {} is not present", full.display());
        None
    }
}

/// A replayed caller: a synthetic API key that is stable per corpus and
/// agent, so identity is resolved per corpus.
pub(crate) fn replay_client(corpus: &str, agent: &str, session: Option<String>) -> ClientContext {
    let digest = Blake3::of(format!("{corpus}/{agent}").as_bytes());
    ClientContext {
        ingress: IngressMode::ReverseProxy {
            route: RouteName(format!("replay-{corpus}")),
        },
        upstream: anthropic_api(),
        credential: Some(CredentialRef {
            scheme: CredentialScheme::ApiKey,
            hash: CredentialHash::from_keyed_digest(SecretVersion(1), digest),
        }),
        account: None,
        previous_digests: None,
        harness: Some(HarnessClaim {
            family: HarnessFamily::ClaudeCode,
            version: None,
            user_agent: "replay".to_owned(),
        }),
        ids: HarnessIds {
            session,
            agent: None,
            parent_agent: None,
        },
        class: RequestClass::Main,
    }
}

/// `2026-03-24 20:51:25.986661` (UTC) as a timestamp.
pub(crate) fn parse_time(text: &str) -> Option<Timestamp> {
    let (date, time) = text.split_once(' ')?;
    let mut date = date.split('-').map(str::parse::<i64>);
    let (y, m, d) = (date.next()?.ok()?, date.next()?.ok()?, date.next()?.ok()?);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, "0"));
    let mut clock = clock.split(':').map(str::parse::<i64>);
    let (hh, mm, ss) = (
        clock.next()?.ok()?,
        clock.next()?.ok()?,
        clock.next()?.ok()?,
    );
    let micros: i64 = format!("{fraction:0<6}").get(..6)?.parse().ok()?;
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let seconds = days * 86_400 + hh * 3_600 + mm * 60 + ss;
    u64::try_from(seconds * 1_000_000 + micros)
        .ok()
        .map(Timestamp::from_micros)
}

#[test]
fn parse_time_reads_dataset_timestamps() {
    assert_eq!(
        parse_time("1970-01-02 00:00:01.5"),
        Some(Timestamp::from_micros(86_401_500_000))
    );
    assert_eq!(
        parse_time("2026-10-01 00:00:00"),
        Some(crosstalk_testkit::time::T0)
    );
}
