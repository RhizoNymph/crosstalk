//! Markup shared across pages. Keep page-specific markup in the page.

pub mod badge;
pub mod feedback;
pub mod form;
pub mod href;
pub mod locator;
pub mod nav;
pub mod paging;
pub mod sparkline;
pub mod table;

pub use badge::{Badge, Tone, kind_badge, state_badge};
pub use feedback::flash_banner;
pub use form::state_inputs;
pub use href::href;
pub use locator::{locator_text, pattern_text};
pub use nav::{Tab, filter_chip, segmented, tabs};
pub use paging::{PageLinks, pagination};
pub use table::data_table;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily};
use crosstalk_spec::support::{Blake3, Timestamp};
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::contract::agents::{AgentLabel, AgentName, AgentSummary};
use crate::contract::errors::QueryError;
use crate::url::ulid::UlidId;

/// An agent's display name: its label, else its id's last six characters.
pub fn agent_name(agent: &AgentSummary) -> String {
    display_name(agent.id, agent.label.as_ref())
}

/// [`agent_name`] for a batch-looked-up name: the canonical agent's label,
/// else its id's tail.
pub fn agent_name_of(name: &AgentName) -> String {
    display_name(name.id, name.label.as_ref())
}

fn display_name(id: crosstalk_spec::ids::AgentId, label: Option<&AgentLabel>) -> String {
    match label {
        Some(label) => label.as_str().to_owned(),
        None => short_id(id.to_ulid()),
    }
}

pub fn short_id(ulid: String) -> String {
    let start = ulid.len().saturating_sub(6);
    format!("…{}", &ulid[start..])
}

/// The first four bytes of a digest in hex, for display. Digests identify;
/// they are never shown whole.
pub fn abbrev_digest(digest: &Blake3) -> String {
    let hex: String = digest.as_bytes()[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{hex}…")
}

pub fn family_name(family: &HarnessFamily) -> &'static str {
    match family {
        HarnessFamily::ClaudeCode => "Claude Code",
        HarnessFamily::Codex => "Codex",
        HarnessFamily::Pi => "pi",
        HarnessFamily::OhMyPi => "oh-my-pi",
        HarnessFamily::Unknown => "unknown harness",
    }
}

pub fn route_kind_name(kind: RouteKind) -> &'static str {
    match kind {
        RouteKind::Channel => "channel",
        RouteKind::Delegation => "delegation",
        RouteKind::Direct => "direct",
        RouteKind::Unobserved => "unobserved",
    }
}

/// Tailwind classes for a route kind's colour. Literal strings so Tailwind
/// finds them.
pub fn route_kind_classes(kind: RouteKind) -> &'static str {
    match kind {
        RouteKind::Channel => "bg-route-channel/10 text-route-channel",
        RouteKind::Delegation => "bg-route-delegation/10 text-route-delegation",
        RouteKind::Direct => "bg-route-direct/10 text-route-direct",
        RouteKind::Unobserved => "bg-route-unobserved/10 text-route-unobserved",
    }
}

/// UTC, to the second.
pub fn format_time(at: Timestamp) -> String {
    i64::try_from(at.as_micros())
        .ok()
        .and_then(|micros| jiff::Timestamp::from_microsecond(micros).ok())
        .map_or_else(
            || at.as_micros().to_string(),
            |ts| ts.strftime("%Y-%m-%d %H:%M:%S UTC").to_string(),
        )
}

/// UTC to the minute without the year, for dense lists.
pub fn format_time_short(at: Timestamp) -> String {
    i64::try_from(at.as_micros())
        .ok()
        .and_then(|micros| jiff::Timestamp::from_microsecond(micros).ok())
        .map_or_else(
            || at.as_micros().to_string(),
            |ts| ts.strftime("%m-%d %H:%M").to_string(),
        )
}

/// A byte count in binary units with one decimal above a kibibyte.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// A share in `[0, 1]` as a percentage with one decimal ("< 0.1%" for tiny
/// non-zero shares).
pub fn format_share(share: f64) -> String {
    if share > 0.0 && share < 0.001 {
        return "< 0.1%".to_owned();
    }
    format!("{:.1}%", share * 100.0)
}

/// A duration as its two largest units: `850 ms`, `42 s`, `3 m 20 s`,
/// `2 h 5 m`, `1 d 4 h`.
pub fn format_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0 => format!("{} ms", duration.as_millis()),
        1..60 => format!("{secs} s"),
        60..3600 => format!("{} m {} s", secs / 60, secs % 60),
        3600..86_400 => format!("{} h {} m", secs / 3600, secs % 3600 / 60),
        _ => format!("{} d {} h", secs / 86_400, secs % 86_400 / 3600),
    }
}

#[component]
pub async fn route_badge(kind: RouteKind) -> Result<impl View> {
    let classes = format!(
        "inline-flex items-center rounded px-1.5 py-0.5 text-xs font-medium {}",
        route_kind_classes(kind)
    );
    Ok(view! { <span class=(classes)>(route_kind_name(kind))</span> })
}

/// A harness claim. Always marked as claimed: pi and oh-my-pi send Claude
/// Code's headers, so a claim is never identity.
#[component]
pub async fn claim_badge(claim: &HarnessClaim) -> Result<impl View> {
    let text = match &claim.version {
        Some(version) => format!("{} {version}", family_name(&claim.family)),
        None => family_name(&claim.family).to_owned(),
    };
    Ok(view! {
        <span
            class="inline-flex items-center gap-1 rounded border border-dashed border-zinc-400 px-1.5 py-0.5 text-xs text-zinc-600 dark:text-zinc-300"
            title=(claim.user_agent.clone())
        >
            <span class="text-zinc-400">"claims"</span>
            (text)
        </span>
    })
}

/// Shown in place of message text, snippets, topic labels and terms when the
/// caller lacks `Content`.
#[component]
pub async fn content_hidden() -> Result<impl View> {
    Ok(view! {
        <span class="rounded bg-zinc-100 px-1.5 py-0.5 text-xs italic text-zinc-500 dark:bg-zinc-800">
            "content hidden"
        </span>
    })
}

#[component]
pub async fn error_panel(error: &QueryError) -> Result<impl View> {
    let message = error.to_string();
    Ok(view! {
        <div class="rounded border border-red-300 bg-red-50 p-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-200">
            (message)
        </div>
    })
}

#[component]
pub async fn empty_state(message: &str) -> Result<impl View> {
    Ok(view! {
        <div class="rounded border border-dashed border-zinc-300 p-6 text-center text-sm text-zinc-500 dark:border-zinc-700">
            (message)
        </div>
    })
}

#[component]
pub async fn page_header(title: &str, subtitle: &str) -> Result<impl View> {
    Ok(view! {
        <header class="mb-4">
            <h1 class="text-lg font-semibold">(title)</h1>
            <p class="text-sm text-zinc-500">(subtitle)</p>
        </header>
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_id_keeps_the_tail() {
        assert_eq!(short_id("01J9ZQ3W8D0000000000ABCDEF".to_owned()), "…ABCDEF");
    }

    #[test]
    fn digests_abbreviate_to_eight_hex_digits() {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&[0xde, 0xad, 0x00, 0x0f]);
        assert_eq!(abbrev_digest(&Blake3::from_bytes(bytes)), "dead000f…");
    }

    #[test]
    fn formats_sizes_shares_and_durations() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(3 * 1024 * 1024), "3.0 MiB");
        assert_eq!(format_share(0.25), "25.0%");
        assert_eq!(format_share(0.0004), "< 0.1%");
        assert_eq!(format_share(0.0), "0.0%");
        use std::time::Duration;
        assert_eq!(format_duration(Duration::from_millis(850)), "850 ms");
        assert_eq!(format_duration(Duration::from_secs(200)), "3 m 20 s");
        assert_eq!(format_duration(Duration::from_secs(7500)), "2 h 5 m");
        assert_eq!(format_duration(Duration::from_secs(100_800)), "1 d 4 h");
        assert_eq!(
            format_time_short(Timestamp::from_micros(1_790_985_600_000_000)),
            "10-03 00:00"
        );
    }

    #[test]
    fn formats_utc_seconds() {
        assert_eq!(
            format_time(Timestamp::from_micros(1_790_985_600_000_000)),
            "2026-10-03 00:00:00 UTC"
        );
    }
}
