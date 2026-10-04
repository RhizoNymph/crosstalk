//! Markup shared across pages. Keep page-specific markup in the page.

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily};
use crosstalk_spec::support::Timestamp;
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::contract::agents::AgentSummary;
use crate::contract::errors::QueryError;
use crate::url::ulid::UlidId;

/// An agent's display name: its label, else its id's last six characters.
pub fn agent_name(agent: &AgentSummary) -> String {
    match &agent.label {
        Some(label) => label.as_str().to_owned(),
        None => short_id(agent.id.to_ulid()),
    }
}

pub fn short_id(ulid: String) -> String {
    let start = ulid.len().saturating_sub(6);
    format!("…{}", &ulid[start..])
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
    fn formats_utc_seconds() {
        assert_eq!(
            format_time(Timestamp::from_micros(1_790_985_600_000_000)),
            "2026-10-03 00:00:00 UTC"
        );
    }
}
