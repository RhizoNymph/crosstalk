//! State badges: a short label coloured by what it means to the operator.

use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::interfaces::l8_surface::{AlertStateKind, PolicyKind};
use crosstalk_spec::observed::agent::Strength;
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::pending::channel_semantics::{Confirmation, Listing};
use crosstalk_spec::aggregates::node::{CanonicalOriginKind, CanonicalStateKind};
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;

/// What a badge's colour says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Neutral,
    Info,
    Good,
    Warn,
    Bad,
    Muted,
}

impl Tone {
    /// Literal class strings so Tailwind finds them.
    pub fn classes(self) -> &'static str {
        match self {
            Self::Neutral => {
                "border-zinc-300 bg-zinc-50 text-zinc-700 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
            }
            Self::Info => {
                "border-sky-300 bg-sky-50 text-sky-800 dark:border-sky-800 dark:bg-sky-950 dark:text-sky-200"
            }
            Self::Good => {
                "border-emerald-300 bg-emerald-50 text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-200"
            }
            Self::Warn => {
                "border-amber-300 bg-amber-50 text-amber-800 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-200"
            }
            Self::Bad => {
                "border-red-300 bg-red-50 text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-200"
            }
            Self::Muted => {
                "border-zinc-200 bg-transparent text-zinc-500 dark:border-zinc-800 dark:text-zinc-500"
            }
        }
    }
}

/// A value with a badge: its label and tone.
pub trait Badge {
    fn label(&self) -> &'static str;
    fn tone(&self) -> Tone;
}

impl Badge for PolicyKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Unreviewed => "unreviewed",
            Self::Sanctioned => "sanctioned",
            Self::Unsanctioned => "unsanctioned",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Unreviewed => Tone::Warn,
            Self::Sanctioned => Tone::Good,
            Self::Unsanctioned => Tone::Bad,
        }
    }
}

/// A channel's origin. A superseded channel was discovered; pages show its
/// supersession beside this badge.
impl Badge for CanonicalOriginKind {
    fn label(&self) -> &'static str {
        match self {
            Self::DeclaredBeforeTraffic => "declared",
            Self::Promoted => "promoted",
            Self::Discovered => "discovered",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::DeclaredBeforeTraffic | Self::Promoted => Tone::Neutral,
            Self::Discovered => Tone::Info,
        }
    }
}

impl Badge for DetectionKind {
    fn label(&self) -> &'static str {
        match self {
            Self::AwaitingTraffic => "awaiting traffic",
            Self::Unused => "unused",
            Self::Observed => "observed",
            Self::Candidate => "candidate",
            Self::Active => "active",
            Self::Dormant => "dormant",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::AwaitingTraffic | Self::Unused | Self::Dormant => Tone::Muted,
            Self::Observed => Tone::Neutral,
            Self::Candidate => Tone::Warn,
            Self::Active => Tone::Info,
        }
    }
}

/// Whether a channel's cross-agent traffic holds a confirmed transmission:
/// the marker an unconfirmed channel carries wherever it is listed.
impl Badge for Confirmation {
    fn label(&self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Unconfirmed => "unconfirmed",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Confirmed => Tone::Good,
            Self::Unconfirmed => Tone::Warn,
        }
    }
}

/// Where a channel in force is listed: a channel (by its confirmation), a
/// declaration with no cross-agent traffic yet, or hidden by a merge.
impl Badge for Listing {
    fn label(&self) -> &'static str {
        match self {
            Self::Channel(confirmation) => confirmation.label(),
            Self::Declaration => "no traffic yet",
            Self::Hidden => "hidden",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Channel(confirmation) => confirmation.tone(),
            Self::Declaration | Self::Hidden => Tone::Muted,
        }
    }
}

impl Badge for CanonicalStateKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Provisional => "provisional",
            Self::Established => "established",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Registered => Tone::Muted,
            Self::Provisional => Tone::Warn,
            Self::Established => Tone::Good,
        }
    }
}

impl Badge for AlertStateKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Acknowledged => "acknowledged",
            Self::Resolved => "resolved",
            Self::Suppressed => "suppressed",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Open => Tone::Bad,
            Self::Acknowledged => Tone::Warn,
            Self::Resolved => Tone::Good,
            Self::Suppressed => Tone::Muted,
        }
    }
}

impl Badge for Strength {
    fn label(&self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Weak => "weak",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Strong => Tone::Good,
            Self::Weak => Tone::Muted,
        }
    }
}

/// Content-backed states read as settled; access-pattern-only states as
/// weaker; a discarded transmission as gone.
impl Badge for TransmissionStateKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Detected => "detected",
            Self::AwaitingContent => "awaiting content",
            Self::Suspected => "suspected",
            Self::Confirmed => "confirmed",
            Self::Classified => "classified",
            Self::Aggregated => "aggregated",
            Self::Discarded => "discarded",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Detected | Self::AwaitingContent => Tone::Neutral,
            Self::Suspected => Tone::Warn,
            Self::Confirmed | Self::Classified | Self::Aggregated => Tone::Info,
            Self::Discarded => Tone::Muted,
        }
    }
}

impl Badge for Verdict {
    fn label(&self) -> &'static str {
        match self {
            Self::Genuine => "genuine",
            Self::FalseDetection => "false detection",
        }
    }

    fn tone(&self) -> Tone {
        match self {
            Self::Genuine => Tone::Good,
            Self::FalseDetection => Tone::Bad,
        }
    }
}

#[component]
pub async fn state_badge(label: &str, tone: Tone) -> Result<impl View> {
    let classes = format!(
        "inline-flex items-center whitespace-nowrap rounded border px-1.5 py-0.5 text-xs font-medium {}",
        tone.classes()
    );
    Ok(view! { <span class=(classes)>(label)</span> })
}

/// The badge of any [`Badge`] value.
#[component]
pub async fn kind_badge<T: Badge + Send + Sync>(value: T) -> Result<impl View> {
    let label = value.label();
    let tone = value.tone();
    Ok(view! { state_badge(label: label, tone: tone) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_read_as_risk() {
        assert_eq!(PolicyKind::Sanctioned.tone(), Tone::Good);
        assert_eq!(PolicyKind::Unsanctioned.tone(), Tone::Bad);
        assert_eq!(PolicyKind::Unreviewed.tone(), Tone::Warn);
        assert_eq!(PolicyKind::Unreviewed.label(), "unreviewed");
    }

    #[test]
    fn open_alerts_are_loud_and_suppressed_quiet() {
        assert_eq!(AlertStateKind::Open.tone(), Tone::Bad);
        assert_eq!(AlertStateKind::Suppressed.tone(), Tone::Muted);
    }
}
