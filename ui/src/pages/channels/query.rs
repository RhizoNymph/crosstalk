//! The channel list's own query keys: `tab`, `origin`, `detection`,
//! `policy` and `superseded`. List keys hold comma-separated codes, like the
//! shared view state's filter keys. `origin` and `superseded` together are
//! the spec's `OriginFilter`: origin codes `declared` (before traffic),
//! `promoted` and `discovered`; `superseded=1` adds superseded channels,
//! `superseded=only` lists only them (with no origin codes, since a
//! superseded channel has no origin kind of its own).
//!
//! `tab` picks the spec's listings: active channels (`tab` absent; every
//! channel with cross-agent traffic, unconfirmed ones marked), unconfirmed
//! channels (`unconfirmed`), declarations with no traffic yet
//! (`declared`), and the review queue (`review`). The shared view state's
//! "confirmed only" (`u=confirmed`) leaves unconfirmed channels out of
//! every tab.

use crosstalk_spec::aggregates::filter::UnconfirmedChannels;
use crosstalk_spec::aggregates::node::CanonicalOriginKind;
use crosstalk_spec::derived::flow::channel::confirmation::ListingKind;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::support::TimeWindow;
use topcoat::router::query_params;

use crate::error::UiError;
use crate::pages::common::form::{POLICIES, invalid, policy_code};

#[query_params]
pub struct RawListQuery {
    pub tab: Option<String>,
    pub origin: Option<String>,
    pub detection: Option<String>,
    pub policy: Option<String>,
    pub superseded: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tab {
    /// Channels with cross-agent traffic: confirmed, and unconfirmed ones
    /// (marked) unless "confirmed only" is on. Superseded channels join on
    /// request.
    #[default]
    Active,
    /// Channels whose cross-agent traffic is all suspected: what to review
    /// before trusting them as channels.
    Unconfirmed,
    /// Declared channels with no cross-agent transmission yet: operator
    /// intent, counted as no channel.
    Declared,
    /// Unreviewed channels and declarations in force: the channels waiting
    /// for a policy decision, unconfirmed ones marked.
    Review,
}

impl Tab {
    pub const ALL: [Self; 4] = [
        Self::Active,
        Self::Unconfirmed,
        Self::Declared,
        Self::Review,
    ];

    pub fn code(self) -> &'static str {
        match self {
            Self::Active => "",
            Self::Unconfirmed => "unconfirmed",
            Self::Declared => "declared",
            Self::Review => "review",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Active => "Active channels",
            Self::Unconfirmed => "Unconfirmed",
            Self::Declared => "Declared, no traffic yet",
            Self::Review => "Review queue",
        }
    }

    /// The listings the tab keeps, under the view's choice about
    /// unconfirmed channels. `None`: the tab lists only unconfirmed channels
    /// and "confirmed only" leaves it nothing to list.
    pub fn listings(self, unconfirmed: UnconfirmedChannels) -> Option<Vec<ListingKind>> {
        let include = unconfirmed == UnconfirmedChannels::Include;
        match self {
            Self::Active if include => Some(vec![ListingKind::Confirmed, ListingKind::Unconfirmed]),
            Self::Active => Some(vec![ListingKind::Confirmed]),
            Self::Unconfirmed => include.then(|| vec![ListingKind::Unconfirmed]),
            Self::Declared => Some(vec![ListingKind::Declaration]),
            Self::Review if include => Some(Vec::new()),
            Self::Review => Some(vec![ListingKind::Confirmed, ListingKind::Declaration]),
        }
    }
}

pub const ORIGINS: [CanonicalOriginKind; 3] = [
    CanonicalOriginKind::Discovered,
    CanonicalOriginKind::Promoted,
    CanonicalOriginKind::DeclaredBeforeTraffic,
];

pub const DETECTIONS: [DetectionKind; 4] = [
    DetectionKind::Active,
    DetectionKind::Dormant,
    DetectionKind::AwaitingTraffic,
    DetectionKind::Unused,
];

pub fn origin_code(origin: CanonicalOriginKind) -> &'static str {
    match origin {
        CanonicalOriginKind::DeclaredBeforeTraffic => "declared",
        CanonicalOriginKind::Promoted => "promoted",
        CanonicalOriginKind::Discovered => "discovered",
    }
}

pub fn detection_code(detection: DetectionKind) -> &'static str {
    match detection {
        DetectionKind::AwaitingTraffic => "awaiting",
        DetectionKind::Unused => "unused",
        DetectionKind::Active => "active",
        DetectionKind::Dormant => "dormant",
    }
}

/// The parsed list query: everything of the spec's `ChannelFilter` but
/// the window, which is the view's.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListQuery {
    pub tab: Tab,
    pub origin: OriginFilter,
    pub detections: Vec<DetectionKind>,
    pub policies: Vec<PolicyKind>,
}

fn parse_codes<T: Copy>(
    text: Option<&str>,
    key: &'static str,
    all: &[T],
    code: fn(T) -> &'static str,
) -> Result<Vec<T>, UiError> {
    let mut out = Vec::new();
    for item in text
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let value = all
            .iter()
            .copied()
            .find(|v| code(*v) == item)
            .ok_or_else(|| invalid(key, format!("unknown value {item:?}")))?;
        out.push(value);
    }
    Ok(out)
}

impl ListQuery {
    pub fn parse(raw: &RawListQuery) -> Result<Self, UiError> {
        let tab = match raw.tab.as_deref() {
            None | Some("") => Tab::Active,
            Some(code) => Tab::ALL
                .into_iter()
                .find(|tab| tab.code() == code)
                .ok_or_else(|| invalid("tab", format!("unknown tab {code:?}")))?,
        };
        let kinds = parse_codes(raw.origin.as_deref(), "origin", &ORIGINS, origin_code)?;
        let origin = match raw.superseded.as_deref() {
            None | Some("0") => OriginFilter::InForce(kinds),
            Some("1") => OriginFilter::WithSuperseded(kinds),
            Some("only") if kinds.is_empty() => OriginFilter::Superseded,
            Some("only") => {
                return Err(invalid(
                    "superseded",
                    "superseded channels have no origin kind; drop the origin filter",
                ));
            }
            Some(_) => return Err(invalid("superseded", "expected 0, 1 or only")),
        };
        Ok(Self {
            tab,
            origin,
            detections: parse_codes(
                raw.detection.as_deref(),
                "detection",
                &DETECTIONS,
                detection_code,
            )?,
            policies: parse_codes(raw.policy.as_deref(), "policy", &POLICIES, policy_code)?,
        })
    }

    /// The origin kinds the filter lists; none for superseded only.
    pub fn origin_kinds(&self) -> &[CanonicalOriginKind] {
        match &self.origin {
            OriginFilter::InForce(kinds) | OriginFilter::WithSuperseded(kinds) => kinds,
            OriginFilter::Superseded => &[],
        }
    }

    pub fn includes_superseded(&self) -> bool {
        matches!(
            self.origin,
            OriginFilter::WithSuperseded(_) | OriginFilter::Superseded
        )
    }

    pub fn superseded_only(&self) -> bool {
        self.origin == OriginFilter::Superseded
    }

    /// The filter sent to the backend, counting in `window`, with the
    /// view's choice about unconfirmed channels: the active tab takes the
    /// superseded toggles, the others list channels in force only, and the
    /// review queue fixes the policy. `None` when the tab has nothing to
    /// list under that choice (unconfirmed channels, confirmed only).
    pub fn filter(
        &self,
        window: Option<TimeWindow>,
        unconfirmed: UnconfirmedChannels,
    ) -> Option<ChannelFilter> {
        let listings = self.tab.listings(unconfirmed)?;
        let in_force = OriginFilter::InForce(self.origin_kinds().to_vec());
        Some(match self.tab {
            Tab::Active => ChannelFilter {
                origin: self.origin.clone(),
                listings,
                detections: self.detections.clone(),
                policies: self.policies.clone(),
                window,
            },
            Tab::Unconfirmed | Tab::Declared => ChannelFilter {
                origin: in_force,
                listings,
                detections: self.detections.clone(),
                policies: self.policies.clone(),
                window,
            },
            Tab::Review => ChannelFilter {
                origin: in_force,
                listings,
                detections: self.detections.clone(),
                policies: vec![PolicyKind::Unreviewed],
                window,
            },
        })
    }

    /// The canonical query pairs, empty values left out by the link builder.
    pub fn pairs(&self) -> Vec<(&'static str, String)> {
        let join = |codes: Vec<&str>| codes.join(",");
        vec![
            ("tab", self.tab.code().to_owned()),
            (
                "origin",
                join(
                    self.origin_kinds()
                        .iter()
                        .map(|o| origin_code(*o))
                        .collect(),
                ),
            ),
            (
                "detection",
                join(self.detections.iter().map(|d| detection_code(*d)).collect()),
            ),
            (
                "policy",
                join(self.policies.iter().map(|p| policy_code(*p)).collect()),
            ),
            (
                "superseded",
                match self.origin {
                    OriginFilter::InForce(_) => String::new(),
                    OriginFilter::WithSuperseded(_) => "1".to_owned(),
                    OriginFilter::Superseded => "only".to_owned(),
                },
            ),
        ]
    }

    pub fn with_tab(&self, tab: Tab) -> Self {
        Self {
            tab,
            ..self.clone()
        }
    }

    /// Adds or removes an origin kind; from superseded only, lists the
    /// channels in force of that kind.
    pub fn toggle_origin(&self, origin: CanonicalOriginKind) -> Self {
        let mut next = self.clone();
        next.origin = match &self.origin {
            OriginFilter::InForce(kinds) => OriginFilter::InForce(toggle(kinds, origin)),
            OriginFilter::WithSuperseded(kinds) => {
                OriginFilter::WithSuperseded(toggle(kinds, origin))
            }
            OriginFilter::Superseded => OriginFilter::InForce(vec![origin]),
        };
        next
    }

    pub fn toggle_detection(&self, detection: DetectionKind) -> Self {
        let mut next = self.clone();
        next.detections = toggle(&self.detections, detection);
        next
    }

    pub fn toggle_policy(&self, policy: PolicyKind) -> Self {
        let mut next = self.clone();
        next.policies = toggle(&self.policies, policy);
        next
    }

    /// Includes or leaves out superseded channels, keeping the origin kinds.
    pub fn toggle_superseded(&self) -> Self {
        let mut next = self.clone();
        next.origin = match &self.origin {
            OriginFilter::InForce(kinds) => OriginFilter::WithSuperseded(kinds.clone()),
            OriginFilter::WithSuperseded(kinds) => OriginFilter::InForce(kinds.clone()),
            OriginFilter::Superseded => OriginFilter::InForce(Vec::new()),
        };
        next
    }

    /// Lists only superseded channels, or goes back to every channel in
    /// force.
    pub fn toggle_superseded_only(&self) -> Self {
        let mut next = self.clone();
        next.origin = match &self.origin {
            OriginFilter::Superseded => OriginFilter::InForce(Vec::new()),
            OriginFilter::InForce(_) | OriginFilter::WithSuperseded(_) => OriginFilter::Superseded,
        };
        next
    }
}

/// `list` with `value` removed if present, else added.
fn toggle<T: PartialEq + Copy>(list: &[T], value: T) -> Vec<T> {
    if list.contains(&value) {
        list.iter().copied().filter(|v| *v != value).collect()
    } else {
        let mut out = list.to_vec();
        out.push(value);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(origin: &str, detection: &str, policy: &str) -> RawListQuery {
        let some = |s: &str| (!s.is_empty()).then(|| s.to_owned());
        RawListQuery {
            tab: None,
            origin: some(origin),
            detection: some(detection),
            policy: some(policy),
            superseded: None,
        }
    }

    #[test]
    fn empty_query_is_unfiltered() {
        let query = ListQuery::parse(&raw("", "", "")).expect("parse");
        assert_eq!(query, ListQuery::default());
        assert_eq!(
            query.filter(None, UnconfirmedChannels::Include),
            Some(ChannelFilter {
                listings: vec![ListingKind::Confirmed, ListingKind::Unconfirmed],
                ..ChannelFilter::default()
            })
        );
        assert!(query.pairs().iter().all(|(_, v)| v.is_empty()));
    }

    #[test]
    fn lists_parse_and_render_back() {
        let query =
            ListQuery::parse(&raw("discovered", "active,dormant", "unreviewed")).expect("parse");
        assert_eq!(
            query.origin,
            OriginFilter::InForce(vec![CanonicalOriginKind::Discovered])
        );
        assert_eq!(
            query.detections,
            vec![DetectionKind::Active, DetectionKind::Dormant]
        );
        let pairs = query.pairs();
        assert!(pairs.contains(&("detection", "active,dormant".to_owned())));
        let promoted = ListQuery::parse(&raw("promoted,declared", "", "")).expect("parse");
        assert_eq!(
            promoted.origin_kinds(),
            [
                CanonicalOriginKind::Promoted,
                CanonicalOriginKind::DeclaredBeforeTraffic
            ]
        );
        assert!(
            promoted
                .pairs()
                .contains(&("origin", "promoted,declared".to_owned()))
        );
    }

    #[test]
    fn unknown_codes_are_rejected_with_their_key() {
        let err = ListQuery::parse(&raw("", "busy", ""));
        assert_eq!(err, Err(invalid("detection", "unknown value \"busy\"")));
        let tab = RawListQuery {
            tab: Some("spam".into()),
            ..raw("", "", "")
        };
        assert!(ListQuery::parse(&tab).is_err());
    }

    #[test]
    fn superseded_maps_onto_the_origin_filter() {
        let with = |origin: &str, superseded: &str| RawListQuery {
            superseded: Some(superseded.to_owned()),
            ..raw(origin, "", "")
        };
        let both = ListQuery::parse(&with("declared", "1")).expect("parse");
        assert_eq!(
            both.origin,
            OriginFilter::WithSuperseded(vec![CanonicalOriginKind::DeclaredBeforeTraffic])
        );
        let only = ListQuery::parse(&with("", "only")).expect("parse");
        assert_eq!(only.origin, OriginFilter::Superseded);
        assert!(only.pairs().contains(&("superseded", "only".to_owned())));
        assert!(ListQuery::parse(&with("discovered", "only")).is_err());
        assert!(ListQuery::parse(&with("", "yes")).is_err());
    }

    #[test]
    fn review_queue_fixes_policy_and_hides_superseded() {
        let mut query = ListQuery::parse(&raw("discovered", "", "sanctioned")).expect("parse");
        query.origin = OriginFilter::WithSuperseded(vec![CanonicalOriginKind::Discovered]);
        let filter = query
            .with_tab(Tab::Review)
            .filter(None, UnconfirmedChannels::Include)
            .expect("the review queue lists");
        assert_eq!(filter.policies, vec![PolicyKind::Unreviewed]);
        assert_eq!(
            filter.origin,
            OriginFilter::InForce(vec![CanonicalOriginKind::Discovered])
        );
        let only = ListQuery {
            origin: OriginFilter::Superseded,
            ..ListQuery::default()
        };
        assert_eq!(
            only.with_tab(Tab::Review)
                .filter(None, UnconfirmedChannels::Include)
                .expect("the review queue lists")
                .origin,
            OriginFilter::InForce(Vec::new())
        );
    }

    #[test]
    fn tabs_pick_listings_and_confirmed_only_drops_unconfirmed() {
        use UnconfirmedChannels::{Exclude, Include};
        let listings = |tab: Tab, unconfirmed| {
            ListQuery::default()
                .with_tab(tab)
                .filter(None, unconfirmed)
                .map(|f| f.listings)
        };
        assert_eq!(
            listings(Tab::Active, Exclude),
            Some(vec![ListingKind::Confirmed])
        );
        assert_eq!(
            listings(Tab::Unconfirmed, Include),
            Some(vec![ListingKind::Unconfirmed])
        );
        assert_eq!(listings(Tab::Unconfirmed, Exclude), None);
        assert_eq!(
            listings(Tab::Declared, Exclude),
            Some(vec![ListingKind::Declaration])
        );
        assert_eq!(listings(Tab::Review, Include), Some(Vec::new()));
        assert_eq!(
            listings(Tab::Review, Exclude),
            Some(vec![ListingKind::Confirmed, ListingKind::Declaration])
        );
        // Only the active tab takes superseded channels.
        let with = ListQuery::default().toggle_superseded();
        let declared = with
            .with_tab(Tab::Declared)
            .filter(None, Include)
            .expect("lists");
        assert_eq!(declared.origin, OriginFilter::InForce(Vec::new()));
    }

    #[test]
    fn tabs_parse_and_render_their_codes() {
        for tab in Tab::ALL {
            let raw = RawListQuery {
                tab: Some(tab.code().to_owned()),
                ..raw("", "", "")
            };
            let query = ListQuery::parse(&raw).expect("parse");
            assert_eq!(query.tab, tab);
            assert!(query.pairs().contains(&("tab", tab.code().to_owned())));
        }
        let old = RawListQuery {
            tab: Some("all".to_owned()),
            ..raw("", "", "")
        };
        assert!(ListQuery::parse(&old).is_err());
    }

    #[test]
    fn toggles_add_and_remove() {
        let declared = CanonicalOriginKind::DeclaredBeforeTraffic;
        let query = ListQuery::default().toggle_origin(declared);
        assert_eq!(query.origin_kinds(), [declared]);
        assert!(query.toggle_origin(declared).origin_kinds().is_empty());
        let with = query.toggle_superseded();
        assert_eq!(with.origin, OriginFilter::WithSuperseded(vec![declared]));
        assert_eq!(with.toggle_superseded(), query);
        let only = with.toggle_superseded_only();
        assert!(only.superseded_only() && only.includes_superseded());
        assert_eq!(
            only.toggle_origin(declared).origin,
            OriginFilter::InForce(vec![declared])
        );
        assert_eq!(
            only.toggle_superseded_only().origin,
            OriginFilter::InForce(Vec::new())
        );
    }
}
