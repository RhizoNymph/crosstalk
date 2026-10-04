//! The channel list's own query keys: `tab`, `origin`, `detection`,
//! `policy` and `superseded`. List keys hold comma-separated codes, like the
//! shared view state's filter keys.

use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use topcoat::router::query_params;

use crate::contract::channels::{ChannelListFilter, DetectionKind, OriginKind};
use crate::contract::errors::QueryError;
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
    #[default]
    All,
    /// Unreviewed channels that are not superseded: the channels waiting for
    /// a policy decision.
    Review,
}

pub const ORIGINS: [OriginKind; 2] = [OriginKind::Discovered, OriginKind::Declared];

pub const DETECTIONS: [DetectionKind; 6] = [
    DetectionKind::Active,
    DetectionKind::Candidate,
    DetectionKind::Observed,
    DetectionKind::Dormant,
    DetectionKind::AwaitingTraffic,
    DetectionKind::Unused,
];

pub fn origin_code(origin: OriginKind) -> &'static str {
    match origin {
        OriginKind::Declared => "declared",
        OriginKind::Discovered => "discovered",
    }
}

pub fn detection_code(detection: DetectionKind) -> &'static str {
    match detection {
        DetectionKind::AwaitingTraffic => "awaiting",
        DetectionKind::Unused => "unused",
        DetectionKind::Observed => "observed",
        DetectionKind::Candidate => "candidate",
        DetectionKind::Active => "active",
        DetectionKind::Dormant => "dormant",
    }
}

/// The parsed list query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListQuery {
    pub tab: Tab,
    pub filter: ChannelListFilter,
}

fn parse_codes<T: Copy>(
    text: Option<&str>,
    key: &'static str,
    all: &[T],
    code: fn(T) -> &'static str,
) -> Result<Vec<T>, QueryError> {
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
    pub fn parse(raw: &RawListQuery) -> Result<Self, QueryError> {
        let tab = match raw.tab.as_deref() {
            None | Some("all") => Tab::All,
            Some("review") => Tab::Review,
            Some(other) => return Err(invalid("tab", format!("unknown tab {other:?}"))),
        };
        let include_superseded = match raw.superseded.as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            Some(_) => return Err(invalid("superseded", "expected 0 or 1")),
        };
        Ok(Self {
            tab,
            filter: ChannelListFilter {
                origins: parse_codes(raw.origin.as_deref(), "origin", &ORIGINS, origin_code)?,
                detections: parse_codes(
                    raw.detection.as_deref(),
                    "detection",
                    &DETECTIONS,
                    detection_code,
                )?,
                policies: parse_codes(raw.policy.as_deref(), "policy", &POLICIES, policy_code)?,
                include_superseded,
            },
        })
    }

    /// The filter sent to the backend: the review queue fixes the policy and
    /// leaves superseded channels out.
    pub fn effective_filter(&self) -> ChannelListFilter {
        match self.tab {
            Tab::All => self.filter.clone(),
            Tab::Review => ChannelListFilter {
                policies: vec![PolicyKind::Unreviewed],
                include_superseded: false,
                ..self.filter.clone()
            },
        }
    }

    /// The canonical query pairs, empty values left out by the link builder.
    pub fn pairs(&self) -> Vec<(&'static str, String)> {
        let join = |codes: Vec<&str>| codes.join(",");
        vec![
            (
                "tab",
                match self.tab {
                    Tab::All => String::new(),
                    Tab::Review => "review".to_owned(),
                },
            ),
            (
                "origin",
                join(
                    self.filter
                        .origins
                        .iter()
                        .map(|o| origin_code(*o))
                        .collect(),
                ),
            ),
            (
                "detection",
                join(
                    self.filter
                        .detections
                        .iter()
                        .map(|d| detection_code(*d))
                        .collect(),
                ),
            ),
            (
                "policy",
                join(
                    self.filter
                        .policies
                        .iter()
                        .map(|p| policy_code(*p))
                        .collect(),
                ),
            ),
            (
                "superseded",
                if self.filter.include_superseded {
                    "1".to_owned()
                } else {
                    String::new()
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

    pub fn toggle_origin(&self, origin: OriginKind) -> Self {
        let mut next = self.clone();
        next.filter.origins = toggle(&self.filter.origins, origin);
        next
    }

    pub fn toggle_detection(&self, detection: DetectionKind) -> Self {
        let mut next = self.clone();
        next.filter.detections = toggle(&self.filter.detections, detection);
        next
    }

    pub fn toggle_policy(&self, policy: PolicyKind) -> Self {
        let mut next = self.clone();
        next.filter.policies = toggle(&self.filter.policies, policy);
        next
    }

    pub fn toggle_superseded(&self) -> Self {
        let mut next = self.clone();
        next.filter.include_superseded = !self.filter.include_superseded;
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
        assert!(query.pairs().iter().all(|(_, v)| v.is_empty()));
    }

    #[test]
    fn lists_parse_and_render_back() {
        let query =
            ListQuery::parse(&raw("discovered", "active,candidate", "unreviewed")).expect("parse");
        assert_eq!(query.filter.origins, vec![OriginKind::Discovered]);
        assert_eq!(
            query.filter.detections,
            vec![DetectionKind::Active, DetectionKind::Candidate]
        );
        let pairs = query.pairs();
        assert!(pairs.contains(&("detection", "active,candidate".to_owned())));
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
    fn review_queue_fixes_policy_and_hides_superseded() {
        let mut query = ListQuery::parse(&raw("discovered", "", "sanctioned")).expect("parse");
        query.filter.include_superseded = true;
        let filter = query.with_tab(Tab::Review).effective_filter();
        assert_eq!(filter.policies, vec![PolicyKind::Unreviewed]);
        assert!(!filter.include_superseded);
        assert_eq!(filter.origins, vec![OriginKind::Discovered]);
    }

    #[test]
    fn toggles_add_and_remove() {
        let query = ListQuery::default().toggle_origin(OriginKind::Declared);
        assert_eq!(query.filter.origins, vec![OriginKind::Declared]);
        assert!(
            query
                .toggle_origin(OriginKind::Declared)
                .filter
                .origins
                .is_empty()
        );
        assert!(
            ListQuery::default()
                .toggle_superseded()
                .filter
                .include_superseded
        );
    }
}
