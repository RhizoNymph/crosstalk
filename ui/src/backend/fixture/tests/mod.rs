//! Tests of the fixture backend: the generated world and its scenarios,
//! then every read and action through the `Backend` trait.

mod actions_support;
mod governance;
mod graph;
mod lists;
mod reads_support;
mod scenarios;
mod triage;
mod world;

use std::num::NonZeroU32;
use std::sync::OnceLock;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::support::TimeWindow;

use super::FixtureBackend;
use super::clock::{DAY, NOW, START, ago};
use super::world::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use crate::backend::Result;
use crate::contract::lists::{Page, PageRequest};
use crate::contract::scope::{Scope, TopologyFilter};

pub const SEED: u64 = 7;

/// One generated world shared by tests that only read.
pub fn shared() -> &'static FixtureBackend {
    static BACKEND: OnceLock<FixtureBackend> = OnceLock::new();
    BACKEND.get_or_init(|| FixtureBackend::try_new(SEED).expect("fixture generates"))
}

/// A world of its own, for tests that act.
pub fn fresh() -> FixtureBackend {
    FixtureBackend::try_new(SEED).expect("fixture generates")
}

pub const ALL: [Permission; 5] = [
    Permission::View,
    Permission::Content,
    Permission::Govern,
    Permission::Triage,
    Permission::Operate,
];

pub fn researcher() -> Caller {
    crate::testing::caller_of(OPERATOR_RESEARCHER, &ALL)
}

pub fn caller(permissions: &[Permission]) -> Caller {
    crate::testing::caller_of(OPERATOR_ONCALL, permissions)
}

pub fn window(start: crosstalk_spec::support::Timestamp) -> TimeWindow {
    TimeWindow::new(start, NOW).expect("window")
}

pub fn scope_with(window: TimeWindow, filter: TopologyFilter) -> Scope {
    Scope {
        window,
        topic_version: TopicModelVersion(2),
        filter,
    }
}

/// The UI's default view: the last 24 hours under the latest version.
pub fn day() -> Scope {
    scope_with(window(ago(DAY)), TopologyFilter::default())
}

/// The whole generated week.
pub fn week() -> Scope {
    scope_with(window(START), TopologyFilter::default())
}

pub fn first(limit: u32) -> PageRequest {
    PageRequest::first(NonZeroU32::new(limit).expect("limit"))
}

/// Follows cursors to the end, checking each page's size.
pub async fn collect<T>(
    limit: u32,
    mut fetch: impl AsyncFnMut(PageRequest) -> Result<Page<T>>,
) -> Vec<T> {
    let mut out = Vec::new();
    let mut request = first(limit);
    loop {
        let page = fetch(request.clone()).await.expect("page");
        assert!(page.items.len() <= limit as usize);
        if page.next.is_some() {
            assert_eq!(
                page.items.len(),
                limit as usize,
                "only the last page is short"
            );
        }
        out.extend(page.items);
        match page.next {
            Some(cursor) => request.cursor = Some(cursor),
            None => return out,
        }
    }
}
