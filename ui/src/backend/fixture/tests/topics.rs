//! The topic catalog's reads: the version history, topic sizes, lineage
//! and a version's topics, against the generated world.

use std::collections::HashSet;

use crosstalk_spec::aggregates::alert::{TopicWatch, WatchedTopics};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::TopicSizes;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{PageRequest, TopicList};
use crosstalk_spec::support::{NonEmpty, Similarity, TimeWindow, Timestamp};

use crosstalk_spec::derived::flow::transmission::Crossing;

use super::super::clock::WATERMARK;
use super::super::queries::Ctx;
use super::super::world::confirmed;
use super::super::world::topics::{REMAP_THRESHOLD, V2_AT};
use super::{collect, first, researcher, shared, week};
use crosstalk_spec::interfaces::l8_surface::QueryApi;

const V0: TopicModelVersion = TopicModelVersion(0);
const V1: TopicModelVersion = TopicModelVersion(1);
const V2: TopicModelVersion = TopicModelVersion(2);

/// Every assignment under `version` of a transmission confirmed in
/// `window` (all when `None`) and by `cut` whose agents have not merged
/// into one, counted from the world.
async fn reference(
    version: TopicModelVersion,
    window: Option<TimeWindow>,
    cut: Option<Timestamp>,
) -> (u64, u64) {
    let b = shared();
    let state = b.state.read().await;
    let ctx = Ctx::new(&b.world, &state);
    let (mut topics, mut outliers) = (0, 0);
    for record in &b.world.transmissions {
        let Some(at) = confirmed(&record.transmission.state).map(|c| c.at()) else {
            continue;
        };
        if window.is_some_and(|w| !w.contains(at)) || cut.is_some_and(|cut| at > cut) {
            continue;
        }
        if ctx.crossing(&record.transmission) != Crossing::Crosses {
            continue;
        }
        match record.assignment(version) {
            Some(Assignment::Topic { .. }) => topics += 1,
            Some(Assignment::Outlier) => outliers += 1,
            None => {}
        }
    }
    (topics, outliers)
}

fn counted(sizes: &TopicSizes) -> (u64, u64) {
    let topics = sizes
        .topics()
        .iter()
        .filter_map(|size| size.stats)
        .map(|stats| stats.transmissions.get())
        .sum();
    let outliers = sizes.outliers().map_or(0, |s| s.transmissions.get());
    (topics, outliers)
}

#[tokio::test]
async fn sizes_count_every_assignment_once() {
    let b = shared();
    let c = researcher();
    let window = week().window;
    for (asked, version) in [(None, V2), (Some(V2), V2), (Some(V1), V1)] {
        let sizes = b.topic_sizes(&c, asked, Some(window)).await.expect("sizes");
        assert_eq!(sizes.watermark.at(), WATERMARK);
        assert_eq!(sizes.value.version(), version);
        assert_eq!(sizes.value.window(), Some(window));
        let listed: HashSet<_> = sizes.value.topics().iter().map(|s| s.topic).collect();
        let of_version: HashSet<_> = b.world.topics.topics_of(version).map(|t| t.id).collect();
        assert_eq!(listed, of_version, "every topic of the version, once");
        assert_eq!(
            counted(&sizes.value),
            reference(version, Some(window), None).await
        );
        assert!(sizes.value.outliers().is_some());
    }
    let all_time = b.topic_sizes(&c, None, None).await.expect("all time");
    assert_eq!(all_time.value.window(), None);
    assert_eq!(counted(&all_time.value), reference(V2, None, None).await);
}

#[tokio::test]
async fn a_dropped_version_has_frozen_all_time_sizes_only() {
    let b = shared();
    let c = researcher();
    assert_eq!(
        b.topic_sizes(&c, Some(V0), Some(week().window)).await.err(),
        Some(QueryError::VersionNotRetained { version: V0 })
    );
    let frozen = b.topic_sizes(&c, Some(V0), None).await.expect("frozen");
    assert!(frozen.value.topics().is_empty(), "v0 has no topics");
    let (topics, outliers) = reference(V0, None, Some(V2_AT)).await;
    assert_eq!(topics, 0, "v0 classifies everything as an outlier");
    assert_eq!(counted(&frozen.value), (0, outliers));
    assert!(
        outliers < reference(V0, None, None).await.1,
        "transmissions confirmed after the drop are not counted"
    );
    assert_eq!(
        b.topic_sizes(&c, Some(TopicModelVersion(9)), None)
            .await
            .err(),
        Some(QueryError::NotFound)
    );
}

#[tokio::test]
async fn topics_page_newest_first_and_pin_their_version() {
    let b = shared();
    let c = researcher();
    let pinned = TopicVersionSelector::Pinned(V2);
    let topics = collect(3, async |page| {
        b.topics(&c, pinned, &page).await.map(|t| {
            assert_eq!(t.version, V2);
            t.page
        })
    })
    .await;
    assert_eq!(topics.len(), 10);
    assert!(topics.iter().all(|t| t.version == V2));
    assert!(topics.windows(2).all(|pair| pair[0].id > pair[1].id));
    let current = b
        .topics(&c, TopicVersionSelector::Current, &first(50))
        .await
        .expect("current");
    assert_eq!(current.version, V2);
    // A cursor issued for v2 is not one for v1.
    let next = b
        .topics(&c, pinned, &first(3))
        .await
        .expect("page")
        .page
        .next()
        .cloned();
    let request = PageRequest::<TopicList> {
        after: next,
        ..first(3)
    };
    assert_eq!(
        b.topics(&c, TopicVersionSelector::Pinned(V1), &request)
            .await
            .err(),
        Some(QueryError::InvalidCursor)
    );
    // A dropped version's topics stay readable.
    let v0 = b
        .topics(&c, TopicVersionSelector::Pinned(V0), &first(5))
        .await
        .expect("v0");
    assert_eq!(v0.version, V0);
    assert!(v0.page.items().is_empty());
}

#[tokio::test]
async fn lineage_follows_the_history_and_drives_the_remap() {
    let b = shared();
    let c = researcher();
    let lineage = b
        .topic_lineage(&c, V1)
        .await
        .expect("lineage")
        .expect("v1 has a successor");
    assert_eq!((lineage.from(), lineage.to()), (V1, V2));
    let v1: Vec<_> = b.world.topics.topics_of(V1).map(|t| t.id).collect();
    assert_eq!(lineage.entries().len(), v1.len());
    for entry in lineage.entries() {
        let best = entry.best().expect("v2 has topics");
        assert!(
            entry
                .others()
                .iter()
                .all(|other| other.similarity <= best.similarity)
        );
    }
    let threshold = Similarity::new(REMAP_THRESHOLD).expect("threshold");
    let everything = WatchedTopics {
        version: V1,
        topics: NonEmpty::from_vec(v1).expect("topics"),
    };
    assert!(matches!(
        lineage.remap(&everything, threshold),
        Ok(TopicWatch::Stale { unmapped, .. }) if unmapped.iter().count() == 1
    ));
    assert_eq!(b.topic_lineage(&c, V2).await, Ok(None));
    assert!(b.topic_lineage(&c, V0).await.expect("v0").is_some());
    assert_eq!(
        b.topic_lineage(&c, TopicModelVersion(9)).await,
        Err(QueryError::NotFound)
    );
}
