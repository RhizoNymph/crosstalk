//! Serde support for the correlator's checkpointed state.
//!
//! The state's maps are keyed by ids, tuples and enums, which JSON cannot
//! use as object keys, so [`pairs`] writes a map as the list of its
//! `(key, value)` pairs, in key order, and reads it back into the map.
//! `NonChannelRoute` has no wire form of its own; it is written as the
//! spec's `Route` it converts into ([`non_channel`]).

/// `#[serde(with = "pairs")]` for a `BTreeMap`.
pub(crate) mod pairs {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Serialize,
        V: Serialize,
        S: Serializer,
    {
        serializer.collect_seq(map.iter())
    }

    pub(crate) fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let pairs: Vec<(K, V)> = Vec::deserialize(deserializer)?;
        Ok(pairs.into_iter().collect())
    }
}

/// `#[serde(with = "non_channel")]` for a `NonChannelRoute`: its `Route`.
pub(crate) mod non_channel {
    use crosstalk_spec::derived::flow::transmission::{NonChannelRoute, Route};
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        route: &NonChannelRoute,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Route::from(route.clone()).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<NonChannelRoute, D::Error> {
        from_route(Route::deserialize(deserializer)?).ok_or_else(|| {
            D::Error::custom("a channel route where a non-channel route was expected")
        })
    }

    /// The non-channel route `route` is, if it is one.
    pub(crate) fn from_route(route: Route) -> Option<NonChannelRoute> {
        match route {
            Route::Channel(_) => None,
            Route::Delegation(direction) => Some(NonChannelRoute::Delegation(direction)),
            Route::Direct(carrier) => Some(NonChannelRoute::Direct(carrier)),
            Route::Unobserved => Some(NonChannelRoute::Unobserved),
        }
    }
}

/// `#[serde(with = "timeless")]` for the matches waiting for their
/// exchange's start: a list of `(key, match, route)`.
pub(crate) mod timeless {
    use std::collections::BTreeMap;

    use crosstalk_spec::derived::flow::transmission::{NonChannelRoute, Route};
    use crosstalk_spec::derived::provenance::matching::ContentMatch;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::non_channel::from_route;
    use crate::correlate::medium::MatchKey;

    type Map = BTreeMap<MatchKey, (ContentMatch, NonChannelRoute)>;

    pub(crate) fn serialize<S: Serializer>(map: &Map, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            map.iter()
                .map(|(key, (content, route))| (key, content, Route::from(route.clone()))),
        )
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Map, D::Error> {
        let rows: Vec<(MatchKey, ContentMatch, Route)> = Vec::deserialize(deserializer)?;
        rows.into_iter()
            .map(|(key, content, route)| {
                from_route(route)
                    .map(|route| (key, (content, route)))
                    .ok_or_else(|| D::Error::custom("a channel route among the timeless matches"))
            })
            .collect()
    }
}
