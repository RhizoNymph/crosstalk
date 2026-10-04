//! Links that carry the shared view state, so the filter follows the user.

use crate::url::view_state::{ViewState, encode_component};

/// `path` with the canonical view state and then `extra` pairs. Pairs with an
/// empty value are left out.
pub fn href(path: &str, state: &ViewState, extra: &[(&str, &str)]) -> String {
    let mut out = format!("{path}?{}", state.to_query());
    for (key, value) in extra {
        if !value.is_empty() {
            out.push('&');
            out.push_str(key);
            out.push('=');
            out.push_str(&encode_component(value));
        }
    }
    out
}

/// The view state as decoded key and value pairs, for the hidden inputs of a
/// `GET` form (a `GET` form replaces the action's query with its fields).
pub fn state_pairs(state: &ViewState) -> Vec<(String, String)> {
    state
        .to_query()
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (decode_component(k), decode_component(v)))
        .collect()
}

/// Reverses [`encode_component`]. Malformed escapes are kept as written.
fn decode_component(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%')
            .then(|| bytes.get(i + 1..i + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
pub(crate) mod tests {
    use crosstalk_spec::aggregates::edge::Weighting;
    use crosstalk_spec::aggregates::topic::TopicModelVersion;
    use crosstalk_spec::ids::AgentId;
    use crosstalk_spec::support::{TimeWindow, Timestamp};

    use super::*;
    use crate::contract::scope::{Scope, TopologyFilter};
    use crate::url::view_state::GraphMode;

    /// 2026-10-02T00:00:00Z to 2026-10-03T00:00:00Z, version 3, no filter.
    pub fn state() -> ViewState {
        let day = 86_400_000_000;
        let end = 1_790_985_600_000_000;
        ViewState {
            scope: Scope {
                window: TimeWindow::new(
                    Timestamp::from_micros(end - day),
                    Timestamp::from_micros(end),
                )
                .expect("window"),
                topic_version: TopicModelVersion(3),
                filter: TopologyFilter::default(),
            },
            weighting: Weighting::Transmissions,
            graph: GraphMode::Agents,
        }
    }

    #[test]
    fn links_carry_state_then_extras() {
        let link = href("/channels", &state(), &[("cursor", "a b"), ("tab", "")]);
        assert_eq!(
            link,
            "/channels?from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z&v=3&w=tx&g=agents&cursor=a%20b"
        );
    }

    #[test]
    fn state_pairs_are_decoded() {
        let mut state = state();
        state.scope.filter.agents = vec![AgentId::from_ulid(1), AgentId::from_ulid(2)];
        let pairs = state_pairs(&state);
        assert_eq!(
            pairs[0],
            ("from".to_owned(), "2026-10-02T00:00:00Z".to_owned())
        );
        assert_eq!(
            pairs.last().map(|(k, _)| k.as_str()),
            Some("a"),
            "filter keys follow the required ones"
        );
        assert!(pairs.iter().all(|(_, v)| !v.contains('%')));
    }

    #[test]
    fn decoding_reverses_encoding() {
        let text = "x y/ü%";
        assert_eq!(decode_component(&encode_component(text)), text);
        assert_eq!(decode_component("%zz"), "%zz");
    }
}
