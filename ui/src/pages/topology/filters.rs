//! The shared filter as a form (agents, channels, route kinds, topics,
//! verdicts) and as chips with remove links.
//!
//! Choices: agents in the window's unfiltered graph, every channel, and the
//! topics of the view's topic version (only with `Content`, since topic
//! labels come from message text). Values already in the filter stay
//! offered even when the window no longer shows them.

use crosstalk_spec::aggregates::edge::{RouteKind, Weighting};
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::view::{View, component, view};

use super::query::fields;
use crate::app::{backend, can};
use crate::backend::Backend;
use crate::components::form::{BUTTON_PRIMARY, FACET, INPUT, LINK};
use crate::components::href::state_pairs;
use crate::components::{agent_name, href, route_kind_name, short_id};
use crate::contract::channels::ChannelListFilter;
use crate::contract::lists::PageRequest;
use crate::contract::scope::{TopologyFilter, VerdictFilter};
use crate::pages::common::lookup::agent_names;
use crate::pages::common::transmissions::summary_name;
use crate::url::route::encode_kind;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

pub const ROUTE_KINDS: [RouteKind; 4] = [
    RouteKind::Channel,
    RouteKind::Delegation,
    RouteKind::Direct,
    RouteKind::Unobserved,
];

/// The view-state keys a filter form carries as hidden inputs; the filter
/// keys themselves come from its fields.
const BASE_KEYS: [&str; 5] = ["from", "to", "v", "w", "g"];

/// One offered value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choice {
    pub value: String,
    pub label: String,
    pub checked: bool,
}

/// What the filter form offers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FilterChoices {
    pub agents: Vec<(AgentId, String)>,
    pub channels: Vec<(ChannelId, String)>,
    /// `None` without `Content`.
    pub topics: Option<Vec<(TopicId, String)>>,
}

impl FilterChoices {
    fn agent_label(&self, id: AgentId) -> String {
        self.agents
            .iter()
            .find(|(a, _)| *a == id)
            .map_or_else(|| short_id(id.to_ulid()), |(_, name)| name.clone())
    }

    fn channel_label(&self, id: ChannelId) -> String {
        self.channels
            .iter()
            .find(|(c, _)| *c == id)
            .map_or_else(|| short_id(id.to_ulid()), |(_, name)| name.clone())
    }

    fn topic_label(&self, id: TopicId) -> String {
        self.topics
            .as_ref()
            .and_then(|topics| topics.iter().find(|(t, _)| *t == id))
            .map_or_else(
                || format!("topic {}", short_id(id.to_ulid())),
                |(_, l)| l.clone(),
            )
    }
}

/// Loads the choices. Failures degrade to fewer choices, never a failed
/// page: the filter in the URL still applies.
pub async fn load_choices(cx: &Cx, caller: &Caller, state: &ViewState) -> FilterChoices {
    let backend = backend(cx);
    let mut unfiltered = state.scope.clone();
    unfiltered.filter = TopologyFilter::default();

    let mut agents: Vec<(AgentId, String)> = match backend
        .topology(caller, &unfiltered, Weighting::Transmissions)
        .await
    {
        Ok(view) => view.nodes().iter().map(|n| (n.id, agent_name(n))).collect(),
        Err(error) => {
            tracing::warn!(%error, "filter agent choices unavailable");
            Vec::new()
        }
    };
    let missing: Vec<AgentId> = state
        .scope
        .filter
        .agents
        .iter()
        .copied()
        .filter(|id| !agents.iter().any(|(a, _)| a == id))
        .collect();
    if !missing.is_empty() {
        let names = agent_names(cx, caller, missing.clone()).await;
        agents.extend(missing.into_iter().map(|id| (id, names.name(id))));
    }
    agents.sort_by_key(|a| a.1.to_lowercase());

    let request = PageRequest::first(crate::pages::common::paging::PAGE_SIZE);
    let mut channels: Vec<(ChannelId, String)> = match backend
        .channels(caller, &ChannelListFilter::default(), &request)
        .await
    {
        Ok(page) => page
            .items
            .iter()
            .map(|s| (s.channel.id, summary_name(s)))
            .collect(),
        Err(error) => {
            tracing::warn!(%error, "filter channel choices unavailable");
            Vec::new()
        }
    };
    channels.sort_by(|a, b| a.1.cmp(&b.1));

    let topics = if can(caller, Permission::Content) {
        match backend.topics(caller, state.scope.topic_version).await {
            Ok(topics) => Some(topics.into_iter().map(|t| (t.id, t.label)).collect()),
            Err(error) => {
                tracing::warn!(%error, "filter topic choices unavailable");
                Some(Vec::new())
            }
        }
    } else {
        None
    };
    FilterChoices {
        agents,
        channels,
        topics,
    }
}

/// An active filter value and the link that removes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chip {
    pub facet: &'static str,
    pub label: String,
    pub remove: String,
}

fn without<T: PartialEq + Copy>(list: &[T], value: T) -> Vec<T> {
    list.iter().copied().filter(|v| *v != value).collect()
}

/// One chip per active filter value, each linking to `path` without it.
pub fn chips(
    path: &str,
    state: &ViewState,
    extra: &[(&str, &str)],
    choices: &FilterChoices,
) -> Vec<Chip> {
    let filter = &state.scope.filter;
    let link = |change: &dyn Fn(&mut TopologyFilter)| {
        let mut next = state.clone();
        change(&mut next.scope.filter);
        href(path, &next, extra)
    };
    let mut out = Vec::new();
    for id in &filter.agents {
        out.push(Chip {
            facet: "agent",
            label: choices.agent_label(*id),
            remove: link(&|f| f.agents = without(&filter.agents, *id)),
        });
    }
    for id in &filter.channels {
        out.push(Chip {
            facet: "channel",
            label: choices.channel_label(*id),
            remove: link(&|f| f.channels = without(&filter.channels, *id)),
        });
    }
    for kind in &filter.route_kinds {
        out.push(Chip {
            facet: "route",
            label: route_kind_name(*kind).to_owned(),
            remove: link(&|f| f.route_kinds = without(&filter.route_kinds, *kind)),
        });
    }
    for id in &filter.topics {
        out.push(Chip {
            facet: "topic",
            label: choices.topic_label(*id),
            remove: link(&|f| f.topics = without(&filter.topics, *id)),
        });
    }
    if filter.verdicts == VerdictFilter::ExcludeFalseDetections {
        out.push(Chip {
            facet: "verdict",
            label: "excluding false detections".to_owned(),
            remove: link(&|f| f.verdicts = VerdictFilter::IncludeAll),
        });
    }
    out
}

/// The link that clears the whole filter.
pub fn clear_href(path: &str, state: &ViewState, extra: &[(&str, &str)]) -> String {
    let mut next = state.clone();
    next.scope.filter = TopologyFilter::default();
    href(path, &next, extra)
}

/// The view state's base keys as hidden inputs.
pub fn base_inputs(state: &ViewState) -> Vec<(String, String)> {
    state_pairs(state)
        .into_iter()
        .filter(|(k, _)| BASE_KEYS.contains(&k.as_str()))
        .collect()
}

fn agent_choices(choices: &FilterChoices, filter: &TopologyFilter) -> Vec<Choice> {
    choices
        .agents
        .iter()
        .map(|(id, name)| Choice {
            value: id.to_ulid(),
            label: name.clone(),
            checked: filter.agents.contains(id),
        })
        .collect()
}

fn channel_choices(choices: &FilterChoices, filter: &TopologyFilter) -> Vec<Choice> {
    let mut out: Vec<Choice> = choices
        .channels
        .iter()
        .map(|(id, name)| Choice {
            value: id.to_ulid(),
            label: name.clone(),
            checked: filter.channels.contains(id),
        })
        .collect();
    for id in &filter.channels {
        if !choices.channels.iter().any(|(c, _)| c == id) {
            out.push(Choice {
                value: id.to_ulid(),
                label: short_id(id.to_ulid()),
                checked: true,
            });
        }
    }
    out
}

fn route_choices(filter: &TopologyFilter) -> Vec<Choice> {
    ROUTE_KINDS
        .iter()
        .map(|k| Choice {
            value: encode_kind(*k).to_owned(),
            label: route_kind_name(*k).to_owned(),
            checked: filter.route_kinds.contains(k),
        })
        .collect()
}

/// Topic choices, or `None` without `Content`. Filtered topics stay
/// checked (by id) even when the version's list lacks them.
fn topic_choices(choices: &FilterChoices, filter: &TopologyFilter) -> Option<Vec<Choice>> {
    let topics = choices.topics.as_ref()?;
    let mut out: Vec<Choice> = topics
        .iter()
        .map(|(id, label)| Choice {
            value: id.to_ulid(),
            label: label.clone(),
            checked: filter.topics.contains(id),
        })
        .collect();
    for id in &filter.topics {
        if !topics.iter().any(|(t, _)| t == id) {
            out.push(Choice {
                value: id.to_ulid(),
                label: format!("topic {}", short_id(id.to_ulid())),
                checked: true,
            });
        }
    }
    Some(out)
}

const SUMMARY: &str = "flex cursor-pointer list-none items-center gap-1 rounded border border-zinc-300 bg-white px-2 py-1 text-xs hover:bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900 dark:hover:bg-zinc-800";
const MENU: &str = "absolute left-0 z-20 mt-1 max-h-80 w-72 overflow-auto rounded border border-zinc-200 bg-white p-1.5 text-xs shadow-lg dark:border-zinc-700 dark:bg-zinc-900";

/// A dropdown of checkboxes named `name`.
#[component]
async fn facet(title: &str, name: &str, items: Vec<Choice>, empty: &str) -> Result<impl View> {
    let checked = items.iter().filter(|c| c.checked).count();
    let none = items.is_empty();
    let summary = if checked > 0 {
        format!("{title} · {checked}")
    } else {
        title.to_owned()
    };
    Ok(view! {
        <details class="relative">
            <summary class=(SUMMARY)>
                (summary)
                <span class="text-zinc-400">"▾"</span>
            </summary>
            <div class=(MENU)>
                if none {
                    <p class="px-1.5 py-1 text-zinc-500">(empty)</p>
                }
                for item in items {
                    <label class="flex cursor-pointer items-center gap-2 rounded px-1.5 py-1 hover:bg-zinc-100 dark:hover:bg-zinc-800">
                        <input type="checkbox" name=(name) value=(item.value) checked=(item.checked)>
                        <span class="truncate" title=(item.label.clone())>(item.label)</span>
                    </label>
                }
            </div>
        </details>
    })
}

/// The `GET` filter form. `extra` are the page's own pairs, kept as hidden
/// inputs; the page redirects the submission to the canonical URL.
#[component]
pub async fn filter_form(
    action: String,
    state: &ViewState,
    extra: Vec<(&'static str, String)>,
    choices: FilterChoices,
) -> Result<impl View> {
    let filter = &state.scope.filter;
    let hidden: Vec<(String, String)> = base_inputs(state)
        .into_iter()
        .chain(
            extra
                .into_iter()
                .filter(|(_, v)| !v.is_empty())
                .map(|(k, v)| (k.to_owned(), v)),
        )
        .collect();
    let agents = agent_choices(&choices, filter);
    let channels = channel_choices(&choices, filter);
    let routes = route_choices(filter);
    let topics = topic_choices(&choices, filter);
    let exclude = filter.verdicts == VerdictFilter::ExcludeFalseDetections;
    Ok(view! {
        <form method="get" action=(action) class="flex flex-wrap items-center gap-1.5">
            for (name, value) in hidden {
                <input type="hidden" name=(name) value=(value)>
            }
            <input type="hidden" name=(fields::APPLY) value="1">
            <span class=(format!("{FACET} mr-1"))>"Filter"</span>
            facet(title: "Agents", name: fields::AGENTS, items: agents, empty: "No agents in this window.")
            facet(title: "Channels", name: fields::CHANNELS, items: channels, empty: "No channels.")
            facet(title: "Routes", name: fields::ROUTES, items: routes, empty: "")
            match topics {
                Some(items) => facet(title: "Topics", name: fields::TOPICS, items: items, empty: "No topics in this version."),
                None => <span class="rounded border border-dashed border-zinc-300 px-2 py-1 text-xs text-zinc-400 dark:border-zinc-700" title="Topic labels need the Content permission">"Topics hidden"</span>,
            }
            <select name=(fields::VERDICTS) class=(format!("{INPUT} py-0.5 text-xs")) aria-label="Verdicts">
                <option value="all" selected=(!exclude)>"All verdicts"</option>
                <option value="exclude-false" selected=(exclude)>"Exclude false detections"</option>
            </select>
            <button type="submit" class=(format!("{BUTTON_PRIMARY} py-0.5 text-xs"))>"Apply"</button>
        </form>
    })
}

/// The active filter as removable chips.
#[component]
pub async fn filter_chips(chips: Vec<Chip>, clear: String) -> Result<impl View> {
    let any = !chips.is_empty();
    Ok(view! {
        if any {
            <div class="flex flex-wrap items-center gap-1.5 text-xs">
                for chip in chips {
                    <span class="inline-flex items-center gap-1 rounded-full border border-sky-600 bg-sky-50 py-0.5 pl-2 pr-1 text-sky-900 dark:border-sky-500 dark:bg-sky-950 dark:text-sky-100">
                        <span class="text-sky-700/70 dark:text-sky-300/70">(chip.facet)</span>
                        <span class="max-w-56 truncate">(chip.label.clone())</span>
                        <a
                            href=(chip.remove)
                            class="rounded-full px-1 text-sky-700 hover:bg-sky-200 dark:text-sky-300 dark:hover:bg-sky-800"
                            aria-label=(format!("Remove {} {}", chip.facet, chip.label))
                        >"×"</a>
                    </span>
                }
                <a class=(LINK) href=(clear)>"Clear all"</a>
            </div>
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;

    fn choices() -> FilterChoices {
        FilterChoices {
            agents: vec![(AgentId::from_ulid(1), "pi-scraper".to_owned())],
            channels: vec![(ChannelId::from_ulid(2), "wiki.example.org/*".to_owned())],
            topics: None,
        }
    }

    #[test]
    fn chips_remove_one_value_each() {
        let mut state = state();
        state.scope.filter = TopologyFilter {
            agents: vec![AgentId::from_ulid(1), AgentId::from_ulid(3)],
            channels: vec![ChannelId::from_ulid(2)],
            route_kinds: vec![RouteKind::Channel],
            topics: vec![TopicId::from_ulid(4)],
            verdicts: VerdictFilter::ExcludeFalseDetections,
        };
        let chips = chips("/topology", &state, &[("sel", "")], &choices());
        let labels: Vec<_> = chips.iter().map(|c| (c.facet, c.label.as_str())).collect();
        assert_eq!(
            labels,
            vec![
                ("agent", "pi-scraper"),
                ("agent", "…000003"),
                ("channel", "wiki.example.org/*"),
                ("route", "channel"),
                ("topic", "topic …000004"),
                ("verdict", "excluding false detections"),
            ]
        );
        // Removing the first agent keeps the second.
        assert!(chips[0].remove.contains("&a=00000000000000000000000003"));
        assert!(!chips[0].remove.contains("00000000000000000000000001"));
        assert!(!chips[5].remove.contains("x=exclude-false"));
        let clear = clear_href("/topology", &state, &[]);
        assert!(clear.ends_with("g=agents"), "{clear}");
    }

    #[test]
    fn hidden_inputs_leave_the_filter_to_the_fields() {
        let mut state = state();
        state.scope.filter.route_kinds = vec![RouteKind::Direct];
        let keys: Vec<String> = base_inputs(&state).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["from", "to", "v", "w", "g"]);
    }

    #[test]
    fn filtered_values_stay_offered() {
        let filter = TopologyFilter {
            channels: vec![ChannelId::from_ulid(9)],
            topics: vec![TopicId::from_ulid(5)],
            ..TopologyFilter::default()
        };
        let channels = channel_choices(&choices(), &filter);
        assert_eq!(channels.len(), 2);
        assert!(channels[1].checked);
        assert_eq!(
            topic_choices(&choices(), &filter),
            None,
            "hidden without Content"
        );
        let with_topics = FilterChoices {
            topics: Some(Vec::new()),
            ..choices()
        };
        let topics = topic_choices(&with_topics, &filter).expect("offered");
        assert_eq!(topics.len(), 1);
        assert!(topics[0].checked);
        assert!(route_choices(&filter).iter().all(|c| !c.checked));
    }
}
