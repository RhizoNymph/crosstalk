//! The operator rule forms: watched topics and semantic queries. Both parse
//! into the spec's [`RuleName`] and [`UserRule`]; the gateway embeds a
//! semantic query's text (and refuses text too long to embed as
//! `QueryTooLong`), so the UI never handles embeddings.

use crosstalk_spec::aggregates::alert::RuleQueryText;
use crosstalk_spec::aggregates::alert::{RuleName, UserRule, WatchedTopics};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::support::InvalidQueryText;
use crosstalk_spec::support::{InvalidText, NonEmpty};
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL, PANEL};
use crate::components::{content_hidden, error_panel, short_id};
use crate::error::UiError;
use crate::pages::common::form::{FormFields, invalid, required, similarity};
use crate::url::ulid::UlidId;
use crosstalk_spec::ids::SinkId;

pub const DEFAULT_REMAP: &str = "0.80";
pub const DEFAULT_SEMANTIC: &str = "0.75";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleKindChoice {
    Watched,
    Semantic,
}

impl RuleKindChoice {
    pub fn code(self) -> &'static str {
        match self {
            Self::Watched => "watched",
            Self::Semantic => "semantic",
        }
    }

    pub fn parse(text: Option<&str>) -> std::result::Result<Self, UiError> {
        match text {
            Some("watched") => Ok(Self::Watched),
            Some("semantic") => Ok(Self::Semantic),
            Some(other) => Err(invalid("kind", format!("unknown rule kind {other:?}"))),
            None => Err(invalid("kind", "required")),
        }
    }
}

/// What a watched-topic form may choose from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choices {
    pub version: TopicModelVersion,
    pub topics: Vec<TopicId>,
    pub sinks: Vec<SinkId>,
}

/// Why a rule name was refused, in words.
pub fn name_error(error: InvalidText) -> String {
    match error {
        InvalidText::Blank => "rule name is empty".to_owned(),
        InvalidText::TooLong { max, .. } => format!("rule name is longer than {max} characters"),
        InvalidText::ControlCharacter => "rule name holds a control character".to_owned(),
    }
}

/// A rule name, checked as the spec's `RuleName` (`DisplayText<80>`).
pub fn parse_name(fields: &FormFields) -> std::result::Result<RuleName, UiError> {
    RuleName::new(fields.text("name").unwrap_or("")).map_err(|e| invalid("name", name_error(e)))
}

pub fn parse_sinks(
    fields: &FormFields,
    known: &[SinkId],
) -> std::result::Result<Vec<SinkId>, UiError> {
    fields
        .all("sink")
        .map(|text| {
            let id = SinkId::parse_ulid(text).map_err(|e| invalid("sink", e))?;
            if known.contains(&id) {
                Ok(id)
            } else {
                Err(invalid("sink", "unknown sink"))
            }
        })
        .collect()
}

/// A watched-topic rule. Topics must belong to the current version, which
/// the form carries so a re-fit between opening and posting is caught. A
/// blank remap threshold takes the configured default.
pub fn parse_watched(
    fields: &FormFields,
    choices: &Choices,
) -> std::result::Result<(RuleName, UserRule, Vec<SinkId>), UiError> {
    let name = parse_name(fields)?;
    let version: u32 = required(fields, "version")?
        .parse()
        .map_err(|_| invalid("version", "not a topic model version"))?;
    if version != choices.version.0 {
        return Err(invalid(
            "version",
            "the topic model was re-fitted since this form was opened; pick the topics again",
        ));
    }
    let topics = fields
        .all("topic")
        .map(|text| {
            let id = TopicId::parse_ulid(text).map_err(|e| invalid("topic", e))?;
            if choices.topics.contains(&id) {
                Ok(id)
            } else {
                Err(invalid("topic", "not a topic of the current version"))
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let topics =
        NonEmpty::from_vec(topics).ok_or_else(|| invalid("topic", "pick at least one topic"))?;
    let remap_threshold = match fields.text("remap_threshold") {
        Some(_) => Some(similarity(fields, "remap_threshold")?),
        None => None,
    };
    let rule = UserRule::WatchedTopic {
        topics: WatchedTopics {
            version: choices.version,
            topics,
        },
        remap_threshold,
    };
    Ok((name, rule, parse_sinks(fields, &choices.sinks)?))
}

/// A semantic query rule: its text, checked as [`RuleQueryText`], and the
/// similarity threshold. How long the text may be is the embedder's to say.
pub fn parse_semantic(
    fields: &FormFields,
    sinks: &[SinkId],
) -> std::result::Result<(RuleName, UserRule, Vec<SinkId>), UiError> {
    let name = parse_name(fields)?;
    let text = RuleQueryText::new(required(fields, "text")?).map_err(|e| match e {
        InvalidQueryText::Blank => invalid("text", "required"),
        InvalidQueryText::TooLong { .. } => invalid("text", "at most 1000 characters"),
    })?;
    let rule = UserRule::SemanticQuery {
        text,
        threshold: similarity(fields, "threshold")?,
    };
    Ok((name, rule, parse_sinks(fields, sinks)?))
}

/// What a form shows in its inputs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Values {
    pub name: String,
    pub topics: Vec<String>,
    pub threshold: String,
    pub text: String,
    pub sinks: Vec<String>,
}

impl Values {
    pub fn defaults(kind: RuleKindChoice) -> Self {
        Self {
            threshold: match kind {
                RuleKindChoice::Watched => DEFAULT_REMAP.to_owned(),
                RuleKindChoice::Semantic => DEFAULT_SEMANTIC.to_owned(),
            },
            ..Self::default()
        }
    }

    /// The values a failed post sent, so nothing typed is lost.
    pub fn from_fields(kind: RuleKindChoice, fields: &FormFields) -> Self {
        let threshold_key = match kind {
            RuleKindChoice::Watched => "remap_threshold",
            RuleKindChoice::Semantic => "threshold",
        };
        Self {
            name: fields.text("name").unwrap_or("").to_owned(),
            topics: fields.all("topic").map(str::to_owned).collect(),
            threshold: fields.text(threshold_key).unwrap_or("").to_owned(),
            text: fields.text("text").unwrap_or("").to_owned(),
            sinks: fields.all("sink").map(str::to_owned).collect(),
        }
    }
}

/// A topic the form offers. `label` is `None` without `Content`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOption {
    pub id: String,
    pub label: Option<String>,
}

#[component]
pub async fn rule_form(
    kind: RuleKindChoice,
    action: String,
    values: Values,
    version: u32,
    topics: Vec<TopicOption>,
    sinks: Vec<(String, String)>,
    submit: &str,
    error: Option<UiError>,
) -> Result<impl View> {
    let Values {
        name,
        topics: checked_topics,
        threshold,
        text,
        sinks: checked_sinks,
    } = values;
    let no_topics = topics.is_empty();
    let no_sinks = sinks.is_empty();
    let semantic = kind == RuleKindChoice::Semantic;
    Ok(view! {
        <form method="post" action=(action) class=(format!("{PANEL} space-y-4"))>
            <input type="hidden" name="kind" value=(kind.code())>
            <input type="hidden" name="version" value=(version.to_string())>
            <label class="block">
                <span class=(LABEL)>"Name"</span>
                <input type="text" name="name" value=(name) required="" maxlength=(RuleName::MAX_CHARS.to_string()) class=(format!("{INPUT} w-96"))>
            </label>
            if semantic {
                <label class="block">
                    <span class=(LABEL)>"Describe what to look for"</span>
                    <textarea name="text" rows="3" required="" class=(format!("{INPUT} w-full"))>(text)</textarea>
                    <span class="mt-1 block text-xs text-zinc-500">"The gateway embeds this text; transmissions at or above the threshold raise an alert."</span>
                </label>
                <label class="block">
                    <span class=(LABEL)>"Similarity threshold (0 to 1)"</span>
                    <input type="number" name="threshold" value=(threshold) min="0" max="1" step="0.01" class=(format!("{INPUT} w-28"))>
                </label>
            } else {
                <fieldset>
                    <legend class=(LABEL)>"Topics of version " (version)</legend>
                    if no_topics {
                        <p class="mt-1 text-sm text-zinc-500">"No topics to pick from in the current version."</p>
                    } else {
                        <div class="mt-1 grid max-h-72 gap-1 overflow-y-auto sm:grid-cols-2">
                            for topic in topics {
                                let checked = checked_topics.contains(&topic.id);
                                <label class="flex items-center gap-2 text-sm">
                                    <input type="checkbox" name="topic" value=(topic.id.clone()) checked=(checked)>
                                    match topic.label {
                                        Some(label) => <span>(label)</span>,
                                        None => {
                                            content_hidden()
                                            <span class="font-mono text-xs text-zinc-400">(short_id(topic.id))</span>
                                        },
                                    }
                                </label>
                            }
                        </div>
                    }
                </fieldset>
                <label class="block">
                    <span class=(LABEL)>"Remap threshold after a re-fit (0 to 1)"</span>
                    <input type="number" name="remap_threshold" value=(threshold) min="0" max="1" step="0.01" class=(format!("{INPUT} w-28"))>
                </label>
            }
            <fieldset>
                <legend class=(LABEL)>"Deliver to (none checked: inbox only)"</legend>
                if no_sinks {
                    <p class="mt-1 text-sm text-zinc-500">"No sinks are configured."</p>
                } else {
                    <div class="mt-1 flex flex-wrap gap-3">
                        for (id, sink) in sinks {
                            let checked = checked_sinks.contains(&id);
                            <label class="flex items-center gap-1.5 text-sm">
                                <input type="checkbox" name="sink" value=(id) checked=(checked)>
                                (sink)
                            </label>
                        }
                    </div>
                }
            </fieldset>
            if let Some(error) = error {
                error_panel(error: &error)
            }
            <button type="submit" class=(BUTTON_PRIMARY)>(submit)</button>
        </form>
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn choices() -> Choices {
        Choices {
            version: TopicModelVersion(3),
            topics: vec![TopicId::from_ulid(1), TopicId::from_ulid(2)],
            sinks: vec![SinkId::from_ulid(9)],
        }
    }

    fn topic(id: u128) -> String {
        TopicId::from_ulid(id).to_ulid()
    }

    #[test]
    fn watched_rules_parse() {
        let t1 = topic(1);
        let sink = SinkId::from_ulid(9).to_ulid();
        let fields = FormFields::from_pairs(&[
            ("name", " keys "),
            ("version", "3"),
            ("topic", &t1),
            ("remap_threshold", "0.8"),
            ("sink", &sink),
        ]);
        let (name, rule, sinks) = parse_watched(&fields, &choices()).expect("valid");
        assert_eq!(name.as_str(), "keys");
        assert_eq!(sinks, vec![SinkId::from_ulid(9)]);
        let UserRule::WatchedTopic {
            topics: WatchedTopics { version, topics },
            remap_threshold,
        } = rule
        else {
            panic!("watched topic rule");
        };
        assert_eq!(version, TopicModelVersion(3));
        assert_eq!(topics.first(), &TopicId::from_ulid(1));
        assert!(remap_threshold.is_some_and(|t| (t.get() - 0.8).abs() < 1e-6));
        let defaulted =
            FormFields::from_pairs(&[("name", "keys"), ("version", "3"), ("topic", &t1)]);
        assert!(
            matches!(
                parse_watched(&defaulted, &choices()),
                Ok((
                    _,
                    UserRule::WatchedTopic {
                        remap_threshold: None,
                        ..
                    },
                    _
                ))
            ),
            "a blank threshold takes the configured default"
        );
    }

    #[test]
    fn watched_rules_reject_bad_input() {
        let base = |pairs: &[(&str, &str)]| {
            let mut all = vec![("name", "n"), ("version", "3"), ("remap_threshold", "0.5")];
            all.extend_from_slice(pairs);
            parse_watched(&FormFields::from_pairs(&all), &choices())
        };
        assert_eq!(
            base(&[]).err(),
            Some(invalid("topic", "pick at least one topic"))
        );
        let t9 = topic(9);
        assert_eq!(
            base(&[("topic", &t9)]).err(),
            Some(invalid("topic", "not a topic of the current version"))
        );
        let t1 = topic(1);
        let stale = FormFields::from_pairs(&[
            ("name", "n"),
            ("version", "2"),
            ("topic", &t1),
            ("remap_threshold", "0.5"),
        ]);
        assert!(matches!(
            parse_watched(&stale, &choices()),
            Err(UiError::Field {
                field: "version",
                ..
            })
        ));
        let unnamed =
            FormFields::from_pairs(&[("version", "3"), ("topic", &t1), ("remap_threshold", "0.5")]);
        assert_eq!(
            parse_watched(&unnamed, &choices()).err(),
            Some(invalid("name", "rule name is empty"))
        );
        let other_sink = SinkId::from_ulid(1).to_ulid();
        assert_eq!(
            base(&[("topic", &t1), ("sink", &other_sink)]).err(),
            Some(invalid("sink", "unknown sink"))
        );
    }

    #[test]
    fn semantic_rules_parse_their_text() {
        let missing = FormFields::from_pairs(&[("name", "n"), ("threshold", "0.7")]);
        assert_eq!(
            parse_semantic(&missing, &[]).err(),
            Some(invalid("text", "required"))
        );
        let blank = FormFields::from_pairs(&[("name", "n"), ("text", "  "), ("threshold", "0.7")]);
        assert_eq!(
            parse_semantic(&blank, &[]).err(),
            Some(invalid("text", "required"))
        );
        // The spec bounds the text (`RULE_QUERY_MAX_CHARS`); the form says so.
        let long = "x".repeat(1001);
        let long_text =
            FormFields::from_pairs(&[("name", "n"), ("text", &long), ("threshold", "0.7")]);
        assert_eq!(
            parse_semantic(&long_text, &[]).err(),
            Some(invalid("text", "at most 1000 characters"))
        );
        let longest = "x".repeat(1000);
        let longest_text =
            FormFields::from_pairs(&[("name", "n"), ("text", &longest), ("threshold", "0.7")]);
        assert!(parse_semantic(&longest_text, &[]).is_ok());
        let sink = SinkId::from_ulid(9).to_ulid();
        let complete = FormFields::from_pairs(&[
            ("name", "Keys"),
            ("text", " api keys "),
            ("threshold", "0.7"),
            ("sink", &sink),
        ]);
        let (name, rule, sinks) =
            parse_semantic(&complete, &[SinkId::from_ulid(9)]).expect("valid");
        assert_eq!(name.as_str(), "Keys");
        assert_eq!(sinks, vec![SinkId::from_ulid(9)]);
        let UserRule::SemanticQuery { text, threshold } = rule else {
            panic!("a semantic query")
        };
        assert_eq!(text.as_str(), "api keys");
        assert!((threshold.get() - 0.7).abs() < 1e-6);
    }

    #[test]
    fn kinds_parse() {
        assert_eq!(
            RuleKindChoice::parse(Some("watched")),
            Ok(RuleKindChoice::Watched)
        );
        assert!(RuleKindChoice::parse(Some("regex")).is_err());
        assert!(RuleKindChoice::parse(None).is_err());
    }
}
