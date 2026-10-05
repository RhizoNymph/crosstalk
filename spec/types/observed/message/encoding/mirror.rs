//! The JSON shape of a message body: private serde mirrors of the spec's
//! message types, in the wire contract's conventions (snake_case keys,
//! adjacently tagged enums, all-unit enums as strings, strict decoding).
//! An optional field added after P0.7 (`ToolCall`'s `signature`) is
//! omitted when absent, so existing bodies keep their bytes and hashes;
//! `Reasoning::Visible`'s `signature`, from P0.7, is written as `null`.
//!
//! `Text` is its string, `CanonicalJson` its text as a JSON string (so its
//! numbers stay exact), `MessageHash` its lower-case hex.
//!
//! [`MessageBody`]'s `Serialize` and `Deserialize` go through the mirror,
//! so its serde form is the shape [`super::encode`] writes canonically;
//! decoding refuses what no body holds (a tool message without results,
//! canonical JSON that is not canonical) as [`super::DecodeError`]s.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::json;
use super::super::{
    AssistantPart, CanonicalJson, Media, MediaKind, MessageBody, Reasoning, SystemPart, Text,
    ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult,
    ToolResultContent, Unknown, UserPart,
};
use crate::ids::MessageHash;
use crate::support::NonEmpty;
use crate::wire::Rejected;

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum Body {
    System(Vec<SystemItem>),
    User(Vec<UserItem>),
    Assistant(Vec<AssistantItem>),
    Tool(Vec<ResultItem>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum SystemItem {
    Text(String),
    Unknown(UnknownItem),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum UserItem {
    Text(String),
    Media(MediaItem),
    Unknown(UnknownItem),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum AssistantItem {
    Text(String),
    Reasoning(ReasoningItem),
    ToolCall(CallItem),
    ServerToolResult(ResultItem),
    Unknown(UnknownItem),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct UnknownItem {
    kind: String,
    raw: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum ReasoningItem {
    Visible {
        text: String,
        signature: Option<String>,
    },
    Opaque {
        signature: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct MediaItem {
    kind: MediaKindItem,
    blob: MessageHash,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MediaKindItem {
    Image,
    Audio,
    Document,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct CallItem {
    id: String,
    name: String,
    arguments: ArgumentsItem,
    execution: ExecutionItem,
    /// Omitted when absent, so adding the field changed no existing tool
    /// call's encoding or hash. Serde reads both an omitted field and an
    /// explicit `null` as `None`; the canonical decoder refuses the `null`
    /// because `encode` never writes it (`super::decode` re-encodes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum ArgumentsItem {
    Json(String),
    Invalid(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExecutionItem {
    Client,
    Server,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct ResultItem {
    call_id: String,
    content: Vec<ContentItem>,
    outcome: OutcomeItem,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum ContentItem {
    Text(String),
    Media(MediaItem),
    Unknown(UnknownItem),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum OutcomeItem {
    Success,
    Error,
    Unknown,
}

/// What the mirror holds that no message body can: a `Tool` body with no
/// result, or canonical JSON text (an `Unknown` block's `raw`, `Json`
/// arguments) that is not canonical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Invalid {
    EmptyTool,
    NonCanonicalJson,
}

impl From<&MessageBody> for Body {
    fn from(body: &MessageBody) -> Self {
        match body {
            MessageBody::System(parts) => {
                Self::System(parts.iter().map(SystemItem::from).collect())
            }
            MessageBody::User(parts) => Self::User(parts.iter().map(UserItem::from).collect()),
            MessageBody::Assistant(parts) => {
                Self::Assistant(parts.iter().map(AssistantItem::from).collect())
            }
            MessageBody::Tool(results) => {
                Self::Tool(results.iter().map(ResultItem::from).collect())
            }
        }
    }
}

impl From<&SystemPart> for SystemItem {
    fn from(part: &SystemPart) -> Self {
        match part {
            SystemPart::Text(text) => Self::Text(text.0.clone()),
            SystemPart::Unknown(unknown) => Self::Unknown(unknown.into()),
        }
    }
}

impl From<&UserPart> for UserItem {
    fn from(part: &UserPart) -> Self {
        match part {
            UserPart::Text(text) => Self::Text(text.0.clone()),
            UserPart::Media(media) => Self::Media(media.into()),
            UserPart::Unknown(unknown) => Self::Unknown(unknown.into()),
        }
    }
}

impl From<&AssistantPart> for AssistantItem {
    fn from(part: &AssistantPart) -> Self {
        match part {
            AssistantPart::Text(text) => Self::Text(text.0.clone()),
            AssistantPart::Reasoning(Reasoning::Visible { text, signature }) => {
                Self::Reasoning(ReasoningItem::Visible {
                    text: text.0.clone(),
                    signature: signature.clone(),
                })
            }
            AssistantPart::Reasoning(Reasoning::Opaque { signature }) => {
                Self::Reasoning(ReasoningItem::Opaque {
                    signature: signature.clone(),
                })
            }
            AssistantPart::ToolCall(call) => Self::ToolCall(call.into()),
            AssistantPart::ServerToolResult(result) => Self::ServerToolResult(result.into()),
            AssistantPart::Unknown(unknown) => Self::Unknown(unknown.into()),
        }
    }
}

impl From<&Unknown> for UnknownItem {
    fn from(unknown: &Unknown) -> Self {
        Self {
            kind: unknown.kind.clone(),
            raw: unknown.raw.0.clone(),
        }
    }
}

impl From<&Media> for MediaItem {
    fn from(media: &Media) -> Self {
        Self {
            kind: match media.kind {
                MediaKind::Image => MediaKindItem::Image,
                MediaKind::Audio => MediaKindItem::Audio,
                MediaKind::Document => MediaKindItem::Document,
            },
            blob: media.blob,
        }
    }
}

impl From<&ToolCall> for CallItem {
    fn from(call: &ToolCall) -> Self {
        Self {
            id: call.id.0.clone(),
            name: call.name.0.clone(),
            arguments: match &call.arguments {
                ToolArguments::Json(json) => ArgumentsItem::Json(json.0.clone()),
                ToolArguments::Invalid(text) => ArgumentsItem::Invalid(text.clone()),
            },
            execution: match call.execution {
                ToolExecution::Client => ExecutionItem::Client,
                ToolExecution::Server => ExecutionItem::Server,
            },
            signature: call.signature.clone(),
        }
    }
}

impl From<&ToolResult> for ResultItem {
    fn from(result: &ToolResult) -> Self {
        Self {
            call_id: result.call_id.0.clone(),
            content: result
                .content
                .iter()
                .map(|content| match content {
                    ToolResultContent::Text(text) => ContentItem::Text(text.0.clone()),
                    ToolResultContent::Media(media) => ContentItem::Media(media.into()),
                    ToolResultContent::Unknown(unknown) => ContentItem::Unknown(unknown.into()),
                })
                .collect(),
            outcome: match result.outcome {
                ToolOutcome::Success => OutcomeItem::Success,
                ToolOutcome::Error => OutcomeItem::Error,
                ToolOutcome::Unknown => OutcomeItem::Unknown,
            },
        }
    }
}

/// `text` as canonical JSON, when it already is canonical JSON.
fn canonical(text: String) -> Result<CanonicalJson, Invalid> {
    match json::canonicalize(&text) {
        Ok(canonical) if canonical.0 == text => Ok(canonical),
        _ => Err(Invalid::NonCanonicalJson),
    }
}

impl TryFrom<Body> for MessageBody {
    type Error = Invalid;

    fn try_from(body: Body) -> Result<Self, Invalid> {
        Ok(match body {
            Body::System(items) => Self::System(
                items
                    .into_iter()
                    .map(|item| {
                        Ok(match item {
                            SystemItem::Text(text) => SystemPart::Text(Text(text)),
                            SystemItem::Unknown(unknown) => {
                                SystemPart::Unknown(unknown.try_into()?)
                            }
                        })
                    })
                    .collect::<Result<_, Invalid>>()?,
            ),
            Body::User(items) => Self::User(
                items
                    .into_iter()
                    .map(|item| {
                        Ok(match item {
                            UserItem::Text(text) => UserPart::Text(Text(text)),
                            UserItem::Media(media) => UserPart::Media(media.into()),
                            UserItem::Unknown(unknown) => UserPart::Unknown(unknown.try_into()?),
                        })
                    })
                    .collect::<Result<_, Invalid>>()?,
            ),
            Body::Assistant(items) => Self::Assistant(
                items
                    .into_iter()
                    .map(AssistantPart::try_from)
                    .collect::<Result<_, Invalid>>()?,
            ),
            Body::Tool(items) => {
                let results = items
                    .into_iter()
                    .map(ToolResult::try_from)
                    .collect::<Result<Vec<_>, Invalid>>()?;
                Self::Tool(NonEmpty::from_vec(results).ok_or(Invalid::EmptyTool)?)
            }
        })
    }
}

impl TryFrom<AssistantItem> for AssistantPart {
    type Error = Invalid;

    fn try_from(item: AssistantItem) -> Result<Self, Invalid> {
        Ok(match item {
            AssistantItem::Text(text) => Self::Text(Text(text)),
            AssistantItem::Reasoning(ReasoningItem::Visible { text, signature }) => {
                Self::Reasoning(Reasoning::Visible {
                    text: Text(text),
                    signature,
                })
            }
            AssistantItem::Reasoning(ReasoningItem::Opaque { signature }) => {
                Self::Reasoning(Reasoning::Opaque { signature })
            }
            AssistantItem::ToolCall(call) => Self::ToolCall(call.try_into()?),
            AssistantItem::ServerToolResult(result) => Self::ServerToolResult(result.try_into()?),
            AssistantItem::Unknown(unknown) => Self::Unknown(unknown.try_into()?),
        })
    }
}

impl TryFrom<UnknownItem> for Unknown {
    type Error = Invalid;

    fn try_from(item: UnknownItem) -> Result<Self, Invalid> {
        Ok(Self {
            kind: item.kind,
            raw: canonical(item.raw)?,
        })
    }
}

impl From<MediaItem> for Media {
    fn from(item: MediaItem) -> Self {
        Self {
            kind: match item.kind {
                MediaKindItem::Image => MediaKind::Image,
                MediaKindItem::Audio => MediaKind::Audio,
                MediaKindItem::Document => MediaKind::Document,
            },
            blob: item.blob,
        }
    }
}

impl TryFrom<CallItem> for ToolCall {
    type Error = Invalid;

    fn try_from(item: CallItem) -> Result<Self, Invalid> {
        Ok(Self {
            id: ToolCallId(item.id),
            name: ToolName(item.name),
            arguments: match item.arguments {
                ArgumentsItem::Json(text) => ToolArguments::Json(canonical(text)?),
                ArgumentsItem::Invalid(text) => ToolArguments::Invalid(text),
            },
            execution: match item.execution {
                ExecutionItem::Client => ToolExecution::Client,
                ExecutionItem::Server => ToolExecution::Server,
            },
            signature: item.signature,
        })
    }
}

impl TryFrom<ResultItem> for ToolResult {
    type Error = Invalid;

    fn try_from(item: ResultItem) -> Result<Self, Invalid> {
        Ok(Self {
            call_id: ToolCallId(item.call_id),
            content: item
                .content
                .into_iter()
                .map(|content| {
                    Ok(match content {
                        ContentItem::Text(text) => ToolResultContent::Text(Text(text)),
                        ContentItem::Media(media) => ToolResultContent::Media(media.into()),
                        ContentItem::Unknown(unknown) => {
                            ToolResultContent::Unknown(unknown.try_into()?)
                        }
                    })
                })
                .collect::<Result<_, Invalid>>()?,
            outcome: match item.outcome {
                OutcomeItem::Success => ToolOutcome::Success,
                OutcomeItem::Error => ToolOutcome::Error,
                OutcomeItem::Unknown => ToolOutcome::Unknown,
            },
        })
    }
}

impl From<Invalid> for super::DecodeError {
    fn from(invalid: Invalid) -> Self {
        match invalid {
            Invalid::EmptyTool => Self::EmptyTool,
            Invalid::NonCanonicalJson => Self::NonCanonicalJson,
        }
    }
}

impl Serialize for MessageBody {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Body::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MessageBody {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let body = Body::deserialize(deserializer)?;
        Self::try_from(body).map_err(|invalid| {
            D::Error::custom(Rejected::new(
                "message body",
                super::DecodeError::from(invalid),
            ))
        })
    }
}
