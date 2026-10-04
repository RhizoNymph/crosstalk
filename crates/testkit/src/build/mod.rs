//! Builders for common spec values.
//!
//! Each builder is constructed from an [`Ids`](crate::ids::Ids), which hands
//! it every id it needs up front, so `build` is pure: the same calls under the
//! same seed build the same value. Defaults are sensible and fluent methods
//! override them. Builders of plain spec types return the value; builders of
//! checked types build through the spec's constructor and return
//! `Result<_, BuildError>`, whose defaults always succeed.
//!
//! | Builder | Builds |
//! | --- | --- |
//! | [`agent::AgentBuilder`] | `Agent` in any state |
//! | [`flow::ResourceBuilder`], [`flow::AccessBuilder`], [`flow::ChannelBuilder`] | `Resource`, `Access`, `Channel` of any origin |
//! | [`exchange::ExchangeBuilder`], [`exchange::NormalizedExchangeBuilder`] | `Exchange`, `NormalizedExchange` |
//! | [`message`] | canonical message bodies and their content hashes |
//! | [`provenance::ContentMatchBuilder`], [`provenance::CrossAccessBuilder`] | `ContentMatch`; a write, a read and their `CoAccess` |
//! | [`transmission::TransmissionBuilder`] | `Transmission` in every state, with its evidence |
//! | [`alert::AlertBuilder`], [`alert::UserRuleBuilder`], [`alert::builtin_rule`] | `Alert`, `AlertRuleDef` |
//! | [`topic::TopicHistoryBuilder`] | `TopicVersionInfo`s and `TopicVersionHistory` |
//! | [`event::EnvelopeBuilder`] and the [`event`] functions | `BusEvent`, `Envelope` |

pub mod agent;
pub mod alert;
pub mod error;
pub mod event;
pub mod exchange;
pub mod flow;
pub mod message;
pub mod provenance;
pub mod topic;
pub mod transmission;

pub use agent::AgentBuilder;
pub use alert::{AlertBuilder, UserRuleBuilder, builtin_rule};
pub use error::BuildError;
pub use event::EnvelopeBuilder;
pub use exchange::{ExchangeBuilder, NormalizedExchangeBuilder};
pub use flow::{AccessBuilder, ChannelBuilder, ResourceBuilder};
pub use provenance::{ContentMatchBuilder, CrossAccess, CrossAccessBuilder};
pub use topic::TopicHistoryBuilder;
pub use transmission::{TransmissionBuilder, TransmissionParts};
