//! Values a client must never be able to supply, and the compile-time checks
//! that keep them out of requests.
//!
//! Two rules:
//!
//! 1. **Authority never serializes.** A [`Caller`] is the authenticated
//!    caller of one request, built only by `OperatorDirectory::caller` from
//!    the verified session. It implements neither `Serialize` nor
//!    `Deserialize`: no response, event or log carries it (they carry the
//!    `OperatorId`), and no request can decode one. The same holds for
//!    what it is built from and by: [`RequestIdentity`] (what session
//!    verification established) and the [`OperatorDirectory`].
//!    [`Promotion`] is built by the surface from a `PromoteChannel` request,
//!    the caller and the acceptance time, and handed in process to
//!    `ChannelRegistry::promote`; nothing sends it anywhere, so it gets
//!    neither trait either.
//! 2. **Server-stamped records are never requests.** Each type below holds
//!    an author or an acceptance time the surface stamps from the caller and
//!    its clock. They are responses or bus payloads, so they serialize and
//!    the UI or another node decodes them, but none implements
//!    [`WireRequest`], so the HTTP layer ([`decode_request`]) cannot accept
//!    one from a client.
//!
//! | Type | Stamped | Rule |
//! | --- | --- | --- |
//! | `Caller` | operator, permissions | 1 |
//! | `RequestIdentity`, `OperatorDirectory` | session, config | 1 |
//! | `Promotion` | operator, time | 1 |
//! | `MergeRequest`, `MergeAuthor` | author | 2 |
//! | `MergeRecord`, `Reversal`, `MergeVeto` | operator, time | 2 |
//! | `TransmissionVerdict`, `VerdictLog` | operator, time | 2 |
//! | `Pin` | operator, time | 2 |
//! | `PolicyDecision`, `Decision`, `PolicyAuthor`, `Declaration`, `PolicyHistory` | author, time | 2 |
//! | `PromotionPreview` | operator, time of the would-be declaration | 2 |
//! | `OperatorAction` | holds a `MergeRequest`, so an author | 2 |
//! | `OperatorRecord`, `AuditEntry` | caller, time | 2 |
//! | `ExportHeader`, `ExportRecord` | caller, time | 2 |
//! | `ProjectionInfo` | requester, time | 2 |
//! | `AlertRuleDef` | creator, time | 2 |
//! | `Alert`, `AlertState` | who acknowledged or resolved, when | 2 |
//! | `Envelope` (every bus event) | node, time | 2 |
//! | `Supersession`, `SupersededInto` | the promotion's time (and operator) | 2 |
//! | `Policy` | holds a `Decision` | 2 |
//! | `Retention` | holds a `Pin` | 2 |
//! | `AlertRule` | creator, time | 2 |
//! | `VerdictRow` | operator, time | 2 |
//! | `ConfigChange`, `ConfigRecord` | config, permissions | 2 |
//! | `Operator`, `PermissionSet` | permissions | 2 |
//!
//! `OperatorAction` is what the surface acts on and the audit log stores,
//! after `OperatorAction::merge_agents` has stamped the caller into a merge.
//! The request a client sends for an action is a separate type without
//! the author (see `docs/features/wire_contract.md`).
//!
//! An audit record's caller is written through `RecordedCaller` (private to
//! the surface): the operator and permissions it held, a response's copy of
//! a `Caller`, never decoded from a client.
//!
//! The checks are `assert_not_impl!` items below: each fails to compile if
//! its type gains a listed trait. The doctests show the same from outside
//! the crate: decoding a request type compiles, decoding a `Caller` or a
//! `Promotion` does not.
//!
//! ```
//! use crosstalk_spec::interfaces::l8_surface::AlertFilter;
//! use crosstalk_spec::wire::decode_request;
//!
//! let filter = decode_request::<AlertFilter>(br#"{"states": [], "channel": null}"#);
//! assert!(filter.is_ok());
//! ```
//!
//! ```compile_fail,E0277
//! use crosstalk_spec::interfaces::l8_surface::Caller;
//! use crosstalk_spec::wire::decode_request;
//!
//! // Caller is not a WireRequest.
//! let _ = decode_request::<Caller>(b"{}");
//! ```
//!
//! ```compile_fail,E0277
//! use crosstalk_spec::interfaces::l8_surface::Caller;
//!
//! // Caller does not deserialize at all.
//! let _: Caller = serde_json::from_str("{}").unwrap();
//! ```
//!
//! ```compile_fail,E0277
//! use crosstalk_spec::derived::flow::channel::promotion::Promotion;
//! use crosstalk_spec::wire::decode_request;
//!
//! // A promotion is stamped by the server.
//! let _ = decode_request::<Promotion>(b"{}");
//! ```
//!
//! [`Caller`]: crate::interfaces::l8_surface::Caller
//! [`RequestIdentity`]: crate::interfaces::l8_surface::operators::RequestIdentity
//! [`OperatorDirectory`]: crate::interfaces::l8_surface::operators::OperatorDirectory
//! [`Promotion`]: crate::derived::flow::channel::promotion::Promotion
//! [`WireRequest`]: super::WireRequest
//! [`decode_request`]: super::decode_request

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::WireRequest;
use crate::aggregates::alert::{Alert, AlertRule, AlertRuleDef, AlertState};
use crate::aggregates::projection::ProjectionInfo;
use crate::aggregates::retention::{Pin, Retention};
use crate::derived::flow::channel::policy::{
    Decision, Policy, PolicyAuthor, PolicyDecision, PolicyHistory,
};
use crate::derived::flow::channel::promotion::Promotion;
use crate::derived::flow::channel::{Declaration, Supersession};
use crate::derived::flow::verdict::{TransmissionVerdict, VerdictLog};
use crate::events::Envelope;
use crate::interfaces::l8_surface::audit::{
    AuditEntry, ConfigChange, ConfigRecord, OperatorRecord,
};
use crate::interfaces::l8_surface::channels::{PromotionPreview, SupersededInto};
use crate::interfaces::l8_surface::export::rows::VerdictRow;
use crate::interfaces::l8_surface::export::{ExportHeader, ExportRecord};
use crate::interfaces::l8_surface::operators::{Operator, OperatorDirectory, RequestIdentity};
use crate::interfaces::l8_surface::{Caller, OperatorAction, PermissionSet};
use crate::observed::agent::{MergeAuthor, MergeRecord, MergeRequest, MergeVeto, Reversal};

// Rule 1: authority never serializes, either way.
assert_not_impl!(Caller: Serialize, DeserializeOwned, WireRequest);
assert_not_impl!(RequestIdentity: Serialize, DeserializeOwned, WireRequest);
assert_not_impl!(OperatorDirectory: Serialize, DeserializeOwned, WireRequest);
assert_not_impl!(Promotion: Serialize, DeserializeOwned, WireRequest);

// Rule 2: server-stamped records are never requests.
assert_not_impl!(MergeRequest: WireRequest);
assert_not_impl!(MergeAuthor: WireRequest);
assert_not_impl!(MergeRecord: WireRequest);
assert_not_impl!(Reversal: WireRequest);
assert_not_impl!(MergeVeto: WireRequest);
assert_not_impl!(TransmissionVerdict: WireRequest);
assert_not_impl!(VerdictLog: WireRequest);
assert_not_impl!(Pin: WireRequest);
assert_not_impl!(PolicyDecision: WireRequest);
assert_not_impl!(Decision: WireRequest);
assert_not_impl!(PolicyAuthor: WireRequest);
assert_not_impl!(Declaration: WireRequest);
assert_not_impl!(PolicyHistory: WireRequest);
assert_not_impl!(PromotionPreview: WireRequest);
assert_not_impl!(OperatorAction: WireRequest);
assert_not_impl!(OperatorRecord: WireRequest);
assert_not_impl!(AuditEntry: WireRequest);
assert_not_impl!(ExportHeader: WireRequest);
assert_not_impl!(ExportRecord: WireRequest);
assert_not_impl!(ProjectionInfo: WireRequest);
assert_not_impl!(AlertRuleDef: WireRequest);
assert_not_impl!(Alert: WireRequest);
assert_not_impl!(AlertState: WireRequest);
assert_not_impl!(Envelope: WireRequest);
assert_not_impl!(Supersession: WireRequest);
assert_not_impl!(SupersededInto: WireRequest);
assert_not_impl!(Policy: WireRequest);
assert_not_impl!(Retention: WireRequest);
assert_not_impl!(AlertRule: WireRequest);
assert_not_impl!(VerdictRow: WireRequest);
assert_not_impl!(ConfigChange: WireRequest);
assert_not_impl!(ConfigRecord: WireRequest);
assert_not_impl!(Operator: WireRequest);
assert_not_impl!(PermissionSet: WireRequest);
