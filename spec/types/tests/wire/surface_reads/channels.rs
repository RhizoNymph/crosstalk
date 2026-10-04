//! Channel read models on the wire: `QueryApi::channel` and `channels`
//! (`ChannelRow`, watermarked), `channel_names` (`ChannelName` by id) and
//! `promotion_preview` (`PromotionPreview`).

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::super::{ULID_C, id, ts};
use super::fixtures::{
    ULID_D, ULID_H, edited, field, notes, operator, page_resource, planner, team, team_pattern,
    url, wiki,
};
use crate::aggregates::watermark::Watermarked;
use crate::batch::IdBatch;
use crate::derived::flow::channel::confirmation::{Confirmation, CrossTraffic};
use crate::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crate::derived::flow::channel::promotion::{PromotionRefusal, Registered, coverage};
use crate::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crate::derived::flow::resource::{Locator, Resource};
use crate::ids::{ChannelId, ResourceId, TransmissionId};
use crate::interfaces::l5_flow::PromoteError;
use crate::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelName, ChannelRow, ChannelShape, ChannelStanding,
    PromotionPreview, SupersededInto, resolve_names,
};
use crate::paging::{ChannelList, Cursor, Page, PageSize};
use crate::support::{NonEmpty, Timestamp, Watermark};

const AREA: &str = "surface_reads/channels";

fn promoted_at() -> Timestamp {
    ts("2026-10-04T10:02:11.000000Z")
}

/// A channel discovered from `seed` by its first cross-agent transmission,
/// `first`.
fn discovered(channel: ChannelId, seed: ResourceId, first: &str) -> Channel {
    Channel {
        id: channel,
        origin: ChannelOrigin::Discovered {
            seed: Seed {
                resource: seed,
                first_transmission: id(TransmissionId::from_ulid_text, first),
                opened_at: ts("2026-10-04T09:16:40.002513Z"),
            },
            detection: TrafficDetection::Active {
                since: ts("2026-10-04T09:16:41.250000Z"),
                last_transmission: id(TransmissionId::from_ulid_text, first),
            },
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    }
}

/// The wiki channel, discovered from the release plan page.
fn wiki_channel() -> Channel {
    discovered(wiki(), page_resource().id, ULID_C)
}

fn team_seed() -> Resource {
    Resource {
        id: id(ResourceId::from_ulid_text, ULID_C),
        locator: url("/team/index"),
        first_seen: ts("2026-10-04T08:00:00.000000Z"),
    }
}

/// The team channel: discovered from `/team/index`, then promoted over
/// `/team` by the operator.
fn team_channel() -> Channel {
    let mut channel = discovered(team(), team_seed().id, ULID_H);
    channel.origin = channel
        .origin
        .promoted(Declaration {
            pattern: team_pattern(),
            by: PolicyAuthor::Operator(operator()),
            at: promoted_at(),
        })
        .expect("a discovered channel can be promoted");
    channel.policy = Policy::Sanctioned(Decision {
        by: PolicyAuthor::Operator(operator()),
        at: promoted_at(),
        note: Some("release planning".into()),
    });
    channel
}

fn notes_channel_id() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_H)
}

/// The notes channel, superseded by the team channel's promotion.
fn notes_channel() -> Channel {
    let mut channel = discovered(notes_channel_id(), notes().id, ULID_H);
    channel.origin = channel
        .origin
        .superseded(Supersession {
            by: team(),
            at: promoted_at(),
        })
        .expect("a discovered channel can be superseded");
    channel
}

/// A channel declared in config before any traffic.
fn quiet_channel() -> Channel {
    Channel {
        id: id(ChannelId::from_ulid_text, ULID_C),
        origin: ChannelOrigin::Declared {
            declaration: Declaration {
                pattern: team_pattern(),
                by: PolicyAuthor::Config,
                at: ts("2026-10-01T00:00:00.000000Z"),
            },
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    }
}

fn row_in_force() -> ChannelRow {
    ChannelRow::new(
        wiki_channel(),
        Some(page_resource()),
        ChannelStanding::InForce {
            traffic: CrossTraffic {
                confirmed: 14,
                unconfirmed: 3,
            },
            activity: ChannelActivity::Seen {
                last: ts("2026-10-04T11:42:07.531000Z"),
                counts: ChannelCounts {
                    writers: 1,
                    readers: 2,
                    transmissions: 14,
                },
            },
        },
    )
    .expect("a discovered channel with its seed and activity")
}

/// The wiki channel while its only cross-agent traffic is a suspected
/// transmission: listed, marked unconfirmed.
fn row_unconfirmed() -> ChannelRow {
    ChannelRow::new(
        wiki_channel(),
        Some(page_resource()),
        ChannelStanding::InForce {
            traffic: CrossTraffic {
                confirmed: 0,
                unconfirmed: 1,
            },
            activity: ChannelActivity::Seen {
                last: ts("2026-10-04T09:16:40.002513Z"),
                counts: ChannelCounts {
                    writers: 1,
                    readers: 1,
                    transmissions: 0,
                },
            },
        },
    )
    .expect("a discovered channel with suspected traffic")
}

fn row_superseded() -> ChannelRow {
    let channel = notes_channel();
    let supersession = channel.origin.supersession().expect("superseded");
    let into = SupersededInto::of(supersession, &team_channel()).expect("the promoted channel");
    ChannelRow::new(channel, Some(notes()), ChannelStanding::Superseded(into))
        .expect("a superseded channel with its own supersession")
}

fn row_never_active() -> ChannelRow {
    ChannelRow::new(
        quiet_channel(),
        None,
        ChannelStanding::InForce {
            traffic: CrossTraffic::NONE,
            activity: ChannelActivity::Never,
        },
    )
    .expect("a declared channel that saw no traffic")
}

fn watermark() -> Watermark {
    Watermark(ts("2026-10-04T12:00:00.000000Z"))
}

#[test]
fn channel_rows_golden_in_every_standing() {
    fn declared(row: ChannelRow) -> ChannelRow {
        match row.standing() {
            ChannelStanding::InForce {
                activity: ChannelActivity::Seen { .. },
                ..
            }
            | ChannelStanding::InForce {
                activity: ChannelActivity::Never,
                ..
            }
            | ChannelStanding::Superseded(_) => row,
        }
    }
    assert_golden(AREA, "channel_row_in_force", &declared(row_in_force()));
    assert_golden(
        AREA,
        "channel_row_never_active",
        &declared(row_never_active()),
    );
    assert_golden(AREA, "channel_row_superseded", &declared(row_superseded()));
    assert_golden(
        AREA,
        "channel_row_unconfirmed",
        &declared(row_unconfirmed()),
    );
}

/// A row's cross-agent traffic and the confirmation it derives.
#[test]
fn cross_traffic_and_confirmations_golden() {
    assert_golden(
        AREA,
        "cross_traffic",
        &CrossTraffic {
            confirmed: 14,
            unconfirmed: 3,
        },
    );
    fn declared(confirmation: Confirmation) -> Confirmation {
        match confirmation {
            Confirmation::Unconfirmed | Confirmation::Confirmed => confirmation,
        }
    }
    let every = [Confirmation::Unconfirmed, Confirmation::Confirmed].map(declared);
    assert_golden(AREA, "confirmations", &every.to_vec());
    assert_rejected::<CrossTraffic>(
        r#"{"confirmed": 1, "unconfirmed": 0, "hidden": 0}"#,
        "unknown field `hidden`",
    );
    assert_rejected::<Confirmation>(r#""suspected""#, "unknown variant `suspected`");
}

/// `QueryApi::channel` and `QueryApi::channels`.
#[test]
fn channel_responses_golden() {
    assert_golden(
        AREA,
        "channel_found",
        &Some(Watermarked {
            watermark: watermark(),
            value: row_in_force(),
        }),
    );
    assert_golden(AREA, "channel_unknown", &None::<Watermarked<ChannelRow>>);
    let next: Cursor<ChannelList> =
        Cursor::from_token("Y2hhbm5lbHMtYWZ0ZXItMDFKOVo".into()).expect("URL-safe base64");
    let page = Page::more(
        PageSize::new(2).expect("a valid size"),
        NonEmpty::from_vec(vec![row_in_force(), row_superseded()]).expect("two rows"),
        next,
    )
    .expect("two rows fit a page of two");
    assert_golden(
        AREA,
        "channels_page",
        &Watermarked {
            watermark: watermark(),
            value: page,
        },
    );
}

fn registry_seeds() -> (Vec<Channel>, Vec<Locator>) {
    (
        vec![wiki_channel(), team_channel(), notes_channel()],
        vec![
            page_resource().locator,
            team_seed().locator,
            notes().locator,
        ],
    )
}

fn registered<'a>(channels: &'a [Channel], seeds: &'a [Locator]) -> Vec<Registered<'a>> {
    channels
        .iter()
        .zip(seeds)
        .map(|(channel, seed)| Registered {
            channel,
            seed: channel.origin.seed().map(|_| seed),
        })
        .collect()
}

/// `QueryApi::channel_names`: one name per shape, and a superseded id
/// answered with the name of the channel it resolves to.
#[test]
fn channel_names_golden() {
    let (channels, seeds) = registry_seeds();
    let registry = registered(&channels, &seeds);
    let seed_name = ChannelName::of(&registry[0]).expect("the wiki channel is in force");
    assert_eq!(
        seed_name.shape(),
        &ChannelShape::Seed(page_resource().locator)
    );
    assert_golden(AREA, "channel_name_seed", &seed_name);
    let pattern_name = ChannelName::of(&registry[1]).expect("the team channel is in force");
    assert_eq!(pattern_name.shape(), &ChannelShape::Pattern(team_pattern()));
    assert_golden(AREA, "channel_name_pattern", &pattern_name);

    let ids = IdBatch::new([notes_channel_id()]).expect("one id");
    let names: BTreeMap<ChannelId, ChannelName> =
        resolve_names(&ids, &registry).expect("every channel is named");
    assert_eq!(names.get(&notes_channel_id()), Some(&pattern_name));
    assert_golden(AREA, "channel_names_superseded_id", &names);

    // The map is ordered by id, so several names have one encoding.
    let ids = IdBatch::new([wiki(), team(), notes_channel_id()]).expect("three ids");
    let several = resolve_names(&ids, &registry).expect("every channel is named");
    assert_eq!(several.get(&notes_channel_id()), Some(&pattern_name));
    assert_eq!(several.len(), 3);
    assert_golden(AREA, "channel_names_several", &several);
}

/// `QueryApi::promotion_preview`: what promoting the wiki channel over
/// `/team` would do, and a promotion refused with a conflict.
#[test]
fn promotion_previews_golden() {
    let channels = [wiki_channel(), notes_channel_unsuperseded()];
    let seeds = [page_resource().locator, notes().locator];
    let entries = registered(&channels, &seeds);
    let runbook = Resource {
        id: id(ResourceId::from_ulid_text, ULID_D),
        locator: url("/ops/runbook"),
        first_seen: ts("2026-10-04T09:00:00.000000Z"),
    };
    let held = |channel: ChannelId| {
        if channel == wiki() {
            vec![page_resource(), runbook.clone()]
        } else if channel == notes_channel_id() {
            vec![notes()]
        } else {
            Vec::new()
        }
    };
    let declaration = Declaration {
        pattern: team_pattern(),
        by: PolicyAuthor::Operator(operator()),
        at: promoted_at(),
    };
    let preview = PromotionPreview::from_registry(
        coverage(wiki(), &declaration, &entries, held).map_err(PromoteError::Refused),
    )
    .expect("an accepted promotion is an answer");
    assert_eq!(preview.conflict(), None);
    assert_eq!(
        preview.superseded_channels().as_slice(),
        &[notes_channel_id()]
    );
    assert_golden(AREA, "promotion_preview_promotes", &preview);

    let refused = PromotionPreview::from_registry(Err(PromoteError::Refused(
        PromotionRefusal::PatternOverlaps { existing: team() },
    )))
    .expect("a conflict is an answer");
    assert!(refused.conflict().is_some());
    assert_golden(AREA, "promotion_preview_refused", &refused);
}

/// The notes channel before the team channel's promotion superseded it.
fn notes_channel_unsuperseded() -> Channel {
    discovered(notes_channel_id(), notes().id, ULID_H)
}

#[test]
fn channel_rows_are_decoded_through_their_constructor() {
    assert_rejected::<ChannelRow>(
        &edited(&row_in_force(), |json| *field(json, "seed") = Value::Null),
        "invalid channel row: SeedMismatch",
    );
    assert_rejected::<ChannelRow>(
        &edited(&row_in_force(), |json| {
            *field(json, "seed") = serde_json::to_value(notes()).expect("encodes");
        }),
        "invalid channel row: SeedMismatch",
    );
    let superseded_standing = serde_json::to_value(row_superseded().standing()).expect("encodes");
    assert_rejected::<ChannelRow>(
        &edited(&row_in_force(), |json| {
            *field(json, "standing") = superseded_standing;
        }),
        "invalid channel row: StandingMismatch",
    );
    let never = json!({
        "type": "in_force",
        "data": {
            "traffic": {"confirmed": 0, "unconfirmed": 0},
            "activity": {"type": "never"},
        },
    });
    assert_rejected::<ChannelRow>(
        &edited(&row_superseded(), |json| {
            *field(json, "standing") = never.clone();
        }),
        "invalid channel row: StandingMismatch",
    );
    assert_rejected::<ChannelRow>(
        &edited(&row_in_force(), |json| {
            *field(json, "standing") = never.clone();
        }),
        "invalid channel row: TrafficWithoutActivity",
    );
    assert_rejected::<ChannelRow>(
        &edited(&row_never_active(), |json| {
            *field(json, "standing") = json!({
                "type": "in_force",
                "data": {
                    "traffic": {"confirmed": 1, "unconfirmed": 0},
                    "activity": {"type": "never"},
                },
            });
        }),
        "invalid channel row: TrafficWithoutDetection",
    );
    assert_rejected::<ChannelRow>(
        &edited(&row_in_force(), |json| {
            json["unread"] = json!(3);
        }),
        "unknown field `unread`",
    );
}

#[test]
fn channel_enums_refuse_unknown_variants_and_fields() {
    assert_rejected::<ChannelStanding>(
        r#"{"type": "retired", "data": null}"#,
        "unknown variant `retired`",
    );
    assert_rejected::<ChannelActivity>(
        r#"{"type": "seen", "data": {"last": "2026-10-04T11:42:07.531000Z", "counts": {"writers": 1, "readers": 2, "transmissions": 0, "bytes": 9}}}"#,
        "unknown field `bytes`",
    );
    assert_rejected::<ChannelShape>(
        r#"{"type": "glob", "data": "/team/*"}"#,
        "unknown variant `glob`",
    );
    assert_rejected::<SupersededInto>(
        &format!(
            r#"{{"into": "{}", "by": "{}", "at": "2026-10-04T10:02:11.000000Z", "note": null}}"#,
            team().ulid_text(),
            planner().ulid_text()
        ),
        "unknown field `note`",
    );
    assert_rejected::<ChannelName>(
        &format!(
            r#"{{"id": "{}", "shape": {{"type": "seed", "data": {{"type": "url", "data": {{"scheme": "https", "host": "wiki.example.com", "path": "/", "query": null}}}}}}, "label": "wiki"}}"#,
            wiki().ulid_text()
        ),
        "unknown field `label`",
    );
}

/// A refused preview carries a conflict `PromoteChannel` is refused with;
/// any other conflict is not a preview.
#[test]
fn promotion_previews_refuse_conflicts_no_promotion_has() {
    let too_large = json!({
        "type": "refused",
        "data": {"type": "export_too_large", "data": {"rows": 11, "limit": 10}},
    });
    assert_rejected::<PromotionPreview>(
        &too_large.to_string(),
        "invalid promotion preview: NotAPromotionConflict(ExportTooLarge",
    );
    assert_rejected::<PromotionPreview>(
        r#"{"type": "maybe", "data": null}"#,
        "unknown variant `maybe`",
    );
}
