//! Verdicts: the log (the spec's `VerdictLog`, read with `View`), and the
//! form that records one (genuine, false detection, or withdraw the
//! verdict in force). Recording needs `Triage` (`SetVerdict`'s one
//! permission).

use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictLog};
use crosstalk_spec::ids::TransmissionId;
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL, SECTION, SECTION_TITLE};
use crate::components::table::{ROW, TD, TD_MUTED};
use crate::components::{data_table, error_panel, format_time, kind_badge};
use crate::error::UiError;
use crate::pages::common::flash::Flash;
use crate::pages::common::form::{FormFields, invalid, note, required};
use crate::pages::common::lookup::OperatorNames;
use crosstalk_spec::interfaces::l8_surface::OperatorAction;

/// The choices of the form's `verdict` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    Genuine,
    FalseDetection,
    Withdraw,
}

impl Choice {
    pub const ALL: [Self; 3] = [Self::Genuine, Self::FalseDetection, Self::Withdraw];

    pub fn code(self) -> &'static str {
        match self {
            Self::Genuine => "genuine",
            Self::FalseDetection => "false-detection",
            Self::Withdraw => "withdraw",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Genuine => "Genuine",
            Self::FalseDetection => "False detection",
            Self::Withdraw => "Withdraw my verdict",
        }
    }

    pub fn verdict(self) -> Option<Verdict> {
        match self {
            Self::Genuine => Some(Verdict::Genuine),
            Self::FalseDetection => Some(Verdict::FalseDetection),
            Self::Withdraw => None,
        }
    }
}

/// The `SetVerdict` action a posted form asks for, and the flash to show.
pub fn parse(
    transmission: TransmissionId,
    fields: &FormFields,
) -> std::result::Result<(OperatorAction, Flash), UiError> {
    let text = required(fields, "verdict")?;
    let choice = Choice::ALL
        .into_iter()
        .find(|c| c.code() == text)
        .ok_or_else(|| invalid("verdict", format!("unknown verdict {text:?}")))?;
    let flash = match choice {
        Choice::Withdraw => Flash::VerdictWithdrawn,
        _ => Flash::VerdictRecorded,
    };
    Ok((
        OperatorAction::SetVerdict {
            transmission,
            verdict: choice.verdict(),
            note: note(fields, "note")?,
        },
        flash,
    ))
}

/// One entry of the verdict log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictRow {
    pub at: String,
    pub by: String,
    pub verdict: Option<Verdict>,
    pub note: Option<String>,
    /// The latest entry is the one in force.
    pub current: bool,
}

/// The log, newest first.
pub fn verdict_rows(log: &VerdictLog, operators: &OperatorNames) -> Vec<VerdictRow> {
    let records = log.records();
    let last = records.len().checked_sub(1);
    let mut rows: Vec<VerdictRow> = records
        .iter()
        .enumerate()
        .map(|(i, v)| VerdictRow {
            at: format_time(v.at()),
            by: operators.name(v.by()),
            verdict: v.verdict(),
            note: v.note().map(str::to_owned),
            current: Some(i) == last,
        })
        .collect();
    rows.reverse();
    rows
}

/// What the verdict section offers the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormState {
    /// The form, posting to `action`. `content`: whether the caller can
    /// read the matched text; without it the form says so.
    Open { action: String, content: bool },
    /// Why there is no form.
    Closed(&'static str),
}

#[component]
pub async fn verdict_section(
    rows: Vec<VerdictRow>,
    form: FormState,
    retained: Option<FormFields>,
    error: Option<UiError>,
) -> Result<impl View> {
    let empty = rows.is_empty();
    let chosen = retained
        .as_ref()
        .and_then(|f| f.text("verdict"))
        .unwrap_or(Choice::Genuine.code())
        .to_owned();
    let note_text = retained
        .as_ref()
        .and_then(|f| f.text("note"))
        .unwrap_or("")
        .to_owned();
    let choices: Vec<(&str, &str, bool)> = Choice::ALL
        .iter()
        .map(|c| (c.code(), c.label(), c.code() == chosen))
        .collect();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Verdicts"</h2>
            if empty {
                <p class="mb-3 text-xs text-zinc-500">"No verdict yet. A verdict labels the detection; it never changes the transmission's state."</p>
            } else {
                <div class="mb-3">
                    data_table(
                        headers: &["When", "By", "Verdict", "Note"],
                        for row in rows {
                            <tr class=(ROW)>
                                <td class=(TD_MUTED)>
                                    (row.at)
                                    if row.current {
                                        <span class="ml-1 rounded bg-zinc-100 px-1 text-[10px] uppercase text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">"in force"</span>
                                    }
                                </td>
                                <td class=(TD)>(row.by)</td>
                                <td class=(TD)>
                                    match row.verdict {
                                        Some(verdict) => kind_badge(value: verdict),
                                        None => <span class="text-xs italic text-zinc-500">"withdrawn"</span>,
                                    }
                                </td>
                                <td class=(TD)>(row.note.unwrap_or_default())</td>
                            </tr>
                        }
                    )
                </div>
            }
            match form {
                FormState::Closed(reason) => <p class="text-xs text-zinc-500">(reason)</p>,
                FormState::Open { action, content } => {
                    <form method="post" action=(action) class="rounded border border-zinc-200 p-3 dark:border-zinc-800">
                        if !content {
                            <p class="mb-2 text-xs text-zinc-500">"The matched text needs the Content permission; judge from the parties, route and timing shown."</p>
                        }
                        <input type="hidden" name="action" value="set-verdict">
                        <fieldset class="flex flex-wrap items-center gap-4 text-sm">
                            <legend class=(format!("{LABEL} mb-1"))>"Record a verdict"</legend>
                            for (code, label, checked) in choices {
                                <label class="inline-flex items-center gap-1.5">
                                    <input type="radio" name="verdict" value=(code) checked=(checked)>
                                    (label)
                                </label>
                            }
                        </fieldset>
                        <div class="mt-2 flex flex-wrap items-end gap-3">
                            <label class="block min-w-64 flex-1">
                                <span class=(LABEL)>"Note (optional)"</span>
                                <input type="text" name="note" value=(note_text) maxlength="2000" class=(format!("{INPUT} w-full")) placeholder="What you checked">
                            </label>
                            <button type="submit" class=(BUTTON_PRIMARY)>"Record verdict"</button>
                        </div>
                        if let Some(error) = error {
                            <div class="mt-2">error_panel(error: &error)</div>
                        }
                    </form>
                },
            }
        </section>
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction, WriteOutcome};
    use crosstalk_spec::derived::flow::evidence::CoAccess;
    use crosstalk_spec::derived::flow::transmission::{Route, Transmission, TransmissionState};
    use crosstalk_spec::derived::flow::verdict::TransmissionVerdict;
    use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, MessageHash, OperatorId, ResourceId};
    use crosstalk_spec::observed::message::PartRef;
    use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};

    use super::*;

    /// A suspected transmission: one write and one read of a resource.
    fn suspected(id: TransmissionId) -> Transmission {
        let part = PartRef {
            message: MessageHash::from_digest(Blake3::from_bytes([3; 32])),
            index: 0,
        };
        let access = |n: u128, agent: u128, at: u64, op: AccessOp| Access {
            id: AccessId::from_ulid(n),
            agent: AgentId::from_ulid(agent),
            exchange: ExchangeId::from_ulid(n),
            resource: ResourceId::from_ulid(1),
            at: Timestamp::from_micros(at),
            via: Extraction::Structured,
            op,
        };
        let write = access(
            1,
            1,
            10,
            AccessOp::Write {
                call: part,
                spans: Vec::new(),
                outcome: WriteOutcome::Delivered,
            },
        );
        let read = access(2, 2, 20, AccessOp::Read { result: part });
        let co = CoAccess::new(&write, &read, Duration::from_secs(60)).expect("co-access");
        Transmission {
            id,
            to: AgentId::from_ulid(2),
            route: Route::Unobserved,
            opened_at: Timestamp::from_micros(20),
            state: TransmissionState::Suspected {
                co_access: NonEmpty::new(co),
                since: Timestamp::from_micros(30),
            },
        }
    }

    #[test]
    fn choices_parse_into_set_verdict() {
        let id = TransmissionId::from_ulid(4);
        let fields = FormFields::from_pairs(&[("verdict", "false-detection"), ("note", "echo")]);
        assert_eq!(
            parse(id, &fields),
            Ok((
                OperatorAction::SetVerdict {
                    transmission: id,
                    verdict: Some(Verdict::FalseDetection),
                    note: Some("echo".into()),
                },
                Flash::VerdictRecorded
            ))
        );
        let withdraw = FormFields::from_pairs(&[("verdict", "withdraw")]);
        assert!(matches!(
            parse(id, &withdraw),
            Ok((
                OperatorAction::SetVerdict { verdict: None, .. },
                Flash::VerdictWithdrawn
            ))
        ));
    }

    #[test]
    fn bad_choices_name_the_field() {
        let id = TransmissionId::from_ulid(4);
        for fields in [
            FormFields::from_pairs(&[("verdict", "maybe")]),
            FormFields::default(),
        ] {
            assert!(matches!(
                parse(id, &fields),
                Err(UiError::Field {
                    field: "verdict",
                    ..
                })
            ));
        }
    }

    #[test]
    fn the_latest_entry_is_in_force_and_listed_first() {
        let transmission = suspected(TransmissionId::from_ulid(4));
        let entry = |at: u64, verdict| {
            TransmissionVerdict::new(
                &transmission,
                verdict,
                OperatorId::from_ulid(1),
                Timestamp::from_micros(at),
                None,
            )
            .expect("judgeable")
        };
        let mut log = VerdictLog::new(transmission.id);
        for record in [entry(1, Some(Verdict::Genuine)), entry(2, None)] {
            log.record(record).expect("recorded");
        }
        let operators = OperatorNames::new([(OperatorId::from_ulid(1), "ada".to_owned())]);
        let rows = verdict_rows(&log, &operators);
        assert_eq!(rows[0].verdict, None);
        assert!(rows[0].current);
        assert!(!rows[1].current);
        assert_eq!(rows[1].by, "ada");
    }
}
