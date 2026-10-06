//! The remote a `git push`, `pull` or `fetch` prints, which wins over the
//! clone binding the call was resolved through.
//!
//! git names the remote it talked to: `To <url>` for a push, `From <url>`
//! for a pull or fetch that fetched anything. When a script has one push
//! (or one pull or fetch) that named its remote by name or not at all:
//!
//! - a push whose repository the binding gave, while the output prints
//!   `To` lines none of which is that repository, did not write it: the
//!   write is refuted (`Rejected`), its locator staying the call's
//!   (`flow.extract.write-locators-from-call`);
//! - a pull or fetch reads the one repository its `From` lines print,
//!   whatever the binding said (or with none);
//! - the context binds that remote of the clone the command ran in to the
//!   printed repository ([`learn`]), so the next call resolves it.
//!
//! A URL or path operand names the repository itself and is never
//! corrected. With several pushes (or pulls) in one script the lines
//! cannot be told apart, and nothing is corrected or learnt.

use crosstalk_spec::derived::flow::access::Extraction;

use crate::extract::op::Candidate;
use crate::extract::outcome::CommandRule;
use crate::extract::resource::RepoId;
use crate::extract::resource::repo::ORIGIN;

use super::commands::{Found, RemoteRef, Transfer, TransferKind};
use super::evidence::{Evidence, Ran};
use super::state::ShellState;

/// The remotes an output prints, each list distinct, in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Printed {
    to: Vec<RepoId>,
    from: Vec<RepoId>,
}

impl Printed {
    pub fn scan(output: &str) -> Self {
        let mut printed = Self::default();
        for line in output.lines().map(str::trim) {
            let (list, rest) = if let Some(rest) = line.strip_prefix("To ") {
                (&mut printed.to, rest)
            } else if let Some(rest) = line.strip_prefix("From ") {
                (&mut printed.from, rest)
            } else {
                continue;
            };
            let Some(repo) = rest
                .split_whitespace()
                .next()
                .and_then(|remote| RepoId::parse(remote, None))
            else {
                continue;
            };
            if !list.contains(&repo) {
                list.push(repo);
            }
        }
        printed
    }

    fn of(&self, kind: TransferKind) -> &[RepoId] {
        match kind {
            TransferKind::Push => &self.to,
            TransferKind::Fetch => &self.from,
        }
    }

    /// The one repository printed for `kind`, if exactly one is.
    fn single(&self, kind: TransferKind) -> Option<&RepoId> {
        match self.of(kind) {
            [repo] => Some(repo),
            _ => None,
        }
    }
}

/// The script's only transfer of `kind` that named its remote by the
/// clone's binding, when it ran.
fn sole<'t>(
    transfers: &'t [Transfer],
    kind: TransferKind,
    evidence: &Evidence,
) -> Option<&'t Transfer> {
    let mut of_kind = transfers.iter().filter(|transfer| transfer.kind == kind);
    let transfer = of_kind.next()?;
    if of_kind.next().is_some() || transfer.remote == RemoteRef::Explicit {
        return None;
    }
    let ran = evidence
        .step(transfer.step)
        .map_or(Ran::Yes, |step| step.ran);
    (ran != Ran::No).then_some(transfer)
}

/// Correct the accesses of a run's transfers by the remotes the output
/// printed. Every candidate keeps its place; a pull that resolved to no
/// repository but printed one gains its read at the end.
pub(crate) fn correct(
    transfers: &[Transfer],
    found: &mut Vec<Found>,
    printed: &Printed,
    evidence: &Evidence,
) {
    if let Some(push) = sole(transfers, TransferKind::Push, evidence)
        && let Some(repo) = &push.repo
        && !printed.to.is_empty()
        && !printed.to.contains(repo)
        && let Some(access) = found.iter_mut().find(|found| {
            found.step == push.step && found.candidate.rule == Some(CommandRule::GitPush)
        })
    {
        access.candidate.refuted = true;
    }
    if let Some(fetch) = sole(transfers, TransferKind::Fetch, evidence)
        && let Some(repo) = printed.single(TransferKind::Fetch)
    {
        let locator = repo.locator().clone();
        match found.iter_mut().find(|found| {
            found.step == fetch.step && found.candidate.rule == Some(CommandRule::GitTransfer)
        }) {
            Some(access) => access.candidate.locator = locator,
            None => found.push(Found {
                step: fetch.step,
                operand: None,
                candidate: Candidate::read(locator, Extraction::Parsed)
                    .judged_by(CommandRule::GitTransfer),
            }),
        }
    }
}

/// Bind the remote each sole transfer printed, in the clone it ran in
/// (the clone's root when the directory is in a known clone, else the
/// directory itself), as the remote it named (`origin` when none).
pub(crate) fn learn(
    state: &mut ShellState,
    transfers: &[Transfer],
    printed: &Printed,
    evidence: &Evidence,
) {
    for kind in [TransferKind::Push, TransferKind::Fetch] {
        let Some(transfer) = sole(transfers, kind, evidence) else {
            continue;
        };
        let (Some(repo), Some(dir), RemoteRef::Clone(name)) =
            (printed.single(kind), &transfer.dir, &transfer.remote)
        else {
            continue;
        };
        let dir = state.normalized(dir.clone());
        let root = state.repos().root_of(&dir).cloned().unwrap_or(dir);
        state.bind(root, name.as_deref().unwrap_or(ORIGIN), repo.clone());
    }
}
