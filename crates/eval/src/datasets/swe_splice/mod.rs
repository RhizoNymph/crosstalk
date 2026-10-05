//! Splices: exact Channel transmissions planted in realistic SWE traces.
//!
//! Each world is two unrelated Open-SWE trajectories (different
//! repositories): a sender `A` and a reader `B`.
//!
//! 1. **The write.** A whole-file write of `A` ([`write`]): an editor
//!    `create` or a `cat > P <<EOF` heredoc, at least
//!    [`MIN_CONTENT`] bytes over three lines or more.
//! 2. **One working directory.** `A`'s working directory (from its task's
//!    `<uploaded_files>`, else `/testbed`) is rewritten to `B`'s in every
//!    message of `A`, so the path `A` wrote is a path in `B`'s workspace.
//! 3. **The read.** Before one of `B`'s calls (never its first), `B` gets an
//!    extra call reading that path and its result in `B`'s harness format
//!    ([`read`]): the file as the [`variant`] renders it, numbered like
//!    `cat -n`.
//! 4. **The clock.** `A`'s call `i` is at `compose(a0 + i, 1, 0)` and `B`'s
//!    call `j` at `compose(b0 + j, 0, 0)`, with the offsets chosen so `B`'s
//!    read call comes right after `A`'s writing call, and the reader
//!    exchange (the call after the read) later still.
//!
//! **Label.** `A → B`, Channel route through `Locator::File` of the
//! shared absolute path, ToolResult carrier, at `B`'s first exchange
//! carrying the result, located at the numbered lines, needing what the
//! variant and read form need, tier Construction. The rest of the world is
//! background: complete coverage, and a negative control (Boilerplate, or
//! SharedSource for one repository) for every other (sender, reader
//! exchange).
//!
//! **Determinism.** Trajectories are read round-robin from the selected
//! shards into a pool; splice `n` draws its pair, write and insertion point
//! from a stream derived from the seed and `n`, and uses variant
//! `n mod 4`.

pub mod read;
pub mod variant;
pub mod write;

use std::path::{Path, PathBuf};

use crosstalk_spec::derived::flow::resource::Locator;

pub use read::ReadForm;
pub use variant::Variant;
pub use write::{FileWrite, WriteForm};

use crate::corpus::clock::compose;
use crate::corpus::{SourceError, TraceSource, World};
use crate::datasets::background::{BackgroundError, BackgroundWorld, Trajectory};
use crate::datasets::chat::{ChatMessage, convert};
use crate::datasets::open_swe::{self, OpenSweError, OpenSweRow, RoundRobin, Shard};
use crate::datasets::rng::SplitMix64;
use crate::datasets::salt::Selection;
use crate::keys::{DatasetId, SourceRef, WorldKey};
use crate::location::{self, LocationError, SpanLocationExt};
use crate::reference::route::normalize_path;
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, InvalidLabel,
    RouteExpectation, Tier, TransmissionLabel,
};

/// The dataset's id.
pub const DATASET: &str = "swe_splice";

/// Splices per run unless configured.
pub const SPLICES: usize = 40;

/// The shortest file worth splicing, in bytes.
pub const MIN_CONTENT: usize = 160;

/// The longest file spliced, in bytes (a view of a huge file is cut).
pub const MAX_CONTENT: usize = 16_000;

/// The working directory when a task names none (SWE-agent and
/// mini-swe-agent mount the repository there).
pub const DEFAULT_WORKDIR: &str = "/testbed";

#[derive(Debug, thiserror::Error)]
pub enum SpliceError {
    #[error(transparent)]
    OpenSwe(#[from] OpenSweError),
    #[error("splice {0}: no trajectory in the pool has a file write to splice")]
    NoWriter(usize),
    #[error("splice {0}: no reader from another repository with a call to splice before")]
    NoReader(usize),
    #[error("location: {0}")]
    Location(#[from] LocationError),
    #[error("label: {0}")]
    Label(#[from] InvalidLabel),
    #[error(transparent)]
    Background(#[from] BackgroundError),
}

/// A trajectory of the pool.
#[derive(Debug, Clone)]
pub struct Pooled {
    pub shard: Shard,
    pub row: usize,
    pub record: OpenSweRow,
    pub workdir: String,
}

impl Pooled {
    pub fn new(shard: Shard, row: usize, record: OpenSweRow) -> Self {
        let workdir = workdir(&record.messages);
        Self {
            shard,
            row,
            record,
            workdir,
        }
    }

    /// Writes worth splicing.
    pub fn writes(&self) -> Vec<FileWrite> {
        write::writes(&self.record.messages, &self.workdir)
            .into_iter()
            .filter(splicable)
            .collect()
    }
}

/// Whether a write is long enough to carry a span and short enough to view.
pub fn splicable(write: &FileWrite) -> bool {
    (MIN_CONTENT..=MAX_CONTENT).contains(&write.content.len()) && write.content.lines().count() >= 3
}

/// The working directory a trajectory's task names in
/// `<uploaded_files>…</uploaded_files>`, else [`DEFAULT_WORKDIR`].
pub fn workdir(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .filter(|message| message.role == "user")
        .find_map(|message| {
            let text = message.text();
            let start = text.find("<uploaded_files>")? + "<uploaded_files>".len();
            let end = text[start..].find("</uploaded_files>")? + start;
            let dir = text[start..end].trim();
            (dir.starts_with('/') && !dir.contains(char::is_whitespace))
                .then(|| normalize_path(dir))
        })
        .unwrap_or_else(|| DEFAULT_WORKDIR.to_owned())
}

/// `messages` with every occurrence of directory `from` replaced by `to`.
pub fn rewrite_workdir(messages: &[ChatMessage], from: &str, to: &str) -> Vec<ChatMessage> {
    if from == to {
        return messages.to_vec();
    }
    let swap = |text: &Option<String>| text.as_ref().map(|text| text.replace(from, to));
    messages
        .iter()
        .map(|message| ChatMessage {
            role: message.role.clone(),
            content: swap(&message.content),
            reasoning_content: swap(&message.reasoning_content),
            tool_calls: message.tool_calls.as_ref().map(|calls| {
                calls
                    .iter()
                    .map(|call| {
                        let mut call = call.clone();
                        call.function.arguments = swap(&call.function.arguments);
                        call
                    })
                    .collect()
            }),
            tool_call_id: message.tool_call_id.clone(),
        })
        .collect()
}

/// Message indices before which a read can be spliced: an assistant
/// message that is not the trajectory's first and follows a non-assistant
/// message.
pub fn insertion_points(messages: &[ChatMessage]) -> Vec<usize> {
    let first = messages.iter().position(ChatMessage::is_assistant);
    (1..messages.len())
        .filter(|&at| {
            messages[at].is_assistant() && Some(at) != first && !messages[at - 1].is_assistant()
        })
        .collect()
}

/// How many assistant messages (calls) come before message `index`.
fn calls_before(messages: &[ChatMessage], index: usize) -> u64 {
    messages[..index]
        .iter()
        .filter(|message| message.is_assistant())
        .count() as u64
}

/// One planned splice.
#[derive(Debug, Clone)]
pub struct Plan {
    pub number: usize,
    pub variant: Variant,
    pub sender: usize,
    pub write: usize,
    pub reader: usize,
    pub insert_at: usize,
    pub call_id: String,
}

/// Plans splice `number` over `pool`.
pub fn plan(pool: &[Pooled], number: usize, seed: u64) -> Result<Plan, SpliceError> {
    let mut rng = SplitMix64::derived(seed, &format!("splice/{number}"));
    let writers: Vec<(usize, usize)> = pool
        .iter()
        .enumerate()
        .map(|(at, pooled)| (at, pooled.writes().len()))
        .filter(|(_, writes)| *writes > 0)
        .collect();
    let &(sender, writes) = rng.pick(&writers).ok_or(SpliceError::NoWriter(number))?;
    let write = rng.index(writes).ok_or(SpliceError::NoWriter(number))?;
    let readers: Vec<usize> = pool
        .iter()
        .enumerate()
        .filter(|(at, pooled)| {
            *at != sender
                && pooled.record.repo != pool[sender].record.repo
                && !insertion_points(&pooled.record.messages).is_empty()
        })
        .map(|(at, _)| at)
        .collect();
    let &reader = rng.pick(&readers).ok_or(SpliceError::NoReader(number))?;
    let points = insertion_points(&pool[reader].record.messages);
    let &insert_at = rng.pick(&points).ok_or(SpliceError::NoReader(number))?;
    let call_id = format!("chatcmpl-tool-{:016x}", rng.next_u64());
    Ok(Plan {
        number,
        variant: Variant::ALL[number % Variant::ALL.len()],
        sender,
        write,
        reader,
        insert_at,
        call_id,
    })
}

/// The world of one planned splice.
pub fn world(pool: &[Pooled], plan: &Plan) -> Result<World, SpliceError> {
    let sender = &pool[plan.sender];
    let reader = &pool[plan.reader];
    let form = ReadForm::of(&reader.record.messages);

    // The sender, moved into the reader's working directory.
    let chosen = sender
        .writes()
        .into_iter()
        .nth(plan.write)
        .ok_or(SpliceError::NoWriter(plan.number))?;
    let sent = rewrite_workdir(&sender.record.messages, &sender.workdir, &reader.workdir);
    let same_call = |w: &FileWrite| w.message == chosen.message && w.call == chosen.call;
    let nth_in_call = write::writes(&sender.record.messages, &sender.workdir)
        .into_iter()
        .filter(same_call)
        .position(|w| w == chosen)
        .ok_or(SpliceError::NoWriter(plan.number))?;
    let written = write::writes(&sent, &reader.workdir)
        .into_iter()
        .filter(same_call)
        .nth(nth_in_call)
        .ok_or(SpliceError::NoWriter(plan.number))?;

    // The reader, with the read spliced in.
    let body = plan.variant.render(&written.content);
    let (result, (start, end)) = form.result(&written.path, &plan.call_id, &body);
    let mut read = reader.record.messages.clone();
    read.splice(
        plan.insert_at..plan.insert_at,
        [form.call(&written.path, &plan.call_id), result],
    );

    // Clocks: the read call right after the writing call.
    let write_call = calls_before(&sent, written.message);
    let read_call = calls_before(&read, plan.insert_at);
    let (a0, b0) = if read_call > write_call {
        (read_call - write_call - 1, 0)
    } else {
        (0, write_call + 1 - read_call)
    };
    let a_file = sender.shard.relative.clone();
    let b_file = reader.shard.relative.clone();
    let sender_calls = open_swe::calls(&sent, &a_file, sender.row, |i| compose(a0 + i, 1, 0))?;
    let reader_calls = open_swe::calls(&read, &b_file, reader.row, |j| compose(b0 + j, 0, 0))?;

    let hashed = convert(&read).map_err(|source| OpenSweError::Chat {
        file: b_file.clone(),
        row: reader.row,
        source,
    })?;
    let message = hashed[plan.insert_at + 1].message();
    let at = location::in_message(
        message,
        0,
        u32::try_from(start).unwrap_or(u32::MAX),
        u32::try_from(end).unwrap_or(u32::MAX),
    )?;
    let text = at.text(message)?;

    let key = WorldKey::new(format!(
        "splice-{:04}-{}-{}",
        plan.number, plan.variant, form
    ));
    let mut world = BackgroundWorld::new(DatasetId::new(DATASET), key);
    let (from, sender_ids) = world.add(Trajectory {
        name: format!("sender/{}", open_swe::agent_name(&sender.shard, sender.row)),
        model: sender.shard.model.clone(),
        group: sender.record.repo.clone(),
        calls: sender_calls,
    })?;
    let (to, reader_ids) = world.add(Trajectory {
        name: format!("reader/{}", open_swe::agent_name(&reader.shard, reader.row)),
        model: reader.shard.model.clone(),
        group: reader.record.repo.clone(),
        calls: reader_calls,
    })?;
    let reader_exchange = usize::try_from(read_call + 1)
        .ok()
        .and_then(|at| reader_ids.get(at).copied())
        .ok_or(SpliceError::NoReader(plan.number))?;
    let sender_exchange = usize::try_from(write_call)
        .ok()
        .and_then(|at| sender_ids.get(at).copied());
    world.expect(Expectation::Transmission(ExpectedTransmission::new(
        TransmissionLabel {
            from,
            to,
            sender_exchange,
            reader_exchange,
            route: RouteExpectation::Channel {
                resource: Locator::File {
                    host: None,
                    path: written.path.clone(),
                },
            },
            carrier: CarrierKind::ToolResult,
            content: ExpectedContent { text, at },
            needs: plan.variant.needs(form),
            tier: Tier::Construction,
            source: SourceRef::new(
                b_file,
                format!(
                    "/rows/{}/messages/{}/splice/{}/from/{}/rows/{}/messages/{}",
                    reader.row,
                    plan.insert_at + 1,
                    plan.number,
                    a_file,
                    sender.row,
                    written.message
                ),
            ),
        },
    )?));
    Ok(world.finish()?)
}

/// Splices as a stream of worlds.
pub struct SpliceSource {
    root: PathBuf,
    shards: Vec<Shard>,
    splices: usize,
    seed: u64,
    pool_size: usize,
}

impl SpliceSource {
    /// Shards from `root` that `selection` picks; `splices` worlds.
    pub fn open(
        root: &Path,
        selection: &Selection,
        splices: usize,
        seed: u64,
    ) -> Result<Self, SpliceError> {
        Ok(Self {
            root: root.to_path_buf(),
            shards: open_swe::files::discover(root, selection)?,
            splices,
            seed,
            pool_size: (splices * 3).max(16),
        })
    }

    /// Reads the pool: `pool_size` trajectories round-robin over the shards.
    pub fn pool(&self) -> Result<Vec<Pooled>, SpliceError> {
        let per_shard = self.pool_size.div_ceil(self.shards.len().max(1));
        let mut rows = RoundRobin::new(&self.root, self.shards.clone(), Some(per_shard));
        let mut pool = Vec::with_capacity(self.pool_size);
        while let Some(row) = rows.next_row() {
            let (shard, row, record) = row?;
            pool.push(Pooled::new(shard, row, record));
        }
        Ok(pool)
    }
}

impl TraceSource for SpliceSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let pool = self.pool();
        let (pool, failure) = match pool {
            Ok(pool) => (pool, None),
            Err(error) => (Vec::new(), Some(error)),
        };
        let seed = self.seed;
        let splices = if failure.is_some() { 0 } else { self.splices };
        failure
            .into_iter()
            .map(|error| Err(SourceError::from(error)))
            .chain((0..splices).map(move |number| {
                plan(&pool, number, seed)
                    .and_then(|plan| world(&pool, &plan))
                    .map_err(SourceError::from)
            }))
    }
}
