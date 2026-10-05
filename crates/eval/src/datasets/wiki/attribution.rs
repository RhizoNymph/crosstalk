//! Line provenance: which revision inserted each line of a page body.
//!
//! A page's revisions are replayed in sequence order. Each revision's body
//! is split into lines; its hunks say which new-line ranges it inserted or
//! replaced. Lines a revision did not touch keep the source they had in the
//! previous body, so the body after revision *k* carries, per line, the
//! index of the revision that first wrote that line. The converter reads
//! these to decide which earlier authors' text a later reader is looking at.

use super::schema::Revision;

/// Why a page's revisions could not be replayed into line provenance.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttributionError {
    #[error("revision {rev} hunk base range {a0}..{a1} is reversed or past the {lines}-line base")]
    BaseRange {
        rev: String,
        a0: usize,
        a1: usize,
        lines: usize,
    },
    #[error("revision {rev} hunk new range {b0}..{b1} is reversed or past the {lines}-line body")]
    NewRange {
        rev: String,
        b0: usize,
        b1: usize,
        lines: usize,
    },
    #[error("revision {rev} hunks leave an unchanged region of unequal base and new length")]
    Unbalanced { rev: String },
}

/// The lines of `body`, as [`str::split`] on `'\n'` yields them (a trailing
/// newline gives a trailing empty line), so joining them with `'\n'`
/// reproduces `body`.
pub fn lines(body: &str) -> Vec<&str> {
    body.split('\n').collect()
}

/// The byte range `[start, end)` of lines `[from, to)` within a body whose
/// lines are `lines`, in the body's own bytes (lines joined by `'\n'`).
/// `None` when the range is empty or out of bounds.
pub fn line_byte_range(lines: &[&str], from: usize, to: usize) -> Option<(u32, u32)> {
    if from >= to || to > lines.len() {
        return None;
    }
    let mut offset = 0usize;
    for line in &lines[..from] {
        offset += line.len() + 1; // +1 for the '\n' separator
    }
    let start = offset;
    let mut end = offset;
    for (at, line) in lines[from..to].iter().enumerate() {
        end += line.len();
        if at + 1 < to - from {
            end += 1;
        }
    }
    Some((u32::try_from(start).ok()?, u32::try_from(end).ok()?))
}

/// The source revision index of each line of each revision's body.
///
/// `revs` must be one page's revisions in ascending `seq` order. Output
/// `[k]` has one entry per line of `revs[k].body`, holding the index into
/// `revs` of the revision that wrote that line.
pub fn attribute(revs: &[&Revision]) -> Result<Vec<Vec<usize>>, AttributionError> {
    let mut out: Vec<Vec<usize>> = Vec::with_capacity(revs.len());
    let mut prev_source: Vec<usize> = Vec::new();
    let mut prev_lines: usize = 0;
    for (index, rev) in revs.iter().enumerate() {
        let body_lines = lines(&rev.body).len();
        let mut source = Vec::with_capacity(body_lines);
        if index == 0 || rev.hunks.is_empty() {
            // The base is unavailable or the whole body is this revision's:
            // attribute every line to it.
            source.resize(body_lines, index);
        } else {
            let mut hunks = rev.hunks.clone();
            hunks.sort_by_key(|h| (h.a0, h.b0));
            let (mut ia, mut ib) = (0usize, 0usize);
            for hunk in &hunks {
                if hunk.a0 < ia || hunk.a1 < hunk.a0 || hunk.a1 > prev_lines {
                    return Err(AttributionError::BaseRange {
                        rev: rev.rev_id.clone(),
                        a0: hunk.a0,
                        a1: hunk.a1,
                        lines: prev_lines,
                    });
                }
                if hunk.b0 < ib || hunk.b1 < hunk.b0 || hunk.b1 > body_lines {
                    return Err(AttributionError::NewRange {
                        rev: rev.rev_id.clone(),
                        b0: hunk.b0,
                        b1: hunk.b1,
                        lines: body_lines,
                    });
                }
                if hunk.a0 - ia != hunk.b0 - ib {
                    return Err(AttributionError::Unbalanced {
                        rev: rev.rev_id.clone(),
                    });
                }
                // Unchanged region before the hunk keeps its source.
                source.extend_from_slice(&prev_source[ia..hunk.a0]);
                // The hunk's new lines are this revision's.
                source.resize(source.len() + (hunk.b1 - hunk.b0), index);
                ia = hunk.a1;
                ib = hunk.b1;
            }
            if prev_lines.saturating_sub(ia) != body_lines.saturating_sub(ib) {
                return Err(AttributionError::Unbalanced {
                    rev: rev.rev_id.clone(),
                });
            }
            source.extend_from_slice(&prev_source[ia..prev_lines]);
        }
        prev_source = source.clone();
        prev_lines = body_lines;
        out.push(source);
    }
    Ok(out)
}

/// A maximal run of consecutive lines sharing one source revision index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Run {
    pub source: usize,
    /// Line range `[from, to)`.
    pub from: usize,
    pub to: usize,
}

/// The maximal runs of equal source in `source`, in line order.
pub fn runs(source: &[usize]) -> Vec<Run> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < source.len() {
        let value = source[at];
        let start = at;
        while at < source.len() && source[at] == value {
            at += 1;
        }
        out.push(Run {
            source: value,
            from: start,
            to: at,
        });
    }
    out
}
