//! k-gram shingles of folded text: a polynomial rolling hash over every
//! window of `k` bytes. Plain and deterministic; no winnowing.

const BASE: u64 = 0x100_0000_01b3;

/// `(hash, offset)` of every `k`-byte window of `text`, in order. Empty when
/// `text` is shorter than `k`.
pub fn shingles(text: &[u8], k: usize) -> Vec<(u64, usize)> {
    if k == 0 || text.len() < k {
        return Vec::new();
    }
    let top = (1..k).fold(1u64, |power, _| power.wrapping_mul(BASE));
    let mut hash = text[..k].iter().fold(0u64, |h, &b| {
        h.wrapping_mul(BASE).wrapping_add(u64::from(b) + 1)
    });
    let mut out = Vec::with_capacity(text.len() - k + 1);
    out.push((hash, 0));
    for start in 1..=text.len() - k {
        let leaving = u64::from(text[start - 1]) + 1;
        let entering = u64::from(text[start + k - 1]) + 1;
        hash = hash
            .wrapping_sub(leaving.wrapping_mul(top))
            .wrapping_mul(BASE)
            .wrapping_add(entering);
        out.push((hash, start));
    }
    out
}

/// The maximal runs of covered bytes when each listed offset covers
/// `[offset, offset + k)`: sorted, merged `(start, end)` ranges.
pub fn covered(offsets: &[usize], k: usize) -> Vec<(usize, usize)> {
    let mut sorted = offsets.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for offset in sorted {
        let end = offset + k;
        match runs.last_mut() {
            Some(last) if offset <= last.1 => last.1 = last.1.max(end),
            _ => runs.push((offset, end)),
        }
    }
    runs
}
