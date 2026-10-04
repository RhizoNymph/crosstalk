//! Generators for property tests: JSON values and their spellings
//! ([`json`]), Anthropic content blocks, turns and responses ([`anthropic`]),
//! and the rendering choices ([`Style`]) that vary between two deliveries of
//! one value.

pub mod anthropic;
pub mod body;
pub mod json;

/// Rendering choices (whitespace, member order, escapes, delta splits,
/// pings), drawn from a seed so a failing case replays.
#[derive(Debug, Clone)]
pub struct Style {
    state: u64,
    /// No line breaks in whitespace: for an event's `data:` line.
    single_line: bool,
}

impl Style {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed,
            single_line: false,
        }
    }

    /// A style that draws its own choices from this one's next value and
    /// never breaks a line.
    pub fn single_line(&mut self) -> Self {
        Self {
            state: self.next(),
            single_line: true,
        }
    }

    /// SplitMix64.
    pub fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..bound` (0 when `bound` is 0).
    pub fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        let bound = u64::try_from(bound).unwrap_or(u64::MAX);
        usize::try_from(self.next() % bound).unwrap_or(0)
    }

    /// True one time in `odds`.
    pub fn chance(&mut self, odds: usize) -> bool {
        self.below(odds) == 0
    }

    /// Insignificant whitespace, often none.
    pub fn space(&mut self) -> &'static str {
        match self.below(6) {
            0 => " ",
            1 if self.single_line => "  ",
            1 => "\n  ",
            2 => "\t",
            _ => "",
        }
    }

    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for at in (1..items.len()).rev() {
            let other = self.below(at + 1);
            items.swap(at, other);
        }
    }

    /// `text` cut into pieces at character boundaries (one piece, possibly
    /// empty, when it is empty).
    pub fn split(&mut self, text: &str) -> Vec<String> {
        let chars: Vec<char> = text.chars().collect();
        if chars.is_empty() {
            return vec![String::new()];
        }
        let mut pieces = Vec::new();
        let mut at = 0;
        while at < chars.len() {
            let take = 1 + self.below(chars.len() - at);
            let take = if self.chance(2) { take.min(4) } else { take };
            pieces.push(chars[at..at + take].iter().collect());
            at += take;
        }
        pieces
    }
}
