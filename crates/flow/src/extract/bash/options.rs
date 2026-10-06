//! getopt-style argument splitting for the commands the interpreter
//! knows: short options cluster (`-sSLo out`, `-qO-`), long options take
//! `=value` or the next word, `--` ends options.

use super::lex::Word;

/// Which options of a command take a value.
pub(crate) struct OptSpec {
    pub short_values: &'static str,
    pub long_values: &'static [&'static str],
}

pub(crate) const NO_VALUES: OptSpec = OptSpec {
    short_values: "",
    long_values: &[],
};

/// A command's arguments split into options (with their values) and
/// operands.
pub(crate) struct Options<'w> {
    pub options: Vec<(String, Option<Word>)>,
    pub operands: Vec<&'w Word>,
}

impl<'w> Options<'w> {
    pub fn parse(args: &'w [Word], spec: &OptSpec) -> Self {
        let mut options = Vec::new();
        let mut operands = Vec::new();
        let mut words = args.iter();
        let mut ended = false;
        while let Some(word) = words.next() {
            let text = word.text.as_str();
            if ended || text == "-" || !text.starts_with('-') {
                operands.push(word);
            } else if text == "--" {
                ended = true;
            } else if let Some(long) = text.strip_prefix("--") {
                match long.split_once('=') {
                    Some((name, value)) => options.push((
                        format!("--{name}"),
                        Some(Word {
                            text: value.to_owned(),
                            literal: word.literal,
                            home: None,
                        }),
                    )),
                    None if spec.long_values.contains(&text) => {
                        options.push((text.to_owned(), words.next().cloned()));
                    }
                    None => options.push((text.to_owned(), None)),
                }
            } else {
                let cluster = &text[1..];
                for (at, letter) in cluster.char_indices() {
                    let name = format!("-{letter}");
                    if spec.short_values.contains(letter) {
                        let rest = &cluster[at + letter.len_utf8()..];
                        let value = if rest.is_empty() {
                            words.next().cloned()
                        } else {
                            Some(Word {
                                text: rest.to_owned(),
                                literal: word.literal,
                                home: None,
                            })
                        };
                        options.push((name, value));
                        break;
                    }
                    options.push((name, None));
                }
            }
        }
        Self { options, operands }
    }

    pub fn has(&self, names: &[&str]) -> bool {
        self.options
            .iter()
            .any(|(name, _)| names.contains(&name.as_str()))
    }

    /// The values of the options named `names`, in order.
    pub fn values(&self, names: &[&str]) -> Vec<&Word> {
        self.options
            .iter()
            .filter(|(name, _)| names.contains(&name.as_str()))
            .filter_map(|(_, value)| value.as_ref())
            .collect()
    }

    /// Every option with its value, in order.
    pub fn with_values(&self) -> impl Iterator<Item = (&str, &Word)> {
        self.options
            .iter()
            .filter_map(|(name, value)| Some((name.as_str(), value.as_ref()?)))
    }

    pub fn last(&self, names: &[&str]) -> Option<&Word> {
        self.values(names).pop()
    }
}
