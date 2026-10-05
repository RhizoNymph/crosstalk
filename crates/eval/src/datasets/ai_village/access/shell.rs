//! A small POSIX-shell word splitter: enough to read the commands agents
//! type into a bash tool, not a shell.
//!
//! The script is cut into simple commands at unquoted `;`, `&`, `|`, `&&`,
//! `||`, newlines and parentheses. Words are unquoted the way the shell
//! would (single quotes literal; double quotes honour `\"`, `\\`, `\$`,
//! `` \` `` and a backslash-newline; an unquoted backslash escapes the next
//! character). `$(…)` and backticks stay inside their word, unsplit.
//! Redirections and their targets are dropped. A `#` starting a word
//! comments out the rest of the line. Here-document bodies after an
//! unquoted `<<WORD` are skipped (their text is data, not commands).

/// One simple command's words, unquoted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleCommand {
    pub words: Vec<String>,
}

#[derive(Default)]
struct Splitter {
    commands: Vec<SimpleCommand>,
    words: Vec<String>,
    word: String,
    /// Whether the current word has started (an empty quoted word counts).
    started: bool,
    /// The next finished word is a redirection target: drop it.
    drop_next: bool,
    /// Here-document delimiters waiting for the end of the line.
    heredocs: Vec<String>,
}

impl Splitter {
    fn end_word(&mut self) {
        if self.started {
            let word = std::mem::take(&mut self.word);
            if self.drop_next {
                self.drop_next = false;
            } else {
                self.words.push(word);
            }
        }
        self.word.clear();
        self.started = false;
    }

    fn end_command(&mut self) {
        self.end_word();
        self.drop_next = false;
        if !self.words.is_empty() {
            self.commands.push(SimpleCommand {
                words: std::mem::take(&mut self.words),
            });
        }
    }
}

/// Copies a balanced `$(…)` (the `$(` already consumed) into `word`.
fn copy_substitution(chars: &[char], mut at: usize, word: &mut String) -> usize {
    let mut depth = 1usize;
    let mut quote: Option<char> = None;
    while at < chars.len() {
        let c = chars[at];
        word.push(c);
        at += 1;
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"') => quote = Some(c),
            (None, '(') => depth += 1,
            (None, ')') => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
    }
    at
}

/// The script's simple commands.
pub fn commands(script: &str) -> Vec<SimpleCommand> {
    let chars: Vec<char> = script.chars().collect();
    let mut s = Splitter::default();
    let mut at = 0usize;
    while at < chars.len() {
        let c = chars[at];
        match c {
            '\n' => {
                s.end_command();
                at += 1;
                if !s.heredocs.is_empty() {
                    at = skip_heredocs(&chars, at, std::mem::take(&mut s.heredocs));
                }
            }
            ' ' | '\t' | '\r' => {
                s.end_word();
                at += 1;
            }
            ';' | '&' | '|' | '(' | ')' => {
                s.end_command();
                at += 1;
            }
            '#' if !s.started => {
                while at < chars.len() && chars[at] != '\n' {
                    at += 1;
                }
            }
            '\'' => {
                s.started = true;
                at += 1;
                while at < chars.len() && chars[at] != '\'' {
                    s.word.push(chars[at]);
                    at += 1;
                }
                at += 1;
            }
            '"' => {
                s.started = true;
                at += 1;
                while at < chars.len() && chars[at] != '"' {
                    match chars[at] {
                        '\\' if at + 1 < chars.len()
                            && matches!(chars[at + 1], '"' | '\\' | '$' | '`') =>
                        {
                            s.word.push(chars[at + 1]);
                            at += 2;
                        }
                        '\\' if at + 1 < chars.len() && chars[at + 1] == '\n' => at += 2,
                        '$' if at + 1 < chars.len() && chars[at + 1] == '(' => {
                            s.word.push_str("$(");
                            at = copy_substitution(&chars, at + 2, &mut s.word);
                        }
                        other => {
                            s.word.push(other);
                            at += 1;
                        }
                    }
                }
                at += 1;
            }
            '\\' => {
                if at + 1 < chars.len() {
                    if chars[at + 1] != '\n' {
                        s.started = true;
                        s.word.push(chars[at + 1]);
                    }
                    at += 2;
                } else {
                    at += 1;
                }
            }
            '$' if at + 1 < chars.len() && chars[at + 1] == '(' => {
                s.started = true;
                s.word.push_str("$(");
                at = copy_substitution(&chars, at + 2, &mut s.word);
            }
            '`' => {
                s.started = true;
                s.word.push('`');
                at += 1;
                while at < chars.len() && chars[at] != '`' {
                    s.word.push(chars[at]);
                    at += 1;
                }
                s.word.push('`');
                at += 1;
            }
            '<' if at + 1 < chars.len() && chars[at + 1] == '<' => {
                // A here-document (`<<WORD`, `<<-WORD`, `<<'WORD'`), or a
                // here-string (`<<<`), whose word is data.
                s.end_word();
                at += 2;
                if at < chars.len() && chars[at] == '<' {
                    s.drop_next = true;
                    at += 1;
                    continue;
                }
                if at < chars.len() && chars[at] == '-' {
                    at += 1;
                }
                while at < chars.len() && matches!(chars[at], ' ' | '\t') {
                    at += 1;
                }
                let mut delimiter = String::new();
                while at < chars.len() && !matches!(chars[at], ' ' | '\t' | '\n' | ';' | '|' | '&')
                {
                    if !matches!(chars[at], '\'' | '"' | '\\') {
                        delimiter.push(chars[at]);
                    }
                    at += 1;
                }
                if !delimiter.is_empty() {
                    s.heredocs.push(delimiter);
                }
            }
            '>' | '<' => {
                // A redirection: drop a pending fd number and the target.
                if s.started && s.word.chars().all(|c| c.is_ascii_digit()) {
                    s.word.clear();
                    s.started = false;
                } else {
                    s.end_word();
                }
                at += 1;
                while at < chars.len() && matches!(chars[at], '>' | '&' | '|') {
                    at += 1;
                }
                if at < chars.len() && chars[at].is_ascii_digit() {
                    // `2>&1`: the target is a descriptor.
                    while at < chars.len() && chars[at].is_ascii_digit() {
                        at += 1;
                    }
                } else {
                    s.drop_next = true;
                }
            }
            other => {
                s.started = true;
                s.word.push(other);
                at += 1;
            }
        }
    }
    s.end_command();
    s.commands
}

/// Skips here-document bodies: for each delimiter, lines up to and
/// including the one that is exactly the delimiter (leading tabs allowed).
fn skip_heredocs(chars: &[char], mut at: usize, delimiters: Vec<String>) -> usize {
    for delimiter in delimiters {
        loop {
            if at >= chars.len() {
                return at;
            }
            let end = chars[at..]
                .iter()
                .position(|c| *c == '\n')
                .map_or(chars.len(), |p| at + p);
            let line: String = chars[at..end].iter().collect();
            at = (end + 1).min(chars.len().max(end));
            if line.trim_start_matches('\t').trim_end() == delimiter {
                break;
            }
        }
    }
    at
}

/// The body of a `$(cat <<'EOF' … EOF)` word, which agents use to pass a
/// multi-line argument; `None` for any other word.
pub fn heredoc_argument(word: &str) -> Option<String> {
    let rest = word.trim().strip_prefix("$(cat")?;
    let rest = rest.trim_start().strip_prefix("<<")?;
    let (header, body) = rest.split_once('\n')?;
    let delimiter: String = header
        .trim()
        .trim_start_matches('-')
        .chars()
        .filter(|c| !matches!(c, '\'' | '"'))
        .collect();
    let mut lines = Vec::new();
    for line in body.lines() {
        if line.trim() == delimiter {
            return Some(lines.join("\n"));
        }
        lines.push(line);
    }
    None
}
