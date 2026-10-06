//! A conservative lexer for the shell commands agents run: simple commands,
//! their words and redirections, split on `;`, `&`, `&&`, `||`, `|`,
//! newlines and parentheses.
//!
//! Quoting is resolved (single and double quotes, backslash escapes, line
//! continuations) so a word's text is what the command sees. A word whose
//! value the shell would compute (a parameter, command or arithmetic
//! substitution, a glob, a leading `~`) is kept but marked not literal: it
//! names no resource the gateway can know, except a path under the home
//! directory (`~`, `~/x`, `$HOME/x`, `${HOME}/x`, `"$HOME"/x` with nothing
//! else expanded), which keeps its rest ([`Word::home`]). Each command
//! records how it joins the one before ([`Join`]: `;`, `&&`, `||`, `|`) and
//! how many subshell parentheses it is in. Here-document bodies are
//! skipped. Anything else of the shell grammar (functions, `if`, `for`) is
//! read as words, which names no resource: the extractor misses accesses
//! rather than inventing them.

/// One word of a command, with quotes removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Word {
    pub text: String,
    /// False when the shell would expand any part of it.
    pub literal: bool,
    /// For a path under the home directory with nothing else expanded
    /// (`~`, `~/x`, `$HOME/x`, `${HOME}/x`), what follows the home: `""`
    /// or `/x`. Such a word is not literal.
    pub home: Option<String>,
}

impl Word {
    pub fn literal(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            literal: true,
            home: None,
        }
    }

    /// The rest of a path under the home directory: `""` for the home
    /// itself, `/x` for `~/x`.
    pub fn home(&self) -> Option<&str> {
        self.home.as_deref()
    }

    /// The text, when the shell passes it through unchanged.
    pub fn as_literal(&self) -> Option<&str> {
        self.literal.then_some(self.text.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedirectOp {
    /// `>` or `>|`.
    Out,
    /// `>>`.
    Append,
    /// `&>` or `&>>`: stdout and stderr.
    Both,
    /// `>&`: a descriptor copy, or `&>` when the target is not a number.
    DupOut,
    /// `<`.
    In,
    /// `<&`.
    DupIn,
    /// `<>`.
    ReadWrite,
    /// `<<` or `<<-`: the target is the delimiter.
    HereDoc,
    /// `<<<`.
    HereString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redirect {
    pub fd: Option<u32>,
    pub op: RedirectOp,
    pub target: Word,
}

/// How a command follows the one before it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Join {
    /// The first command, or after `;`, `&`, a newline or a parenthesis:
    /// it runs whatever the one before did.
    #[default]
    Sequence,
    /// After `&&`: it runs when the list before it succeeded.
    And,
    /// After `||`: it runs when the list before it failed.
    Or,
    /// After `|` or `|&`: the next command of a pipeline, which runs when
    /// the pipeline does.
    Pipe,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Command {
    pub words: Vec<Word>,
    pub redirects: Vec<Redirect>,
    /// Its stdout goes into a pipe.
    pub piped: bool,
    /// How it follows the command before it.
    pub join: Join,
    /// How many subshell parentheses enclose it: a `cd` inside them does
    /// not move the shell outside them.
    pub depth: u16,
}

/// A command line's simple commands, in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Script {
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LexError {
    #[error("unterminated quote")]
    UnterminatedQuote,
    #[error("unterminated command substitution or parameter expansion")]
    UnterminatedSubstitution,
    #[error("a redirection has no target")]
    MissingTarget,
}

impl Script {
    /// The single command an argv array runs, every word literal.
    pub fn from_argv(argv: Vec<String>) -> Self {
        let words: Vec<Word> = argv.into_iter().map(Word::literal).collect();
        if words.is_empty() {
            return Self::default();
        }
        Self {
            commands: vec![Command {
                words,
                ..Command::default()
            }],
        }
    }

    pub fn lex(text: &str) -> Result<Self, LexError> {
        Lexer::new(text).run()
    }
}

#[derive(Debug, Default)]
struct WordBuf {
    text: String,
    literal: bool,
    quoted: bool,
    /// The length of a leading home prefix (`~`, `$HOME`, `${HOME}`).
    home_prefix: Option<usize>,
}

struct Pending {
    fd: Option<u32>,
    op: RedirectOp,
    strip_tabs: bool,
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
    commands: Vec<Command>,
    current: Command,
    word: Option<WordBuf>,
    pending: Option<Pending>,
    heredocs: Vec<(String, bool)>,
    /// How the next command joins the one before.
    join: Join,
    depth: u16,
}

impl Lexer {
    fn new(text: &str) -> Self {
        Self {
            chars: text.chars().collect(),
            pos: 0,
            commands: Vec::new(),
            current: Command::default(),
            word: None,
            pending: None,
            heredocs: Vec::new(),
            join: Join::Sequence,
            depth: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn word(&mut self) -> &mut WordBuf {
        self.word.get_or_insert_with(|| WordBuf {
            literal: true,
            ..WordBuf::default()
        })
    }

    fn push(&mut self, c: char) {
        self.word().text.push(c);
    }

    fn mark_expanded(&mut self) {
        self.word().literal = false;
    }

    fn run(mut self) -> Result<Script, LexError> {
        while let Some(c) = self.peek() {
            match c {
                ' ' | '\t' | '\r' => {
                    self.finish_word();
                    self.pos += 1;
                }
                '\n' => {
                    self.separate(false, Join::Sequence)?;
                    self.pos += 1;
                    self.skip_heredocs();
                }
                '#' if self.word.is_none() => {
                    while self.peek().is_some_and(|c| c != '\n') {
                        self.pos += 1;
                    }
                }
                ';' => {
                    self.separate(false, Join::Sequence)?;
                    self.pos += 1;
                }
                '(' => {
                    // A group opens: the command before it ends, and the
                    // first command inside joins as the group does.
                    self.finish_command(false)?;
                    self.depth = self.depth.saturating_add(1);
                    self.pos += 1;
                }
                ')' => {
                    self.separate(false, Join::Sequence)?;
                    self.depth = self.depth.saturating_sub(1);
                    self.pos += 1;
                }
                '&' => match self.peek_at(1) {
                    Some('>') => {
                        self.finish_word();
                        self.pos += 2;
                        if self.peek() == Some('>') {
                            self.pos += 1;
                        }
                        self.start_redirect(None, RedirectOp::Both, false)?;
                    }
                    Some('&') => {
                        self.separate(false, Join::And)?;
                        self.pos += 2;
                    }
                    _ => {
                        self.separate(false, Join::Sequence)?;
                        self.pos += 1;
                    }
                },
                '|' => match self.peek_at(1) {
                    Some('|') => {
                        self.separate(false, Join::Or)?;
                        self.pos += 2;
                    }
                    Some('&') => {
                        self.separate(true, Join::Pipe)?;
                        self.pos += 2;
                    }
                    _ => {
                        self.separate(true, Join::Pipe)?;
                        self.pos += 1;
                    }
                },
                '<' | '>' => self.redirect()?,
                '\'' => {
                    self.pos += 1;
                    self.single_quoted()?;
                }
                '"' => {
                    self.pos += 1;
                    self.double_quoted()?;
                }
                '\\' => {
                    self.pos += 1;
                    match self.peek() {
                        Some('\n') => self.pos += 1,
                        Some(next) => {
                            self.push(next);
                            self.word().quoted = true;
                            self.pos += 1;
                        }
                        None => self.push('\\'),
                    }
                }
                '$' => self.dollar()?,
                '`' => self.backtick()?,
                '*' | '?' | '[' => {
                    self.push(c);
                    self.mark_expanded();
                    self.pos += 1;
                }
                '~' if self.word.is_none() => {
                    self.push(c);
                    let ends = self
                        .peek_at(1)
                        .is_none_or(|next| next == '/' || is_word_end(next));
                    if ends {
                        self.word().home_prefix = Some(1);
                    } else {
                        // `~user`: another user's home.
                        self.mark_expanded();
                    }
                    self.pos += 1;
                }
                _ => {
                    self.push(c);
                    self.pos += 1;
                }
            }
        }
        self.finish_command(false)?;
        Ok(Script {
            commands: self.commands,
        })
    }

    fn finish_word(&mut self) {
        let Some(word) = self.word.take() else {
            return;
        };
        let home = word
            .home_prefix
            .filter(|_| word.literal)
            .and_then(|len| word.text.get(len..))
            .filter(|rest| rest.is_empty() || rest.starts_with('/'))
            .map(str::to_owned);
        let word = Word {
            literal: word.literal && word.home_prefix.is_none(),
            text: word.text,
            home,
        };
        match self.pending.take() {
            Some(pending) => {
                if pending.op == RedirectOp::HereDoc {
                    self.heredocs.push((word.text.clone(), pending.strip_tabs));
                }
                self.current.redirects.push(Redirect {
                    fd: pending.fd,
                    op: pending.op,
                    target: word,
                });
            }
            None => self.current.words.push(word),
        }
    }

    /// End the current command at a separator; the next command joins
    /// as `join` says, unless no command ended here (a separator after a
    /// parenthesis keeps the group's join, `a && (b)`).
    fn separate(&mut self, piped: bool, join: Join) -> Result<(), LexError> {
        let ended = self.finish_command(piped)?;
        if ended || join != Join::Sequence {
            self.join = join;
        }
        Ok(())
    }

    /// End the current command; whether there was one.
    fn finish_command(&mut self, piped: bool) -> Result<bool, LexError> {
        self.finish_word();
        if self.pending.is_some() {
            return Err(LexError::MissingTarget);
        }
        let mut command = std::mem::take(&mut self.current);
        if command.words.is_empty() && command.redirects.is_empty() {
            return Ok(false);
        }
        command.piped = piped;
        command.join = std::mem::take(&mut self.join);
        command.depth = self.depth;
        self.commands.push(command);
        Ok(true)
    }

    fn start_redirect(
        &mut self,
        fd: Option<u32>,
        op: RedirectOp,
        strip_tabs: bool,
    ) -> Result<(), LexError> {
        if self.pending.is_some() {
            return Err(LexError::MissingTarget);
        }
        self.pending = Some(Pending { fd, op, strip_tabs });
        Ok(())
    }

    /// At `<` or `>`. A word of unquoted digits right before it is the
    /// descriptor.
    fn redirect(&mut self) -> Result<(), LexError> {
        let fd = match &self.word {
            Some(word)
                if !word.quoted
                    && word.literal
                    && !word.text.is_empty()
                    && word.text.bytes().all(|b| b.is_ascii_digit()) =>
            {
                let fd = word.text.parse().ok();
                self.word = None;
                fd
            }
            _ => {
                self.finish_word();
                None
            }
        };
        let rest: String = self.chars[self.pos..].iter().take(3).collect();
        let (op, len, strip_tabs) = if rest.starts_with(">>") {
            (RedirectOp::Append, 2, false)
        } else if rest.starts_with(">|") {
            (RedirectOp::Out, 2, false)
        } else if rest.starts_with(">&") {
            (RedirectOp::DupOut, 2, false)
        } else if rest.starts_with('>') {
            (RedirectOp::Out, 1, false)
        } else if rest.starts_with("<<<") {
            (RedirectOp::HereString, 3, false)
        } else if rest.starts_with("<<-") {
            (RedirectOp::HereDoc, 3, true)
        } else if rest.starts_with("<<") {
            (RedirectOp::HereDoc, 2, false)
        } else if rest.starts_with("<&") {
            (RedirectOp::DupIn, 2, false)
        } else if rest.starts_with("<>") {
            (RedirectOp::ReadWrite, 2, false)
        } else {
            (RedirectOp::In, 1, false)
        };
        self.pos += len;
        self.start_redirect(fd, op, strip_tabs)
    }

    fn single_quoted(&mut self) -> Result<(), LexError> {
        let word = self.word();
        word.quoted = true;
        loop {
            match self.chars.get(self.pos).copied() {
                None => return Err(LexError::UnterminatedQuote),
                Some('\'') => {
                    self.pos += 1;
                    return Ok(());
                }
                Some(c) => {
                    self.push(c);
                    self.pos += 1;
                }
            }
        }
    }

    fn double_quoted(&mut self) -> Result<(), LexError> {
        self.word().quoted = true;
        loop {
            match self.peek() {
                None => return Err(LexError::UnterminatedQuote),
                Some('"') => {
                    self.pos += 1;
                    return Ok(());
                }
                Some('\\') => {
                    self.pos += 1;
                    match self.peek() {
                        None => return Err(LexError::UnterminatedQuote),
                        Some('\n') => self.pos += 1,
                        Some(next @ ('$' | '`' | '"' | '\\')) => {
                            self.push(next);
                            self.pos += 1;
                        }
                        Some(next) => {
                            self.push('\\');
                            self.push(next);
                            self.pos += 1;
                        }
                    }
                }
                Some('$') => self.dollar()?,
                Some('`') => self.backtick()?,
                Some(c) => {
                    self.push(c);
                    self.pos += 1;
                }
            }
        }
    }

    /// At `$`: a parameter, command or arithmetic substitution, or ANSI-C
    /// quoting. Its text is kept raw and the word is no longer literal.
    fn dollar(&mut self) -> Result<(), LexError> {
        if let Some(len) = self.home_variable() {
            let at_start = self.word.as_ref().is_none_or(|word| word.text.is_empty());
            for _ in 0..len {
                if let Some(c) = self.peek() {
                    self.push(c);
                }
                self.pos += 1;
            }
            if at_start {
                self.word().home_prefix = Some(len);
            } else {
                self.mark_expanded();
            }
            return Ok(());
        }
        self.mark_expanded();
        self.push('$');
        self.pos += 1;
        match self.peek() {
            Some('(') => {
                self.push('(');
                self.pos += 1;
                self.balanced()
            }
            Some('{') => loop {
                match self.peek() {
                    None => return Err(LexError::UnterminatedSubstitution),
                    Some(c) => {
                        self.push(c);
                        self.pos += 1;
                        if c == '}' {
                            return Ok(());
                        }
                    }
                }
            },
            Some('\'') => {
                self.pos += 1;
                self.single_quoted()
            }
            _ => Ok(()),
        }
    }

    /// At `$`: the length of `$HOME` or `${HOME}` here, when it is one.
    fn home_variable(&self) -> Option<usize> {
        let rest: String = self.chars[self.pos..].iter().take(8).collect();
        if rest.starts_with("${HOME}") {
            return Some(7);
        }
        let name_ends = rest
            .chars()
            .nth(5)
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        (rest.starts_with("$HOME") && name_ends).then_some(5)
    }

    /// Inside `$(`: up to the matching `)`, skipping quoted text.
    fn balanced(&mut self) -> Result<(), LexError> {
        let mut depth = 1usize;
        while let Some(c) = self.peek() {
            self.push(c);
            self.pos += 1;
            match c {
                '\\' => {
                    if let Some(next) = self.peek() {
                        self.push(next);
                        self.pos += 1;
                    }
                }
                '\'' | '"' => {
                    while let Some(inner) = self.peek() {
                        self.push(inner);
                        self.pos += 1;
                        if inner == '\\' && c == '"' {
                            if let Some(next) = self.peek() {
                                self.push(next);
                                self.pos += 1;
                            }
                        } else if inner == c {
                            break;
                        }
                    }
                }
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
        Err(LexError::UnterminatedSubstitution)
    }

    fn backtick(&mut self) -> Result<(), LexError> {
        self.mark_expanded();
        self.push('`');
        self.pos += 1;
        while let Some(c) = self.peek() {
            self.push(c);
            self.pos += 1;
            match c {
                '\\' => {
                    if let Some(next) = self.peek() {
                        self.push(next);
                        self.pos += 1;
                    }
                }
                '`' => return Ok(()),
                _ => {}
            }
        }
        Err(LexError::UnterminatedSubstitution)
    }

    /// After a newline: the bodies of the here-documents the line opened,
    /// each up to its delimiter line (or the end).
    fn skip_heredocs(&mut self) {
        for (delimiter, strip_tabs) in std::mem::take(&mut self.heredocs) {
            while self.pos < self.chars.len() {
                let start = self.pos;
                while self.peek().is_some_and(|c| c != '\n') {
                    self.pos += 1;
                }
                let line: String = self.chars[start..self.pos].iter().collect();
                if self.peek() == Some('\n') {
                    self.pos += 1;
                }
                let line = if strip_tabs {
                    line.trim_start_matches('\t')
                } else {
                    line.as_str()
                };
                if line == delimiter {
                    break;
                }
            }
        }
    }
}

/// A character that ends an unquoted word.
fn is_word_end(c: char) -> bool {
    c.is_whitespace() || matches!(c, ';' | '&' | '|' | '(' | ')' | '<' | '>')
}
