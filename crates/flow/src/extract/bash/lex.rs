//! A conservative lexer for the shell commands agents run: simple commands,
//! their words and redirections, split on `;`, `&`, `&&`, `||`, `|`,
//! newlines and parentheses.
//!
//! Quoting is resolved (single and double quotes, backslash escapes, line
//! continuations) so a word's text is what the command sees. A word whose
//! value the shell would compute (a parameter, command or arithmetic
//! substitution, a glob, a leading `~`) is kept but marked not literal: it
//! names no resource the gateway can know. Here-document bodies are
//! skipped. Anything else of the shell grammar (functions, `if`, `for`) is
//! read as words, which names no resource: the extractor misses accesses
//! rather than inventing them.

/// One word of a command, with quotes removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Word {
    pub text: String,
    /// False when the shell would expand any part of it.
    pub literal: bool,
}

impl Word {
    pub fn literal(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            literal: true,
        }
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Command {
    pub words: Vec<Word>,
    pub redirects: Vec<Redirect>,
    /// Its stdout goes into a pipe.
    pub piped: bool,
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
                    self.finish_command(false)?;
                    self.pos += 1;
                    self.skip_heredocs();
                }
                '#' if self.word.is_none() => {
                    while self.peek().is_some_and(|c| c != '\n') {
                        self.pos += 1;
                    }
                }
                ';' | '(' | ')' => {
                    self.finish_command(false)?;
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
                        self.finish_command(false)?;
                        self.pos += 2;
                    }
                    _ => {
                        self.finish_command(false)?;
                        self.pos += 1;
                    }
                },
                '|' => match self.peek_at(1) {
                    Some('|') => {
                        self.finish_command(false)?;
                        self.pos += 2;
                    }
                    Some('&') => {
                        self.finish_command(true)?;
                        self.pos += 2;
                    }
                    _ => {
                        self.finish_command(true)?;
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
                    self.mark_expanded();
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
        let word = Word {
            text: word.text,
            literal: word.literal,
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

    fn finish_command(&mut self, piped: bool) -> Result<(), LexError> {
        self.finish_word();
        if self.pending.is_some() {
            return Err(LexError::MissingTarget);
        }
        let mut command = std::mem::take(&mut self.current);
        if !command.words.is_empty() || !command.redirects.is_empty() {
            command.piped = piped;
            self.commands.push(command);
        }
        Ok(())
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
