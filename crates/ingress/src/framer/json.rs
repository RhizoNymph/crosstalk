//! The single-body framer: a 2xx response that is one JSON document.
//!
//! It tracks only where the document begins and ends (bracket depth, outside
//! strings), not whether its contents are valid JSON: that is the
//! normalizer's job. [`FrameEvent::FirstContent`] is reported at the
//! document's first byte, [`FrameEvent::Finished`] at the bracket that closes
//! it, so a body that arrives whole reports both in one push. A body that
//! does not start with `{` or `[` is a [`FrameError::MalformedFrame`] at that
//! byte. Bytes after the document are ignored.

use crosstalk_spec::interfaces::l0_ingress::{FrameError, FrameEvent};

use super::{Emitted, Progress};

#[derive(Debug, Default)]
pub struct JsonFramer {
    offset: u64,
    depth: u64,
    in_string: bool,
    escaped: bool,
    progress: Progress,
}

impl JsonFramer {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn scan(&mut self, chunk: &[u8], emitted: &mut Emitted) -> Result<(), FrameError> {
        for &byte in chunk {
            let at = self.offset;
            self.offset += 1;
            match self.progress {
                Progress::Finished => return Ok(()),
                Progress::NotStarted => match byte {
                    b' ' | b'\t' | b'\n' | b'\r' => {}
                    b'{' | b'[' => {
                        self.progress = Progress::Started;
                        self.depth = 1;
                        emitted.push(FrameEvent::FirstContent);
                    }
                    _ => return Err(FrameError::MalformedFrame { offset: at }),
                },
                Progress::Started if self.in_string => {
                    if self.escaped {
                        self.escaped = false;
                    } else if byte == b'\\' {
                        self.escaped = true;
                    } else if byte == b'"' {
                        self.in_string = false;
                    }
                }
                Progress::Started => match byte {
                    b'"' => self.in_string = true,
                    b'{' | b'[' => self.depth += 1,
                    b'}' | b']' => {
                        self.depth -= 1;
                        if self.depth == 0 {
                            self.progress = Progress::Finished;
                            emitted.push(FrameEvent::Finished);
                            return Ok(());
                        }
                    }
                    _ => {}
                },
            }
        }
        Ok(())
    }
}
