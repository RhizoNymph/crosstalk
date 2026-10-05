//! The extractors' errors as the spec's `ExtractError`.

use crosstalk_spec::interfaces::l5_flow::ExtractError;

use crate::extract::args::ArgError;
use crate::extract::bash::lex::LexError;

impl From<ArgError> for ExtractError {
    fn from(error: ArgError) -> Self {
        Self::Arguments {
            reason: error.to_string(),
        }
    }
}

impl From<LexError> for ExtractError {
    fn from(error: LexError) -> Self {
        Self::Parse {
            reason: error.to_string(),
        }
    }
}
