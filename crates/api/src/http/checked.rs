//! Request values the surface builds through a checked constructor before
//! reading anything: an id batch, a transmission selection, an excerpt
//! window. Each is read in its raw shape (the same JSON) and then built,
//! so the constructor's refusal reaches the client as the `InvalidInput`
//! the spec names (`QueryError::from`: `TooManyIds`, `EmptySelection`,
//! `ExcerptContextTooLong`, all 422), not as an undecodable request (400).
//!
//! The raw shapes are private adapters: the spec's types decode through
//! the same constructors and would report a refusal as a `DecodeError`.
//! JSON that is not even the raw shape is still `MalformedRequest`.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::http::bodies::TransmissionsBody;
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::paging::{PageRequest, TransmissionList};
use crosstalk_spec::wire::WireRequest;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::input::Input;

/// An id batch before its bound: the JSON array of ids.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct RawIds<T>(Vec<T>);

/// Every member is an id the client chooses.
impl<T: Serialize + DeserializeOwned> WireRequest for RawIds<T> {}

/// The body as an [`IdBatch`]: more than `IdBatch::MAX` distinct ids is
/// `InvalidInput(TooManyIds)`.
pub(super) fn id_batch<T>(input: &Input) -> Result<IdBatch<T>, QueryError>
where
    T: Serialize + DeserializeOwned + Ord + Copy,
{
    let RawIds(ids) = input.body::<RawIds<T>>()?;
    Ok(IdBatch::new(ids)?)
}

/// `ExcerptWindow`'s JSON: `{"context": 256}`.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawExcerptWindow {
    context: u16,
}

impl WireRequest for RawExcerptWindow {}

/// The query parameter `name` as an [`ExcerptWindow`]: a context over
/// `ExcerptWindow::MAX_CONTEXT` is `InvalidInput(ExcerptContextTooLong)`.
pub(super) fn excerpt_window(input: &Input, name: &str) -> Result<ExcerptWindow, QueryError> {
    let raw = input.query::<RawExcerptWindow>(name)?;
    Ok(ExcerptWindow::new(raw.context)?)
}

/// [`TransmissionsBody`]'s JSON, its selection a plain array.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawTransmissionsBody {
    selection: Vec<TransmissionId>,
    version: TopicVersionSelector,
    page: PageRequest<TransmissionList>,
}

impl WireRequest for RawTransmissionsBody {}

/// The body as a [`TransmissionsBody`]: no ids is
/// `InvalidInput(EmptySelection)`, more than `TransmissionSelection::MAX`
/// distinct ids `InvalidInput(TooManyIds)`.
pub(super) fn transmissions_body(input: &Input) -> Result<TransmissionsBody, QueryError> {
    let RawTransmissionsBody {
        selection,
        version,
        page,
    } = input.body()?;
    // A struct literal, so a field added to the spec's body does not
    // compile here until the adapter has it too.
    Ok(TransmissionsBody {
        selection: TransmissionSelection::new(selection)?,
        version,
        page,
    })
}
