//! [`SidecarLayoutFitter`]: the spec's `LayoutFitter` over the sidecar's
//! `/v1/layout/fit`, and [`SidecarLayoutFitter::transform`] over
//! `/v1/layout/transform`.
//!
//! The fitter takes embeddings of any one model (UMAP needs only that they
//! share a space); embeddings of mixed models are a caller bug, reported as
//! `LayoutError::Backend` without a request. Fewer points than `neighbors +
//! 1` is `FitFailure::TooFewPoints` without a request. The sidecar's deterministic refusals (`too_few_points`,
//! `non_finite_layout`) and a non-finite coordinate in a reply are
//! `LayoutError::Failed`; everything else that goes wrong is
//! `LayoutError::Backend` (`analysis.layout.backend-failure-not-recorded`).

use crosstalk_spec::aggregates::projection::{FitFailure, ProjectionParams};
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
use crosstalk_spec::interfaces::l6_analysis::{LayoutError, LayoutFitter};

use super::wire::{
    ErrorBody, LAYOUT_FIT, LAYOUT_TRANSFORM, LayoutFitRequest, LayoutReply, LayoutTransformRequest,
};
use super::{ContractViolation, Outcome, SidecarClient, SidecarError};
use crate::remote::matrix::{Matrix, MatrixError};

/// The spec's `LayoutFitter` over the topics sidecar.
#[derive(Debug, Clone)]
pub struct SidecarLayoutFitter {
    client: SidecarClient,
}

/// Why [`SidecarLayoutFitter::transform`] returned no coordinates.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransformError {
    #[error("the base layout cannot be fitted: {0:?}")]
    Layout(LayoutError),
    /// Counting the base first, then the points.
    #[error("embedding {index} is from model {got:?}, not {expected:?}")]
    WrongModel {
        index: usize,
        expected: EmbeddingModel,
        got: EmbeddingModel,
    },
}

impl SidecarLayoutFitter {
    pub fn new(client: SidecarClient) -> Self {
        Self { client }
    }

    /// Place `points` onto the layout `fit(base, params)` returns, without
    /// moving it: one coordinate pair per point, in order.
    pub async fn transform(
        &self,
        base: &[Embedding],
        params: ProjectionParams,
        points: &[Embedding],
    ) -> Result<Vec<[f32; 2]>, TransformError> {
        let model = enough(base, params)
            .map_err(|failure| TransformError::Layout(LayoutError::Failed(failure)))?;
        let encode = |embeddings: &[Embedding], offset: usize| {
            encode(model, embeddings).map_err(|error| match error {
                Encoding::WrongModel { index, got } => TransformError::WrongModel {
                    index: offset + index,
                    expected: model.clone(),
                    got,
                },
                Encoding::Matrix(error) => TransformError::Layout(backend(&SidecarError::Encode {
                    route: LAYOUT_TRANSFORM,
                    reason: error.to_string(),
                })),
            })
        };
        let request = LayoutTransformRequest {
            base: encode(base, 0)?,
            params,
            points: encode(points, base.len())?,
        };
        let rows = u64::try_from(base.len() + points.len()).unwrap_or(u64::MAX);
        coordinates(
            LAYOUT_TRANSFORM,
            self.client
                .post::<_, LayoutReply>(LAYOUT_TRANSFORM, &request, rows)
                .await,
            points.len(),
        )
        .map_err(TransformError::Layout)
    }
}

impl LayoutFitter for SidecarLayoutFitter {
    async fn fit(
        &self,
        embeddings: &[Embedding],
        params: ProjectionParams,
    ) -> Result<Vec<[f32; 2]>, LayoutError> {
        let model = enough(embeddings, params).map_err(LayoutError::Failed)?;
        let matrix = encode(model, embeddings).map_err(|error| {
            backend(&SidecarError::Encode {
                route: LAYOUT_FIT,
                reason: error.to_string(),
            })
        })?;
        let request = LayoutFitRequest {
            embeddings: matrix,
            params,
        };
        let rows = u64::try_from(embeddings.len()).unwrap_or(u64::MAX);
        coordinates(
            LAYOUT_FIT,
            self.client
                .post::<_, LayoutReply>(LAYOUT_FIT, &request, rows)
                .await,
            embeddings.len(),
        )
    }
}

/// Why embeddings could not be encoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum Encoding {
    #[error("embedding {index} is from model {got:?}")]
    WrongModel { index: usize, got: EmbeddingModel },
    #[error(transparent)]
    Matrix(MatrixError),
}

/// `embeddings`, every one from `model`.
fn encode(model: &EmbeddingModel, embeddings: &[Embedding]) -> Result<Matrix, Encoding> {
    if let Some((index, embedding)) = embeddings
        .iter()
        .enumerate()
        .find(|(_, embedding)| embedding.model() != model)
    {
        return Err(Encoding::WrongModel {
            index,
            got: embedding.model().clone(),
        });
    }
    Matrix::encode(model.dimension, embeddings.iter().map(Embedding::values))
        .map_err(Encoding::Matrix)
}

/// UMAP needs more points than neighbours. Returns the first point's
/// model, which every other must share.
fn enough(
    embeddings: &[Embedding],
    params: ProjectionParams,
) -> Result<&EmbeddingModel, FitFailure> {
    let needed = u32::from(params.neighbors()) + 1;
    let got = u64::try_from(embeddings.len()).unwrap_or(u64::MAX);
    match embeddings.first() {
        Some(first) if got >= u64::from(needed) => Ok(first.model()),
        _ => Err(FitFailure::TooFewPoints { needed, got }),
    }
}

fn backend(error: &SidecarError) -> LayoutError {
    LayoutError::Backend {
        reason: error.to_string(),
    }
}

/// The coordinates of a layout route's outcome, `expected` rows of two
/// finite values.
fn coordinates(
    route: &'static str,
    outcome: Result<Outcome<LayoutReply>, SidecarError>,
    expected: usize,
) -> Result<Vec<[f32; 2]>, LayoutError> {
    let reply = match outcome.map_err(|error| backend(&error))? {
        Outcome::Reply(reply) => reply,
        Outcome::Refused(ErrorBody::TooFewPoints { needed, got }) => {
            return Err(LayoutError::Failed(FitFailure::TooFewPoints {
                needed,
                got,
            }));
        }
        Outcome::Refused(ErrorBody::NonFiniteLayout) => {
            return Err(LayoutError::Failed(FitFailure::NonFiniteLayout));
        }
        Outcome::Refused(other) => return Err(backend(&SidecarError::refused(route, other))),
    };
    let violation = |violation| backend(&SidecarError::Contract { route, violation });
    let matrix = reply.coordinates;
    let expected_rows = u64::try_from(expected).unwrap_or(u64::MAX);
    if u64::from(matrix.rows()) != expected_rows {
        return Err(violation(ContractViolation::RowCount {
            expected: expected_rows,
            got: matrix.rows(),
        }));
    }
    if matrix.columns().get() != 2 {
        return Err(violation(ContractViolation::Columns {
            got: matrix.columns().get(),
        }));
    }
    let values = match matrix.decode() {
        Ok(values) => values,
        // A NaN or infinity is UMAP's output, not a transport fault.
        Err(MatrixError::NotFinite { .. }) => {
            return Err(LayoutError::Failed(FitFailure::NonFiniteLayout));
        }
        Err(error) => return Err(violation(ContractViolation::Matrix(error))),
    };
    // Two columns: no remainder.
    Ok(values.as_chunks::<2>().0.to_vec())
}
