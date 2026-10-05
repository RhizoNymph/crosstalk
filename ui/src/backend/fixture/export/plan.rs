//! The fixture's `ExportSource`: one query context (the store snapshot an
//! export reads, with agent and channel resolution and the verdicts in
//! force captured once), resolved and counted as `ExportSource::plan`
//! defines.
//!
//! The fixture reads every row while it plans: its datasets are a few
//! thousand rows, and reading them up front is what makes the count exact
//! before the first row is sent. The rows are then served from memory by
//! [`PlannedRows`], so a planned export cannot fail mid-stream.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l7_topology::EdgeQueryError;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportBasis, ExportDataset, ExportPlan, ExportPlanError, ExportRequest, ExportRow, ExportScope,
    ExportSource, RowSource, SourceFailure, settled_window,
};
use crosstalk_spec::support::{TimeWindow, Watermark};

use crate::backend::fixture::clock::BUCKET;
use crate::backend::fixture::queries::Ctx;
use crate::backend::fixture::queries::linked::{Linked, resolve};
use crate::backend::fixture::queries::projection::stored;

use super::rows;

/// The store snapshot an export is planned against.
pub struct Snapshot<'a> {
    pub ctx: Ctx<'a>,
}

/// The rows of a planned export, in key order, served one at a time.
#[derive(Debug)]
pub struct PlannedRows(std::vec::IntoIter<ExportRow>);

impl RowSource for PlannedRows {
    async fn next(&mut self) -> Result<Option<ExportRow>, SourceFailure> {
        Ok(self.0.next())
    }
}

fn aligned(window: TimeWindow) -> Result<(), ExportPlanError> {
    if BUCKET.is_boundary(window.start()) && BUCKET.is_boundary(window.end()) {
        Ok(())
    } else {
        Err(ExportPlanError::UnalignedWindow)
    }
}

/// The datasets that take an `ExportScope`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scoped {
    Transmissions,
    Edges,
    Accesses,
    Topics,
}

impl Snapshot<'_> {
    /// The version the scope's filter resolves to, as every linked view
    /// resolves it, mapped to the plan's errors.
    fn version(&self, scope: &ExportScope) -> Result<TopicModelVersion, ExportPlanError> {
        let (world, state) = (self.ctx.world, self.ctx.state);
        resolve(
            &scope.filter,
            &state.catalog,
            |version| state.retains(version),
            |topic| {
                world
                    .topics
                    .topics
                    .iter()
                    .find(|t| t.id == topic)
                    .map(|t| t.version)
            },
        )
        .map_err(|error| match error {
            EdgeQueryError::Version(version) => ExportPlanError::Version(version),
            EdgeQueryError::TopicsNotInVersion { version, topics } => {
                ExportPlanError::TopicsNotInVersion { version, topics }
            }
            EdgeQueryError::UnalignedWindow | EdgeQueryError::BucketWidthMismatch { .. } => {
                ExportPlanError::UnalignedWindow
            }
            EdgeQueryError::Store { reason } => ExportPlanError::Store { reason },
            EdgeQueryError::InvalidCursor => ExportPlanError::Store {
                reason: "version resolution reported a cursor".to_owned(),
            },
        })
    }

    /// A scoped dataset: the version resolved and pinned, the window cut at
    /// the watermark, the rows read through one linked view.
    fn scoped(
        &self,
        kind: Scoped,
        scope: &ExportScope,
        content: bool,
        watermark: Watermark,
    ) -> Result<(ExportBasis, Vec<ExportRow>), ExportPlanError> {
        if matches!(kind, Scoped::Edges | Scoped::Accesses) {
            aligned(scope.window)?;
        }
        let version = self.version(scope)?;
        let settled = settled_window(scope.window, watermark);
        let filter = scope.filter.clone().pinned(version);
        let rows = match (settled, kind) {
            // Nothing settled: every topic, nothing counted; no other row.
            (None, Scoped::Topics) => {
                rows::topics(&self.ctx, &filter.topics, version, &[], content)
            }
            (None, Scoped::Transmissions | Scoped::Edges | Scoped::Accesses) => Vec::new(),
            (Some(settled), kind) => {
                let linked = Linked::at(&self.ctx, Some(settled), &filter, version);
                match kind {
                    Scoped::Transmissions => rows::transmissions(&linked, content)?,
                    Scoped::Edges => rows::edges(&linked, content)?,
                    Scoped::Accesses => rows::accesses(&linked)?,
                    Scoped::Topics => rows::topics(
                        &self.ctx,
                        &filter.topics,
                        version,
                        &linked.admitted(),
                        content,
                    ),
                }
            }
        };
        let basis = ExportBasis::Scoped {
            topic_version: version,
            filter,
            settled,
        };
        Ok((basis, rows))
    }
}

impl ExportSource for Snapshot<'_> {
    type Rows = PlannedRows;

    async fn plan(
        &self,
        request: &ExportRequest,
        watermark: Watermark,
    ) -> Result<ExportPlan<PlannedRows>, ExportPlanError> {
        let content = request.include_content();
        let (basis, rows) = match request.dataset() {
            ExportDataset::Transmissions(scope) => {
                self.scoped(Scoped::Transmissions, scope, content, watermark)?
            }
            ExportDataset::Edges(scope) => self.scoped(Scoped::Edges, scope, content, watermark)?,
            ExportDataset::Accesses(scope) => {
                self.scoped(Scoped::Accesses, scope, content, watermark)?
            }
            ExportDataset::Topics(scope) => {
                self.scoped(Scoped::Topics, scope, content, watermark)?
            }
            ExportDataset::Verdicts(window) => {
                let settled = settled_window(*window, watermark);
                let rows = match settled {
                    Some(settled) => rows::verdicts(&self.ctx, settled)?,
                    None => Vec::new(),
                };
                (ExportBasis::Verdicts { settled }, rows)
            }
            ExportDataset::Projection(id) => {
                let projection =
                    stored(self.ctx.state, *id).map_err(ExportPlanError::Projection)?;
                let info = projection.info();
                let fitted = match info.status() {
                    crosstalk_spec::aggregates::projection::ProjectionStatus::Ready(fitted) => {
                        *fitted
                    }
                    other => {
                        return Err(ExportPlanError::Store {
                            reason: format!("a stored frame for a {:?} job", other.kind()),
                        });
                    }
                };
                let rows = rows::points(&self.ctx, &projection, content);
                let basis = ExportBasis::Projection {
                    projection: *id,
                    spec: info.spec().clone(),
                    fitted,
                };
                (basis, rows)
            }
        };
        Ok(ExportPlan {
            basis,
            embedding_model: self.ctx.world.topics.model.clone(),
            rows: u64::try_from(rows.len()).unwrap_or(u64::MAX),
            source: PlannedRows(rows.into_iter()),
        })
    }
}
