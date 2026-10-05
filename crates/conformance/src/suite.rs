//! Running the suite: [`run`] drives one test on a current-thread runtime,
//! and [`suite!`](crate::suite) instantiates every test for a harness.

use crate::harness::Harness;

/// The runtime a test needed could not be built.
#[derive(Debug, thiserror::Error)]
#[error("building the test runtime: {0}")]
pub struct RunError(#[from] std::io::Error);

/// Runs `test` against `harness` on a fresh current-thread runtime with
/// time enabled. Nothing is spawned, so no future needs to be `Send`.
pub fn run<H: Harness>(harness: H, test: impl AsyncFnOnce(&H)) -> Result<(), RunError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(test(&harness));
    Ok(())
}

/// Instantiates every conformance test for a harness: one `#[test]` per
/// suite test, in a module per area, each building its own harness with
/// `$harness` (an expression evaluated in a child module of the caller,
/// which sees the caller's names).
///
/// ```ignore
/// mod conformance {
///     crosstalk_conformance::suite!(crate::conformance::MyHarness::new());
/// }
/// ```
#[macro_export]
macro_rules! suite {
    ($harness:expr) => {
        $crate::__suite_tests! {
            ($harness)
            scenarios {
                every_named_scenario_is_valid,
                hijacked_wiki,
                late_confirmation,
                impersonation,
                merges,
                hidden_channel,
                suspected,
                declared,
                lone_resource,
                promotion,
                policies,
                topics,
                verdicts,
                dropped_bodies,
                pipeline,
                routes,
                registered,
                everything,
            }
            graph {
                topology_is_canonical_with_shares_summing_to_one,
                edges_count_confirmations_by_their_time,
                windows_add_up,
                edge_transmissions_are_exactly_the_edge,
                the_channel_centred_view_shares_the_topology_edges,
                agent_filter_matches_after_alias_resolution,
                channel_filter_follows_supersession,
                route_and_topic_filters_and_their_conjunction,
                false_detections_are_subtracted,
                channel_graph_draws_only_listed_channels,
                no_view_counts_a_transmission_within_one_agent,
                agent_nodes_carry_labels_and_claims,
            }
            channels {
                the_default_list_is_every_channel_in_force,
                listings_follow_cross_agent_traffic,
                listings_split_channels_declarations_and_unconfirmed_ones,
                a_declaration_without_traffic_is_never_active,
                row_counts_are_the_resources_tally_and_the_graphs_routed_counts,
                the_window_counts_but_never_filters,
                resources_page_through_the_channel_in_force,
                names_resolve_supersession_from_one_batch,
                policy_histories_hold_every_decision,
                an_unconfirmed_channel_lists_its_suspected_transmissions,
                channel_transmissions_are_cross_agent_only,
                channel_transmissions_need_view,
                confirmed_only_changes_no_transmission_view,
                a_merge_hides_the_channel_and_an_unmerge_restores_it,
                alerts_on_a_hidden_channel_are_not_listed,
                discovered_channels_raised_an_alert_and_hold_their_resources,
                rows_by_id_leave_out_transmissions_within_one_agent,
            }
            series {
                series_totals_match_the_graph,
                grouped_series_sum_to_the_graph_and_its_edges,
                a_coarser_step_sums_the_finer_points,
                a_grid_for_another_bucket_width_is_refused,
                the_overview_counts_the_graph,
                the_overview_queues_are_the_lists,
                overview_queues_honour_confirmed_only,
                watermarked_reads_carry_the_watermark,
            }
            refusals {
                unaligned_windows_are_refused,
                unknown_topic_versions_are_not_found,
                dropped_versions_are_not_retained,
                cursors_are_bound_to_their_request,
                unknown_ids,
            }
            projections {
                every_fit_is_a_new_job_with_a_reproducible_frame,
                samples_honour_the_window_and_filter,
                a_narrower_fit_keeps_what_it_admits_of_a_wider_sample,
                too_few_points_fail_the_job,
                jobs_list_newest_first,
            }
        }
    };
}

/// The expansion of [`suite!`](crate::suite).
#[doc(hidden)]
#[macro_export]
macro_rules! __suite_tests {
    (($harness:expr) $($area:ident { $($test:ident),* $(,)? })*) => {
        $(
            mod $area {
                #[allow(unused_imports)]
                use super::*;
                $(
                    #[test]
                    fn $test() -> ::core::result::Result<(), $crate::RunError> {
                        $crate::run($harness, async |harness| {
                            $crate::tests::$area::$test(harness).await
                        })
                    }
                )*
            }
        )*
    };
}
