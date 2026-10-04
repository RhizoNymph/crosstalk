//! Alert rules, alerts, the configured sinks and dead letters.

use crosstalk_spec::aggregates::alert::{Alert, AlertRuleDef};
use crosstalk_spec::ids::{AlertId, AlertRuleId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter, DeadLetterStore};
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::interfaces::l8_surface::sinks::{SinkInfo, SinkRegistry};
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller, Permission, QueryError};
use crosstalk_spec::paging::{AlertList, AlertRuleList, DeadLetterList, Page, PageRequest};

use crate::service::{Surface, require};
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    pub(crate) async fn alert_rules_query(
        &self,
        caller: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.alerts().rules(filter, page).await?)
    }

    pub(crate) async fn alert_rule_query(
        &self,
        caller: &Caller,
        id: AlertRuleId,
    ) -> Result<Option<AlertRuleDef>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.alerts().rule(id).await?)
    }

    pub(crate) async fn sinks_query(&self, caller: &Caller) -> Result<Vec<SinkInfo>, QueryError> {
        require(caller, Permission::Govern)?;
        Ok(self.stores.sinks().sinks().await?)
    }

    pub(crate) async fn dead_letters_query(
        &self,
        caller: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, QueryError> {
        require(caller, Permission::Operate)?;
        Ok(self.stores.dead_letters().list(group, page).await?)
    }

    pub(crate) async fn alerts_query(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.alerts().alerts(filter, page).await?)
    }

    pub(crate) async fn alert_query(
        &self,
        caller: &Caller,
        id: AlertId,
    ) -> Result<Option<Alert>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.alerts().alert(id).await?)
    }
}
