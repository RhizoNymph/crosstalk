//! Alert rules, alerts, the configured sinks and dead letters.
//!
//! The alerts list leaves out the alerts readers do not show
//! (`AlertSubject::shown`): about a hidden channel, or a transmission whose
//! agents have since merged into one. Each page is the alert store's page
//! with those removed, so a page can be shorter than asked while its cursor
//! still continues the store's traversal.

use crosstalk_spec::aggregates::alert::{Alert, AlertRuleDef, AlertSubject};
use crosstalk_spec::derived::flow::channel::confirmation::Listing;
use crosstalk_spec::derived::flow::transmission::Crossing;
use crosstalk_spec::ids::{AlertId, AlertRuleId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter, DeadLetterStore};
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::interfaces::l8_surface::sinks::{SinkInfo, SinkRegistry};
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller, Permission, QueryError};
use crosstalk_spec::paging::{AlertList, AlertRuleList, DeadLetterList, Page, PageRequest};

use crate::service::{Surface, page_of, require};
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
        let listed = self.stores.alerts().alerts(filter, page).await?;
        let (alerts, next) = listed.into_parts();
        let shown = self.shown_alerts(alerts).await?;
        page_of(page.size, shown, next)
    }

    /// The alerts of `alerts` that readers show
    /// ([`AlertSubject::shown`]), each subject read now.
    pub(crate) async fn shown_alerts(&self, alerts: Vec<Alert>) -> Result<Vec<Alert>, QueryError> {
        let mut shown = Vec::with_capacity(alerts.len());
        for alert in alerts {
            if self.shown(alert.subject).await? {
                shown.push(alert);
            }
        }
        Ok(shown)
    }

    async fn shown(&self, subject: AlertSubject) -> Result<bool, QueryError> {
        let aliases = self.aliases();
        let (hidden, within_one_agent) = match subject.resolved(aliases) {
            AlertSubject::Channel(channel) => {
                let read = self.stores.channels().channel(channel).await?;
                let hidden = read.and_then(|read| read.listing()) == Some(Listing::Hidden);
                (hidden, false)
            }
            AlertSubject::Transmission(transmission) => {
                let stored = self
                    .stores
                    .transmissions()
                    .transmission(transmission)
                    .await?;
                let within = stored
                    .is_some_and(|stored| stored.crossing(aliases) == Crossing::WithinOneAgent);
                (false, within)
            }
            AlertSubject::Agent(_) => (false, false),
        };
        Ok(subject.shown(aliases, |_| hidden, |_| within_one_agent))
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
