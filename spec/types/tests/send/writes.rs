//! The store write and read traits P0.6 added, one check per trait (the
//! traits whose methods only changed shape stay with their layer's file).

use crate::aggregates::alert::Alert;
use crate::aggregates::alert::rules::AlertRuleDef;
use crate::aggregates::topic::{EmbeddingModel, Topic, TopicModelVersion};
use crate::aggregates::topic_history::TopicLineage;
use crate::derived::flow::access::Access;
use crate::derived::flow::resource::Resource;
use crate::derived::flow::transmission::Transmission;
use crate::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crate::ids::{
    AlertId, AlertRuleId, ChannelId, ConfigHash, OperatorId, ResourceId, SinkId, TopicId,
    TransmissionId,
};
use crate::interfaces::l5_flow::channels::{
    ChannelReads, ChannelTraffic, ChannelWithTraffic, DetectionUpdate, TrafficError,
};
use crate::interfaces::l5_flow::transmissions::{TransmissionStore, TransmissionStoreError};
use crate::interfaces::l5_flow::{Discovery, RegistryError};
use crate::interfaces::l6_analysis::RuleError;
use crate::interfaces::l6_analysis::alerts::{
    AlertActionError, AlertActions, AlertReadError, AlertReads, AlertRuleMaintenance,
};
use crate::interfaces::l6_analysis::corpus::{CorpusError, IndexedTransmission, SearchCorpus};
use crate::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crate::interfaces::l8_surface::AlertFilter;
use crate::interfaces::l8_surface::audit::ConfigChange;
use crate::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crate::interfaces::l8_surface::lists::{AlertRuleFilter, ChannelFilter};
use crate::interfaces::l8_surface::operators::{
    AccessConfig, CallerError, Operator, OperatorLoadError, OperatorStore, OperatorStoreError,
    RequestIdentity,
};
use crate::interfaces::l8_surface::permissions::Caller;
use crate::interfaces::l8_surface::sinks::{SinkError, SinkInfo, SinkRegistry, SinkRegistryError};
use crate::paging::{
    AlertList, AlertRuleList, ChannelList, ChannelTransmissionList, Page, PageRequest,
};
use crate::support::{Change, Timestamp};

use super::{Dummy, arg, assert_send};

// ── L5 flow ────────────────────────────────────────────────────────────

impl ChannelTraffic for Dummy {
    async fn add_resource(
        &mut self,
        _resource: Resource,
    ) -> Result<Option<ChannelId>, TrafficError> {
        match *self {}
    }
    async fn record_access(&mut self, _access: Access) -> Result<(), TrafficError> {
        match *self {}
    }
    async fn set_detection(
        &mut self,
        _channel: ChannelId,
        _update: DetectionUpdate,
    ) -> Result<Change, TrafficError> {
        match *self {}
    }
    async fn discover(
        &mut self,
        _channel: ChannelId,
        _resource: ResourceId,
        _transmission: TransmissionId,
        _at: Timestamp,
    ) -> Result<Discovery, TrafficError> {
        match *self {}
    }
    async fn record_transmission(
        &mut self,
        _transmission: &Transmission,
    ) -> Result<Change, TrafficError> {
        match *self {}
    }
}

impl ChannelReads for Dummy {
    async fn channel(&self, _id: ChannelId) -> Result<Option<ChannelWithTraffic>, RegistryError> {
        match *self {}
    }
    async fn channels(
        &self,
        _filter: &ChannelFilter,
        _page: &PageRequest<ChannelList>,
    ) -> Result<Page<ChannelWithTraffic, ChannelList>, RegistryError> {
        match *self {}
    }
    async fn transmissions(
        &self,
        _channel: ChannelId,
        _filter: &ChannelTransmissionFilter,
        _page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<Page<Transmission, ChannelTransmissionList>, RegistryError> {
        match *self {}
    }
}

impl TransmissionStore for Dummy {
    async fn save(&mut self, _transmission: Transmission) -> Result<(), TransmissionStoreError> {
        match *self {}
    }
    async fn transmission(
        &self,
        _id: TransmissionId,
    ) -> Result<Option<Transmission>, TransmissionStoreError> {
        match *self {}
    }
}

fn channel_traffic<T: ChannelTraffic>(x: &mut T, never: &Dummy) {
    assert_send(x.add_resource(arg(never)));
    assert_send(x.record_access(arg(never)));
    assert_send(x.discover(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.record_transmission(arg(never)));
    assert_send(x.set_detection(arg(never), arg(never)));
}

fn channel_reads<T: ChannelReads>(x: &T, never: &Dummy) {
    assert_send(x.channel(arg(never)));
    assert_send(x.channels(arg(never), arg(never)));
    assert_send(x.transmissions(arg(never), arg(never), arg(never)));
}

fn transmission_store<T: TransmissionStore>(x: &mut T, never: &Dummy) {
    assert_send(x.save(arg(never)));
    assert_send(x.transmission(arg(never)));
}

// ── L6 analysis ────────────────────────────────────────────────────────

impl TopicLifecycle for Dummy {
    async fn begin_fit(
        &mut self,
        _at: Timestamp,
    ) -> Result<TopicModelVersion, TopicLifecycleError> {
        match *self {}
    }
    async fn complete_fit(
        &mut self,
        _version: TopicModelVersion,
        _topics: Vec<Topic>,
        _fitted_at: Timestamp,
    ) -> Result<TopicLineage, TopicLifecycleError> {
        match *self {}
    }
    async fn fail_fit(&mut self, _version: TopicModelVersion) -> Result<(), TopicLifecycleError> {
        match *self {}
    }
    async fn mark_ready(
        &mut self,
        _version: TopicModelVersion,
        _at: Timestamp,
    ) -> Result<(), TopicLifecycleError> {
        match *self {}
    }
    async fn mark_active(
        &mut self,
        _version: TopicModelVersion,
        _at: Timestamp,
    ) -> Result<CatalogActivation, TopicLifecycleError> {
        match *self {}
    }
    async fn assign(
        &mut self,
        _transmission: TransmissionId,
        _version: TopicModelVersion,
        _assignment: StoredAssignment,
    ) -> Result<Change, TopicLifecycleError> {
        match *self {}
    }
}

impl SearchCorpus for Dummy {
    async fn index(&mut self, _document: IndexedTransmission) -> Result<(), CorpusError> {
        match *self {}
    }
    async fn remove(&mut self, _transmission: TransmissionId) -> Result<(), CorpusError> {
        match *self {}
    }
    async fn judge(
        &mut self,
        _transmission: TransmissionId,
        _verdict: Option<Verdict>,
        _revision: VerdictRevision,
    ) -> Result<Observed, CorpusError> {
        match *self {}
    }
    async fn set_model(&mut self, _model: EmbeddingModel) -> Result<(), CorpusError> {
        match *self {}
    }
    async fn drop_model(&mut self, _model: &EmbeddingModel) -> Result<(), CorpusError> {
        match *self {}
    }
}

impl AlertRuleMaintenance for Dummy {
    async fn topic_version_ready(
        &mut self,
        _lineage: &TopicLineage,
        _topics: &[TopicId],
    ) -> Result<Vec<AlertRuleId>, RuleError> {
        match *self {}
    }
    async fn embedding_model_changed(
        &mut self,
        _model: &EmbeddingModel,
    ) -> Result<Vec<AlertRuleId>, RuleError> {
        match *self {}
    }
}

impl AlertActions for Dummy {
    async fn acknowledge(
        &mut self,
        _alert: AlertId,
        _by: OperatorId,
        _at: Timestamp,
    ) -> Result<Change, AlertActionError> {
        match *self {}
    }
    async fn resolve(
        &mut self,
        _alert: AlertId,
        _by: OperatorId,
        _at: Timestamp,
        _note: Option<String>,
    ) -> Result<Change, AlertActionError> {
        match *self {}
    }
}

impl AlertReads for Dummy {
    async fn rule(&self, _id: AlertRuleId) -> Result<Option<AlertRuleDef>, AlertReadError> {
        match *self {}
    }
    async fn rules(
        &self,
        _filter: &AlertRuleFilter,
        _page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, AlertReadError> {
        match *self {}
    }
    async fn alert(&self, _id: AlertId) -> Result<Option<Alert>, AlertReadError> {
        match *self {}
    }
    async fn alerts(
        &self,
        _filter: &AlertFilter,
        _page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, AlertReadError> {
        match *self {}
    }
    async fn rule_version(&self) -> Result<TopicModelVersion, AlertReadError> {
        match *self {}
    }
}

fn topic_lifecycle<T: TopicLifecycle>(x: &mut T, never: &Dummy) {
    assert_send(x.begin_fit(arg(never)));
    assert_send(x.complete_fit(arg(never), arg(never), arg(never)));
    assert_send(x.fail_fit(arg(never)));
    assert_send(x.mark_ready(arg(never), arg(never)));
    assert_send(x.mark_active(arg(never), arg(never)));
    assert_send(x.assign(arg(never), arg(never), arg(never)));
}

fn search_corpus<T: SearchCorpus>(x: &mut T, never: &Dummy) {
    assert_send(x.index(arg(never)));
    assert_send(x.remove(arg(never)));
    assert_send(x.judge(arg(never), arg(never), arg(never)));
    assert_send(x.set_model(arg(never)));
    assert_send(x.drop_model(arg(never)));
}

fn alert_rule_maintenance<T: AlertRuleMaintenance>(x: &mut T, never: &Dummy) {
    assert_send(x.topic_version_ready(arg(never), arg(never)));
    assert_send(x.embedding_model_changed(arg(never)));
}

fn alert_actions<T: AlertActions>(x: &mut T, never: &Dummy) {
    assert_send(x.acknowledge(arg(never), arg(never), arg(never)));
    assert_send(x.resolve(arg(never), arg(never), arg(never), arg(never)));
}

fn alert_reads<T: AlertReads>(x: &T, never: &Dummy) {
    assert_send(x.rule(arg(never)));
    assert_send(x.rules(arg(never), arg(never)));
    assert_send(x.alert(arg(never)));
    assert_send(x.alerts(arg(never), arg(never)));
    assert_send(x.rule_version());
}

// ── L8 surface ─────────────────────────────────────────────────────────

impl OperatorStore for Dummy {
    async fn load(
        &mut self,
        _config: &AccessConfig,
        _hash: ConfigHash,
        _at: Timestamp,
    ) -> Result<Vec<ConfigChange>, OperatorLoadError> {
        match *self {}
    }
    async fn operators(&self) -> Result<Vec<Operator>, OperatorStoreError> {
        match *self {}
    }
    async fn caller(&self, _identity: RequestIdentity) -> Result<Caller, CallerError> {
        match *self {}
    }
}

impl SinkRegistry for Dummy {
    async fn record_delivery(
        &mut self,
        _sink: SinkId,
        _outcome: Result<Timestamp, SinkError>,
    ) -> Result<(), SinkRegistryError> {
        match *self {}
    }
    async fn sinks(&self) -> Result<Vec<SinkInfo>, SinkRegistryError> {
        match *self {}
    }
}

fn operator_store<T: OperatorStore>(x: &mut T, never: &Dummy) {
    assert_send(x.load(arg(never), arg(never), arg(never)));
    assert_send(x.operators());
    assert_send(x.caller(arg(never)));
}

fn sink_registry<T: SinkRegistry>(x: &mut T, never: &Dummy) {
    assert_send(x.record_delivery(arg(never), arg(never)));
    assert_send(x.sinks());
}

#[test]
fn l5_flow_write_and_read_futures_are_send() {
    let _ = channel_traffic::<Dummy>;
    let _ = channel_reads::<Dummy>;
    let _ = transmission_store::<Dummy>;
}

#[test]
fn l6_analysis_write_and_read_futures_are_send() {
    let _ = topic_lifecycle::<Dummy>;
    let _ = search_corpus::<Dummy>;
    let _ = alert_rule_maintenance::<Dummy>;
    let _ = alert_actions::<Dummy>;
    let _ = alert_reads::<Dummy>;
}

#[test]
fn l8_surface_store_futures_are_send() {
    let _ = operator_store::<Dummy>;
    let _ = sink_registry::<Dummy>;
}
