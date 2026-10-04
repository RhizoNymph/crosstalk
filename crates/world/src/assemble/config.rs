//! What config did: the operator directory's load, the config entries of
//! each document the deployment loaded, the search corpus's model and the
//! sinks' last deliveries.
//!
//! The first document (at `config_at`) set the access mode and operators
//! (`OperatorStore::load` records those), declared the declared channels
//! that existed then, registered the registered agents, provisioned the
//! built-in rules, configured the sinks and set the topic and frame
//! retention; a later document added the design-docs channel. The
//! declarations themselves are made before generation
//! ([`crate::seed`]), since traffic is routed by the ids the registry
//! assigns.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::alert::BuiltinRule;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::audit::ConfigChange;
use crosstalk_spec::interfaces::l8_surface::{SinkError, SinkKind};

use crate::clock::{HOUR, MINUTE, SECOND, minus};
use crate::config::{WorldConfig, document};
use crate::error::WorldError;
use crate::generate::Generated;
use crate::generate::agents::Planned;
use crate::generate::drafts::{DraftOrigin, config_decision, drafts};
use crate::scenario::ChannelKey;
use crate::script::{Op, Script};

pub fn assemble(
    generated: &Generated,
    config: &WorldConfig,
    declared: &BTreeMap<ChannelKey, ChannelId>,
    script: &mut Script,
) -> Result<(), WorldError> {
    let times = &generated.times;
    let first = document(1);
    script.push(times.config_at, Op::LoadAccess { hash: first });
    script.push(times.config_at, Op::SetModel);

    let mut loads: BTreeMap<_, Vec<ConfigChange>> = BTreeMap::new();
    for draft in drafts(times) {
        let DraftOrigin::Declared { pattern, at } = draft.origin else {
            continue;
        };
        let channel = *declared
            .get(&draft.key)
            .ok_or_else(|| WorldError::missing(format!("declared channel {:?}", draft.key)))?;
        let decision = config_decision(at);
        loads
            .entry(at)
            .or_default()
            .push(ConfigChange::DeclareChannel {
                channel,
                pattern,
                policy: decision.kind,
                note: decision.decision.note,
            });
    }
    let first_load = loads.entry(times.config_at).or_default();
    for agent in &generated.cast.agents {
        if agent.state == Planned::Registered {
            first_load.push(ConfigChange::RegisterAgent {
                agent: agent.id,
                evidence: agent.evidence.clone(),
            });
        }
    }
    first_load.extend(BuiltinRule::ALL.map(|rule| ConfigChange::ProvisionRule {
        rule: rule.id(),
        kind: rule.kind(),
    }));
    first_load.extend(config.sinks.iter().map(|sink| ConfigChange::SetSink {
        sink: sink.id,
        kind: sink.kind,
        name: sink.name.clone(),
    }));
    first_load.push(ConfigChange::SetTopicRetention(config.retention));
    first_load.push(ConfigChange::SetFrameRetention {
        frame_retention_micros: config.frame_retention,
    });
    for (n, (at, changes)) in (1u8..).zip(loads) {
        let hash = document(n);
        for change in changes {
            script.push(at, Op::ConfigEntry { hash, change });
        }
    }

    let now = times.now;
    for sink in &config.sinks {
        let (at, outcome) = match sink.kind {
            SinkKind::Webhook => (minus(now, HOUR), Err(SinkError::Rejected { status: 503 })),
            SinkKind::Slack => (minus(now, 25 * MINUTE), Ok(minus(now, 25 * MINUTE))),
            SinkKind::Log => (minus(now, 10 * SECOND), Ok(minus(now, 10 * SECOND))),
        };
        script.push(
            at,
            Op::Delivery {
                sink: sink.id,
                outcome,
            },
        );
    }
    Ok(())
}
