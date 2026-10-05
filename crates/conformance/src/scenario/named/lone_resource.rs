//! A key-value entry only one agent ever writes and reads: a resource on
//! no channel. Its accesses are recorded, but it is in no channel list,
//! graph or count and raised no new-channel alert (INV-749).

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::observed::client::HarnessFamily;
use crosstalk_spec::observed::message::ToolName;

use super::super::{
    AccessFact, AgentFact, AgentRole, Op, ResourceFact, ResourceRole, Scenario, ScenarioError,
};

pub const NAME: &str = "lone_resource";

pub const LONER: AgentRole = AgentRole::new(NAME, "loner");
/// `kv_put scratch/notes`.
pub const SCRATCH: ResourceRole = ResourceRole::new(NAME, "scratch");

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(AgentFact::new(LONER, HarnessFamily::ClaudeCode))
        .fact(ResourceFact {
            role: SCRATCH,
            locator: Locator::Opaque {
                tool: ToolName("kv_put".to_owned()),
                key: "scratch/notes".to_owned(),
            },
        })
        .fact(AccessFact {
            agent: LONER,
            resource: SCRATCH,
            op: Op::Write,
        })
        .fact(AccessFact {
            agent: LONER,
            resource: SCRATCH,
            op: Op::Read,
        })
        .done()
}
