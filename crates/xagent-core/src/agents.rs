//! The agent directory: `agent_list`, and the display name a session shows its peers.

use serde::Serialize;
use serde_json::Value;
use xagent_proto::{Error, ErrorCode, WORKSPACE};

use crate::daemon::Daemon;
use crate::protocol::{AgentList, SessionUpdate};
use crate::session::Session;

/// Longest display name, in characters (longer names are cut).
const DISPLAY_NAME_MAX_CHARS: usize = 128;

#[derive(Debug, Serialize)]
struct AgentView {
    /// The address to pass to `dm_send`.
    id: String,
    client: String,
    /// Some session holds the identity's lease now.
    online: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_topic: Option<String>,
    #[serde(rename = "self", skip_serializing_if = "std::ops::Not::not")]
    is_self: bool,
}

fn registry_unavailable(err: &xagent_bus::BusError) -> Error {
    tracing::warn!(error = %err, "agent registry read failed");
    Error::new(
        ErrorCode::NatsUnavailable,
        "agent registry unavailable: NATS unreachable",
    )
}

impl Daemon {
    pub(crate) async fn agent_list(
        &self,
        session: &Session,
        params: AgentList,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        let bus = self.require_bus("listing agents")?;
        let records = bus
            .list_agents(WORKSPACE)
            .await
            .map_err(|e| registry_unavailable(&e))?;
        let live = bus
            .live_agents(WORKSPACE)
            .await
            .map_err(|e| registry_unavailable(&e))?;
        let agents: Vec<AgentView> = records
            .into_iter()
            .map(|r| AgentView {
                online: live.contains(r.agent_id.as_str()),
                is_self: r.agent_id == session.agent,
                id: r.agent_id.to_string(),
                client: r.client,
                display_name: r.display_name,
                project_topic: r.project_topic,
            })
            .filter(|a| a.online || !params.online_only)
            .collect();
        Ok(serde_json::json!({ "agents": agents }))
    }

    pub(crate) async fn session_update(
        &self,
        session: &Session,
        params: SessionUpdate,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        let name: String = params
            .display_name
            .trim()
            .chars()
            .filter(|c| !c.is_control())
            .take(DISPLAY_NAME_MAX_CHARS)
            .collect();
        let bus = self.require_bus("updating the agent registry")?;
        bus.set_display_name(WORKSPACE, &session.agent, &name)
            .await
            .map_err(|e| registry_unavailable(&e))?;
        Ok(serde_json::json!({ "display_name": name }))
    }
}
