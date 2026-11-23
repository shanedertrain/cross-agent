//! Listing the agent registry and who is live, and naming an agent for its peers.

use std::collections::HashSet;

use futures_util::TryStreamExt;
use xagent_proto::{AgentId, AgentRecord, now_ts, registry_key};

use crate::registry::{encode, live_entry};
use crate::{Bus, BusError};

impl Bus {
    /// Every registered agent of `workspace`, in key order. Unreadable records are skipped.
    pub async fn list_agents(&self, workspace: &str) -> Result<Vec<AgentRecord>, BusError> {
        let mut records = Vec::new();
        for key in workspace_keys(&self.agents, workspace).await? {
            let Some((value, _)) = live_entry(&self.agents, &key).await? else {
                continue;
            };
            match serde_json::from_slice::<AgentRecord>(&value) {
                Ok(record) => records.push(record),
                Err(err) => {
                    tracing::warn!(key, error = %err, "skipping an unreadable agent record");
                }
            }
        }
        Ok(records)
    }

    /// The agents of `workspace` that hold a lease now (a live session somewhere).
    pub async fn live_agents(&self, workspace: &str) -> Result<HashSet<String>, BusError> {
        let prefix = format!("{workspace}.");
        Ok(workspace_keys(&self.leases, workspace)
            .await?
            .into_iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(str::to_owned))
            .collect())
    }

    /// Sets the name peers see for an agent (`agent_list`); a no-op for an unregistered agent or
    /// an unchanged name.
    pub async fn set_display_name(
        &self,
        workspace: &str,
        agent: &AgentId,
        display_name: &str,
    ) -> Result<(), BusError> {
        let Some(mut record) = self.agent(workspace, agent).await? else {
            return Ok(());
        };
        if record.display_name == display_name {
            return Ok(());
        }
        display_name.clone_into(&mut record.display_name);
        record.updated_at = now_ts();
        self.agents
            .put(registry_key(workspace, agent.as_str()), encode(&record))
            .await
            .map_err(BusError::nats)?;
        Ok(())
    }
}

/// The live keys of a bucket under `<workspace>.`, sorted.
async fn workspace_keys(
    store: &async_nats::jetstream::kv::Store,
    workspace: &str,
) -> Result<Vec<String>, BusError> {
    let prefix = format!("{workspace}.");
    let mut keys: Vec<String> = store
        .keys()
        .await
        .map_err(BusError::nats)?
        .try_filter(|key| std::future::ready(key.starts_with(&prefix)))
        .try_collect()
        .await
        .map_err(BusError::nats)?;
    keys.sort();
    Ok(keys)
}
