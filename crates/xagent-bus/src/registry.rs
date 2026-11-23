//! The agent registry and live-session leases on JetStream KV.

use async_nats::jetstream::kv;
use bytes::Bytes;
use xagent_proto::{AgentId, AgentRecord, LeaseRecord, MessageId, now_ts, registry_key};

use crate::{Bus, BusError};

/// Attempts at a lease create before giving up under contention.
const ACQUIRE_ATTEMPTS: usize = 3;

pub(crate) fn encode<T: serde::Serialize>(value: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(value).expect("record serialization cannot fail"))
}

/// The live value and revision of a key; `None` when absent or deleted.
pub(crate) async fn live_entry(
    store: &kv::Store,
    key: &str,
) -> Result<Option<(Bytes, u64)>, BusError> {
    match store.entry(key).await.map_err(BusError::nats)? {
        Some(entry) if entry.operation == kv::Operation::Put => {
            Ok(Some((entry.value, entry.revision)))
        }
        _ => Ok(None),
    }
}

impl Bus {
    /// The registered agent, if any.
    pub async fn agent(
        &self,
        workspace: &str,
        agent: &AgentId,
    ) -> Result<Option<AgentRecord>, BusError> {
        match live_entry(&self.agents, &registry_key(workspace, agent.as_str())).await? {
            Some((value, _)) => Ok(Some(serde_json::from_slice(&value)?)),
            None => Ok(None),
        }
    }

    /// Registers the agent, keeping the fields an earlier registration set that this one does not
    /// know (display name, project topic, capabilities, creation time).
    pub async fn upsert_agent(
        &self,
        workspace: &str,
        agent: &AgentId,
        client: &str,
        host_id: &str,
    ) -> Result<AgentRecord, BusError> {
        let now = now_ts();
        let existing = self.agent(workspace, agent).await.ok().flatten();
        let record = AgentRecord {
            workspace: workspace.to_owned(),
            agent_id: agent.clone(),
            client: client.to_owned(),
            host_id: host_id.to_owned(),
            updated_at: now.clone(),
            ..existing.unwrap_or_else(|| AgentRecord {
                workspace: String::new(),
                agent_id: agent.clone(),
                display_name: String::new(),
                project_topic: None,
                client: String::new(),
                host_id: String::new(),
                capabilities: Vec::new(),
                created_at: now.clone(),
                updated_at: now,
            })
        };
        self.agents
            .put(registry_key(workspace, agent.as_str()), encode(&record))
            .await
            .map_err(BusError::nats)?;
        Ok(record)
    }

    /// The current lease of an agent and its revision; `None` when nobody holds it.
    pub async fn lease(
        &self,
        workspace: &str,
        agent: &AgentId,
    ) -> Result<Option<(LeaseRecord, u64)>, BusError> {
        match live_entry(&self.leases, &registry_key(workspace, agent.as_str())).await? {
            Some((value, revision)) => {
                Ok(serde_json::from_slice(&value).ok().map(|r| (r, revision)))
            }
            None => Ok(None),
        }
    }

    /// Creates the lease, or re-confirms this same instance's own record (its earlier write whose
    /// reply was lost). Returns the new revision.
    ///
    /// # Errors
    ///
    /// [`BusError::LeaseHeld`] when any other instance owns it, even on this host.
    pub async fn acquire_lease(&self, lease: &LeaseRecord) -> Result<u64, BusError> {
        let key = registry_key(&lease.workspace, lease.agent_id.as_str());
        for _ in 0..ACQUIRE_ATTEMPTS {
            match self.leases.create(&key, encode(lease)).await {
                Ok(revision) => return Ok(revision),
                Err(err) if err.kind() == kv::CreateErrorKind::AlreadyExists => {}
                Err(err) => return Err(BusError::nats(err)),
            }
            let Some((held, revision)) = self.lease(&lease.workspace, &lease.agent_id).await?
            else {
                continue; // deleted between create and get: retry the create
            };
            if held.instance_id != lease.instance_id {
                return Err(BusError::LeaseHeld {
                    agent: lease.agent_id.clone(),
                    host_id: held.host_id,
                    client: held.client,
                    since: held.acquired_at,
                });
            }
            match self.leases.update(&key, encode(lease), revision).await {
                Ok(revision) => return Ok(revision),
                Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => {}
                Err(err) => return Err(BusError::nats(err)),
            }
        }
        Err(BusError::nats(format!(
            "could not acquire lease for {}: contention",
            lease.agent_id
        )))
    }

    /// Rewrites the lease if it is still at `revision`. Returns the new revision.
    ///
    /// # Errors
    ///
    /// [`BusError::LeaseConflict`] when the key moved on (expired, taken over, or an earlier
    /// write of ours landed without its reply).
    pub async fn refresh_lease(&self, lease: &LeaseRecord, revision: u64) -> Result<u64, BusError> {
        let key = registry_key(&lease.workspace, lease.agent_id.as_str());
        match self.leases.update(&key, encode(lease), revision).await {
            Ok(revision) => Ok(revision),
            Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => {
                Err(BusError::LeaseConflict)
            }
            Err(err) => Err(BusError::nats(err)),
        }
    }

    /// Deletes the lease if `instance` still holds it; otherwise leaves it alone.
    pub async fn release_lease(
        &self,
        workspace: &str,
        agent: &AgentId,
        instance: MessageId,
    ) -> Result<(), BusError> {
        let Some((held, revision)) = self.lease(workspace, agent).await? else {
            return Ok(());
        };
        if held.instance_id != instance {
            return Ok(());
        }
        match self
            .leases
            .delete_expect_revision(registry_key(workspace, agent.as_str()), Some(revision))
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => Ok(()),
            Err(err) => Err(BusError::nats(err)),
        }
    }
}
