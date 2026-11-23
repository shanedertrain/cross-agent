//! Topics, persistent subscription sets and the agent fields that go with them, on JetStream KV.

use async_nats::jetstream::kv;
use futures_util::TryStreamExt;
use xagent_proto::{
    AgentId, AgentRecord, PersistentSubscription, SubscriptionSet, TopicRecord, now_ts,
    registry_key,
};

use crate::registry::{encode, live_entry};
use crate::{Bus, BusError, STREAM};

/// Compare-and-set attempts on a subscription set before giving up under contention.
const CAS_ATTEMPTS: usize = 8;

/// The live keys of `store` under `<workspace>.`.
async fn keys_in(store: &kv::Store, workspace: &str) -> Result<Vec<String>, BusError> {
    let prefix = format!("{workspace}.");
    let keys: Vec<String> = store
        .keys()
        .await
        .map_err(BusError::nats)?
        .try_collect()
        .await
        .map_err(BusError::nats)?;
    let mut keys: Vec<String> = keys
        .into_iter()
        .filter(|k| k.starts_with(&prefix))
        .collect();
    keys.sort();
    Ok(keys)
}

impl Bus {
    /// The registered topic, if any.
    pub async fn topic(
        &self,
        workspace: &str,
        name: &str,
    ) -> Result<Option<TopicRecord>, BusError> {
        match live_entry(&self.topics, &registry_key(workspace, name)).await? {
            Some((value, _)) => Ok(Some(serde_json::from_slice(&value)?)),
            None => Ok(None),
        }
    }

    /// Registers a new topic.
    ///
    /// # Errors
    ///
    /// [`BusError::Exists`] when the topic is already registered.
    pub async fn create_topic(&self, record: &TopicRecord) -> Result<(), BusError> {
        let key = registry_key(&record.workspace, &record.name);
        match self.topics.create(&key, encode(record)).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == kv::CreateErrorKind::AlreadyExists => Err(BusError::Exists),
            Err(err) => Err(BusError::nats(err)),
        }
    }

    /// Every registered topic of `workspace`, by name. Unreadable records are skipped.
    pub async fn list_topics(&self, workspace: &str) -> Result<Vec<TopicRecord>, BusError> {
        let mut out = Vec::new();
        for key in keys_in(&self.topics, workspace).await? {
            if let Some((value, _)) = live_entry(&self.topics, &key).await?
                && let Ok(record) = serde_json::from_slice::<TopicRecord>(&value)
                && record.workspace == workspace
            {
                out.push(record);
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Applies `change` to the agent's persistent subscription set with compare-and-set, creating
    /// the set if it was never written. The write is made even when nothing changed: the new
    /// revision fences off any delayed write based on an older read. `authority` is checked
    /// right before each write.
    ///
    /// # Errors
    ///
    /// [`BusError::NoAuthority`] when `authority` fails, [`BusError::Contended`] after repeated
    /// compare-and-set conflicts, [`BusError::Nats`] when the outcome of a write is unknown.
    pub async fn update_subscriptions(
        &self,
        workspace: &str,
        agent: &AgentId,
        change: impl Fn(&mut Vec<PersistentSubscription>),
        authority: impl Fn() -> bool,
    ) -> Result<SubscriptionSet, BusError> {
        let key = registry_key(workspace, agent.as_str());
        for _ in 0..CAS_ATTEMPTS {
            let current = live_entry(&self.subscriptions, &key).await?;
            let mut items = match &current {
                Some((value, _)) => serde_json::from_slice::<SubscriptionSet>(value)?.subscriptions,
                None => Vec::new(),
            };
            change(&mut items);
            items.sort_by(|a, b| a.pattern.cmp(&b.pattern));
            let set = SubscriptionSet {
                workspace: workspace.to_owned(),
                agent_id: agent.clone(),
                subscriptions: items,
                updated_at: now_ts(),
            };
            if !authority() {
                return Err(BusError::NoAuthority);
            }
            let stored = match current {
                Some((_, revision)) => {
                    match self
                        .subscriptions
                        .update(&key, encode(&set), revision)
                        .await
                    {
                        Ok(_) => true,
                        Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => false,
                        Err(err) => return Err(BusError::nats(err)),
                    }
                }
                None => match self.subscriptions.create(&key, encode(&set)).await {
                    Ok(_) => true,
                    Err(err) if err.kind() == kv::CreateErrorKind::AlreadyExists => false,
                    Err(err) => return Err(BusError::nats(err)),
                },
            };
            if stored {
                return Ok(set);
            }
            // Lost a compare-and-set: read again.
        }
        Err(BusError::Contended(format!(
            "the persistent subscriptions of {agent}"
        )))
    }

    /// The agent's persistent subscription set as stored; empty if never written.
    pub async fn subscriptions(
        &self,
        workspace: &str,
        agent: &AgentId,
    ) -> Result<Vec<PersistentSubscription>, BusError> {
        match live_entry(
            &self.subscriptions,
            &registry_key(workspace, agent.as_str()),
        )
        .await?
        {
            Some((value, _)) => {
                Ok(serde_json::from_slice::<SubscriptionSet>(&value)?.subscriptions)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Records the project root topic an agent works in.
    pub async fn set_project_topic(
        &self,
        workspace: &str,
        agent: &AgentId,
        topic: &str,
    ) -> Result<(), BusError> {
        let key = registry_key(workspace, agent.as_str());
        let Some((value, _)) = live_entry(&self.agents, &key).await? else {
            return Ok(());
        };
        let mut record: AgentRecord = serde_json::from_slice(&value)?;
        if record.project_topic.as_deref() == Some(topic) {
            return Ok(());
        }
        record.project_topic = Some(topic.to_owned());
        record.updated_at = now_ts();
        self.agents
            .put(key, encode(&record))
            .await
            .map_err(BusError::nats)?;
        Ok(())
    }

    /// Agents of `workspace` whose registration was last written by `host_id`.
    pub async fn agents_last_on_host(
        &self,
        workspace: &str,
        host_id: &str,
    ) -> Result<Vec<AgentId>, BusError> {
        let mut out = Vec::new();
        for key in keys_in(&self.agents, workspace).await? {
            if let Some((value, _)) = live_entry(&self.agents, &key).await?
                && let Ok(record) = serde_json::from_slice::<AgentRecord>(&value)
                && record.workspace == workspace
                && record.host_id == host_id
            {
                out.push(record.agent_id);
            }
        }
        Ok(out)
    }

    /// The message stream's last sequence: the boundary a new subscription starts after.
    pub async fn last_sequence(&self) -> Result<u64, BusError> {
        let stream = self.js.get_stream(STREAM).await.map_err(BusError::nats)?;
        Ok(stream.cached_info().state.last_sequence)
    }

    /// `(name, filter subjects)` of every durable consumer on the message stream: who the broker
    /// will deliver to, on every host.
    pub async fn consumer_filters(&self) -> Result<Vec<(String, Vec<String>)>, BusError> {
        let stream = self.js.get_stream(STREAM).await.map_err(BusError::nats)?;
        let infos: Vec<_> = stream
            .consumers()
            .try_collect()
            .await
            .map_err(BusError::nats)?;
        Ok(infos
            .into_iter()
            .filter(|info| info.config.durable_name.is_some())
            .map(|info| {
                let mut filters = info.config.filter_subjects;
                if !info.config.filter_subject.is_empty() {
                    filters.push(info.config.filter_subject);
                }
                (info.name, filters)
            })
            .collect())
    }
}
