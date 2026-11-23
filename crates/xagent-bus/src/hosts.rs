//! Host leases: one live daemon run per `host_id`, kept like an agent lease in `XAGENT_LEASES`.

use async_nats::jetstream::kv;
use xagent_proto::{HostLeaseRecord, host_lease_key};

use crate::registry::{encode, live_entry};
use crate::{Bus, BusError};

/// Attempts at a host lease create before giving up under contention.
const ACQUIRE_ATTEMPTS: usize = 3;

impl Bus {
    /// The current lease of `host_id` and its revision; `None` when no daemon holds it.
    pub async fn host_lease(
        &self,
        host_id: &str,
    ) -> Result<Option<(HostLeaseRecord, u64)>, BusError> {
        match live_entry(&self.leases, &host_lease_key(host_id)).await? {
            Some((value, revision)) => {
                Ok(serde_json::from_slice(&value).ok().map(|r| (r, revision)))
            }
            None => Ok(None),
        }
    }

    /// Creates the host lease, or takes it over from a run that cannot be live: this same run
    /// (an earlier write whose reply was lost), or an earlier run on the same machine with the same
    /// socket, which the caller now holds. Returns the new revision.
    ///
    /// # Errors
    ///
    /// [`BusError::HostIdTaken`] when another live daemon owns the `host_id`.
    pub async fn acquire_host_lease(&self, lease: &HostLeaseRecord) -> Result<u64, BusError> {
        let key = host_lease_key(&lease.host_id);
        for _ in 0..ACQUIRE_ATTEMPTS {
            match self.leases.create(&key, encode(lease)).await {
                Ok(revision) => return Ok(revision),
                Err(err) if err.kind() == kv::CreateErrorKind::AlreadyExists => {}
                Err(err) => return Err(BusError::nats(err)),
            }
            let Some((held, revision)) = self.host_lease(&lease.host_id).await? else {
                continue; // expired between create and get: retry the create
            };
            let earlier_run_here = lease.machine_id.is_some()
                && held.machine_id == lease.machine_id
                && held.socket == lease.socket;
            if held.daemon_run_id != lease.daemon_run_id && !earlier_run_here {
                return Err(BusError::HostIdTaken {
                    host_id: held.host_id,
                    run_id: held.daemon_run_id,
                    machine_id: held.machine_id.unwrap_or_else(|| "unknown".to_owned()),
                    socket: held.socket,
                    since: held.acquired_at,
                });
            }
            match self.leases.update(&key, encode(lease), revision).await {
                Ok(revision) => return Ok(revision),
                Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => {}
                Err(err) => return Err(BusError::nats(err)),
            }
        }
        Err(BusError::Contended(format!("host lease {}", lease.host_id)))
    }

    /// Rewrites the host lease if it is still at `revision`. Returns the new revision.
    ///
    /// # Errors
    ///
    /// [`BusError::LeaseConflict`] when the key moved on (expired, taken over, or an earlier write
    /// of ours landed without its reply).
    pub async fn refresh_host_lease(
        &self,
        lease: &HostLeaseRecord,
        revision: u64,
    ) -> Result<u64, BusError> {
        let key = host_lease_key(&lease.host_id);
        match self.leases.update(&key, encode(lease), revision).await {
            Ok(revision) => Ok(revision),
            Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => {
                Err(BusError::LeaseConflict)
            }
            Err(err) => Err(BusError::nats(err)),
        }
    }

    /// Deletes the host lease if `run_id` still holds it.
    pub async fn release_host_lease(&self, host_id: &str, run_id: &str) -> Result<(), BusError> {
        let Some((held, revision)) = self.host_lease(host_id).await? else {
            return Ok(());
        };
        if held.daemon_run_id != run_id {
            return Ok(());
        }
        match self
            .leases
            .delete_expect_revision(host_lease_key(host_id), Some(revision))
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == kv::UpdateErrorKind::WrongLastRevision => Ok(()),
            Err(err) => Err(BusError::nats(err)),
        }
    }
}
