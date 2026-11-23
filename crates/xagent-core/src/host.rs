//! The daemon's host lease: `host_id` scopes `xagent recover`, so one live daemon run holds each.
//!
//! Kept like an agent lease, in `XAGENT_LEASES` under `_host.<host_id>`: created before the
//! daemon becomes ready, refreshed every third of the TTL, released at shutdown, and expired by
//! the server when a run dies. A daemon whose `host_id` is held by another live run waits (not
//! ready, `xagent status` says why) until that lease ends.

use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info, warn};
use xagent_bus::{Bus, BusError};
use xagent_proto::{HostLeaseRecord, now_ts};

use crate::daemon::{Daemon, Readiness};

/// Delay between attempts while the `host_id` is held elsewhere or NATS fails.
const HOST_RETRY: Duration = Duration::from_secs(3);
/// Where systemd keeps the machine's stable id; a run restarted on the same machine with the same
/// socket takes over its predecessor's lease at once.
const MACHINE_ID: &str = "/etc/machine-id";

fn machine_id() -> Option<String> {
    let id = std::fs::read_to_string(MACHINE_ID).ok()?;
    let id = id.trim();
    (!id.is_empty()).then(|| id.to_owned())
}

impl Daemon {
    fn host_record(&self, bus: &Bus) -> HostLeaseRecord {
        HostLeaseRecord {
            host_id: self.config.host_id.clone(),
            daemon_run_id: self.run_id.clone(),
            machine_id: machine_id(),
            socket: self.config.socket.display().to_string(),
            acquired_at: self.started_at.clone(),
            refreshed_at: now_ts(),
            ttl_seconds: bus.lease_ttl().as_secs_f64(),
        }
    }

    /// Takes the host lease, waiting while another live run holds it. Returns its revision.
    pub(crate) async fn acquire_host(&self, bus: &Bus) -> u64 {
        loop {
            match bus.acquire_host_lease(&self.host_record(bus)).await {
                Ok(revision) => return revision,
                Err(err @ BusError::HostIdTaken { .. }) => {
                    let reason = err.to_string();
                    let mut readiness = self.readiness.lock().expect("readiness lock poisoned");
                    if !matches!(&*readiness, Readiness::HostIdTaken(r) if *r == reason) {
                        error!(error = %reason, "host_id in use; waiting for its lease to end");
                        *readiness = Readiness::HostIdTaken(reason);
                    }
                }
                Err(err) => warn!(error = %err, "host lease acquire failed; retrying"),
            }
            tokio::time::sleep(HOST_RETRY).await;
        }
    }

    /// Refreshes the host lease until the daemon stops. Losing it to another run is fatal: two
    /// live daemons would then share one `host_id`.
    pub(crate) async fn keep_host_lease(self: Arc<Self>, bus: Bus, mut revision: u64) {
        let interval = bus.lease_ttl() / 3;
        loop {
            tokio::time::sleep(interval).await;
            let record = self.host_record(&bus);
            let result = match bus.refresh_host_lease(&record, revision).await {
                // Expired, or an earlier write of ours landed without its reply: take it again.
                Err(BusError::LeaseConflict) => bus.acquire_host_lease(&record).await,
                other => other,
            };
            match result {
                Ok(next) => revision = next,
                Err(err @ BusError::HostIdTaken { .. }) => {
                    error!(error = %err, "host lease lost to another daemon; stopping");
                    let _ = self.fatal.set(err.to_string());
                    self.fatal_kick.notify_one();
                    return;
                }
                Err(err) => warn!(error = %err, "host lease refresh failed; retrying"),
            }
        }
    }

    /// Releases the host lease (shutdown), so the next run starts at once.
    pub(crate) async fn release_host(&self) {
        let Some(bus) = self.bus() else { return };
        match bus
            .release_host_lease(&self.config.host_id, &self.run_id)
            .await
        {
            Ok(()) => info!("host lease released"),
            Err(err) => warn!(error = %err, "host lease release failed; it expires on its own"),
        }
    }
}
