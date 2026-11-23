//! The xagent daemon: one per host. It owns the NATS connection, the local SQLite state and the
//! sessions of the conversations running on this host, and serves them over a local Unix socket.

mod admin;
mod agents;
mod api;
pub mod client;
mod daemon;
mod host;
mod ipc;
mod messaging;
mod outbox;
pub mod protocol;
mod receiver;
mod session;
mod topics;
mod wake;
mod windows;

use std::future::Future;
use std::sync::Arc;

use tracing::{error, info};
use xagent_bus::BusError;
use xagent_store::{Store, StoreError};

pub use daemon::{DaemonConfig, Project};

/// Why the daemon could not start, or had to stop.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("opening the state database")]
    Store(#[from] StoreError),
    #[error("binding the daemon socket")]
    Socket(#[from] std::io::Error),
    #[error("configuring the NATS connection")]
    Nats(#[from] BusError),
    /// Another daemon took over this host's `host_id` while it ran.
    #[error("{0}")]
    HostLeaseLost(String),
}

/// Runs the daemon until `shutdown` completes, then closes every session (releasing its lease)
/// and removes the socket.
///
/// # Errors
///
/// At startup: the state database, the socket or the NATS settings. While running: the host
/// lease was taken over by another daemon with the same `host_id`.
pub async fn run(
    config: DaemonConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), StartError> {
    let store = Store::open(&config.state_db)?;
    let failed = store.fail_all_pending("sender_not_authorized: session ended before delivery")?;
    if failed > 0 {
        error!(
            failed,
            "queued sends of a previous run will not be published"
        );
    }
    let connection = xagent_bus::connect(&config.nats).await?;
    let listener = ipc::bind(&config.socket).await?;
    let daemon = daemon::Daemon::new(config, store, connection);
    info!(host_id = %daemon.config.host_id, run_id = %daemon.run_id, "daemon started");

    let tasks = [
        tokio::spawn(Arc::clone(&daemon).bootstrap_loop()),
        tokio::spawn(Arc::clone(&daemon).outbox_loop()),
        tokio::spawn(ipc::serve(Arc::clone(&daemon), listener)),
        tokio::spawn(Arc::clone(&daemon).gc_loop()),
        tokio::spawn(Arc::clone(&daemon).peek_loop()),
    ];
    tokio::select! {
        () = shutdown => info!("shutting down"),
        () = daemon.fatal_kick.notified() => error!("stopping: host lease lost"),
    }
    for task in &tasks {
        task.abort();
    }
    daemon.close_all().await;
    daemon.release_host().await;
    daemon.connection.drain().await;
    if let Err(err) = std::fs::remove_file(&daemon.config.socket) {
        error!(error = %err, "removing the daemon socket failed");
    }
    match daemon.fatal.get() {
        Some(reason) => Err(StartError::HostLeaseLost(reason.clone())),
        None => Ok(()),
    }
}
