//! Connecting to NATS from host configuration.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use async_nats::jetstream;
use tracing::{info, warn};

use crate::bootstrap::bootstrap;
use crate::{BootstrapReport, Bus, BusError};

/// Bound on every JetStream API request (lease writes included).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// User and password authentication. `Debug` redacts the password.
#[derive(Clone, PartialEq, Eq)]
pub struct UserPassword {
    pub user: String,
    pub password: String,
}

impl fmt::Debug for UserPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UserPassword")
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// How the daemon authenticates to NATS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NatsAuth {
    UserPassword(UserPassword),
    /// A `.creds` file (user JWT and `NKey` seed) for decentralized JWT auth, read at connect.
    CredentialsFile(PathBuf),
}

/// Where and how to connect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsSettings {
    pub servers: Vec<String>,
    /// `None` connects anonymously.
    pub auth: Option<NatsAuth>,
    /// CA bundle for `tls://` servers signed by a private CA.
    pub tls_ca: Option<PathBuf>,
    /// The connection name the server shows.
    pub client_name: String,
}

/// A NATS connection that reconnects on its own for as long as it lives.
#[derive(Debug, Clone)]
pub struct Connection {
    client: async_nats::Client,
}

/// Connects without waiting for the server: the client keeps retrying in the background, and
/// operations fail fast until it is connected.
///
/// # Errors
///
/// Invalid server addresses, or an unreadable CA or credentials file.
pub async fn connect(settings: &NatsSettings) -> Result<Connection, BusError> {
    let mut options = async_nats::ConnectOptions::new()
        .name(&settings.client_name)
        .connection_timeout(CONNECT_TIMEOUT)
        .retry_on_initial_connect()
        .max_reconnects(None)
        .event_callback(|event| async move {
            match event {
                async_nats::Event::Connected => info!("nats connected"),
                async_nats::Event::Disconnected => warn!("nats disconnected"),
                other => warn!(event = %other, "nats event"),
            }
        });
    match &settings.auth {
        Some(NatsAuth::UserPassword(auth)) => {
            options = options.user_and_password(auth.user.clone(), auth.password.clone());
        }
        Some(NatsAuth::CredentialsFile(path)) => {
            options = options.credentials_file(path).await.map_err(|err| {
                BusError::nats(format!("NATS credentials file {}: {err}", path.display()))
            })?;
        }
        None => {}
    }
    if let Some(ca) = &settings.tls_ca {
        options = options.add_root_certificates(ca.clone()).require_tls(true);
    }
    let servers: Vec<async_nats::ServerAddr> = settings
        .servers
        .iter()
        .map(|s| s.parse())
        .collect::<Result<_, _>>()
        .map_err(BusError::nats)?;
    let client = options.connect(servers).await.map_err(BusError::nats)?;
    Ok(Connection { client })
}

impl Connection {
    /// Whether the client currently has a live connection.
    pub fn is_connected(&self) -> bool {
        self.client.connection_state() == async_nats::connection::State::Connected
    }

    /// Ensures and verifies the stream and buckets, returning the handles the daemon works with.
    ///
    /// # Errors
    ///
    /// [`BusError::Incompatible`] for a fatal mismatch; [`BusError::Nats`] when unreachable.
    pub async fn bootstrap(&self) -> Result<(Bus, BootstrapReport), BusError> {
        let js = jetstream::ContextBuilder::new()
            .timeout(REQUEST_TIMEOUT)
            .build(self.client.clone());
        bootstrap(js).await
    }

    /// Flushes pending writes and closes the connection.
    pub async fn drain(&self) {
        if let Err(err) = self.client.drain().await {
            warn!(error = %err, "nats drain failed");
        }
    }
}
