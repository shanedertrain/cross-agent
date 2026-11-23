/// Why a transport call failed. Transports map every failure to "cannot wake now", so this only
/// feeds logs.
#[derive(Debug, thiserror::Error)]
pub enum WakeError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("timed out")]
    Timeout,
    #[error("websocket: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Protocol(String),
}

impl From<tokio::time::error::Elapsed> for WakeError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}
