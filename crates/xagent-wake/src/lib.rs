//! Wake an idle agent when messages arrive for it.
//!
//! One policy ([`Waker`]), transports chosen by the target's client: Claude Code's native
//! cross-session inbox, the Codex app-server, dsh's RPC, and for any other client the command or
//! webhook its host config names ([`Hook`]). The daemon decides when to wake; a
//! transport carries one fixed line built from the unread counts and the agent's own id
//! ([`wake_text`]). Peer-provided text (summary, body, sender) never reaches a client.
//!
//! The daemon feeds the policy through [`WakeSource`] and calls [`Waker::delivered`] each time a
//! message is stored for a target.

mod claude;
mod codex;
mod dsh;
mod error;
mod hook;
mod policy;
pub mod sessions;
mod text;

/// A reqwest builder whose TLS uses the ring provider (the one async-nats links), installed once
/// per process: reqwest's `rustls-no-provider` build panics without a process default.
pub(crate) fn http_builder() -> reqwest::ClientBuilder {
    static PROVIDER: std::sync::Once = std::sync::Once::new();
    PROVIDER.call_once(|| {
        // Err means another provider is already the default; that one serves as well.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    reqwest::Client::builder()
}

pub use claude::{ClaudeInbox, SessionReport, inbox_frame, key_file_name};
pub use codex::{CodexAppServer, LoadedThread};
pub use dsh::is_loopback_url;
pub use error::WakeError;
pub use hook::{Hook, payload};
pub use policy::{Endpoint, Readiness, Target, WakeConfig, WakeSource, Waker};
pub use text::{Counts, waiting_text, wake_text};
