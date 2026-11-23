//! Wake an idle agent when messages arrive for it.
//!
//! One policy ([`Waker`]), three transports chosen by the target's client: Claude Code's native
//! cross-session inbox, the Codex app-server, and dsh's RPC. The daemon decides when to wake; a
//! transport carries one fixed line built from the unread counts and the agent's own id
//! ([`wake_text`]). Peer-provided text (summary, body, sender) never reaches a client.
//!
//! The daemon feeds the policy through [`WakeSource`] and calls [`Waker::delivered`] each time a
//! message is stored for a target.

mod claude;
mod codex;
mod dsh;
mod error;
mod policy;
pub mod sessions;
mod text;

pub use claude::{ClaudeInbox, SessionReport, inbox_frame, key_file_name};
pub use codex::{CodexAppServer, LoadedThread};
pub use dsh::is_loopback_url;
pub use error::WakeError;
pub use policy::{Readiness, Target, WakeConfig, WakeSource, Waker};
pub use text::{Counts, waiting_text, wake_text};
