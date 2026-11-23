//! The one line a wake carries. Built from counts and the agent's own id only.

/// Unread counters of one agent, as the policy reads them just before sending.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub unread_total: u64,
    pub dm_unread: u64,
    /// Messages already read that asked for a reply nobody sent.
    pub awaiting_reply: u64,
    /// For an identity nobody holds yet: messages waiting, no DM split known.
    pub waiting: Option<u64>,
}

fn noun(n: u64) -> &'static str {
    if n == 1 { "message" } else { "messages" }
}

/// The wake line of a bound conversation.
pub fn wake_text(workspace: &str, agent: &str, counts: &Counts) -> String {
    let owed = counts.awaiting_reply;
    let tail = if owed > 0 {
        format!(", {owed} awaiting your reply")
    } else {
        String::new()
    };
    format!(
        "[xagent] {} unread {} for {workspace}/{agent} ({} DM{tail}). Check your xagent inbox.",
        counts.unread_total,
        noun(counts.unread_total),
        counts.dm_unread,
    )
}

/// The wake line of a conversation that has not bound its identity yet.
pub fn waiting_text(workspace: &str, agent: &str, waiting: u64) -> String {
    format!(
        "[xagent] {waiting} {} waiting for {workspace}/{agent}. Check your xagent inbox.",
        noun(waiting)
    )
}

impl Counts {
    pub(crate) fn line(&self, workspace: &str, agent: &str) -> String {
        match self.waiting {
            Some(waiting) => waiting_text(workspace, agent, waiting),
            None => wake_text(workspace, agent, self),
        }
    }
}
