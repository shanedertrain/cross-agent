//! Codex thread names: `<codex home>/session_index.jsonl`, one line appended per (re)name.

use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;

#[derive(Deserialize)]
struct IndexEntry {
    id: String,
    #[serde(default)]
    thread_name: Option<String>,
}

/// The thread's latest name, trimmed; `None` when it has none.
///
/// Codex titles an unnamed thread from its first prompt, so this may be Codex's title rather than
/// the user's; for a display name either serves.
pub fn thread_name(codex_home: &Path, thread: &str) -> Option<String> {
    let file = std::fs::File::open(codex_home.join("session_index.jsonl")).ok()?;
    BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter(|line| line.contains(thread))
        .filter_map(|line| serde_json::from_str::<IndexEntry>(&line).ok())
        .filter(|entry| entry.id == thread)
        .last()
        .and_then(|entry| entry.thread_name)
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
}
