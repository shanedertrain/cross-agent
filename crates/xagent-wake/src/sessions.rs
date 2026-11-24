//! Claude Code's live-session registry: `<sessions dir>/<pid>.json`, one record per running
//! session, written by Claude itself outside every repository. Read by the MCP layer (which
//! conversation a window is) and by the wake transport (whether and where to wake it).
//!
//! A record is read by the Claude process's pid first: a `/clear` (or a resume) gives the same
//! process a new `sessionId` while the MCP server it started keeps the old
//! `CLAUDE_CODE_SESSION_ID` in its environment. Records outlive their
//! process and pids are reused, so a record counts only while its process runs and, when it
//! carries `procStart`, that process's start time matches.

use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

/// The fields of a registry record xagent uses. Unknown fields are ignored.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRecord {
    pub pid: u32,
    #[serde(default)]
    pub session_id: Option<String>,
    /// The session's name: the user's `/rename`, else a derived one. Either way it is the name
    /// `SendMessage` addresses the session by.
    #[serde(default)]
    pub name: Option<String>,
    /// `procStart` as Claude writes it (a string or a number of clock ticks).
    #[serde(default, deserialize_with = "start_ticks")]
    pub proc_start: Option<String>,
    /// `busy` or `idle`.
    #[serde(default)]
    pub status: Option<String>,
    /// When `status` last changed, in milliseconds since the epoch.
    #[serde(default)]
    pub status_updated_at: Option<f64>,
    /// The session's native inbox (the transport behind `SendMessage`).
    #[serde(default)]
    pub messaging_socket_path: Option<String>,
    /// `interactive` for a user's session.
    #[serde(default)]
    pub kind: Option<String>,
}

fn start_ticks<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Option<String>, D::Error> {
    Ok(ticks(Option::<Value>::deserialize(de)?.as_ref()))
}

/// A start time written as a string or a number, as text.
pub(crate) fn ticks(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

impl SessionRecord {
    /// The session's name, trimmed; `None` when blank.
    pub fn display_name(&self) -> Option<&str> {
        self.name
            .as_deref()
            .map(str::trim)
            .filter(|n| !n.is_empty())
    }
}

/// The Claude process an MCP server was started by, from what Claude sets in its environment:
/// `CLAUDE_CODE_MESSAGING_SOCKET=.../<pid>.sock`, else `CLAUDE_PID`.
pub fn pid_from_env(messaging_socket: Option<&str>, claude_pid: Option<&str>) -> Option<u32> {
    let from_socket = messaging_socket
        .and_then(|s| Path::new(s).file_name()?.to_str()?.strip_suffix(".sock"))
        .and_then(parse_pid);
    from_socket.or_else(|| claude_pid.and_then(parse_pid))
}

fn parse_pid(s: &str) -> Option<u32> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok().filter(|&pid| pid > 0)
}

/// The start time of a process in clock ticks since boot (`/proc/<pid>/stat` field 22).
pub fn proc_start(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name (field 2) may contain spaces and parentheses; fields resume after the last ')'.
    let rest = &stat[stat.rfind(')')? + 2..];
    rest.split_whitespace().nth(19).map(str::to_owned)
}

/// Whether the record belongs to the process that runs under its pid now.
fn alive(record: &SessionRecord) -> bool {
    let Some(started) = proc_start(record.pid) else {
        return false;
    };
    if let Some(recorded) = &record.proc_start {
        return *recorded == started;
    }
    // No recorded start time (older Claude): the process must at least be `claude` itself.
    let Ok(cmdline) = std::fs::read(format!("/proc/{}/cmdline", record.pid)) else {
        return false;
    };
    let argv0 = String::from_utf8_lossy(cmdline.split(|&b| b == 0).next().unwrap_or_default());
    Path::new(argv0.as_ref())
        .file_name()
        .is_some_and(|n| n == "claude")
        || argv0.contains("/claude/versions/")
}

fn read_record(path: &Path) -> Option<SessionRecord> {
    let text = std::fs::read(path).ok()?;
    serde_json::from_slice::<SessionRecord>(&text)
        .ok()
        .filter(alive)
}

/// Every live record in the registry, unordered. `None` when the directory cannot be read.
pub fn live_records(sessions_dir: &Path) -> Option<Vec<SessionRecord>> {
    Some(
        std::fs::read_dir(sessions_dir)
            .ok()?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|n| n.strip_suffix(".json"))
                    .is_some_and(|stem| parse_pid(stem).is_some())
            })
            .filter_map(|entry| read_record(&entry.path()))
            .collect(),
    )
}

/// The live record of a Claude session: the process's own record when `pid` is known, else the
/// live record carrying `session_id`.
pub fn find_record(
    sessions_dir: &Path,
    pid: Option<u32>,
    session_id: Option<&str>,
) -> Option<SessionRecord> {
    if let Some(record) = pid.and_then(|pid| read_record(&sessions_dir.join(format!("{pid}.json"))))
    {
        return Some(record);
    }
    let session_id = session_id.filter(|s| !s.is_empty())?;
    live_records(sessions_dir)?
        .into_iter()
        .find(|record| record.session_id.as_deref() == Some(session_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pid_comes_from_the_messaging_socket_then_claude_pid() {
        let cases = [
            (Some("/tmp/cc-socks-1000/4242.sock"), Some("7"), Some(4242)),
            (Some("/tmp/cc-socks-1000/x42.sock"), Some("7"), Some(7)),
            (Some("/tmp/cc-socks-1000/4242"), None, None),
            (Some("/tmp/cc-socks-1000/0.sock"), Some("0"), None),
            (None, Some("+7"), None),
            (None, Some("99999999999"), None),
            (None, None, None),
        ];
        for (socket, pid, want) in cases {
            assert_eq!(pid_from_env(socket, pid), want, "{socket:?} {pid:?}");
        }
    }

    #[test]
    fn a_record_counts_only_while_its_process_runs_with_the_recorded_start() {
        let dir = tempfile::tempdir().expect("temp dir");
        let me = std::process::id();
        let started = proc_start(me).expect("own start time");
        let write = |pid: u32, start: &str, session: &str| {
            std::fs::write(
                dir.path().join(format!("{pid}.json")),
                format!(
                    r#"{{"pid":{pid},"sessionId":"{session}","procStart":{start},"name":" n "}}"#
                ),
            )
            .expect("write record");
        };
        write(me, &format!("\"{started}\""), "s-1");
        let found = find_record(dir.path(), Some(me), None).expect("live record");
        assert_eq!(found.session_id.as_deref(), Some("s-1"));
        assert_eq!(found.display_name(), Some("n"));
        // Found by session id when the pid is not known.
        assert!(find_record(dir.path(), None, Some("s-1")).is_some());
        assert!(find_record(dir.path(), None, Some("s-2")).is_none());
        // A reused pid (another start time) is stale; a numeric procStart is accepted too.
        write(me, "\"1\"", "s-1");
        assert!(find_record(dir.path(), Some(me), Some("s-1")).is_none());
        write(me, &started, "s-1");
        assert!(find_record(dir.path(), Some(me), None).is_some());
    }
}
