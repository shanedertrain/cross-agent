//! Antigravity conversation titles: read-only lookup in `<app_data_dir>/conversation_summaries.db`.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension};

const BUSY_TIMEOUT: Duration = Duration::from_millis(100);

/// The conversation's title in Antigravity's local summary database, trimmed; `None` when absent
/// or unreadable.
pub fn conversation_title(app_data_dir: &Path, conversation: &str) -> Option<String> {
    let db = app_data_dir.join("conversation_summaries.db");
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    let _ = conn.busy_timeout(BUSY_TIMEOUT);
    let title: Option<String> = conn
        .query_row(
            "SELECT title FROM conversation_summaries WHERE conversation_id = ?1",
            [conversation],
            |row| row.get(0),
        )
        .optional()
        .ok()
        .flatten();
    title
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_trimmed_title_and_handles_missing_rows() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert_eq!(conversation_title(dir.path(), "c-1"), None);

        let conn = Connection::open(dir.path().join("conversation_summaries.db")).expect("db");
        conn.execute(
            "CREATE TABLE conversation_summaries (conversation_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '')",
            [],
        )
        .expect("schema");
        conn.execute(
            "INSERT INTO conversation_summaries (conversation_id, title) VALUES (?1, ?2)",
            ["c-1", "  Wire XAgent  "],
        )
        .expect("insert");
        conn.execute(
            "INSERT INTO conversation_summaries (conversation_id, title) VALUES (?1, ?2)",
            ["c-2", "   "],
        )
        .expect("insert blank");

        assert_eq!(
            conversation_title(dir.path(), "c-1").as_deref(),
            Some("Wire XAgent")
        );
        assert_eq!(conversation_title(dir.path(), "c-2"), None);
        assert_eq!(conversation_title(dir.path(), "c-missing"), None);
    }
}
