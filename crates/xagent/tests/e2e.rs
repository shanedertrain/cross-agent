//! End-to-end: two `xagent daemon` hosts on one private nats-server, driven only through the CLI
//! and the daemon's IPC socket.

mod harness;

use std::time::Duration;

use harness::{Host, Nats, wait_for};
use serde_json::Value;

fn code(err: &Value) -> &str {
    err["code"].as_str().unwrap_or_default()
}

fn unread(status: &Value) -> u64 {
    status["unread_total"].as_u64().expect("unread_total")
}

/// A conversation on host A DMs a conversation on host B; B sees it unread, lists its header,
/// reads its body, and it is then read.
#[test]
fn dm_across_hosts_is_listed_read_and_marked_read() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let b = Host::start(&nats, "host-b");
    let bob = b.agent_id("conv-bob");
    let alice = a.agent_id("conv-alice");

    let sent = a
        .as_conv(
            "conv-alice",
            "dm",
            &[
                "--to",
                &bob,
                "--summary",
                "build is green",
                "--body",
                "naïve 日本語 body",
            ],
        )
        .expect("dm sent");
    assert_eq!(sent["status"], "published");
    let id = sent["id"].as_str().expect("message id");

    let status = b.as_conv("conv-bob", "inbox", &[]).expect("inbox status");
    assert_eq!(unread(&status), 1, "{status}");
    assert_eq!(status["dm_unread"], 1);

    let list = b.as_conv("conv-bob", "list", &[]).expect("inbox list");
    let messages = list["messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 1, "{list}");
    let header = &messages[0];
    assert_eq!(header["id"], id);
    assert_eq!(header["sender"], alice.as_str());
    assert_eq!(header["recipient"], bob.as_str());
    assert_eq!(header["summary"], "build is green");
    assert_eq!(header["body_bytes"], "naïve 日本語 body".len());
    assert!(
        header.get("body").is_none(),
        "headers never carry the body: {header}"
    );

    let read = b.as_conv("conv-bob", "read", &[id]).expect("read");
    let message = &read["messages"][0];
    assert_eq!(message["body"], "naïve 日本語 body");
    assert_eq!(message["from"], alice.as_str());
    assert_eq!(message["to"], bob.as_str());

    assert_eq!(
        unread(&b.as_conv("conv-bob", "inbox", &[]).expect("inbox status")),
        0
    );
    let all = b.as_conv("conv-bob", "list", &["--all"]).expect("list all");
    assert_eq!(all["messages"][0]["read"], true, "{all}");

    // The sender's own copy is not in its inbox, and a third party cannot read the DM.
    assert_eq!(
        unread(&a.as_conv("conv-alice", "inbox", &[]).expect("sender inbox")),
        0
    );
    let other = b
        .as_conv("conv-eve", "read", &[id])
        .expect("read as another conversation");
    assert_eq!(other["not_found"][0], id, "{other}");
}

/// While one session holds a conversation's lease, a second session for it is refused on the same
/// host and on another host; once the first closes, the conversation opens at once.
#[test]
fn second_session_is_refused_until_the_first_closes() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let b = Host::start(&nats, "host-b");

    let held = a.hold("conv-x").expect("first session opens");
    let remote = b
        .as_conv("conv-x", "inbox", &[])
        .expect_err("refused on another host");
    assert_eq!(code(&remote), "duplicate_active_agent", "{remote}");
    assert!(
        remote["message"]
            .as_str()
            .unwrap_or_default()
            .contains("host-a"),
        "{remote}"
    );
    let local = a
        .as_conv("conv-x", "inbox", &[])
        .expect_err("refused on the same host");
    assert_eq!(code(&local), "duplicate_active_agent", "{local}");

    drop(held);
    // The lease is released on close, not left to expire: the next open succeeds promptly.
    wait_for(Duration::from_secs(5), "conv-x to open on host-b", || {
        b.as_conv("conv-x", "inbox", &[]).ok()
    });
}

/// A message whose serialized envelope exceeds 16384 bytes is rejected before anything is
/// stored or published.
#[test]
fn oversized_message_is_rejected() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let b = Host::start(&nats, "host-b");
    let bob = b.agent_id("conv-bob");

    let body = "x".repeat(16_384);
    let err = a
        .as_conv(
            "conv-alice",
            "dm",
            &["--to", &bob, "--summary", "too big", "--body", &body],
        )
        .expect_err("rejected");
    assert_eq!(code(&err), "message_too_large", "{err}");

    let status = a.run(&["status"]).expect("status");
    assert_eq!(
        status["outbox"],
        serde_json::json!({}),
        "nothing queued: {status}"
    );
    assert_eq!(
        unread(&b.as_conv("conv-bob", "inbox", &[]).expect("inbox")),
        0
    );
}

/// Host B crashes while its session holds the lease and a DM is on its way. After a restart (and
/// the crashed lease's server-side expiry) the conversation receives every message exactly once.
#[test]
fn dm_in_flight_across_a_crash_is_delivered_exactly_once() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let mut b = Host::start(&nats, "host-b");
    let bob = b.agent_id("conv-bob");

    let mut held = b.hold("conv-bob").expect("session on host-b");
    let first = a
        .as_conv(
            "conv-alice",
            "dm",
            &["--to", &bob, "--summary", "before the crash"],
        )
        .expect("first dm");
    wait_for(
        Duration::from_secs(10),
        "the first DM to reach host-b",
        || (unread(&held.inbox_status()) == 1).then_some(()),
    );

    b.crash();
    drop(held);
    let second = a
        .as_conv(
            "conv-alice",
            "dm",
            &["--to", &bob, "--summary", "during the outage"],
        )
        .expect("second dm, recipient down");
    assert_eq!(second["status"], "published");

    b.spawn_daemon();
    // The crashed session's lease is still live until the server expires it (TTL 30 s).
    let refused = b
        .as_conv("conv-bob", "inbox", &[])
        .expect_err("crashed lease still held");
    assert_eq!(code(&refused), "duplicate_active_agent", "{refused}");
    let status = wait_for(
        Duration::from_secs(45),
        "the crashed lease to expire",
        || b.as_conv("conv-bob", "inbox", &[]).ok(),
    );
    assert_eq!(unread(&status), 2, "{status}");

    let list = b.as_conv("conv-bob", "list", &["--all"]).expect("list");
    let ids: Vec<&str> = list["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["id"].as_str().expect("id"))
        .collect();
    assert_eq!(
        ids,
        [
            first["id"].as_str().expect("id"),
            second["id"].as_str().expect("id")
        ]
    );

    // Nothing is redelivered later (no unacknowledged leftovers).
    std::thread::sleep(Duration::from_secs(2));
    let again = b
        .as_conv("conv-bob", "list", &["--all"])
        .expect("list again");
    assert_eq!(
        again["messages"].as_array().expect("messages").len(),
        2,
        "{again}"
    );
}
