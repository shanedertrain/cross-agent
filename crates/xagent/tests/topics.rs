//! End-to-end: topics, persistent subscriptions, the project root topic, replies and threads,
//! and inbox recovery. Two `xagent daemon` hosts on one private nats-server, driven only through
//! the CLI.

mod harness;

use std::collections::BTreeSet;
use std::time::Duration;

use harness::{Host, Nats, Outcome, wait_for};
use serde_json::Value;

/// Runs `xagent <cmd...> --conversation <conversation> --client codex <rest...>`.
fn conv(host: &Host, conversation: &str, cmd: &[&str], rest: &[&str]) -> Outcome {
    let mut args = cmd.to_vec();
    args.extend_from_slice(&["--conversation", conversation, "--client", "codex"]);
    args.extend_from_slice(rest);
    host.run(&args)
}

fn ok(outcome: Outcome, what: &str) -> Value {
    outcome.unwrap_or_else(|err| panic!("{what}: {err}"))
}

fn code(err: &Value) -> &str {
    err["code"].as_str().unwrap_or_default()
}

fn id(value: &Value) -> String {
    value["id"].as_str().expect("id").to_owned()
}

fn inbox(host: &Host, conversation: &str) -> Value {
    ok(conv(host, conversation, &["inbox"], &[]), "inbox status")
}

/// Waits until `conversation`'s unread count reaches `n`.
fn wait_unread(host: &Host, conversation: &str, n: u64) -> Value {
    wait_for(
        Duration::from_secs(10),
        &format!("{conversation} to have {n} unread"),
        || {
            let status = inbox(host, conversation);
            (status["unread_total"] == n).then_some(status)
        },
    )
}

fn listed_ids(host: &Host, conversation: &str, rest: &[&str]) -> Vec<String> {
    let list = ok(conv(host, conversation, &["list"], rest), "inbox list");
    list["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(id)
        .collect()
}

fn send(host: &Host, conversation: &str, topic: &str, summary: &str, rest: &[&str]) -> Value {
    let mut args = vec!["--topic", topic, "--summary", summary];
    args.extend_from_slice(rest);
    let sent = ok(conv(host, conversation, &["send"], &args), "send");
    assert_eq!(sent["status"], "published", "{sent}");
    sent
}

/// A creates a topic and B subscribes; A's post reaches B (and is counted as one receiver) but not
/// C, who exists and is not subscribed.
#[test]
fn topic_message_reaches_subscribers_only() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let b = Host::start(&nats, "host-b");
    a.agent_id("conv-alice");
    b.agent_id("conv-carol");

    let created = ok(
        conv(
            &a,
            "conv-alice",
            &["topic", "create"],
            &["build", "--description", "CI"],
        ),
        "create",
    );
    assert_eq!(created["status"], "created");
    let again = conv(&a, "conv-alice", &["topic", "create"], &["build"]).expect_err("exists");
    assert_eq!(code(&again), "topic_exists", "{again}");
    let joined = ok(
        conv(&b, "conv-bob", &["topic", "subscribe"], &["build"]),
        "subscribe",
    );
    assert_eq!(joined["status"], "subscribed");
    let twice = ok(
        conv(&b, "conv-bob", &["topic", "subscribe"], &["build"]),
        "subscribe",
    );
    assert_eq!(twice["status"], "already_subscribed");

    let sent = send(
        &a,
        "conv-alice",
        "build",
        "build is green",
        &["--body", "all 412 passed"],
    );
    assert_eq!(sent["topic"], "build");
    assert_eq!(sent["receivers"], 1, "only bob's consumer matches: {sent}");

    let status = wait_unread(&b, "conv-bob", 1);
    assert_eq!(status["by_topic"]["build"], 1, "{status}");
    assert_eq!(listed_ids(&b, "conv-bob", &[]), [id(&sent)]);
    let read = ok(conv(&b, "conv-bob", &["read"], &[&id(&sent)]), "read");
    assert_eq!(read["messages"][0]["body"], "all 412 passed");
    assert_eq!(read["messages"][0]["topic"], "build");

    assert_eq!(inbox(&b, "conv-carol")["unread_total"], 0);
    assert_eq!(
        inbox(&a, "conv-alice")["unread_total"],
        0,
        "no copy back to the sender"
    );

    let topics = ok(conv(&b, "conv-bob", &["topic", "list"], &[]), "list");
    assert_eq!(
        topics["topics"],
        serde_json::json!([{ "name": "build", "description": "CI", "subscribed": true }])
    );
    let missing = conv(
        &a,
        "conv-alice",
        &["send"],
        &["--topic", "nope", "--summary", "x"],
    )
    .expect_err("unknown topic");
    assert_eq!(code(&missing), "topic_not_found", "{missing}");
}

/// A subscription starts at the stream position of the subscribe: a post from before it is not
/// delivered, even though the agent's consumer existed earlier. History still shows both, and the
/// earlier one can be read from there.
#[test]
fn subscribing_later_does_not_deliver_earlier_messages() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let b = Host::start(&nats, "host-b");
    b.agent_id("conv-bob"); // bob's consumer exists before the first post
    ok(
        conv(&a, "conv-alice", &["topic", "create"], &["design"]),
        "create",
    );

    let before = send(&a, "conv-alice", "design", "draft 1", &["--body", "early"]);
    ok(
        conv(&b, "conv-bob", &["topic", "subscribe"], &["design"]),
        "subscribe",
    );
    let after = send(&a, "conv-alice", "design", "draft 2", &[]);

    wait_unread(&b, "conv-bob", 1);
    assert_eq!(listed_ids(&b, "conv-bob", &["--all"]), [id(&after)]);

    let history = ok(
        conv(&b, "conv-bob", &["topic", "history"], &["design", "--body"]),
        "history",
    );
    let messages = history["messages"].as_array().expect("messages");
    let ids: Vec<String> = messages.iter().map(id).collect();
    assert_eq!(ids, [id(&before), id(&after)], "{history}");
    assert_eq!(messages[0]["body"], "early");
    let newest = ok(
        conv(
            &b,
            "conv-bob",
            &["topic", "history"],
            &["design", "--limit", "1"],
        ),
        "history",
    );
    assert_eq!(newest["messages"][0]["id"], id(&after).as_str());
    assert!(newest["messages"][0].get("body").is_none(), "{newest}");

    let read = ok(conv(&b, "conv-bob", &["read"], &[&id(&before)]), "read");
    assert_eq!(read["messages"][0]["summary"], "draft 1", "{read}");
    assert_eq!(
        inbox(&b, "conv-bob")["unread_total"],
        1,
        "history does not fill the inbox"
    );
}

/// A session opened inside a configured project directory joins the project's root topic
/// (creating it); one outside joins nothing; children are opt-in.
#[test]
fn project_root_topic_is_joined_by_cwd() {
    let project = tempfile::tempdir().expect("project dir");
    let inside = project.path().join("crates/core");
    std::fs::create_dir_all(&inside).expect("subdir");
    let elsewhere = tempfile::tempdir().expect("other dir");
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let b = Host::start_with(
        &nats,
        "host-b",
        &format!(
            "[[projects]]\npath = \"{}\"\ntopic = \"proj\"\n",
            project.path().display()
        ),
    );
    let cwd = inside.display().to_string();
    let subs = ok(
        conv(
            &b,
            "conv-bob",
            &["topic", "subscriptions"],
            &["--cwd", &cwd],
        ),
        "subscriptions",
    );
    assert_eq!(subs["topics"], serde_json::json!(["proj"]), "{subs}");
    assert_eq!(subs["project_topic"], "proj");
    let outside = elsewhere.path().display().to_string();
    let none = ok(
        conv(
            &b,
            "conv-carol",
            &["topic", "subscriptions"],
            &["--cwd", &outside],
        ),
        "subscriptions",
    );
    assert_eq!(
        none["project_topic"], "scratch",
        "no project, no checkout: {none}"
    );
    // Outside any configured project, a git checkout names the topic; a linked worktree counts
    // as its main checkout.
    let repos = tempfile::tempdir().expect("repos dir");
    let main = repos.path().join("My Repo.v2");
    std::fs::create_dir_all(main.join(".git/worktrees/wt")).expect("main checkout");
    let worktree = repos.path().join("wt");
    std::fs::create_dir_all(&worktree).expect("worktree");
    std::fs::write(
        worktree.join(".git"),
        format!("gitdir: {}\n", main.join(".git/worktrees/wt").display()),
    )
    .expect("worktree marker");
    let in_worktree = worktree.display().to_string();
    let named = ok(
        conv(
            &b,
            "conv-dave",
            &["topic", "subscriptions"],
            &["--cwd", &in_worktree],
        ),
        "subscriptions",
    );
    assert_eq!(named["project_topic"], "my-repo-v2", "{named}");

    ok(
        conv(&a, "conv-alice", &["topic", "create"], &["proj.audio"]),
        "create child",
    );
    let root = send(&a, "conv-alice", "proj", "root post", &[]);
    assert_eq!(root["receivers"], 1, "{root}");
    let child = send(&a, "conv-alice", "proj.audio", "child post", &[]);
    assert_eq!(child["receivers"], 0, "children are opt-in: {child}");
    assert!(
        child["warnings"][0]
            .as_str()
            .unwrap_or_default()
            .starts_with("nobody receives"),
        "{child}"
    );
    // A later session without a cwd keeps the persistent subscription.
    wait_unread(&b, "conv-bob", 1);
    assert_eq!(listed_ids(&b, "conv-bob", &[]), [id(&root)]);
}

/// A reply to a topic message lands in the same topic and thread. Bob owes a reply to the
/// question he read until he answers; a DM reply goes back to the DM's sender.
#[test]
fn reply_stays_in_thread_and_discharges_the_obligation() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let b = Host::start(&nats, "host-b");
    let alice = a.agent_id("conv-alice");
    let bob = b.agent_id("conv-bob");
    ok(
        conv(&a, "conv-alice", &["topic", "create"], &["review"]),
        "create",
    );
    ok(
        conv(&a, "conv-alice", &["topic", "subscribe"], &["review"]),
        "subscribe",
    );
    ok(
        conv(&b, "conv-bob", &["topic", "subscribe"], &["review"]),
        "subscribe",
    );

    let question = send(
        &a,
        "conv-alice",
        "review",
        "ship it?",
        &["--kind", "question"],
    );
    let qid = id(&question);
    let status = wait_unread(&b, "conv-bob", 1);
    assert_eq!(
        status["awaiting_reply"], 0,
        "unread is not owed yet: {status}"
    );
    ok(conv(&b, "conv-bob", &["read"], &[&qid]), "read");
    let owed = inbox(&b, "conv-bob");
    assert_eq!(
        (
            owed["unread_total"].as_u64(),
            owed["awaiting_reply"].as_u64()
        ),
        (Some(0), Some(1))
    );

    let reply = ok(
        conv(
            &b,
            "conv-bob",
            &["reply"],
            &[&qid, "--summary", "yes", "--kind", "answer"],
        ),
        "reply",
    );
    assert_eq!(reply["topic"], "review");
    assert_eq!(reply["thread_id"], qid.as_str());
    assert_eq!(inbox(&b, "conv-bob")["awaiting_reply"], 0);

    wait_unread(&a, "conv-alice", 1);
    let thread = ok(conv(&a, "conv-alice", &["thread"], &[&qid]), "thread");
    let messages = thread["messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 2, "{thread}");
    assert_eq!(messages[0]["id"], qid.as_str());
    assert_eq!(messages[1]["id"], id(&reply).as_str());
    assert_eq!(messages[1]["reply_to"], qid.as_str());
    assert_eq!(messages[1]["from"], bob.as_str());
    let alice_status = inbox(&a, "conv-alice");
    assert_eq!(
        alice_status["unread_total"], 0,
        "thread_read marks read: {alice_status}"
    );
    assert_eq!(
        alice_status["awaiting_reply"], 0,
        "an answer asks nothing back"
    );

    // DM: bob asks alice; alice's reply goes back to bob in the same thread.
    let dm = ok(
        conv(
            &b,
            "conv-bob",
            &["dm"],
            &["--to", &alice, "--summary", "lunch?", "--kind", "question"],
        ),
        "dm",
    );
    assert_eq!(dm["receivers"], 1, "{dm}");
    wait_unread(&a, "conv-alice", 1);
    let back = ok(
        conv(
            &a,
            "conv-alice",
            &["reply"],
            &[&id(&dm), "--summary", "at noon"],
        ),
        "dm reply",
    );
    assert_eq!(back["recipient"], bob.as_str(), "{back}");
    assert_eq!(back["thread_id"], id(&dm).as_str());
    wait_unread(&b, "conv-bob", 1);
}

/// A host's local state is lost; `recover` re-reads the window from JetStream into the inbox:
/// DMs and subscribed topic posts after the subscription's start, nothing else.
#[test]
fn recover_rebuilds_a_wiped_inbox() {
    let nats = Nats::start();
    let a = Host::start(&nats, "host-a");
    let mut b = Host::start(&nats, "host-b");
    let bob = b.agent_id("conv-bob");
    ok(
        conv(&a, "conv-alice", &["topic", "create"], &["ops"]),
        "create",
    );
    ok(
        conv(&a, "conv-alice", &["topic", "create"], &["other"]),
        "create",
    );

    send(&a, "conv-alice", "ops", "before bob joined", &[]);
    ok(
        conv(&b, "conv-bob", &["topic", "subscribe"], &["ops"]),
        "subscribe",
    );
    let post = send(&a, "conv-alice", "ops", "deploy at 5", &[]);
    send(&a, "conv-alice", "other", "not for bob", &[]);
    let dm = ok(
        conv(
            &a,
            "conv-alice",
            &["dm"],
            &["--to", &bob, "--summary", "ping"],
        ),
        "dm",
    );
    wait_unread(&b, "conv-bob", 2);
    ok(conv(&b, "conv-bob", &["read"], &[&id(&post)]), "read");

    b.wipe_state();
    assert_eq!(
        inbox(&b, "conv-bob")["unread_total"],
        0,
        "acknowledged: not redelivered"
    );
    let dry = ok(b.run(&["gc", "--dry-run"]), "gc");
    assert_eq!(dry["deleted"]["messages"], 0, "{dry}");

    let recovered = ok(b.run(&["recover", "--hours", "1"]), "recover");
    let agents = recovered["agents"].as_array().expect("agents");
    assert_eq!(agents.len(), 1, "only bob ran on host-b: {recovered}");
    assert_eq!(agents[0]["agent_id"], bob.as_str());
    assert_eq!(agents[0]["inserted"], 2, "{recovered}");

    let ids: BTreeSet<String> = listed_ids(&b, "conv-bob", &[]).into_iter().collect();
    assert_eq!(ids, BTreeSet::from([id(&post), id(&dm)]));
    let again = ok(
        b.run(&["recover", "--hours", "1", "--agent", &bob]),
        "recover again",
    );
    assert_eq!(again["agents"][0]["inserted"], 0, "idempotent: {again}");
}

/// The broker's retention is one setting for every host, set with `xagent retention --days`;
/// `[retention] local_days` keeps a host's local copies shorter, never longer.
#[test]
fn retention_is_set_on_the_broker_and_capped_per_host() {
    let nats = Nats::start();
    let a = Host::start_with(&nats, "host-a", "[retention]\nlocal_days = 2\n");
    let b = Host::start_with(&nats, "host-b", "[retention]\nlocal_days = 30\n");
    let ready = |host: &Host| {
        wait_for(Duration::from_secs(15), "nats ready", || {
            host.run(&["status"])
                .ok()
                .filter(|s| s["nats"]["ready"] == true)
        })
    };
    ready(&a);
    ready(&b);

    let shown = ok(a.run(&["retention"]), "show");
    assert_eq!(
        shown["broker_days"], 7,
        "a new stream starts at 7 days: {shown}"
    );
    assert_eq!(shown["local_days"], 2, "{shown}");
    assert_eq!(shown["effective_local_days"], 2, "{shown}");

    let set = ok(a.run(&["retention", "--days", "3"]), "set");
    assert_eq!(set["previous_broker_days"], 7, "{set}");
    assert_eq!(set["broker_days"], 3, "{set}");
    // Every host sees the broker's value at once; a longer local setting is capped by it.
    let other = ok(b.run(&["retention"]), "show on b");
    assert_eq!(other["broker_days"], 3, "{other}");
    assert_eq!(other["effective_local_days"], 3, "{other}");
    assert_eq!(ok(b.run(&["gc", "--dry-run"]), "gc")["retention_days"], 3);

    // recover reads at most the broker's window.
    assert!(b.run(&["recover", "--hours", "72"]).is_ok());
    let refused = b
        .run(&["recover", "--hours", "73"])
        .expect_err("past the window");
    assert_eq!(refused["code"], "invalid_argument", "{refused}");
    let bad = a.run(&["retention", "--days", "0"]).expect_err("zero days");
    assert_eq!(bad["code"], "invalid_argument", "{bad}");
}
