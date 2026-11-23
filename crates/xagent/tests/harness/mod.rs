//! Black-box harness: a private `nats-server` with JetStream in a temp dir, and `xagent daemon`
//! processes (the built binary) as separate hosts, each with its own config, state and socket.
//! Nothing here touches the user's real xagent configuration, state or NATS servers.

#![allow(dead_code, reason = "each test file uses its own part of the harness")]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;

const XAGENT: &str = env!("CARGO_BIN_EXE_xagent");
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

fn nats_server_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("NATS_SERVER") {
        return PathBuf::from(path);
    }
    let home_bin =
        Path::new(&std::env::var_os("HOME").expect("HOME is set")).join(".local/bin/nats-server");
    if home_bin.exists() {
        home_bin
    } else {
        PathBuf::from("nats-server")
    }
}

/// A private nats-server on a random localhost port.
pub struct Nats {
    child: Child,
    pub url: String,
    _dir: TempDir,
}

impl Nats {
    pub fn start() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("xa-nats")
            .tempdir()
            .expect("temp dir");
        let mut child = Command::new(nats_server_binary())
            .args(["-a", "127.0.0.1", "-p", "-1", "-js", "-sd"])
            .arg(dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("nats-server starts (set NATS_SERVER if it is not in ~/.local/bin or on PATH)");
        let stderr = child.stderr.take().expect("piped stderr");
        let mut lines = BufReader::new(stderr).lines();
        let mut url = None;
        for line in lines.by_ref() {
            let line = line.expect("nats-server log line");
            if let Some((_, addr)) = line.split_once("Listening for client connections on ") {
                url = Some(format!("nats://{}", addr.trim()));
            }
            if line.contains("Server is ready") {
                break;
            }
        }
        // Keep draining the log so the server never blocks on a full pipe.
        std::thread::spawn(move || lines.for_each(drop));
        Self {
            child,
            url: url.expect("nats-server reported its client port"),
            _dir: dir,
        }
    }
}

impl Drop for Nats {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The outcome of a CLI call: the JSON result, or the daemon's `{code, message}` error.
pub type Outcome = Result<Value, Value>;

/// One host: a daemon process with its own config, state database and socket.
pub struct Host {
    dir: TempDir,
    config: PathBuf,
    daemon: Option<Child>,
}

impl Host {
    pub fn start(nats: &Nats, name: &str) -> Self {
        Self::start_with(nats, name, "")
    }

    /// Like [`Host::start`], with `extra` appended to the config file (TOML tables).
    pub fn start_with(nats: &Nats, name: &str, extra: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("xa-host")
            .tempdir()
            .expect("temp dir");
        std::fs::create_dir(dir.path().join("home")).expect("home dir");
        let config = dir.path().join("config.toml");
        let root = dir.path().display();
        std::fs::write(
            &config,
            format!(
                "host_id = \"{name}\"\n[nats]\nservers = [\"{}\"]\n[daemon]\nsocket = \"{root}/run/x.sock\"\nstate_db = \"{root}/state/x.db\"\n{extra}\n",
                nats.url
            ),
        )
        .expect("write config");
        let mut host = Self {
            dir,
            config,
            daemon: None,
        };
        host.spawn_daemon();
        host
    }

    pub fn command(&self) -> Command {
        let mut cmd = Command::new(XAGENT);
        cmd.arg("--config")
            .arg(&self.config)
            .env("HOME", self.dir.path().join("home"))
            .env_remove("XAGENT_CONFIG");
        cmd
    }

    /// Starts the daemon and waits until it reports JetStream ready.
    pub fn spawn_daemon(&mut self) {
        assert!(self.daemon.is_none(), "daemon already running");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.path().join("daemon.log"))
            .expect("daemon log");
        let child = self
            .command()
            .arg("daemon")
            .stderr(log)
            .spawn()
            .expect("daemon starts");
        self.daemon = Some(child);
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Ok(status) = self.run(&["status"])
                && status["nats"]["ready"] == Value::Bool(true)
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "daemon not ready; log:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Hard crash: SIGKILL, no cleanup (leases stay until their server-side TTL).
    pub fn crash(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            child.kill().expect("SIGKILL");
            child.wait().expect("daemon exits");
        }
    }

    /// Crashes the daemon, deletes its local state database and starts it again (no session
    /// may be open: a crashed session's lease would block its conversation for the lease TTL).
    pub fn wipe_state(&mut self) {
        // A CLI call's session closes after the CLI exits: wait for it to release its lease.
        wait_for(STARTUP_TIMEOUT, "the daemon's sessions to close", || {
            let status = self.run(&["status"]).ok()?;
            status["sessions"]
                .as_array()
                .is_some_and(Vec::is_empty)
                .then_some(())
        });
        self.crash();
        std::fs::remove_dir_all(self.dir.path().join("state")).expect("remove state");
        self.spawn_daemon();
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("daemon.log")).unwrap_or_default()
    }

    /// Runs `xagent <args>` against this host.
    pub fn run(&self, args: &[&str]) -> Outcome {
        let out = self.command().args(args).output().expect("xagent runs");
        if out.status.success() {
            Ok(serde_json::from_slice(&out.stdout).expect("stdout is JSON"))
        } else {
            let err: Value = serde_json::from_slice(&out.stderr).unwrap_or_else(|_| {
                panic!("stderr is JSON: {}", String::from_utf8_lossy(&out.stderr))
            });
            Err(err["error"].clone())
        }
    }

    /// Runs `xagent <args>` with `stdin` as its standard input; returns (exit code, stdout).
    pub fn run_with_stdin(&self, args: &[&str], stdin: &str) -> (Option<i32>, String) {
        use std::process::Stdio;
        let mut child = self
            .command()
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("xagent runs");
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(stdin.as_bytes())
            .expect("write stdin");
        let out = child.wait_with_output().expect("xagent exits");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
        )
    }

    /// Runs a session-scoped command as `conversation` (client codex).
    pub fn as_conv(&self, conversation: &str, command: &str, rest: &[&str]) -> Outcome {
        let mut args = vec![command, "--conversation", conversation, "--client", "codex"];
        args.extend_from_slice(rest);
        self.run(&args)
    }

    /// The agent id of a conversation (opening and closing a session for it).
    pub fn agent_id(&self, conversation: &str) -> String {
        let who = self.as_conv(conversation, "whoami", &[]).expect("whoami");
        who["agent_id"].as_str().expect("agent_id").to_owned()
    }

    /// Opens a session over the raw IPC socket and keeps it until the guard is dropped.
    pub fn hold(&self, conversation: &str) -> Result<HeldSession, Value> {
        self.hold_as(conversation, "codex")
    }

    /// Like [`Host::hold`], for a conversation of `client`.
    pub fn hold_as(&self, conversation: &str, client: &str) -> Result<HeldSession, Value> {
        let stream =
            UnixStream::connect(self.dir.path().join("run/x.sock")).expect("daemon socket");
        let mut held = HeldSession {
            reader: BufReader::new(stream.try_clone().expect("clone")),
            stream,
            agent_id: String::new(),
        };
        let opened = held.call(&serde_json::json!({
            "id": 1, "method": "session_open",
            "params": { "conversation": conversation, "client": client },
        }))?;
        opened["agent_id"]
            .as_str()
            .expect("agent_id")
            .clone_into(&mut held.agent_id);
        Ok(held)
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.crash();
    }
}

/// A session held open on a raw IPC connection; dropping it closes the session.
pub struct HeldSession {
    /// The agent id the session opened as.
    pub agent_id: String,
    stream: UnixStream,
    reader: BufReader<UnixStream>,
}

impl HeldSession {
    pub fn call(&mut self, request: &Value) -> Outcome {
        let mut line = serde_json::to_vec(request).expect("request JSON");
        line.push(b'\n');
        self.stream.write_all(&line).expect("send request");
        let mut reply = String::new();
        self.reader.read_line(&mut reply).expect("read response");
        let response: Value = serde_json::from_str(&reply).expect("response JSON");
        if response["ok"] == Value::Bool(true) {
            Ok(response["result"].clone())
        } else {
            Err(response["error"].clone())
        }
    }

    pub fn inbox_status(&mut self) -> Value {
        self.call(&serde_json::json!({ "id": 2, "method": "inbox_status" }))
            .expect("inbox_status")
    }
}

/// Polls `check` until it returns `Some`, or panics after `timeout`.
pub fn wait_for<T>(timeout: Duration, what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}
