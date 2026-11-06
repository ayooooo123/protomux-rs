//! A Node peer (npm protomux on Hyperswarm) against a Rust peer (protomux-rs
//! on a peeroxide swarm), on a local HyperDHT testnet over UDX, with each side
//! as the Noise initiator in turn.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use compact_encoding::{Bytes, decode, encode};
use peeroxide::{JoinOpts, SwarmConfig, SwarmHandle, spawn};
use peeroxide_dht::hyperdht::HyperDhtConfig;
use peeroxide_dht::rpc::DhtConfig;
use protomux::{Channel, ChannelOptions, Event, Mux};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::timeout;

const TOPIC: [u8; 32] = [0x3d; 32];

fn js_dir() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js"))
}

fn cstr(s: &str) -> Bytes {
    encode(&s.to_owned())
}

async fn next(ch: &mut Channel) -> Event {
    ch.recv().await.expect("channel ended early")
}

async fn message(ch: &mut Channel) -> (u64, Bytes) {
    match next(ch).await {
        Event::Message(m) => (m.kind, m.data.clone()),
        other => panic!("expected a message, got {other:?}"),
    }
}

fn spawn_node(args: &[&str]) -> Child {
    assert!(
        js_dir().join("node_modules").exists(),
        "run `npm install` in {} first",
        js_dir().display()
    );
    Command::new("node")
        .arg("peer.js")
        .args(args)
        .current_dir(js_dir())
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("node on PATH")
}

/// Plays the Rust side of the scenario in `tests/js/peer.js`.
async fn scenario(mux: Mux) {
    let mut js_first = mux.pair("js/first", None);
    let mut rejects = mux.pair("rust/rejects", None);

    let mut both = mux
        .open_channel(ChannelOptions {
            handshake: &cstr("rust"),
            ..ChannelOptions::new("both")
        })
        .unwrap();
    let mut rust_first = mux
        .open_channel(ChannelOptions {
            id: Some(b"r1"),
            handshake: &cstr("hi-js"),
            ..ChannelOptions::new("rust/first")
        })
        .unwrap();
    let mut bulk = mux.open_channel(ChannelOptions::new("bulk")).unwrap();
    let mut bulk_js = mux.open_channel(ChannelOptions::new("bulk-js")).unwrap();
    let mut nobody = mux
        .open_channel(ChannelOptions::new("nobody/home"))
        .unwrap();

    // JS opened first; open ours while holding the pair request.
    let req = js_first.recv().await.unwrap();
    assert_eq!(req.id.as_deref(), Some(&b"abc"[..]));
    let mut first = mux
        .open_channel(ChannelOptions {
            id: Some(b"abc"),
            handshake: &cstr("hello-from-rust"),
            ..ChannelOptions::new("js/first")
        })
        .unwrap();
    drop(req);
    drop(rejects.recv().await.unwrap()); // decline

    assert_eq!(
        decode::<String>(first.opened().await.unwrap()).unwrap(),
        "hello-from-js"
    );
    for want in ["a", "b", "c"] {
        assert_eq!(
            message(&mut first).await.1,
            want.as_bytes(),
            "corked batch from JS"
        );
    }
    first.send_raw(0, b"bye");
    assert!(matches!(
        next(&mut first).await,
        Event::Closed { remote: true }
    ));

    assert_eq!(
        decode::<String>(rust_first.opened().await.unwrap()).unwrap(),
        "js-ack"
    );
    mux.cork();
    rust_first.send_raw(0, b"ping");
    rust_first.send_raw(0, b"pong");
    mux.uncork();
    assert_eq!(message(&mut rust_first).await.1, b"echo:ping"[..]);
    assert_eq!(message(&mut rust_first).await.1, b"echo:pong"[..]);

    assert_eq!(
        decode::<String>(both.opened().await.unwrap()).unwrap(),
        "js"
    );
    assert_eq!(message(&mut both).await.1, b"from-js"[..]);
    both.send_raw(0, b"from-rust");

    assert!(
        matches!(next(&mut nobody).await, Event::Closed { remote: true }),
        "JS rejected the unpaired open"
    );

    // Rust -> JS, 8 MiB, honouring back-pressure.
    bulk.opened().await.unwrap();
    let chunk = vec![9u8; 64 * 1024];
    let total = 8 * 1024 * 1024;
    for _ in 0..total / chunk.len() {
        if !bulk.send_raw(0, &chunk) {
            mux.drained().await;
        }
    }
    bulk.send_raw(1, b"");
    let (kind, count) = message(&mut bulk).await;
    assert_eq!(
        (kind, std::str::from_utf8(&count).unwrap()),
        (1, total.to_string().as_str())
    );

    // JS -> Rust, 4 MiB.
    bulk_js.opened().await.unwrap();
    let mut got = 0;
    loop {
        let (kind, data) = message(&mut bulk_js).await;
        if kind == 1 {
            assert_eq!(std::str::from_utf8(&data).unwrap(), got.to_string());
            break;
        }
        got += data.len();
    }
    assert_eq!(got, 4 * 1024 * 1024);
}

async fn run(mux: Mux, mut node: Child) {
    let stdout = node.stdout.take().unwrap();
    let lines = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut out = vec![];
        while let Ok(Some(l)) = lines.next_line().await {
            out.push(l);
        }
        out
    });
    timeout(Duration::from_secs(60), scenario(mux.clone()))
        .await
        .expect("scenario timed out");
    mux.destroy();
    let status = timeout(Duration::from_secs(10), node.wait())
        .await
        .unwrap()
        .unwrap();
    let out = lines.await.unwrap();
    assert!(status.success(), "node exited with {status}: {out:?}");
    for want in [
        "rust-first-open r1 hi-js",
        "js-first-open hello-from-rust",
        "js-first-closed",
        "both-open rust",
        "both-msg from-rust",
        "js-rejected true",
        "stream-closed",
    ] {
        assert!(
            out.iter().any(|l| l == want),
            "JS never logged {want:?}: {out:?}"
        );
    }
    assert!(
        !out.iter().any(|l| l.starts_with("stream-error")),
        "{out:?}"
    );
}

/// Starts the JS peer, joins its testnet from Rust, and returns the first
/// connection's mux. `rust_client`: Rust looks the topic up (Noise initiator).
async fn connect(js_role: &str, rust_client: bool) -> (Mux, Child, SwarmHandle) {
    let mut node = spawn_node(&[js_role]);
    let mut stdout = BufReader::new(node.stdout.take().unwrap());
    let mut first = String::new();
    stdout.read_line(&mut first).await.unwrap();
    let port = first
        .trim()
        .strip_prefix("bootstrap ")
        .expect("bootstrap line")
        .to_owned();
    if rust_client {
        let mut ready = String::new();
        stdout.read_line(&mut ready).await.unwrap();
        assert_eq!(ready.trim(), "ready");
    }
    node.stdout = Some(stdout.into_inner());
    let mut dht = DhtConfig::default();
    dht.bootstrap = vec![format!("127.0.0.1:{port}")];
    dht.host = "127.0.0.1".into();
    dht.port = 0;
    let mut hyper = HyperDhtConfig::default();
    hyper.dht = dht;
    let mut config = SwarmConfig::default();
    config.dht = hyper;
    let (_task, swarm, mut conns) = spawn(config).await.expect("peeroxide swarm");
    let mut opts = JoinOpts::default();
    opts.client = rust_client;
    opts.server = !rust_client;
    swarm.join(TOPIC, opts).await.unwrap();
    if !rust_client {
        swarm.flush().await.unwrap();
        let stdin = node.stdin.as_mut().unwrap();
        stdin.write_all(b"go\n").await.unwrap();
    }
    let conn = timeout(Duration::from_secs(60), conns.recv())
        .await
        .expect("no connection over the testnet")
        .unwrap();
    assert_eq!(conn.peer.stream.is_initiator(), rust_client);
    (Mux::from_secret_stream(conn.peer.stream), node, swarm)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and `npm install` in tests/js"]
async fn js_initiator_rust_responder() {
    let (mux, node, swarm) = connect("connect", false).await;
    run(mux, node).await;
    let _ = swarm.destroy().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and `npm install` in tests/js"]
async fn rust_initiator_js_responder() {
    let (mux, node, swarm) = connect("listen", true).await;
    run(mux, node).await;
    let _ = swarm.destroy().await;
}
