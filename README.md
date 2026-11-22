# protomux-rs

[Protomux](https://github.com/holepunchto/protomux) 3.13.1 for Rust and tokio: many protocol
channels over one framed, encrypted stream, wire-compatible with the npm module.

```rust
use protomux::{ChannelOptions, Event, Mux, StreamInfo, memory};

# #[tokio::main(flavor = "current_thread")]
# async fn main() {
let ((ar, aw), (br, bw)) = memory::duplex(16);
let a = Mux::new(ar, aw, StreamInfo::default());
let b = Mux::new(br, bw, StreamInfo::default());

// Both sides open the same protocol; the opens pair up.
let mut ca = a.open_channel(ChannelOptions::new("example/echo")).unwrap();
let mut cb = b.open_channel(ChannelOptions::new("example/echo")).unwrap();
ca.opened().await.unwrap();
cb.opened().await.unwrap();

ca.send_raw(0, b"hi");
match cb.recv().await {
    Some(Event::Message(m)) => assert_eq!(&m.data[..], b"hi"),
    other => panic!("{other:?}"),
}
# }
```

- **Channels**: `open_channel` sends an open and pairs with the remote's open of the same
  protocol and id (`aliases` register extra names). A remote open nobody answers is rejected.
- **Pairing** (`mux.pair()` in JS): `Mux::pair(protocol, id)` yields a `PairRequest` when the
  remote opens a channel we have not opened. Open the matching channel while holding the request;
  the remote open is rejected when the request drops unanswered. A remote close cancels an
  unpaired open, releases its buffered messages and rejects the open, even while the request is
  held. The reject frees the remote's channel id, so the remote does not take our next open for
  the same key as the answer to its closed channel, and a late pairing cannot resurrect the
  closed channel id. A pairer that does slow work before it opens can call
  `PairRequest::is_wanted()`: after such a close it returns `false`, and a channel opened anyway
  sends a new open that the remote did not ask for. `id: None` matches any id, which is how
  Corestore opens cores on demand.
- **Setup window** (`mux.ready()`): the reader starts in `Mux::new`, so opens can arrive while the
  caller is still attaching protocols. Those are held until `Mux::pair` or a matching
  `open_channel`, or rejected by `Mux::ready()`, which declares setup done; after it, an open with
  no pairer is rejected at once, as in JS. Already delivered requests remain pending.
  Call `ready()` once the protocols are attached.
- **Batching**: `cork()`/`uncork()` coalesce everything sent in between into one batch frame.
  Batches from JS peers are decoded entry by entry.
- **Back-pressure, outbound**: `Channel::send` returns `false` once more than
  `OUTBOUND_HIGH_WATER` bytes wait for the transport; `Mux::drained().await` waits for room.
- **Back-pressure, inbound**: every received `Message` holds a share of `INBOUND_BUDGET`. Drop a
  message before awaiting `Mux::drained()`. When consumers hold the whole budget the mux stops
  reading message frames, which pushes back on the sender through the transport's own flow control.
  Control frames and channels without held messages continue. Messages for a remote channel that is
  still waiting to be paired are buffered up to 32 KiB, as in JS.
- **Transports**: `FrameRead`/`FrameWrite` over any message-preserving stream. Implemented for
  peeroxide's secret-stream (`Mux::from_secret_stream`, feature `secret-stream`, on by default) and
  an in-memory pair (`memory::duplex`).
  UDX hosts may call `Mux::set_udx_rtt` once, before attaching protocols, to supply
  a live RTT sampler in milliseconds. `Mux::udx_rtt` returns `None` without one;
  this metadata adds no frames or timers and lets Hypercore use JS's UDX windows.

The mux runs one read task and one write task; both sleep until there is I/O. There are no
timers.

## Deviations from npm

- A mux destroys a remote peer that exceeds 1,024 pending remote opens. Pending open handshakes
  count toward the 32 KiB pending buffer. npm protomux 3.13.1 leaves this backlog unbounded.
- A remote close of an open we have not paired yet rejects that open. npm protomux 3.13.1 and
  3.13.0 (the version that `tests/js` pins) ignore such a close: the open stays pending until its
  pairer answers or declines it. If the pairer answers, npm keeps a channel that the remote has
  already closed. When the remote then uses the id of the closed channel again, npm destroys the
  stream with "Invalid open message".
- There is no `watch()` for passively opened channels; a JS sender's batch does not cork our
  replies; a nested batch frame (never produced by either implementation) is an error.

## Tests

- `cargo test`: protocol tests over the in-memory transport.
- `tests/interop.rs`: a Node peer running npm protomux on Hyperswarm and a Rust peer on a
  peeroxide swarm, on a local HyperDHT testnet, so the connection is holepunched UDX with
  secret-stream as apps run it. Both sides open, pair, batch, reject and stream 8 MiB through a
  back-pressured channel, and JS closes an open that Rust is still pairing and opens it again,
  with each side as the Noise initiator in turn. Needs `npm install` in `tests/js`.
- `fuzz/`: `cargo +nightly fuzz run frame` exercises the frame decoder.

## License

MIT OR Apache-2.0
