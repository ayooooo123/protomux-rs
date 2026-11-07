#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use compact_encoding::protomux::{CONTROL_CLOSE, CONTROL_OPEN, CONTROL_REJECT, Open};
use compact_encoding::{Bytes, Codec, Writer, uint_len};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch};

pub mod frame;
pub mod memory;
#[cfg(feature = "secret-stream")]
mod secret_stream;

use frame::{BatchWriter, Frame, Item};

/// Bytes buffered for remote channels nobody has paired with yet before the
/// mux stops reading (same limit as JS).
const MAX_BUFFERED: usize = 32768;
/// A corked batch is flushed once it grows past this (same limit as JS).
const MAX_BATCH: usize = 8 * 1024 * 1024;
/// `send` reports "not drained" once this much is queued for the transport.
pub const OUTBOUND_HIGH_WATER: usize = 2 * 1024 * 1024;
/// Received bytes that channel consumers may hold before the mux stops
/// reading from the transport.
pub const INBOUND_BUDGET: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub enum Error {
    Decode(compact_encoding::Error),
    NestedBatch,
    InvalidOpen(u64),
    InvalidReject(u64),
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Decode(e) => write!(f, "decode: {e}"),
            Error::NestedBatch => f.write_str("nested batch"),
            Error::InvalidOpen(id) => write!(f, "Invalid open message for remote id {id}"),
            Error::InvalidReject(id) => write!(f, "Invalid reject message for local id {id}"),
            Error::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<compact_encoding::Error> for Error {
    fn from(e: compact_encoding::Error) -> Self {
        Error::Decode(e)
    }
}

/// Reads whole frames from a message-preserving transport.
pub trait FrameRead: Send + 'static {
    /// `Ok(None)` at end of stream.
    fn read(&mut self) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;
}

/// Writes whole frames to a message-preserving transport.
pub trait FrameWrite: Send + 'static {
    fn write(&mut self, frame: Bytes) -> impl Future<Output = io::Result<()>> + Send;
    fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

/// Facts about the underlying connection that protocols need, e.g. the
/// Hypercore capability is derived from the Noise handshake hash.
#[derive(Debug, Clone, Default)]
pub struct StreamInfo {
    pub is_initiator: bool,
    pub handshake_hash: Option<[u8; 64]>,
    pub remote_public_key: Option<[u8; 32]>,
}

type Key = (String, Option<Bytes>);

fn key(protocol: &str, id: Option<&[u8]>) -> Key {
    (
        protocol.to_owned(),
        id.filter(|i| !i.is_empty()).map(Bytes::copy_from_slice),
    )
}

#[derive(Debug, Default)]
struct Info {
    opened: usize,
    pairing: usize,
    /// Remote opens that arrived before any pairer existed for this key and
    /// before [`Mux::ready`]. Each is counted in `pairing` and queued in
    /// `incoming`; they are handed to the first matching [`Mux::pair`] call,
    /// or rejected by `Mux::ready` if still unpaired.
    unasked: usize,
    incoming: VecDeque<u64>,
    outgoing: VecDeque<u64>,
    aliases: Vec<Key>,
}

impl Info {
    fn idle(&self) -> bool {
        self.opened == 0
            && self.pairing == 0
            && self.incoming.is_empty()
            && self.outgoing.is_empty()
    }
}

struct Remote {
    session: Option<u64>,
    /// Messages for a remote channel we have not paired with yet.
    pending: Option<Vec<(u64, Bytes, Arc<OwnedSemaphorePermit>)>>,
    handshake: Bytes,
}

struct Slot {
    serial: u64,
    key: Key,
    remote_id: u64,
    tx: mpsc::UnboundedSender<Event>,
}

#[derive(Default)]
struct State {
    local: Vec<Option<Slot>>,
    free: Vec<usize>,
    remote: Vec<Option<Remote>>,
    infos: HashMap<Key, Info>,
    aliases: HashMap<Key, Key>,
    pairs: HashMap<Key, mpsc::UnboundedSender<PairRequest>>,
    buffered: usize,
    corked: usize,
    batch: Option<BatchWriter>,
    destroyed: bool,
    /// Set by [`Mux::ready`]: protocols are attached, so an open nobody pairs
    /// is rejected at once instead of held for a later pairer.
    ready: bool,
}

struct Shared {
    state: Mutex<State>,
    out: mpsc::UnboundedSender<Bytes>,
    queued: AtomicUsize,
    drained: Notify,
    resume: Notify,
    inbound: Arc<Semaphore>,
    closed: watch::Sender<bool>,
    serial: AtomicU64,
    info: StreamInfo,
}

/// A Protomux session over one transport. Cheap to clone.
#[derive(Clone)]
pub struct Mux {
    shared: Arc<Shared>,
}

/// What a channel receives, in order: `Opened` once, any `Message`s, then
/// `Closed` once.
#[derive(Debug)]
pub enum Event {
    Opened { handshake: Bytes },
    Message(Message),
    Closed { remote: bool },
}

/// A received channel message. Holding it holds back inbound flow control.
#[derive(Debug)]
pub struct Message {
    pub kind: u64,
    pub data: Bytes,
    _permit: Arc<OwnedSemaphorePermit>,
}

/// Raised when the remote opens a channel for a paired protocol that has no
/// local channel yet. Open one while holding this (see [`Mux::open_channel`]);
/// remote opens still unmatched when it drops are rejected.
pub struct PairRequest {
    /// `None` once the request was settled inline by the mux itself.
    mux: Option<Mux>,
    key: Key,
    pub protocol: String,
    pub id: Option<Bytes>,
}

impl Drop for PairRequest {
    fn drop(&mut self) {
        if let Some(mux) = self.mux.take() {
            let mut st = mux.lock();
            mux.finish_pairing(&mut st, &self.key);
        }
    }
}

pub struct ChannelOptions<'a> {
    pub protocol: &'a str,
    pub id: Option<&'a [u8]>,
    /// Extra protocol names a remote may open this channel under.
    pub aliases: &'a [&'a str],
    /// Refuse to open a second channel for the same protocol and id.
    pub unique: bool,
    /// Already-encoded handshake, sent raw after the open header.
    pub handshake: &'a [u8],
}

impl<'a> ChannelOptions<'a> {
    pub fn new(protocol: &'a str) -> Self {
        Self {
            protocol,
            id: None,
            aliases: &[],
            unique: true,
            handshake: &[],
        }
    }
}

/// One side of a Protomux channel. Dropping it closes the channel.
pub struct Channel {
    tx: ChannelSender,
    rx: mpsc::UnboundedReceiver<Event>,
}

impl Mux {
    /// Starts a mux over a transport. Spawns one read and one write task on
    /// the current tokio runtime; both only wake on I/O.
    pub fn new(reader: impl FrameRead, writer: impl FrameWrite, info: StreamInfo) -> Self {
        let (out, out_rx) = mpsc::unbounded_channel();
        let (closed, _) = watch::channel(false);
        let mux = Mux {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                out,
                queued: AtomicUsize::new(0),
                drained: Notify::new(),
                resume: Notify::new(),
                inbound: Arc::new(Semaphore::new(INBOUND_BUDGET)),
                closed,
                serial: AtomicU64::new(0),
                info,
            }),
        };
        tokio::spawn(mux.clone().read_loop(reader));
        tokio::spawn(mux.clone().write_loop(writer, out_rx));
        mux
    }

    pub fn stream_info(&self) -> &StreamInfo {
        &self.shared.info
    }

    /// Signals that protocols are attached and the reader may act on opens:
    /// every remote open still waiting for a pairer is rejected, and from now
    /// on an open with no pairer or matching local channel is rejected at once,
    /// as JS does when its handlers exist. Idempotent.
    ///
    /// The reader runs from [`Mux::new`] on, so a caller that attaches
    /// protocols afterwards must call this; without it, opens that arrived
    /// first wait for a [`Mux::pair`] or a matching `open_channel` forever.
    pub fn ready(&self) {
        let mut st = self.lock();
        if st.ready {
            return;
        }
        st.ready = true;
        let keys: Vec<Key> = st
            .infos
            .iter()
            .filter(|(_, info)| info.unasked > 0)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            let Some(info) = st.infos.get_mut(&k) else {
                continue;
            };
            info.unasked = 0;
            info.pairing = 0;
            let ids: Vec<u64> = info.incoming.drain(..).collect();
            for remote_id in ids {
                self.reject_remote(&mut st, remote_id);
            }
            self.gc(&mut st, &k);
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.shared.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Notifies `PairRequest`s for remote opens of `protocol`/`id` that no
    /// local channel is waiting for. `id: None` matches every id.
    ///
    /// Opens that arrived before any pairer existed are not rejected but
    /// delivered here, until [`Mux::ready`] declares setup done: the reader
    /// runs from [`Mux::new`] on, so unlike JS (whose handlers attach before
    /// the first frame is parsed) a Rust caller may still be attaching
    /// protocols when the remote's opens land.
    pub fn pair(&self, protocol: &str, id: Option<&[u8]>) -> mpsc::UnboundedReceiver<PairRequest> {
        let (tx, rx) = mpsc::unbounded_channel();
        let k = key(protocol, id);
        let mut st = self.lock();
        for (held, info) in st.infos.iter_mut() {
            let matches = held.0 == k.0 && (k.1.is_none() || held.1 == k.1);
            if !matches {
                continue;
            }
            for _ in 0..info.unasked {
                let _ = tx.send(PairRequest {
                    mux: Some(self.clone()),
                    key: held.clone(),
                    protocol: held.0.clone(),
                    id: held.1.clone(),
                });
            }
            info.unasked = 0;
        }
        st.pairs.insert(k, tx);
        rx
    }

    pub fn unpair(&self, protocol: &str, id: Option<&[u8]>) {
        self.lock().pairs.remove(&key(protocol, id));
    }

    /// Whether a local channel for this protocol and id is open.
    pub fn is_opened(&self, protocol: &str, id: Option<&[u8]>) -> bool {
        let st = self.lock();
        let k = key(protocol, id);
        let k = st.aliases.get(&k).unwrap_or(&k);
        st.infos.get(k).is_some_and(|i| i.opened > 0)
    }

    pub fn is_destroyed(&self) -> bool {
        self.lock().destroyed
    }

    /// Opens a channel. Pairs with a waiting remote open of the same protocol
    /// and id, otherwise waits for the remote to open (or reject) it. Returns
    /// `None` if the mux is destroyed or `unique` forbids a second channel.
    pub fn open_channel(&self, opts: ChannelOptions<'_>) -> Option<Channel> {
        let mut guard = self.lock();
        let st = &mut *guard;
        if st.destroyed {
            return None;
        }
        let k = key(opts.protocol, opts.id);
        let k = st.aliases.get(&k).cloned().unwrap_or(k);
        let info = st.infos.entry(k.clone()).or_default();
        if opts.unique && info.opened > 0 {
            return None;
        }
        let remote_id = info.incoming.pop_front().unwrap_or(0);
        for alias in opts.aliases {
            let ak = key(alias, opts.id);
            if ak != k && !info.aliases.contains(&ak) {
                info.aliases.push(ak.clone());
                st.aliases.insert(ak, k.clone());
            }
        }
        if remote_id != 0
            && st
                .remote
                .get(remote_id as usize - 1)
                .is_none_or(Option::is_none)
        {
            return None;
        }

        let idx = st.free.pop().unwrap_or_else(|| {
            st.local.push(None);
            st.local.len() - 1
        });
        let local_id = idx as u64 + 1;
        let info = st.infos.get_mut(&k).expect("info exists");
        info.opened += 1;
        if remote_id == 0 {
            info.outgoing.push_back(local_id);
        } else if info.unasked > 0 {
            // A held open nobody asked about yet: this channel settles it.
            info.unasked -= 1;
            info.pairing -= 1;
        }
        let serial = self.shared.serial.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        st.local[idx] = Some(Slot {
            serial,
            key: k,
            remote_id,
            tx,
        });

        let open = Open {
            local_id,
            protocol: opts.protocol.to_owned(),
            id: opts
                .id
                .filter(|i| !i.is_empty())
                .map(Bytes::copy_from_slice),
            handshake: Bytes::copy_from_slice(opts.handshake),
        };
        self.write(st, 0, CONTROL_OPEN, open.encoded_len(), |w| open.encode(w));
        if remote_id != 0 {
            self.fully_open(st, local_id, remote_id);
        }
        Some(Channel {
            tx: ChannelSender {
                mux: self.clone(),
                local_id,
                serial,
            },
            rx,
        })
    }

    /// Starts batching: until the matching `uncork`, everything sent on this
    /// mux goes out as one batch frame.
    pub fn cork(&self) {
        let mut st = self.lock();
        st.corked += 1;
        if st.corked == 1 {
            st.batch = Some(BatchWriter::new());
        }
    }

    pub fn uncork(&self) {
        let mut st = self.lock();
        if st.corked == 0 {
            return;
        }
        st.corked -= 1;
        if st.corked == 0 {
            if let Some(b) = st.batch.take().filter(|b| !b.is_empty()) {
                self.enqueue(b.finish());
            }
        }
    }

    /// Whether the transport queue is below the high-water mark.
    pub fn is_drained(&self) -> bool {
        self.shared.queued.load(Ordering::Acquire) < OUTBOUND_HIGH_WATER
    }

    /// Waits until the transport queue is below the high-water mark.
    pub async fn drained(&self) {
        loop {
            let notified = self.shared.drained.notified();
            if self.is_drained() || self.is_destroyed() {
                return;
            }
            notified.await;
        }
    }

    /// Closes every channel and the transport. Frames already queued are
    /// still written.
    pub fn destroy(&self) {
        let mut st = self.lock();
        self.shutdown(&mut st);
    }

    /// Resolves once the mux is destroyed.
    pub async fn closed(&self) {
        let mut rx = self.shared.closed.subscribe();
        let _ = rx.wait_for(|c| *c).await;
    }

    fn shutdown(&self, st: &mut State) {
        if st.destroyed {
            return;
        }
        st.destroyed = true;
        for idx in 0..st.local.len() {
            if st.local[idx].is_some() {
                self.close_slot(st, idx as u64 + 1, true);
            }
        }
        st.pairs.clear();
        self.shared.closed.send_replace(true);
        self.shared.drained.notify_waiters();
        self.shared.resume.notify_one();
    }

    fn enqueue(&self, frame: Bytes) {
        self.shared.queued.fetch_add(frame.len(), Ordering::AcqRel);
        let _ = self.shared.out.send(frame);
    }

    /// Sends `kind, body` on `channel`, batched while corked.
    fn write(
        &self,
        st: &mut State,
        channel: u64,
        kind: u64,
        body_len: usize,
        body: impl FnOnce(&mut Writer),
    ) -> bool {
        if let Some(batch) = st.batch.as_mut() {
            batch.push(channel, kind, body_len, body);
            if batch.len() >= MAX_BATCH {
                let full = std::mem::replace(batch, BatchWriter::new());
                self.enqueue(full.finish());
            }
            return true;
        }
        self.enqueue(frame::single(channel, kind, body_len, body));
        self.is_drained()
    }

    fn fully_open(&self, st: &mut State, local_id: u64, remote_id: u64) {
        let Some(remote) = st.remote[remote_id as usize - 1].as_mut() else {
            return;
        };
        remote.session = Some(local_id);
        let handshake = std::mem::take(&mut remote.handshake);
        let pending = remote.pending.take().unwrap_or_default();
        let slot = st.local[local_id as usize - 1]
            .as_ref()
            .expect("slot exists");
        let _ = slot.tx.send(Event::Opened { handshake });
        for (kind, data, permit) in pending {
            st.buffered -= 512 + data.len();
            let _ = slot.tx.send(Event::Message(Message {
                kind,
                data,
                _permit: permit,
            }));
        }
        self.resume_maybe(st);
    }

    fn resume_maybe(&self, st: &State) {
        if st.buffered <= MAX_BUFFERED {
            self.shared.resume.notify_one();
        }
    }

    fn close_slot(&self, st: &mut State, local_id: u64, remote: bool) {
        let idx = local_id as usize - 1;
        let Some(slot) = st.local[idx].take() else {
            return;
        };
        if slot.remote_id > 0 {
            st.remote[slot.remote_id as usize - 1] = None;
            st.free.push(idx);
        }
        if let Some(info) = st.infos.get_mut(&slot.key) {
            info.opened -= 1;
        }
        self.gc(st, &slot.key);
        let _ = slot.tx.send(Event::Closed { remote });
    }

    fn gc(&self, st: &mut State, k: &Key) {
        if st.infos.get(k).is_some_and(Info::idle) {
            let info = st.infos.remove(k).expect("checked");
            for a in info.aliases {
                st.aliases.remove(&a);
            }
        }
    }

    fn finish_pairing(&self, st: &mut State, k: &Key) {
        let Some(info) = st.infos.get_mut(k) else {
            return;
        };
        info.pairing = info.pairing.saturating_sub(1);
        if info.pairing > 0 {
            return;
        }
        let rejected: Vec<u64> = info.incoming.drain(..).collect();
        for remote_id in rejected {
            self.reject_remote(st, remote_id);
        }
        self.gc(st, k);
    }

    /// Rejects one open nobody can pair once [`Mux::ready`] has run, leaving
    /// any earlier request delivered to a pairer in its caller's hands.
    fn reject_unpaired_open(&self, st: &mut State, k: &Key, remote_id: u64) {
        if let Some(info) = st.infos.get_mut(k) {
            info.pairing = info.pairing.saturating_sub(1);
            if let Some(i) = info.incoming.iter().position(|&id| id == remote_id) {
                info.incoming.remove(i);
            }
        }
        self.reject_remote(st, remote_id);
        self.gc(st, k);
    }

    fn reject_remote(&self, st: &mut State, remote_id: u64) {
        if remote_id > 0 {
            if let Some(r) = st.remote[remote_id as usize - 1].take() {
                st.buffered -= r
                    .pending
                    .iter()
                    .flatten()
                    .map(|(_, d, _)| 512 + d.len())
                    .sum::<usize>();
            }
            self.resume_maybe(st);
        }
        self.write(st, 0, CONTROL_REJECT, uint_len(remote_id), |w| {
            w.uint(remote_id)
        });
    }

    fn on_item(
        &self,
        st: &mut State,
        item: Item,
        permit: &Arc<OwnedSemaphorePermit>,
    ) -> Result<(), Error> {
        match item {
            Item::Message {
                channel,
                kind,
                payload,
            } => {
                let Some(Some(r)) = st.remote.get_mut((channel as usize).wrapping_sub(1)) else {
                    return Ok(()); // closed channel: ignore, as in JS
                };
                if let Some(pending) = r.pending.as_mut() {
                    pending.push((kind, payload.clone(), permit.clone()));
                    st.buffered += 512 + payload.len();
                } else if let Some(local_id) = r.session {
                    if let Some(slot) = &st.local[local_id as usize - 1] {
                        let msg = Message {
                            kind,
                            data: payload,
                            _permit: permit.clone(),
                        };
                        let _ = slot.tx.send(Event::Message(msg));
                    }
                }
                Ok(())
            }
            Item::Open(open) => self.on_open(st, open),
            Item::Reject { local_id } => {
                let k = st
                    .infos
                    .iter()
                    .find(|(_, i)| i.outgoing.contains(&local_id))
                    .map(|(k, _)| k.clone())
                    .ok_or(Error::InvalidReject(local_id))?;
                st.infos
                    .get_mut(&k)
                    .expect("found")
                    .outgoing
                    .retain(|&l| l != local_id);
                st.free.push(local_id as usize - 1);
                self.close_slot(st, local_id, true);
                self.gc(st, &k);
                Ok(())
            }
            Item::Close { remote_id } => {
                let session = st
                    .remote
                    .get((remote_id as usize).wrapping_sub(1))
                    .and_then(|r| r.as_ref()?.session);
                if let Some(local_id) = session {
                    self.close_slot(st, local_id, true);
                }
                Ok(())
            }
            Item::UnknownControl(_) => Ok(()),
        }
    }

    fn on_open(&self, st: &mut State, open: Open) -> Result<(), Error> {
        let remote_id = open.local_id;
        if remote_id == 0 {
            self.write(st, 0, CONTROL_REJECT, 1, |w| w.uint(0));
            return Ok(());
        }
        let rid = remote_id as usize - 1;
        if st.remote.len() == rid {
            st.remote.push(None);
        }
        if rid >= st.remote.len() || st.remote[rid].is_some() {
            return Err(Error::InvalidOpen(remote_id));
        }
        let k = key(&open.protocol, open.id.as_deref());
        let k = st.aliases.get(&k).cloned().unwrap_or(k);
        let info = st.infos.entry(k.clone()).or_default();

        if let Some(local_id) = info.outgoing.pop_front() {
            if st.local[local_id as usize - 1].is_none() {
                // We closed it before the remote answered.
                st.free.push(local_id as usize - 1);
                self.gc(st, &k);
                return Ok(());
            }
            st.remote[rid] = Some(Remote {
                session: None,
                pending: None,
                handshake: open.handshake,
            });
            st.local[local_id as usize - 1]
                .as_mut()
                .expect("checked")
                .remote_id = remote_id;
            self.fully_open(st, local_id, remote_id);
            return Ok(());
        }

        st.remote[rid] = Some(Remote {
            session: None,
            pending: Some(Vec::new()),
            handshake: open.handshake,
        });
        info.pairing += 1;
        info.incoming.push_back(remote_id);
        let handler = st
            .pairs
            .get(&k)
            .or_else(|| st.pairs.get(&(k.0.clone(), None)))
            .cloned();
        let Some(tx) = handler else {
            if st.ready {
                // JS: no notify registered for the key, so reject this open now.
                self.reject_unpaired_open(st, &k, remote_id);
            } else {
                // Still setting up: hold it for `pair` or a matching local open.
                st.infos.get_mut(&k).expect("entry above").unasked += 1;
            }
            return Ok(());
        };
        let req = PairRequest {
            mux: Some(self.clone()),
            key: k.clone(),
            protocol: open.protocol,
            id: open.id,
        };
        if let Err(mpsc::error::SendError(mut req)) = tx.send(req) {
            // The pairer went away: decline now, without re-entering the lock in Drop.
            req.mux = None;
            self.finish_pairing(st, &k);
        }
        Ok(())
    }

    fn on_frame(&self, buf: Bytes, permit: OwnedSemaphorePermit) -> Result<(), Error> {
        let permit = Arc::new(permit);
        let mut st = self.lock();
        if st.destroyed {
            return Ok(());
        }
        match frame::decode(buf)? {
            Frame::Single(item) => self.on_item(&mut st, item, &permit),
            Frame::Batch(items) => {
                for item in items {
                    self.on_item(&mut st, item?, &permit)?;
                    if st.destroyed {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    async fn read_loop(self, mut reader: impl FrameRead) {
        let mut closed = self.shared.closed.subscribe();
        loop {
            // Back-pressure for unpaired remote channels, as in JS.
            loop {
                let paused = {
                    let st = self.lock();
                    st.buffered > MAX_BUFFERED && !st.destroyed
                };
                if !paused {
                    break;
                }
                self.shared.resume.notified().await;
            }
            let frame = tokio::select! {
                f = reader.read() => f,
                _ = closed.wait_for(|c| *c) => break,
            };
            let buf = match frame {
                Ok(Some(buf)) if buf.is_empty() => continue,
                Ok(Some(buf)) => buf,
                Ok(None) | Err(_) => break,
            };
            let n = buf.len().clamp(1, INBOUND_BUDGET) as u32;
            let permit = tokio::select! {
                p = self.shared.inbound.clone().acquire_many_owned(n) => p.expect("semaphore never closed"),
                _ = closed.wait_for(|c| *c) => break,
            };
            if self.on_frame(buf, permit).is_err() {
                break;
            }
        }
        self.destroy();
    }

    async fn write_loop(self, mut writer: impl FrameWrite, mut rx: mpsc::UnboundedReceiver<Bytes>) {
        let mut closed = self.shared.closed.subscribe();
        loop {
            let frame = tokio::select! {
                biased;
                f = rx.recv() => f,
                _ = closed.wait_for(|c| *c) => None,
            };
            let Some(frame) = frame else { break };
            let len = frame.len();
            if writer.write(frame).await.is_err() {
                self.destroy();
                return;
            }
            self.wrote(len);
        }
        // Flush what was queued before the close, e.g. channel close frames.
        while let Ok(frame) = rx.try_recv() {
            let len = frame.len();
            if writer.write(frame).await.is_err() {
                break;
            }
            self.wrote(len);
        }
        let _ = writer.close().await;
    }

    fn wrote(&self, len: usize) {
        let before = self.shared.queued.fetch_sub(len, Ordering::AcqRel);
        if before >= OUTBOUND_HIGH_WATER && before - len < OUTBOUND_HIGH_WATER {
            self.shared.drained.notify_waiters();
        }
    }
}

impl Channel {
    pub fn local_id(&self) -> u64 {
        self.tx.local_id
    }

    pub fn mux(&self) -> &Mux {
        &self.tx.mux
    }

    /// A cloneable handle for sending on (and closing) this channel from
    /// other tasks.
    pub fn sender(&self) -> ChannelSender {
        self.tx.clone()
    }

    /// Next event; `None` after `Closed` has been received.
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// Waits for the remote side to open. Returns its handshake, or `None`
    /// if the channel closed (or was rejected) first.
    pub async fn opened(&mut self) -> Option<Bytes> {
        match self.rx.recv().await? {
            Event::Opened { handshake } => Some(handshake),
            _ => None,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// See [`ChannelSender::send`].
    pub fn send<T: Codec>(&self, kind: u64, msg: &T) -> bool {
        self.tx.send(kind, msg)
    }

    pub fn send_raw(&self, kind: u64, data: &[u8]) -> bool {
        self.tx.send_raw(kind, data)
    }

    pub fn close(&self) {
        self.tx.close()
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.tx.close()
    }
}

/// Sending half of a [`Channel`]. Outlives nothing: once the channel closes,
/// sends return false.
#[derive(Clone)]
pub struct ChannelSender {
    mux: Mux,
    local_id: u64,
    serial: u64,
}

impl ChannelSender {
    fn live<'a>(&self, st: &'a mut State) -> Option<&'a mut Slot> {
        st.local
            .get_mut(self.local_id as usize - 1)?
            .as_mut()
            .filter(|s| s.serial == self.serial)
    }

    pub fn mux(&self) -> &Mux {
        &self.mux
    }

    pub fn is_closed(&self) -> bool {
        let mut st = self.mux.lock();
        self.live(&mut st).is_none()
    }

    /// Sends a message of type `kind`. Returns false if the channel is closed
    /// or the transport is above its high-water mark (see [`Mux::drained`]).
    pub fn send<T: Codec>(&self, kind: u64, msg: &T) -> bool {
        self.send_with(kind, msg.encoded_len(), |w| msg.encode(w))
    }

    pub fn send_raw(&self, kind: u64, data: &[u8]) -> bool {
        self.send_with(kind, data.len(), |w| w.raw(data))
    }

    fn send_with(&self, kind: u64, len: usize, body: impl FnOnce(&mut Writer)) -> bool {
        let mut st = self.mux.lock();
        if self.live(&mut st).is_none() {
            return false;
        }
        self.mux.write(&mut st, self.local_id, kind, len, body)
    }

    /// Closes the channel and tells the remote.
    pub fn close(&self) {
        let mut st = self.mux.lock();
        if self.live(&mut st).is_none() {
            return;
        }
        let id = self.local_id;
        self.mux.close_slot(&mut st, id, false);
        if !st.destroyed {
            self.mux
                .write(&mut st, 0, CONTROL_CLOSE, uint_len(id), |w| w.uint(id));
        }
    }
}
