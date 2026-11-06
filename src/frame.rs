//! Protomux frame layout: `uint channel, uint type, body`. Channel 0 carries
//! control messages; type 0 on it is a batch of `(len, type ++ body)` entries
//! where a zero length switches the channel id that later entries belong to.

use compact_encoding::protomux::{
    CONTROL_BATCH, CONTROL_CLOSE, CONTROL_OPEN, CONTROL_REJECT, ChannelRef, Open,
};
use compact_encoding::{Bytes, Codec, Reader, Writer, uint_len};

use crate::Error;

/// One logical message, either standalone or out of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Open(Open),
    /// The peer refused our channel with this local id.
    Reject {
        local_id: u64,
    },
    /// The peer closed its channel with this id.
    Close {
        remote_id: u64,
    },
    /// A control type this version does not know; JS ignores these.
    UnknownControl(u64),
    Message {
        channel: u64,
        kind: u64,
        payload: Bytes,
    },
}

pub enum Frame {
    Single(Item),
    Batch(Batch),
}

/// Decodes one frame. Empty frames must be skipped by the caller, as in JS.
pub fn decode(buf: Bytes) -> Result<Frame, Error> {
    let mut r = Reader::new(buf);
    let channel = r.uint()?;
    let kind = r.uint()?;
    if channel == 0 && kind == CONTROL_BATCH {
        let channel = r.uint()?;
        return Ok(Frame::Batch(Batch { r, channel }));
    }
    item(channel, kind, &mut r).map(Frame::Single)
}

fn item(channel: u64, kind: u64, r: &mut Reader) -> Result<Item, Error> {
    if channel != 0 {
        return Ok(Item::Message {
            channel,
            kind,
            payload: r.raw(),
        });
    }
    Ok(match kind {
        CONTROL_OPEN => Item::Open(Open::decode(r)?),
        CONTROL_REJECT => Item::Reject {
            local_id: ChannelRef::decode(r)?.id,
        },
        CONTROL_CLOSE => Item::Close {
            remote_id: ChannelRef::decode(r)?.id,
        },
        // JS would recurse; no JS or Rust sender ever nests batches.
        CONTROL_BATCH => return Err(Error::NestedBatch),
        other => Item::UnknownControl(other),
    })
}

/// Iterator over the items of a batch frame.
pub struct Batch {
    r: Reader,
    channel: u64,
}

impl Iterator for Batch {
    type Item = Result<Item, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut step = || -> Result<Option<Item>, Error> {
            loop {
                if self.r.is_empty() {
                    return Ok(None);
                }
                let len = self.r.uint()?;
                if len == 0 {
                    self.channel = self.r.uint()?;
                    continue;
                }
                // JS clamps an overlong entry to the end of the frame.
                let n = (len as usize).min(self.r.remaining());
                let mut entry = Reader::new(self.r.fixed_bytes(n)?);
                let kind = entry.uint()?;
                return item(self.channel, kind, &mut entry).map(Some);
            }
        };
        match step() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(e) => {
                // Stop after the first error.
                self.r = Reader::new(Bytes::new());
                Some(Err(e))
            }
        }
    }
}

/// Encodes a standalone frame: `uint channel, uint kind, body`.
pub(crate) fn single(
    channel: u64,
    kind: u64,
    body_len: usize,
    body: impl FnOnce(&mut Writer),
) -> Bytes {
    let mut w = Writer::with_capacity(uint_len(channel) + uint_len(kind) + body_len);
    w.uint(channel);
    w.uint(kind);
    body(&mut w);
    w.finish()
}

/// Accumulates corked messages into one batch frame.
#[derive(Debug)]
pub(crate) struct BatchWriter {
    w: Writer,
    prev: Option<u64>,
}

impl BatchWriter {
    pub(crate) fn new() -> Self {
        let mut w = Writer::new();
        w.uint(0);
        w.uint(CONTROL_BATCH);
        Self { w, prev: None }
    }

    pub(crate) fn len(&self) -> usize {
        self.w.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.prev.is_none()
    }

    pub(crate) fn push(
        &mut self,
        channel: u64,
        kind: u64,
        body_len: usize,
        body: impl FnOnce(&mut Writer),
    ) {
        if self.prev != Some(channel) {
            if self.prev.is_some() {
                self.w.uint(0);
            }
            self.w.uint(channel);
            self.prev = Some(channel);
        }
        self.w.uint((uint_len(kind) + body_len) as u64);
        self.w.uint(kind);
        body(&mut self.w);
    }

    pub(crate) fn finish(self) -> Bytes {
        self.w.finish()
    }
}
