//! In-process framed transport, for tests and same-process peers.

use std::io;

use compact_encoding::Bytes;
use tokio::sync::mpsc;

use crate::{FrameRead, FrameWrite};

pub struct MemoryReader(mpsc::Receiver<Bytes>);
pub struct MemoryWriter(Option<mpsc::Sender<Bytes>>);

/// Two connected ends. Each direction holds at most `capacity` frames, so a
/// slow reader pushes back on the writer.
pub fn duplex(capacity: usize) -> ((MemoryReader, MemoryWriter), (MemoryReader, MemoryWriter)) {
    let (a_tx, a_rx) = mpsc::channel(capacity);
    let (b_tx, b_rx) = mpsc::channel(capacity);
    (
        (MemoryReader(b_rx), MemoryWriter(Some(a_tx))),
        (MemoryReader(a_rx), MemoryWriter(Some(b_tx))),
    )
}

impl FrameRead for MemoryReader {
    async fn read(&mut self) -> io::Result<Option<Bytes>> {
        Ok(self.0.recv().await)
    }
}

impl FrameWrite for MemoryWriter {
    async fn write(&mut self, frame: Bytes) -> io::Result<()> {
        let tx = self.0.as_ref().ok_or(io::ErrorKind::BrokenPipe)?;
        tx.send(frame)
            .await
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    async fn close(&mut self) -> io::Result<()> {
        self.0 = None;
        Ok(())
    }
}
