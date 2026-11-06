//! Protomux over peeroxide's secret-stream (the Hyperswarm/HyperDHT transport).

use std::io;

use compact_encoding::Bytes;
use peeroxide_dht::secret_stream::{
    SecretStream, SecretStreamError, SecretStreamReadHalf, SecretStreamWriteHalf,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::{FrameRead, FrameWrite, Mux, StreamInfo};

fn io_err(e: SecretStreamError) -> io::Error {
    match e {
        SecretStreamError::Io(e) => e,
        other => io::Error::other(other.to_string()),
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> FrameRead for SecretStreamReadHalf<R> {
    async fn read(&mut self) -> io::Result<Option<Bytes>> {
        Ok(SecretStreamReadHalf::read(self)
            .await
            .map_err(io_err)?
            .map(Bytes::from))
    }
}

impl<W: AsyncWrite + Unpin + Send + 'static> FrameWrite for SecretStreamWriteHalf<W> {
    async fn write(&mut self, frame: Bytes) -> io::Result<()> {
        SecretStreamWriteHalf::write(self, &frame)
            .await
            .map_err(io_err)
    }

    async fn close(&mut self) -> io::Result<()> {
        self.shutdown().await.map_err(io_err)
    }
}

impl Mux {
    /// Runs a mux over an established secret-stream.
    pub fn from_secret_stream<T>(stream: SecretStream<T>) -> Mux
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        ReadHalf<T>: Send,
        WriteHalf<T>: Send,
    {
        let info = StreamInfo {
            is_initiator: stream.is_initiator(),
            handshake_hash: Some(*stream.handshake_hash()),
            remote_public_key: Some(*stream.remote_public_key()),
        };
        let (r, w) = stream.into_split();
        Mux::new(r, w, info)
    }
}
