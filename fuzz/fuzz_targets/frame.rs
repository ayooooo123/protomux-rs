#![no_main]
//! Input is a sequence of `[u16 le length][frame]`. Each frame goes through the
//! decoder, then the whole sequence is fed to a live Mux as a remote peer
//! would send it. Neither may panic.

use std::sync::LazyLock;
use std::time::Duration;

use libfuzzer_sys::fuzz_target;
use protomux::{ChannelOptions, FrameWrite, Mux, StreamInfo, frame, memory};

static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

fn frames(mut data: &[u8]) -> Vec<bytes::Bytes> {
    let mut out = vec![];
    while data.len() >= 2 {
        let n = (u16::from_le_bytes([data[0], data[1]]) as usize).min(data.len() - 2);
        out.push(bytes::Bytes::copy_from_slice(&data[2..2 + n]));
        data = &data[2 + n..];
    }
    out
}

fuzz_target!(|data: &[u8]| {
    let frames = frames(data);
    for f in &frames {
        if let Ok(frame::Frame::Batch(items)) = frame::decode(f.clone()) {
            for _ in items {}
        }
    }

    RT.block_on(async {
        let ((ar, aw), (_br, mut remote)) = memory::duplex(64);
        let mux = Mux::new(ar, aw, StreamInfo::default());
        let mut requests = mux.pair("p", None);
        let _outgoing = mux.open_channel(ChannelOptions::new("p"));
        let acceptor = tokio::spawn({
            let mux = mux.clone();
            async move {
                let mut open = vec![];
                while let Some(req) = requests.recv().await {
                    open.push(mux.open_channel(ChannelOptions {
                        id: req.id.as_deref(),
                        ..ChannelOptions::new("p")
                    }));
                }
            }
        });
        for f in frames {
            if remote.write(f).await.is_err() {
                break;
            }
        }
        let _ = remote.close().await;
        let _ = tokio::time::timeout(Duration::from_secs(2), mux.closed()).await;
        mux.destroy();
        acceptor.abort();
    });
});
