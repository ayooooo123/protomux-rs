use std::time::Duration;

use protomux::{Channel, ChannelOptions, Event, Mux, StreamInfo, memory};
use tokio::time::timeout;

fn pair_of_muxes(capacity: usize) -> (Mux, Mux) {
    let ((ar, aw), (br, bw)) = memory::duplex(capacity);
    (
        Mux::new(ar, aw, StreamInfo::default()),
        Mux::new(br, bw, StreamInfo::default()),
    )
}

#[tokio::test]
async fn udx_rtt_provider_is_live_shared_and_optional() {
    use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
    let (a, b) = pair_of_muxes(16);
    assert_eq!(a.udx_rtt(), None);
    let rtt = Arc::new(AtomicU32::new(0));
    let sample = rtt.clone();
    a.set_udx_rtt(move || sample.load(Ordering::Relaxed));
    assert_eq!(a.clone().udx_rtt(), Some(0));
    rtt.store(125, Ordering::Relaxed);
    assert_eq!(a.udx_rtt(), Some(125));
    assert_eq!(b.udx_rtt(), None);
    a.destroy();
    b.destroy();
}

async fn next(ch: &mut Channel) -> Event {
    timeout(Duration::from_secs(5), ch.recv())
        .await
        .expect("timed out")
        .expect("channel ended")
}

async fn msg(ch: &mut Channel) -> (u64, Vec<u8>) {
    match next(ch).await {
        Event::Message(m) => (m.kind, m.data.to_vec()),
        other => panic!("expected message, got {other:?}"),
    }
}

fn opts<'a>(protocol: &'a str, id: &'a [u8], handshake: &'a [u8]) -> ChannelOptions<'a> {
    ChannelOptions {
        id: Some(id),
        handshake,
        ..ChannelOptions::new(protocol)
    }
}

#[tokio::test]
async fn simultaneous_opens_pair_and_exchange_handshakes() {
    let (a, b) = pair_of_muxes(16);
    let mut ca = a.open_channel(opts("p", b"id", b"from-a")).unwrap();
    let mut cb = b.open_channel(opts("p", b"id", b"from-b")).unwrap();
    assert_eq!(&ca.opened().await.unwrap()[..], b"from-b");
    assert_eq!(&cb.opened().await.unwrap()[..], b"from-a");
    assert!(a.is_opened("p", Some(b"id")));

    ca.send_raw(3, b"ping");
    assert_eq!(msg(&mut cb).await, (3, b"ping".to_vec()));
    cb.send_raw(4, b"pong");
    assert_eq!(msg(&mut ca).await, (4, b"pong".to_vec()));
}

#[tokio::test]
async fn same_protocol_different_ids_are_separate_channels() {
    let (a, b) = pair_of_muxes(16);
    let mut a1 = a.open_channel(opts("p", b"1", b"")).unwrap();
    let mut a2 = a.open_channel(opts("p", b"2", b"")).unwrap();
    let mut b2 = b.open_channel(opts("p", b"2", b"")).unwrap();
    let mut b1 = b.open_channel(opts("p", b"1", b"")).unwrap();
    for ch in [&mut a1, &mut a2, &mut b1, &mut b2] {
        ch.opened().await.unwrap();
    }
    a1.send_raw(0, b"one");
    a2.send_raw(0, b"two");
    assert_eq!(msg(&mut b1).await.1, b"one");
    assert_eq!(msg(&mut b2).await.1, b"two");
}

#[tokio::test]
async fn pair_request_opens_on_demand_and_gets_buffered_messages() {
    let (a, b) = pair_of_muxes(16);
    let mut requests = b.pair("lazy", None);

    let mut ca = a.open_channel(opts("lazy", b"core-7", b"hs")).unwrap();
    // Sent before the remote has paired: buffered on the remote side.
    ca.send_raw(1, b"early-1");
    ca.send_raw(1, b"early-2");

    let req = timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(req.protocol, "lazy");
    assert_eq!(&req.id.as_ref().unwrap()[..], b"core-7");
    tokio::time::sleep(Duration::from_millis(20)).await; // async work while holding the request
    let mut cb = b.open_channel(opts("lazy", b"core-7", b"")).unwrap();
    drop(req);

    assert_eq!(&cb.opened().await.unwrap()[..], b"hs");
    assert_eq!(msg(&mut cb).await.1, b"early-1");
    assert_eq!(msg(&mut cb).await.1, b"early-2");
    ca.opened().await.unwrap();
}

#[tokio::test]
async fn unanswered_open_is_rejected() {
    let (a, b) = pair_of_muxes(16);
    b.ready();
    let mut requests = b.pair("maybe", None);
    let mut ca = a.open_channel(opts("maybe", b"x", b"")).unwrap();
    drop(requests.recv().await.unwrap()); // declined
    assert!(matches!(
        next(&mut ca).await,
        Event::Closed { remote: true }
    ));

    // No pair handler and setup is done: rejected straight away, as in JS.
    let mut cn = a.open_channel(opts("nobody", b"x", b"")).unwrap();
    assert!(matches!(
        next(&mut cn).await,
        Event::Closed { remote: true }
    ));
    assert!(cn.recv().await.is_none());
}

/// The open lands while the caller is still attaching protocols, so it waits;
/// `ready()` then declares setup done and rejects it, exactly like a JS mux
/// that had no notify registered when it parsed the frame.
#[tokio::test]
async fn open_before_ready_is_held_then_rejected_on_ready() {
    let (a, b) = pair_of_muxes(16);
    let mut cn = a.open_channel(opts("nobody", b"x", b"")).unwrap();
    assert!(
        timeout(Duration::from_millis(100), cn.recv())
            .await
            .is_err(),
        "an open that arrives during setup is held, not rejected"
    );
    b.ready();
    assert!(matches!(
        next(&mut cn).await,
        Event::Closed { remote: true }
    ));
    assert!(cn.recv().await.is_none());
}

/// `ready()` only closes the setup window: a pairer registered before or
/// after it still answers opens, and `ready()` twice is a no-op.
#[tokio::test]
async fn ready_keeps_paired_protocols_working() {
    let (a, b) = pair_of_muxes(16);
    let mut requests = b.pair("p", None);
    b.ready();
    b.ready();
    let mut ca = a.open_channel(opts("p", b"id", b"hs")).unwrap();
    let req = requests.recv().await.unwrap();
    let mut cb = b.open_channel(opts("p", b"id", b"")).unwrap();
    drop(req);
    assert_eq!(&cb.opened().await.unwrap()[..], b"hs");
    ca.opened().await.unwrap();
}

/// A request already delivered to a pairer is still the caller's to answer
/// after `ready()`; a later open for the same key is rejected meanwhile.
#[tokio::test]
async fn ready_rejects_an_open_while_an_earlier_request_is_pending() {
    let (a, b) = pair_of_muxes(16);
    let mut requests = b.pair("p", None);
    let mut first = a.open_channel(opts("p", b"1", b"")).unwrap();
    let req = requests.recv().await.unwrap(); // delivered, not answered yet
    b.unpair("p", None);
    b.ready();

    let mut second = a.open_channel(opts("p", b"2", b"")).unwrap();
    assert!(matches!(
        next(&mut second).await,
        Event::Closed { remote: true }
    ));
    assert!(second.recv().await.is_none());

    let mut cb = b.open_channel(opts("p", b"1", b"")).unwrap();
    first.opened().await.unwrap();
    cb.opened().await.unwrap();
    drop(req);
    first.send_raw(0, b"still-open");
    assert_eq!(msg(&mut cb).await.1, b"still-open");
}

/// The reader runs from `Mux::new` on, so a remote open can land before the
/// caller attached its protocols; it must still pair, by a late `pair` or a
/// late `open_channel`, with the messages sent behind it.
#[tokio::test]
async fn opens_before_any_pairer_wait_for_it() {
    let (a, b) = pair_of_muxes(16);
    let mut by_pair = a.open_channel(opts("late", b"1", b"hs-1")).unwrap();
    by_pair.send_raw(0, b"early");
    let mut by_open = a.open_channel(opts("late", b"2", b"hs-2")).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await; // both opens are read by b

    let mut cb2 = b.open_channel(opts("late", b"2", b"")).unwrap();
    assert_eq!(&cb2.opened().await.unwrap()[..], b"hs-2");
    by_open.opened().await.unwrap();

    let mut requests = b.pair("late", None);
    let req = timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&req.id.as_ref().unwrap()[..], b"1");
    let mut cb1 = b.open_channel(opts("late", b"1", b"")).unwrap();
    drop(req);
    assert_eq!(&cb1.opened().await.unwrap()[..], b"hs-1");
    assert_eq!(msg(&mut cb1).await.1, b"early");
    by_pair.opened().await.unwrap();
    assert!(
        timeout(Duration::from_millis(100), requests.recv())
            .await
            .is_err(),
        "the open settled by open_channel is not offered again"
    );

    // Nothing is left unpaired, so ready() leaves both settled channels alone.
    b.ready();
    cb1.send_raw(0, b"after-ready");
    assert_eq!(msg(&mut by_pair).await.1, b"after-ready");
}

#[tokio::test]
async fn close_reaches_remote_and_ids_are_reused() {
    let (a, b) = pair_of_muxes(16);
    for round in 0..3u8 {
        let mut ca = a.open_channel(opts("c", &[round], b"")).unwrap();
        let mut cb = b.open_channel(opts("c", &[round], b"")).unwrap();
        ca.opened().await.unwrap();
        cb.opened().await.unwrap();
        assert_eq!(ca.local_id(), 1, "local id is recycled after close");
        ca.close();
        assert!(matches!(
            next(&mut ca).await,
            Event::Closed { remote: false }
        ));
        assert!(matches!(
            next(&mut cb).await,
            Event::Closed { remote: true }
        ));
        assert!(!ca.send_raw(0, b"late"));
    }
}

#[tokio::test]
async fn unique_refuses_a_second_channel() {
    let (a, _b) = pair_of_muxes(16);
    let _c = a.open_channel(opts("u", b"1", b"")).unwrap();
    assert!(a.open_channel(opts("u", b"1", b"")).is_none());
    let shared = ChannelOptions {
        unique: false,
        ..opts("u", b"1", b"")
    };
    assert!(a.open_channel(shared).is_some());
}

#[tokio::test]
async fn remote_open_under_an_alias_pairs_with_our_channel() {
    let (a, b) = pair_of_muxes(16);
    let mut ca = a
        .open_channel(ChannelOptions {
            aliases: &["old-name"],
            ..opts("new-name", b"k", b"")
        })
        .unwrap();
    assert!(a.is_opened("old-name", Some(b"k")));
    let mut requests = b.pair("new-name", None);
    let _cb = b.open_channel(opts("old-name", b"k", b"")).unwrap();
    assert!(
        ca.opened().await.is_some(),
        "the alias open paired with new-name"
    );
    drop(requests.recv().await); // b declines new-name, which a has already answered
}

#[tokio::test]
async fn corked_sends_arrive_in_order_across_channels() {
    let (a, b) = pair_of_muxes(16);
    let mut x = a.open_channel(opts("x", b"", b"")).unwrap();
    let mut bx = b.open_channel(opts("x", b"", b"")).unwrap();
    x.opened().await.unwrap();
    bx.opened().await.unwrap();

    let mut requests = b.pair("y", None);
    a.cork();
    x.send_raw(0, b"1");
    let mut y = a.open_channel(opts("y", b"", b"")).unwrap(); // the open goes into the batch too
    y.send_raw(0, b"2");
    x.send_raw(0, b"3");
    a.uncork();

    let req = requests.recv().await.unwrap();
    let mut by = b.open_channel(opts("y", b"", b"")).unwrap();
    drop(req);
    assert_eq!(msg(&mut bx).await.1, b"1");
    assert_eq!(msg(&mut bx).await.1, b"3");
    by.opened().await.unwrap();
    assert_eq!(msg(&mut by).await.1, b"2");
    y.opened().await.unwrap();
}

#[tokio::test]
async fn slow_consumer_pushes_back_on_the_sender() {
    let (a, b) = pair_of_muxes(1);
    let ca = a.open_channel(opts("bulk", b"", b"")).unwrap();
    let mut cb = b.open_channel(opts("bulk", b"", b"")).unwrap();
    cb.opened().await.unwrap();

    let chunk = vec![7u8; 64 * 1024];
    let bound = protomux::INBOUND_BUDGET + protomux::OUTBOUND_HIGH_WATER + 8 * chunk.len();
    let mut sent = 0;
    // The receiver holds every message, so sending must stall for good once its
    // inbound budget and our outbound queue are both full.
    loop {
        if ca.send_raw(0, &chunk) {
            sent += chunk.len();
        } else if timeout(Duration::from_millis(200), a.drained())
            .await
            .is_err()
        {
            break;
        }
        assert!(sent <= bound, "sender never saw back-pressure");
    }
    assert!(
        sent >= protomux::INBOUND_BUDGET,
        "stalled before the receiver's budget was used"
    );
    let waiting = tokio::spawn({
        let a = a.clone();
        async move { a.drained().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !waiting.is_finished(),
        "drained() resolved while the receiver was stalled"
    );

    let mut received = 0;
    while received < sent {
        let (_, data) = msg(&mut cb).await; // dropping messages frees the budget
        received += data.len();
    }
    timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn destroy_closes_both_sides() {
    let (a, b) = pair_of_muxes(16);
    let mut ca = a.open_channel(opts("d", b"", b"")).unwrap();
    let mut cb = b.open_channel(opts("d", b"", b"")).unwrap();
    ca.opened().await.unwrap();
    cb.opened().await.unwrap();
    a.destroy();
    assert!(matches!(
        next(&mut ca).await,
        Event::Closed { remote: true }
    ));
    assert!(matches!(
        next(&mut cb).await,
        Event::Closed { remote: true }
    ));
    timeout(Duration::from_secs(5), b.closed()).await.unwrap();
    assert!(a.open_channel(opts("d", b"", b"")).is_none());
}

#[tokio::test]
async fn close_before_async_pairing_allows_remote_id_reuse() {
    let (a, b) = pair_of_muxes(16);
    let mut requests = b.pair("lazy", None);
    let mut markers = b.pair("marker", None);
    let mut next_requests = b.pair("next", None);
    let old = a.open_channel(opts("lazy", b"id", b"")).unwrap();
    let old_id = old.local_id();
    old.send_raw(0, b"buffered before close");
    old.close();
    let request = requests.recv().await.unwrap();

    // The marker proves B parsed the close while its pair request was still held.
    let mut marker_a = a.open_channel(opts("marker", b"", b"")).unwrap();
    let marker_request = markers.recv().await.unwrap();
    let late = b.open_channel(opts("lazy", b"id", b"")).unwrap();
    drop(request);
    let mut marker_b = b.open_channel(opts("marker", b"", b"")).unwrap();
    drop(marker_request);
    marker_a.opened().await.unwrap();
    marker_b.opened().await.unwrap();

    // A parsed the late acknowledgement and can reuse its closed channel's id.
    let mut next_a = a.open_channel(opts("next", b"id", b"")).unwrap();
    assert_eq!(next_a.local_id(), old_id);
    let next_request = timeout(Duration::from_secs(5), next_requests.recv())
        .await
        .expect("reused channel id was not accepted")
        .expect("mux was destroyed by the reused channel id");
    let mut next_b = b.open_channel(opts("next", b"id", b"")).unwrap();
    drop(next_request);
    next_a.opened().await.unwrap();
    next_b.opened().await.unwrap();
    next_a.send_raw(0, b"still replicating");
    assert_eq!(msg(&mut next_b).await.1, b"still replicating");
    assert!(!a.is_destroyed());
    assert!(!b.is_destroyed());
    late.close();
    a.destroy();
    b.destroy();
}

#[tokio::test]
async fn close_before_setup_does_not_leave_an_unasked_pair_request() {
    let (a, b) = pair_of_muxes(16);
    let mut markers = b.pair("marker", None);
    let old = a.open_channel(opts("lazy", b"id", b"")).unwrap();
    old.close();
    let _marker = a.open_channel(opts("marker", b"", b"")).unwrap();
    let marker_request = markers.recv().await.unwrap();
    let mut requests = b.pair("lazy", None);
    assert!(matches!(
        requests.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    drop(marker_request);
    a.destroy();
    b.destroy();
}

#[tokio::test]
async fn ready_rejects_only_the_unasked_tail_of_a_pairing_queue() {
    let (a, b) = pair_of_muxes(16);
    let mut requests = b.pair("p", None);
    let mut markers = b.pair("marker", None);
    let mut first = a.open_channel(opts("p", b"id", b"")).unwrap();
    let request = requests.recv().await.unwrap();
    b.unpair("p", None);
    let mut second = a
        .open_channel(ChannelOptions {
            unique: false,
            ..opts("p", b"id", b"")
        })
        .unwrap();
    let _marker = a.open_channel(opts("marker", b"", b"")).unwrap();
    let marker_request = markers.recv().await.unwrap();

    b.ready();
    assert!(matches!(
        next(&mut second).await,
        Event::Closed { remote: true }
    ));
    let mut paired = b.open_channel(opts("p", b"id", b"")).unwrap();
    drop(request);
    assert!(first.opened().await.is_some());
    assert!(paired.opened().await.is_some());
    drop(marker_request);
    a.destroy();
    b.destroy();
}
