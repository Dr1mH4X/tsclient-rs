use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::client::Client;
use crate::{AbortSignal, ClientOptions, Error, ErrorCode};

use super::*;

type Responder = dyn Fn(Arc<Client>, &str, i32, &str) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
    + Send
    + Sync;

fn make_client() -> Arc<Client> {
    Arc::new(Client::new(
        crate::crypto::generate_identity(0),
        "127.0.0.1:9987".to_string(),
        "Tester".to_string(),
        ClientOptions::default(),
    ))
}

fn parse_rc(line: &str) -> i32 {
    line.split_whitespace()
        .find_map(|part| part.strip_prefix("return_code="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn install_mock_server(
    client: &Arc<Client>,
    sent: Arc<Mutex<Vec<String>>>,
    responder: Arc<Responder>,
) {
    let weak = Arc::downgrade(client);
    client.install_stream_test_handler(move |line: String| {
        sent.lock().unwrap().push(line.clone());
        let rc = parse_rc(&line);
        let cmd = line.split_whitespace().next().unwrap_or("").to_string();
        let responder = Arc::clone(&responder);
        let weak = weak.clone();
        Box::pin(async move {
            let Some(client) = weak.upgrade() else {
                return Err(Error::Teamspeak("not connected".into()));
            };
            responder(client, &cmd, rc, &line).await
        })
    });
}

fn default_responder() -> Arc<Responder> {
    Arc::new(|client, _cmd, rc, _line| {
        Box::pin(async move {
            tokio::spawn(async move {
                client.resolve_stream_test_rc(rc, None);
            });
            Ok(())
        })
    })
}

fn setup_connected_p2p_client(self_clid: i32) -> (Arc<Client>, Arc<Mutex<Vec<String>>>) {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    install_mock_server(&client, Arc::clone(&sent), default_responder());
    client.inject_stream_test_line(
        self_clid,
        "initserver virtualserver_version=6.0.0-beta13.1 virtualserver_capability_extensions=FILETIME,STREAM_P2P",
    );
    client.set_stream_test_connected(self_clid);
    (client, sent)
}

async fn wait_for<F: FnMut() -> bool>(mut cond: F) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition was not satisfied within timeout");
}

async fn recv_within<E: std::fmt::Debug>(sub: &mut StreamSubscription<E>) -> E {
    tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("timed out waiting for event")
        .expect("subscription closed unexpectedly")
}

#[tokio::test]
async fn unsupported_server_refuses_publish_watch_and_skips_discovery() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    install_mock_server(&client, Arc::clone(&sent), default_responder());
    let streams = Streams::new(&client, StreamsOptions::default());

    assert!(!streams.supported());
    assert!(matches!(
        streams.publish(PublishOptions::new("Screen")).await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(sent.lock().unwrap().is_empty());

    // TS3 server without STREAM_P2P
    client.inject_stream_test_line(1, "initserver virtualserver_version=3.13.8");
    client.set_stream_test_connected(1);
    assert!(!streams.supported());

    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=5 client_nickname=Peer client_is_streaming=1",
    );
    tokio::task::yield_now().await;
    assert!(sent.lock().unwrap().is_empty());
    assert!(matches!(
        streams.watch("s1", WatchOptions::default()).await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn discovers_already_streaming_members_when_attached_after_connect() {
    let (client, sent) = setup_connected_p2p_client(1);
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=1 client_nickname=Self client_is_streaming=1",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=5 client_nickname=Streamer client_is_streaming=1",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=6 client_nickname=Viewer client_is_streaming=0",
    );
    client.inject_stream_test_line(1, "channellistfinished");

    let streams = Streams::new(&client, StreamsOptions::default());
    assert!(streams.supported());

    wait_for(|| sent.lock().unwrap().len() == 1).await;
    assert!(sent.lock().unwrap()[0].starts_with("requeststreaminfo clid=5"));

    client.inject_stream_test_line(
        1,
        "notifystreaminfo id=s-5 clid=5 name=Live\\sDesk type=2 audio=1 bitrate=2000000 accessibility=1 mode=1 viewer_limit=0 viewer=0",
    );
    wait_for(|| !streams.list().is_empty()).await;
    let list = streams.list();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, "s-5");
    assert_eq!(list[0].name, "Live Desk");
}

#[tokio::test]
async fn discovers_streaming_members_on_delayed_directory_snapshot() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    install_mock_server(&client, Arc::clone(&sent), default_responder());
    let streams = Streams::new(&client, StreamsOptions::default());

    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=8 client_nickname=PreJoinStreamer client_is_streaming=1",
    );
    // Before connected / channellistfinished nothing has been sent yet.
    assert!(sent.lock().unwrap().is_empty());

    client.set_stream_test_connected(1);
    client.inject_stream_test_line(1, "channellistfinished");

    wait_for(|| sent.lock().unwrap().len() == 1).await;
    assert!(sent.lock().unwrap()[0].starts_with("requeststreaminfo clid=8"));
    assert!(streams.list().is_empty());
}

#[tokio::test]
async fn discovers_on_enter_update_and_resyncs_on_notification_overflow() {
    let (client, sent) = setup_connected_p2p_client(1);
    let streams = Streams::new(&client, StreamsOptions::default());
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=5 client_nickname=Peer client_is_streaming=0",
    );
    client.inject_stream_test_line(1, "channellistfinished");
    assert!(sent.lock().unwrap().is_empty());

    // Flip client_is_streaming 0 -> 1 triggers discovery once.
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_is_streaming=1");
    wait_for(|| sent.lock().unwrap().len() == 1).await;
    assert!(sent.lock().unwrap()[0].starts_with("requeststreaminfo clid=5"));

    // Unrelated update while still streaming does not re-ask.
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_away=1");
    tokio::task::yield_now().await;
    assert_eq!(sent.lock().unwrap().len(), 1);

    // NotificationOverflow clears asked state and re-queries directory streaming clients.
    client.trigger_stream_test_overflow();
    wait_for(|| sent.lock().unwrap().len() == 2).await;
    assert!(sent.lock().unwrap()[1].starts_with("requeststreaminfo clid=5"));

    // Leaving and re-entering triggers discovery again.
    client.inject_stream_test_line(1, "notifyclientleftview clid=5 reasonid=4 cid=1 ctid=2");
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=5 client_nickname=Peer client_is_streaming=1",
    );
    wait_for(|| sent.lock().unwrap().len() == 3).await;
    assert!(sent.lock().unwrap()[2].starts_with("requeststreaminfo clid=5"));
    let _ = streams;
}

#[tokio::test]
async fn all_eight_notifications_and_signaling_variants_dispatch_cleanly() {
    let (client, _sent) = setup_connected_p2p_client(2);
    let streams = Streams::new(&client, StreamsOptions::default());
    let mut sub = streams.subscribe();

    // 1. notifystreamstarted
    client.inject_stream_test_line(
        2,
        "notifystreamstarted id=s1 clid=5 name=Game type=2 audio=0 bitrate=1500000 access=1 mode=1 viewer_limit=8",
    );
    match recv_within(&mut sub).await {
        StreamsEvent::Started(info) => {
            assert_eq!(info.id, "s1");
            assert_eq!(info.client_id, 5);
            assert_eq!(info.accessibility, Some(StreamAccessibility::Public));
            assert_eq!(info.transport, Some(StreamTransport::P2p));
        }
        other => panic!("expected Started, got {other:?}"),
    }

    // 2. notifystreaminfo (update + empty ignored)
    client.inject_stream_test_line(2, "notifystreaminfo return_code=9");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(sub.try_recv().is_err());
    client.inject_stream_test_line(2, "notifystreaminfo id=s1 viewer=3");
    match recv_within(&mut sub).await {
        StreamsEvent::Updated(info) => assert_eq!(info.viewers, Some(3)),
        other => panic!("expected Updated, got {other:?}"),
    }

    // 3. notifystreamclientjoined + 4. notifystreamclientleft
    client.inject_stream_test_line(2, "notifystreamclientjoined id=s1 clid=9");
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::ViewerJoined(StreamViewerEvent { client_id: 9, .. })
    ));
    client.inject_stream_test_line(2, "notifystreamclientleft id=s1 clid=9");
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::ViewerLeft(StreamViewerEvent { client_id: 9, .. })
    ));

    // 5. notifyrespondjoinstreamrequest + 6. notifystreamsignaling
    let streams_for_watch = streams.clone();
    let watch_task =
        tokio::spawn(async move { streams_for_watch.watch("s1", WatchOptions::default()).await });
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Early signal before answer arrives
    client.inject_stream_test_line(
        2,
        r#"notifystreamsignaling id=s1 clid=5 json={"cmd":"iceCandidate","args":{"sdp":"cand-early","mid":"0","mLine":0}}"#,
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    client.inject_stream_test_line(
        2,
        "notifyrespondjoinstreamrequest id=s1 clid=5 decision=1 offer=v=0\\r\\nm=audio\\s9 msg=",
    );
    let view = watch_task.await.unwrap().expect("watch resolved");
    assert_eq!(view.offer(), "v=0\r\nm=audio 9");
    let mut peer_sub = view.subscribe();

    assert!(matches!(
        recv_within(&mut peer_sub).await,
        PeerEvent::Candidate(IceCandidate { candidate, .. }) if candidate == "cand-early"
    ));

    for (line, check) in [
        (
            r#"notifystreamsignaling id=s1 clid=5 json={"cmd":"offer","args":{"offer":"v=re"}}"#,
            "offer",
        ),
        (
            r#"notifystreamsignaling id=s1 clid=5 json={"cmd":"reconnectOffer","args":{"sdp":"v=rec"}}"#,
            "reconnect",
        ),
        (
            r#"notifystreamsignaling id=s1 clid=5 json={"cmd":"answer","args":{"answer":"v=ans"}}"#,
            "answer",
        ),
        (
            r#"notifystreamsignaling id=s1 clid=5 json={"cmd":"custom","args":{"k":1}}"#,
            "raw",
        ),
    ] {
        client.inject_stream_test_line(2, line);
        let ev = recv_within(&mut peer_sub).await;
        match (check, ev) {
            ("offer", PeerEvent::Offer(s)) => assert_eq!(s, "v=re"),
            ("reconnect", PeerEvent::Offer(s)) => assert_eq!(s, "v=rec"),
            ("answer", PeerEvent::Answer(s)) => assert_eq!(s, "v=ans"),
            ("raw", PeerEvent::Signal(r)) => assert_eq!(r.cmd, "custom"),
            other => panic!("unexpected signal event: {other:?}"),
        }
    }

    // 7. notifystreamstopped
    client.inject_stream_test_line(2, "notifystreamstopped id=s1 reason=42");
    match recv_within(&mut sub).await {
        StreamsEvent::Stopped(ev) => {
            assert_eq!(ev.info.id, "s1");
            assert_eq!(ev.cause, StreamEndCause::Server);
            assert_eq!(ev.reason, 42);
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
    assert!(matches!(
        recv_within(&mut peer_sub).await,
        PeerEvent::Closed(PeerClosed {
            cause: PeerCloseCause::Stopped,
            reason: Some(42)
        })
    ));

    // 8. notifyjoinstreamrequest verified separately below in dedicated publisher tests.
}

#[tokio::test]
async fn gone_grace_drops_stream_after_two_seconds_unless_stopped_or_resumed() {
    assert_eq!(super::streams::GONE_GRACE_MS, 2_000);
    let (client, _sent) = setup_connected_p2p_client(1);
    let streams = Streams::new_with_grace(
        &client,
        StreamsOptions::default(),
        Duration::from_millis(60),
    );
    let mut sub = streams.subscribe();

    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=5 client_nickname=Pub client_is_streaming=1",
    );
    client.inject_stream_test_line(1, "channellistfinished");
    client.inject_stream_test_line(1, "notifystreamstarted id=s-gone clid=5 name=S mode=1");
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::Started(_)
    ));

    // Unrelated update while streaming flag unchanged must NOT start grace timer.
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_away=1");
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert!(streams.get("s-gone").is_some());

    // Flag clears -> starts grace timer; returning before deadline cancels it.
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_is_streaming=0");
    tokio::time::sleep(Duration::from_millis(15)).await;
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_is_streaming=1");
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(streams.get("s-gone").is_some());

    // Clear again and let grace period elapse -> ends with Gone.
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_is_streaming=0");
    wait_for(|| streams.get("s-gone").is_none()).await;
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::Stopped(StreamStoppedEvent {
            cause: StreamEndCause::Gone,
            reason: 0,
            ..
        })
    ));
}

#[tokio::test]
async fn publish_matches_return_code_concurrently_and_cleans_up_orphans() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let pending_setups: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let setups_ref = Arc::clone(&pending_setups);

    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(move |client, cmd, rc, _line| {
            let setups_ref = Arc::clone(&setups_ref);
            let cmd = cmd.to_string();
            Box::pin(async move {
                if cmd == "setupstream" {
                    setups_ref.lock().unwrap().push(rc);
                } else {
                    tokio::spawn(async move {
                        client.resolve_stream_test_rc(rc, None);
                    });
                }
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    let streams = Streams::new(&client, StreamsOptions::default());

    // Concurrent same-name publishes resolved in reverse order, testing delayed error id=0 too.
    let s1 = streams.clone();
    let p1 = tokio::spawn(async move { s1.publish(PublishOptions::new("Same")).await });
    let s2 = streams.clone();
    let p2 = tokio::spawn(async move { s2.publish(PublishOptions::new("Same")).await });
    wait_for(|| pending_setups.lock().unwrap().len() == 2).await;
    let rcs = pending_setups.lock().unwrap().clone();

    client.resolve_stream_test_rc(rcs[1], None);
    client.inject_stream_test_line(
        1,
        &format!(
            "notifystreamstarted id=second clid=1 name=Same mode=1 return_code={}",
            rcs[1]
        ),
    );
    client.inject_stream_test_line(
        1,
        &format!(
            "notifystreamstarted id=first clid=1 name=Same mode=1 return_code={}",
            rcs[0]
        ),
    );
    client.resolve_stream_test_rc(rcs[0], None);

    let pub2 = p2.await.unwrap().expect("second publish");
    let pub1 = p1.await.unwrap().expect("first publish");
    assert_eq!(pub2.id(), "second");
    assert_eq!(pub1.id(), "first");

    // Aborted publish still stops an orphan announced within ANNOUNCE_WINDOW_MS.
    let abort = AbortSignal::new();
    let s3 = streams.clone();
    let mut opts = PublishOptions::new("Orphan");
    opts.abort = Some(abort.clone());
    let p3 = tokio::spawn(async move { s3.publish(opts).await });
    wait_for(|| pending_setups.lock().unwrap().len() == 3).await;
    let orphan_rc = pending_setups.lock().unwrap()[2];
    abort.abort();
    assert!(matches!(p3.await.unwrap(), Err(StreamError::Aborted)));

    client.inject_stream_test_line(
        1,
        &format!(
            "notifystreamstarted id=orphan-1 clid=1 name=Orphan mode=1 return_code={orphan_rc}"
        ),
    );
    client.resolve_stream_test_rc(orphan_rc, None);
    wait_for(|| {
        sent.lock()
            .unwrap()
            .iter()
            .any(|cmd| cmd.starts_with("stopstream id=orphan-1"))
    })
    .await;
    // Late info row for stopped orphan must not resurrect it.
    client.inject_stream_test_line(1, "notifystreaminfo id=orphan-1 clid=1 name=Orphan");
    assert!(streams.get("orphan-1").is_none());
}

#[tokio::test]
async fn publish_refuses_stream_stopped_before_setupstream_answer() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let pending_rc = Arc::new(Mutex::new(None));
    let pending_ref = Arc::clone(&pending_rc);

    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(move |_client, cmd, rc, _line| {
            let pending_ref = Arc::clone(&pending_ref);
            let cmd = cmd.to_string();
            Box::pin(async move {
                if cmd == "setupstream" {
                    *pending_ref.lock().unwrap() = Some(rc);
                }
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    let streams = Streams::new(&client, StreamsOptions::default());

    let s = streams.clone();
    let task = tokio::spawn(async move { s.publish(PublishOptions::new("Race")).await });
    wait_for(|| pending_rc.lock().unwrap().is_some()).await;
    let rc = pending_rc.lock().unwrap().unwrap();

    client.inject_stream_test_line(1, "notifystreamstopped id=raced-1 reason=0");
    tokio::time::sleep(Duration::from_millis(15)).await;
    client.inject_stream_test_line(
        1,
        &format!("notifystreamstarted id=raced-1 clid=1 name=Race mode=1 return_code={rc}"),
    );
    client.resolve_stream_test_rc(rc, None);

    assert!(matches!(
        task.await.unwrap(),
        Err(StreamError::Ended(StreamEndedError { stream_id, .. })) if stream_id == "raced-1"
    ));
}

#[tokio::test]
async fn join_request_accept_deny_audio_warning_uid_lookup_and_remember_denied() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let lookup_gate: Arc<Mutex<HashMap<i32, oneshot::Sender<()>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let gate_ref = Arc::clone(&lookup_gate);

    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(move |client, cmd, rc, line| {
            let cmd = cmd.to_string();
            let line = line.to_string();
            let gate_ref = Arc::clone(&gate_ref);
            Box::pin(async move {
                if cmd == "setupstream" {
                    tokio::spawn(async move {
                        client.inject_stream_test_line(
                            1,
                            &format!("notifystreamstarted id=pub-1 clid=1 name=Pub mode=1 return_code={rc}"),
                        );
                        client.resolve_stream_test_rc(rc, None);
                    });
                } else if cmd == "clientgetuidfromclid" {
                    let clid: i32 = line
                        .split_whitespace()
                        .find_map(|p| p.strip_prefix("clid="))
                        .unwrap()
                        .parse()
                        .unwrap();
                    let (tx, rx) = oneshot::channel();
                    gate_ref.lock().unwrap().insert(clid, tx);
                    tokio::spawn(async move {
                        let _ = rx.await;
                        client.inject_stream_test_line(
                            1,
                            &format!("notifyclientuidfromclid clid={clid} cluid=uid-{clid} nickname=Far{clid} mytsid="),
                        );
                        client.resolve_stream_test_rc(rc, None);
                    });
                } else {
                    tokio::spawn(async move {
                        client.resolve_stream_test_rc(rc, None);
                    });
                }
                Ok(())
            })
        }),
    );

    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=9 client_unique_identifier=near-uid client_nickname=Near client_is_streaming=0",
    );
    client.inject_stream_test_line(1, "channellistfinished");

    let streams = Streams::new(
        &client,
        StreamsOptions {
            remember_denied: true,
        },
    );
    let mut main_sub = streams.subscribe();
    let pub_stream = streams
        .publish(PublishOptions::new("Pub"))
        .await
        .expect("published");
    let mut pub_sub = pub_stream.subscribe();

    // Out-of-view client 40 arrives before in-view client 9: must preserve order.
    client.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-1 clid=40 msg=hi is_remove=0",
    );
    client.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-1 clid=9 msg=hey is_remove=0",
    );
    wait_for(|| lookup_gate.lock().unwrap().contains_key(&40)).await;
    assert!(pub_sub.try_recv().is_err());

    lookup_gate
        .lock()
        .unwrap()
        .remove(&40)
        .unwrap()
        .send(())
        .unwrap();
    let req40 = match pub_sub.recv().await.unwrap() {
        PublishedStreamEvent::Request(req) => req,
        other => panic!("expected Request 40, got {other:?}"),
    };
    let req9 = match pub_sub.recv().await.unwrap() {
        PublishedStreamEvent::Request(req) => req,
        other => panic!("expected Request 9, got {other:?}"),
    };
    assert_eq!(req40.viewer().uid.as_deref(), Some("uid-40"));
    assert_eq!(req40.viewer().nickname.as_deref(), Some("Far40"));
    assert_eq!(req9.viewer().uid.as_deref(), Some("near-uid"));

    // Deny client 9 -> remembered by UID.
    req9.deny("busy").await.expect("denied");
    assert_eq!(req9.state(), JoinRequestState::Denied);
    client.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-1 clid=9 msg=again is_remove=0",
    );
    tokio::task::yield_now().await;
    assert!(pub_sub.try_recv().is_err());

    // Accept client 40 without m=audio -> succeeds + emits OfferWithoutAudio warning.
    let peer = req40
        .accept("v=0\r\nm=video 9", "")
        .await
        .expect("accepted");
    assert_eq!(pub_stream.viewers().len(), 1);
    let warning = wait_for_warning(&mut main_sub).await;
    assert_eq!(warning.code, StreamWarningCode::OfferWithoutAudio);
    assert_eq!(warning.client_id, 40);

    peer.send_offer("v=0\r\nm=video 9")
        .await
        .expect("offer sent");
    let warning2 = wait_for_warning(&mut main_sub).await;
    assert_eq!(warning2.code, StreamWarningCode::OfferWithoutAudio);

    pub_stream.stop(3).await.expect("stopped");
    assert!(pub_stream.ended());
    assert!(peer.closed());
    assert!(matches!(
        pub_sub.recv().await,
        Some(PublishedStreamEvent::Ended(StreamEnded {
            cause: StreamEndCause::Stopped,
            reason: 3
        }))
    ));
}

async fn wait_for_warning(sub: &mut StreamSubscription<StreamsEvent>) -> StreamWarning {
    for _ in 0..50 {
        match sub.try_recv() {
            Ok(StreamsEvent::Warning(w)) => return w,
            Ok(_) => continue,
            Err(_) => tokio::task::yield_now().await,
        }
    }
    panic!("expected OfferWithoutAudio warning");
}

#[tokio::test]
async fn watch_validates_transport_handles_5635_and_conditional_withdraw() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let join_gate: Arc<Mutex<Option<i32>>> = Arc::new(Mutex::new(None));
    let gate_ref = Arc::clone(&join_gate);

    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(move |client, cmd, rc, line| {
            let cmd = cmd.to_string();
            let line = line.to_string();
            let gate_ref = Arc::clone(&gate_ref);
            Box::pin(async move {
                if cmd == "joinstreamrequest" && line.contains("id=s-missing") {
                    tokio::spawn(async move {
                        client.resolve_stream_test_rc(
                            rc,
                            Some(Error::ServerError {
                                id: ErrorCode::StreamNotFound.code().to_string(),
                                server_message: "stream not found".to_string(),
                                extra_message: None,
                            }),
                        );
                    });
                } else if cmd == "joinstreamrequest" && line.contains("id=s-abort-sent") {
                    *gate_ref.lock().unwrap() = Some(rc);
                } else {
                    tokio::spawn(async move {
                        client.resolve_stream_test_rc(rc, None);
                    });
                }
                Ok(())
            })
        }),
    );

    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    let streams = Streams::new(&client, StreamsOptions::default());

    // Unsupported transport rejected without sending.
    client.inject_stream_test_line(1, "notifystreamstarted id=s-sfu clid=5 name=SFU mode=2");
    wait_for(|| streams.get("s-sfu").is_some()).await;
    assert!(matches!(
        streams.watch("s-sfu", WatchOptions::default()).await,
        Err(StreamError::TransportUnsupported(
            StreamTransportUnsupportedError { transport: 2, .. }
        ))
    ));
    assert!(sent.lock().unwrap().is_empty());

    // 5635 maps to StreamEndedError and removes local stream entry.
    client.inject_stream_test_line(
        1,
        "notifystreamstarted id=s-missing clid=5 name=GoneSoon mode=1",
    );
    wait_for(|| streams.get("s-missing").is_some()).await;
    assert!(matches!(
        streams.watch("s-missing", WatchOptions::default()).await,
        Err(StreamError::Ended(_))
    ));
    assert!(streams.get("s-missing").is_none());

    // Pre-aborted watch does not even send joinstreamrequest.
    client.inject_stream_test_line(
        1,
        "notifystreamstarted id=s-abort-pre clid=5 name=Pre mode=1",
    );
    wait_for(|| streams.get("s-abort-pre").is_some()).await;
    let pre_abort = AbortSignal::new();
    pre_abort.abort();
    assert!(matches!(
        streams
            .watch(
                "s-abort-pre",
                WatchOptions {
                    abort: Some(pre_abort),
                    ..Default::default()
                }
            )
            .await,
        Err(StreamError::Aborted)
    ));
    assert!(
        !sent
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.contains("s-abort-pre"))
    );

    // Aborted after sending withdraws with is_remove=1.
    client.inject_stream_test_line(
        1,
        "notifystreamstarted id=s-abort-sent clid=5 name=Sent mode=1",
    );
    wait_for(|| streams.get("s-abort-sent").is_some()).await;
    let abort_sent = AbortSignal::new();
    let s2 = streams.clone();
    let sig = abort_sent.clone();
    let w_task = tokio::spawn(async move {
        s2.watch(
            "s-abort-sent",
            WatchOptions {
                abort: Some(sig),
                ..Default::default()
            },
        )
        .await
    });
    wait_for(|| join_gate.lock().unwrap().is_some()).await;
    abort_sent.abort();
    assert!(matches!(w_task.await.unwrap(), Err(StreamError::Aborted)));
    wait_for(|| {
        sent.lock().unwrap().iter().any(|c| {
            c.starts_with("joinstreamrequest id=s-abort-sent") && c.contains("is_remove=1")
        })
    })
    .await;
}

#[tokio::test]
async fn dispose_sends_best_effort_stop_and_leave_and_clears_state() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(|client, cmd, rc, _line| {
            let cmd = cmd.to_string();
            Box::pin(async move {
                tokio::spawn(async move {
                    if cmd == "setupstream" {
                        client.inject_stream_test_line(
                            1,
                            &format!("notifystreamstarted id=own-1 clid=1 name=Own mode=1 return_code={rc}"),
                        );
                    }
                    client.resolve_stream_test_rc(rc, None);
                });
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    let streams = Streams::new(&client, StreamsOptions::default());

    let pub_stream = streams.publish(PublishOptions::new("Own")).await.unwrap();
    client.inject_stream_test_line(1, "notifystreamstarted id=remote-1 clid=7 name=R mode=1");
    wait_for(|| streams.get("remote-1").is_some()).await;

    let s2 = streams.clone();
    let w = tokio::spawn(async move { s2.watch("remote-1", WatchOptions::default()).await });
    wait_for(|| {
        sent.lock()
            .unwrap()
            .iter()
            .any(|c| c.starts_with("joinstreamrequest id=remote-1"))
    })
    .await;
    client.inject_stream_test_line(
        1,
        "notifyrespondjoinstreamrequest id=remote-1 clid=7 decision=1 offer=v=0\\r\\nm=audio\\s9 msg=",
    );
    let view = w.await.unwrap().unwrap();

    streams.dispose();
    assert!(pub_stream.ended());
    assert!(view.closed());
    assert!(streams.list().is_empty());
    assert!(matches!(
        streams.publish(PublishOptions::new("After")).await,
        Err(StreamError::Disposed)
    ));

    wait_for(|| {
        let log = sent.lock().unwrap();
        log.iter()
            .any(|c| c.starts_with("stopstream id=own-1 reason=0"))
            && log
                .iter()
                .any(|c| c.starts_with("joinstreamrequest id=remote-1 clid=7 msg= is_remove=1"))
    })
    .await;

    // Disconnect safety check: no errors reported on later disconnect.
    client.trigger_stream_test_disconnect(None);
}

#[tokio::test]
async fn watch_drop_guard_cleans_up_pending_entry_allows_reentry_and_leaves_only_when_sent() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let (pre_send_gate_tx, pre_send_gate_rx) = oneshot::channel::<()>();
    let pre_send_gate = Arc::new(tokio::sync::Mutex::new(Some(pre_send_gate_rx)));
    let join_sent_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let join_sent_ref = Arc::clone(&join_sent_count);
    let gate_ref = Arc::clone(&pre_send_gate);

    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(move |client, cmd, rc, line| {
            let cmd = cmd.to_string();
            let line = line.to_string();
            let gate_ref = Arc::clone(&gate_ref);
            let join_sent_ref = Arc::clone(&join_sent_ref);
            Box::pin(async move {
                if cmd == "joinstreamrequest" && line.contains("id=s-pre-drop") {
                    if let Some(rx) = gate_ref.lock().await.take() {
                        let _ = rx.await;
                        return Ok(());
                    }
                }
                if cmd == "joinstreamrequest"
                    && line.contains("id=s-post-drop")
                    && line.contains("is_remove=0")
                {
                    let n = join_sent_ref.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    if n == 0 {
                        // Keep the first join's error reply pending so select! drops it after send.
                        return Ok(());
                    }
                }
                tokio::spawn(async move {
                    client.resolve_stream_test_rc(rc, None);
                });
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    let streams = Streams::new(&client, StreamsOptions::default());

    // 1. Drop watch() before joinstreamrequest completes sending -> removes WatchEntry, no is_remove=1.
    client.inject_stream_test_line(
        1,
        "notifystreamstarted id=s-pre-drop clid=5 name=Pre mode=1",
    );
    wait_for(|| streams.get("s-pre-drop").is_some()).await;
    let pre_fut = streams.watch("s-pre-drop", WatchOptions::default());
    tokio::select! {
        _ = pre_fut => panic!("pre-send watch unexpectedly finished"),
        _ = wait_for(|| sent.lock().unwrap().iter().any(|c| c.contains("s-pre-drop"))) => {}
    }
    drop(pre_send_gate_tx);
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert!(
        !sent
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.contains("s-pre-drop") && c.contains("is_remove=1"))
    );
    // Re-entry must not fail with "already watching stream".
    let reenter_pre = {
        let s = streams.clone();
        tokio::spawn(async move { s.watch("s-pre-drop", WatchOptions::default()).await })
    };
    wait_for(|| {
        sent.lock()
            .unwrap()
            .iter()
            .filter(|c| c.contains("s-pre-drop") && c.contains("is_remove=0"))
            .count()
            == 2
    })
    .await;
    client.inject_stream_test_line(
        1,
        "notifyrespondjoinstreamrequest id=s-pre-drop clid=5 decision=1 offer=v=0\\r\\nm=audio\\s9 msg=",
    );
    let view_pre = reenter_pre.await.unwrap().expect("re-entered watch");
    assert_eq!(view_pre.offer(), "v=0\r\nm=audio 9");

    // 2. Drop watch() after joinstreamrequest was sent -> sends is_remove=1 and allows re-entry.
    client.inject_stream_test_line(
        1,
        "notifystreamstarted id=s-post-drop clid=5 name=Post mode=1",
    );
    wait_for(|| streams.get("s-post-drop").is_some()).await;
    let post_fut = streams.watch("s-post-drop", WatchOptions::default());
    tokio::select! {
        _ = post_fut => panic!("post-send watch unexpectedly finished"),
        _ = wait_for(|| join_sent_count.load(std::sync::atomic::Ordering::Acquire) == 1) => {
            // Yield once so exec_command_parts finishes setting sent_flag before select! drops post_fut.
            tokio::task::yield_now().await;
        }
    }
    wait_for(|| {
        sent.lock()
            .unwrap()
            .iter()
            .any(|c| c.starts_with("joinstreamrequest id=s-post-drop") && c.contains("is_remove=1"))
    })
    .await;

    let reenter_post = {
        let s = streams.clone();
        tokio::spawn(async move { s.watch("s-post-drop", WatchOptions::default()).await })
    };
    wait_for(|| join_sent_count.load(std::sync::atomic::Ordering::Acquire) == 2).await;
    client.inject_stream_test_line(
        1,
        "notifyrespondjoinstreamrequest id=s-post-drop clid=5 decision=1 offer=v=0\\r\\nm=audio\\s9 msg=",
    );
    let view_post = reenter_post
        .await
        .unwrap()
        .expect("re-entered watch after post-send drop");
    assert_eq!(view_post.offer(), "v=0\r\nm=audio 9");
}

#[tokio::test]
async fn watch_drop_after_accept_closes_unreturned_view_and_allows_reentry() {
    let (client, sent) = setup_connected_p2p_client(1);
    let streams = Streams::new(&client, StreamsOptions::default());
    client.inject_stream_test_line(1, "notifystreamstarted id=s-race clid=5 name=Race mode=1");
    wait_for(|| streams.get("s-race").is_some()).await;

    let mut fut = Box::pin(streams.watch("s-race", WatchOptions::default()));
    tokio::select! {
        biased;
        _ = &mut fut => panic!("watch finished early"),
        _ = async {
            wait_for(|| {
                sent.lock()
                    .unwrap()
                    .iter()
                    .any(|c| c.starts_with("joinstreamrequest id=s-race"))
            })
            .await;
            client.inject_stream_test_line(
                1,
                "notifyrespondjoinstreamrequest id=s-race clid=5 decision=1 offer=v=0\\r\\nm=audio\\s9 msg=",
            );
        } => {}
    }
    // Drop the future directly without polling it again to receive the view.
    drop(fut);
    wait_for(|| {
        sent.lock()
            .unwrap()
            .iter()
            .any(|c| c.starts_with("joinstreamrequest id=s-race") && c.contains("is_remove=1"))
    })
    .await;

    // Must be able to watch the stream again immediately.
    let s2 = streams.clone();
    let w2 = tokio::spawn(async move { s2.watch("s-race", WatchOptions::default()).await });
    wait_for(|| {
        sent.lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with("joinstreamrequest id=s-race") && c.contains("is_remove=0"))
            .count()
            == 2
    })
    .await;
    client.inject_stream_test_line(
        1,
        "notifyrespondjoinstreamrequest id=s-race clid=5 decision=1 offer=v=0\\r\\nm=audio\\s9 msg=",
    );
    let view = w2
        .await
        .unwrap()
        .expect("re-watch after unreturned view drop");
    assert_eq!(view.offer(), "v=0\r\nm=audio 9");
}

#[tokio::test]
async fn viewer_peer_kick_sends_command_and_ignores_nonfatal_errors_while_close_stays_local() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let kick_mode = Arc::new(Mutex::new(0u32));
    let mode_ref = Arc::clone(&kick_mode);

    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(move |client, cmd, rc, _line| {
            let cmd = cmd.to_string();
            let mode_ref = Arc::clone(&mode_ref);
            Box::pin(async move {
                let code = *mode_ref.lock().unwrap();
                tokio::spawn(async move {
                    if cmd == "setupstream" {
                        client.inject_stream_test_line(
                            1,
                            &format!("notifystreamstarted id=pub-k clid=1 name=Kick mode=1 return_code={rc}"),
                        );
                        client.resolve_stream_test_rc(rc, None);
                    } else if cmd == "removeclientfromstream" && code != 0 {
                        client.resolve_stream_test_rc(
                            rc,
                            Some(Error::ServerError {
                                id: code.to_string(),
                                server_message: "kick test error".to_string(),
                                extra_message: None,
                            }),
                        );
                    } else {
                        client.resolve_stream_test_rc(rc, None);
                    }
                });
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    for clid in [10, 11, 12, 13, 14] {
        client.inject_stream_test_line(
            1,
            &format!("notifycliententerview cid=1 clid={clid} client_unique_identifier=u-{clid} client_nickname=V{clid}"),
        );
    }
    client.inject_stream_test_line(1, "channellistfinished");

    let streams = Streams::new(&client, StreamsOptions::default());
    let pub_stream = streams.publish(PublishOptions::new("Kick")).await.unwrap();
    let mut pub_sub = pub_stream.subscribe();

    let mut peers = Vec::new();
    for clid in [10, 11, 12, 13, 14] {
        client.inject_stream_test_line(
            1,
            &format!("notifyjoinstreamrequest id=pub-k clid={clid} msg= is_remove=0"),
        );
        let req = match pub_sub.recv().await.unwrap() {
            PublishedStreamEvent::Request(r) => r,
            other => panic!("expected Request, got {other:?}"),
        };
        peers.push(req.accept("v=0\r\nm=audio 9", "").await.unwrap());
    }

    // 1. close() remains purely local.
    let sent_before_close = sent.lock().unwrap().len();
    peers[0].close().await.unwrap();
    assert!(peers[0].closed());
    assert_eq!(sent.lock().unwrap().len(), sent_before_close);

    // 2. kick(reason) sends removeclientfromstream and completes local close on ok.
    peers[1].kick(7).await.unwrap();
    assert!(peers[1].closed());
    assert!(
        sent.lock()
            .unwrap()
            .iter()
            .any(|c| c.starts_with("removeclientfromstream id=pub-k clid=11 reason=7"))
    );

    // 3. StreamNotParticipating (5636) is ignored.
    *kick_mode.lock().unwrap() = ErrorCode::StreamNotParticipating.code();
    peers[2].kick(1).await.unwrap();
    assert!(peers[2].closed());

    // 4. CommandNotFound (256) is ignored.
    *kick_mode.lock().unwrap() = ErrorCode::CommandNotFound.code();
    peers[3].kick(2).await.unwrap();
    assert!(peers[3].closed());

    // 5. Non-ignored error (e.g. 2568) still returns Err while closing local peer.
    *kick_mode.lock().unwrap() = 2568;
    let err = peers[4].kick(3).await.unwrap_err();
    assert_eq!(err.server_error_code(), Some(2568));
    assert!(peers[4].closed());
    assert!(pub_stream.viewers().is_empty());
}

#[tokio::test]
async fn refresh_client_queries_on_demand_with_inflight_dedup_and_cooldown() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let pending_rc: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let pending_ref = Arc::clone(&pending_rc);

    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(move |_client, cmd, rc, _line| {
            let cmd = cmd.to_string();
            let pending_ref = Arc::clone(&pending_ref);
            Box::pin(async move {
                if cmd == "requeststreaminfo" {
                    pending_ref.lock().unwrap().push(rc);
                }
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    let streams = Streams::new_with_refresh_cooldown(
        &client,
        StreamsOptions::default(),
        Duration::from_millis(50),
    );

    // Self (1) and 0 are ignored.
    streams.refresh_client(0);
    streams.refresh_client(1);
    tokio::task::yield_now().await;
    assert!(sent.lock().unwrap().is_empty());

    // Burst calls while first query is in-flight send only one requeststreaminfo.
    streams.refresh_client(77);
    streams.refresh_client(77);
    streams.refresh_client(77);
    wait_for(|| pending_rc.lock().unwrap().len() == 1).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(sent.lock().unwrap().len(), 1);
    assert!(sent.lock().unwrap()[0].starts_with("requeststreaminfo clid=77"));

    // Resolve first query; immediately calling again within cooldown is suppressed.
    let rc1 = pending_rc.lock().unwrap()[0];
    client.resolve_stream_test_rc(rc1, None);
    client.inject_stream_test_line(1, "notifystreaminfo id=s-77 clid=77 name=SubFail mode=1");
    wait_for(|| streams.get("s-77").is_some()).await;
    streams.refresh_client(77);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(sent.lock().unwrap().len(), 1);

    // After cooldown expires, refresh_client issues a fresh requeststreaminfo.
    tokio::time::sleep(Duration::from_millis(55)).await;
    streams.refresh_client(77);
    wait_for(|| pending_rc.lock().unwrap().len() == 2).await;
    assert_eq!(sent.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn notifystreaminfo_mode_zero_clears_p2p_transport() {
    let (client, _sent) = setup_connected_p2p_client(1);
    let streams = Streams::new(&client, StreamsOptions::default());
    let mut sub = streams.subscribe();

    client.inject_stream_test_line(1, "notifystreamstarted id=s-m0 clid=5 name=Mode mode=1");
    match recv_within(&mut sub).await {
        StreamsEvent::Started(info) => assert_eq!(info.transport, Some(StreamTransport::P2p)),
        other => panic!("expected Started, got {other:?}"),
    }

    client.inject_stream_test_line(1, "notifystreaminfo id=s-m0 mode=0");
    match recv_within(&mut sub).await {
        StreamsEvent::Updated(info) => assert_eq!(info.transport, None),
        other => panic!("expected Updated, got {other:?}"),
    }
    assert_eq!(streams.get("s-m0").unwrap().transport, None);
}

#[tokio::test]
async fn published_stream_with_unsubscribed_join_request_has_no_arc_cycle() {
    let make_publisher = |id: &'static str| {
        let client = make_client();
        let sent = Arc::new(Mutex::new(Vec::new()));
        install_mock_server(
            &client,
            Arc::clone(&sent),
            Arc::new(move |client, cmd, rc, _line| {
                let cmd = cmd.to_string();
                Box::pin(async move {
                    tokio::spawn(async move {
                        if cmd == "setupstream" {
                            client.inject_stream_test_line(
                                1,
                                &format!("notifystreamstarted id={id} clid=1 name=NoLeak mode=1 return_code={rc}"),
                            );
                        }
                        client.resolve_stream_test_rc(rc, None);
                    });
                    Ok(())
                })
            }),
        );
        client.inject_stream_test_line(
            1,
            "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
        );
        client.set_stream_test_connected(1);
        client.inject_stream_test_line(
            1,
            "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
        );
        client.inject_stream_test_line(
            1,
            "notifycliententerview cid=1 clid=40 client_unique_identifier=u-40 client_nickname=V40",
        );
        client.inject_stream_test_line(1, "channellistfinished");
        client
    };

    // Case 1: stop() + drop(pub_stream) while Streams stays alive -> PublishedInner must drop.
    let client1 = make_publisher("pub-leak-1");
    let streams1 = Streams::new(&client1, StreamsOptions::default());
    let pub1 = streams1
        .publish(PublishOptions::new("NoLeak1"))
        .await
        .unwrap();
    let weak1 = Arc::downgrade(&pub1.inner);
    // Do NOT subscribe to pub1: request stays in both state.pending and hub.kept.
    client1.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-leak-1 clid=40 msg=hi is_remove=0",
    );
    pub1.stop(0).await.unwrap();
    drop(pub1);
    assert!(
        weak1.upgrade().is_none(),
        "PublishedInner leaked after stop() + drop with unsubscribed JoinRequest"
    );

    // Case 2: drop(pub_stream) + drop(streams) without stop()/dispose() -> PublishedInner must still drop,
    // even if an external JoinRequest handle is still held.
    let client2 = make_publisher("pub-leak-2");
    let streams2 = Streams::new(&client2, StreamsOptions::default());
    let pub2 = streams2
        .publish(PublishOptions::new("NoLeak2"))
        .await
        .unwrap();
    let weak2 = Arc::downgrade(&pub2.inner);
    let mut sub2 = pub2.subscribe();
    client2.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-leak-2 clid=40 msg=hi is_remove=0",
    );
    let req2 = match sub2.recv().await.unwrap() {
        PublishedStreamEvent::Request(r) => r,
        other => panic!("expected Request, got {other:?}"),
    };
    drop(sub2);
    drop(pub2);
    drop(streams2);
    assert!(
        weak2.upgrade().is_none(),
        "PublishedInner leaked after dropping Streams with held JoinRequest"
    );
    assert!(matches!(
        req2.accept("v=0\r\nm=audio 9", "").await,
        Err(StreamError::Disposed)
    ));
}

#[tokio::test]
async fn published_stream_stop_cleans_local_state_on_non_5635_server_error() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(|client, cmd, rc, _line| {
            let cmd = cmd.to_string();
            Box::pin(async move {
                tokio::spawn(async move {
                    if cmd == "setupstream" {
                        client.inject_stream_test_line(
                            1,
                            &format!("notifystreamstarted id=pub-err clid=1 name=ErrStop mode=1 return_code={rc}"),
                        );
                        client.resolve_stream_test_rc(rc, None);
                    } else if cmd == "stopstream" {
                        client.resolve_stream_test_rc(
                            rc,
                            Some(Error::ServerError {
                                id: "2568".to_string(),
                                server_message: "insufficient client permissions".to_string(),
                                extra_message: None,
                            }),
                        );
                    } else {
                        client.resolve_stream_test_rc(rc, None);
                    }
                });
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=20 client_unique_identifier=u-20 client_nickname=V20",
    );
    client.inject_stream_test_line(1, "channellistfinished");

    let streams = Streams::new(&client, StreamsOptions::default());
    let mut main_sub = streams.subscribe();
    let pub_stream = streams
        .publish(PublishOptions::new("ErrStop"))
        .await
        .unwrap();
    let mut pub_sub = pub_stream.subscribe();

    client.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-err clid=20 msg= is_remove=0",
    );
    let req = match pub_sub.recv().await.unwrap() {
        PublishedStreamEvent::Request(r) => r,
        other => panic!("expected Request, got {other:?}"),
    };
    let peer = req.accept("v=0\r\nm=audio 9", "").await.unwrap();

    let err = pub_stream.stop(2).await.unwrap_err();
    assert_eq!(err.server_error_code(), Some(2568));
    assert!(pub_stream.ended());
    assert!(peer.closed());
    assert!(pub_stream.viewers().is_empty());
    assert!(streams.get("pub-err").is_none());
    assert!(streams.list().is_empty());
    assert!(matches!(
        pub_sub.recv().await,
        Some(PublishedStreamEvent::Ended(StreamEnded {
            cause: StreamEndCause::Stopped,
            reason: 2
        }))
    ));
    let mut saw_stopped = false;
    while let Ok(ev) = main_sub.try_recv() {
        if matches!(
            ev,
            StreamsEvent::Stopped(StreamStoppedEvent {
                cause: StreamEndCause::Stopped,
                reason: 2,
                ..
            })
        ) {
            saw_stopped = true;
            break;
        }
    }
    assert!(saw_stopped);
}

#[tokio::test]
async fn client_enter_missing_streaming_flag_does_not_forget_unlike_explicit_zero() {
    let (client, _sent) = setup_connected_p2p_client(1);
    let streams = Streams::new_with_grace(
        &client,
        StreamsOptions::default(),
        Duration::from_millis(50),
    );
    let mut sub = streams.subscribe();

    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(1, "channellistfinished");

    client.inject_stream_test_line(1, "notifystreamstarted id=s-omit clid=5 name=Omit mode=1");
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::Started(_)
    ));

    // notifycliententerview omitting client_is_streaming must NOT trigger 2s forget.
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=5 client_nickname=PeerNoFlag",
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(streams.get("s-omit").is_some());

    // Explicit client_is_streaming=0 on notifyclientupdated after omitted enterview DOES trigger forget.
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_is_streaming=0");
    wait_for(|| streams.get("s-omit").is_none()).await;
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::Stopped(StreamStoppedEvent {
            cause: StreamEndCause::Gone,
            reason: 0,
            ..
        })
    ));

    // Also verify explicit client_is_streaming=0 directly on notifycliententerview triggers forget.
    client.inject_stream_test_line(1, "notifystreamstarted id=s-zero clid=6 name=Zero mode=1");
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::Started(_)
    ));
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=6 client_nickname=PeerZero client_is_streaming=0",
    );
    wait_for(|| streams.get("s-zero").is_none()).await;
    assert!(matches!(
        recv_within(&mut sub).await,
        StreamsEvent::Stopped(StreamStoppedEvent {
            cause: StreamEndCause::Gone,
            reason: 0,
            ..
        })
    ));
}

#[tokio::test]
async fn viewer_reconnect_with_same_clid_closes_old_peer_and_delivers_new_request() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(|client, cmd, rc, _line| {
            let cmd = cmd.to_string();
            Box::pin(async move {
                tokio::spawn(async move {
                    if cmd == "setupstream" {
                        client.inject_stream_test_line(
                            1,
                            &format!(
                                "notifystreamstarted id=pub-reconn clid=1 name=Reconn mode=1 return_code={rc}"
                            ),
                        );
                    }
                    client.resolve_stream_test_rc(rc, None);
                });
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=40 client_unique_identifier=uid-40 client_nickname=Viewer40",
    );
    client.inject_stream_test_line(1, "channellistfinished");

    let streams = Streams::new(&client, StreamsOptions::default());
    let mut main_sub = streams.subscribe();
    let pub_stream = streams
        .publish(PublishOptions::new("Reconn"))
        .await
        .expect("published");
    let mut pub_sub = pub_stream.subscribe();

    client.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-reconn clid=40 msg=first is_remove=0",
    );
    let req1 = match recv_within(&mut pub_sub).await {
        PublishedStreamEvent::Request(req) => req,
        other => panic!("expected first Request, got {other:?}"),
    };
    let peer1 = req1.accept("v=0\r\nm=audio 9", "").await.expect("accept 1");
    let mut peer1_sub = peer1.subscribe();
    assert_eq!(pub_stream.viewers().len(), 1);

    // Same viewer_clid reconnects with a fresh notifyjoinstreamrequest before any left event.
    client.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-reconn clid=40 msg=second is_remove=0",
    );
    assert!(matches!(
        recv_within(&mut peer1_sub).await,
        PeerEvent::Closed(PeerClosed {
            cause: PeerCloseCause::Left,
            reason: None,
        })
    ));
    assert!(peer1.closed());
    assert!(pub_stream.viewers().is_empty());

    let req2 = match recv_within(&mut pub_sub).await {
        PublishedStreamEvent::Request(req) => req,
        other => panic!("expected second Request, got {other:?}"),
    };
    assert_eq!(req2.message(), "second");
    assert_eq!(req2.state(), JoinRequestState::Pending);

    // Late signaling from old session while req2 is pending is ignored without error.
    client.inject_stream_test_line(
        1,
        r#"notifystreamsignaling id=pub-reconn clid=40 json={"cmd":"iceCandidate","args":{"sdp":"stale","mid":"0","mLine":0}}"#,
    );
    // Duplicate join request while req2 is still Pending remains ignored.
    client.inject_stream_test_line(
        1,
        "notifyjoinstreamrequest id=pub-reconn clid=40 msg=dup is_remove=0",
    );
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert!(pub_sub.try_recv().is_err());

    let peer2 = req2.accept("v=0\r\nm=audio 9", "").await.expect("accept 2");
    assert!(!peer2.closed());
    assert_eq!(pub_stream.viewers().len(), 1);
    let mut peer2_sub = peer2.subscribe();

    client.inject_stream_test_line(
        1,
        r#"notifystreamsignaling id=pub-reconn clid=40 json={"cmd":"answer","args":{"sdp":"v=ans2"}}"#,
    );
    assert!(matches!(
        recv_within(&mut peer2_sub).await,
        PeerEvent::Answer(sdp) if sdp == "v=ans2"
    ));
    assert!(peer1_sub.try_recv().is_err());
    while let Ok(ev) = main_sub.try_recv() {
        assert!(
            !matches!(ev, StreamsEvent::Error(_)),
            "unexpected error: {ev:?}"
        );
    }
}

#[tokio::test]
async fn self_published_stream_is_exempt_from_forget_and_expire_gone() {
    let client = make_client();
    let sent = Arc::new(Mutex::new(Vec::new()));
    install_mock_server(
        &client,
        Arc::clone(&sent),
        Arc::new(|client, cmd, rc, _line| {
            let cmd = cmd.to_string();
            Box::pin(async move {
                tokio::spawn(async move {
                    if cmd == "setupstream" {
                        client.inject_stream_test_line(
                            1,
                            &format!(
                                "notifystreamstarted id=own-live clid=1 name=OwnLive mode=1 return_code={rc}"
                            ),
                        );
                    }
                    client.resolve_stream_test_rc(rc, None);
                });
                Ok(())
            })
        }),
    );
    client.inject_stream_test_line(
        1,
        "initserver virtualserver_version=6.0.0 virtualserver_capability_extensions=STREAM_P2P",
    );
    client.set_stream_test_connected(1);
    client.inject_stream_test_line(
        1,
        "channellist cid=1 pid=0 channel_order=0 channel_name=Default",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=1 client_nickname=Self client_is_streaming=0",
    );
    client.inject_stream_test_line(
        1,
        "notifycliententerview cid=1 clid=5 client_nickname=Remote client_is_streaming=1",
    );
    client.inject_stream_test_line(1, "channellistfinished");

    let streams = Streams::new_with_grace(
        &client,
        StreamsOptions::default(),
        Duration::from_millis(50),
    );
    let mut sub = streams.subscribe();
    let pub_stream = streams
        .publish(PublishOptions::new("OwnLive"))
        .await
        .expect("published");
    client.inject_stream_test_line(1, "notifystreamstarted id=remote-5 clid=5 name=R5 mode=1");
    wait_for(|| streams.get("own-live").is_some() && streams.get("remote-5").is_some()).await;

    // 1. on_client_updated setting self_clid to 0 directly or flipping 1 -> 0 must not trigger 2s Gone cleanup.
    client.inject_stream_test_line(1, "notifyclientupdated clid=1 client_is_streaming=0");
    client.inject_stream_test_line(1, "notifyclientupdated clid=1 client_is_streaming=1");
    client.inject_stream_test_line(1, "notifyclientupdated clid=1 client_is_streaming=0");
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert!(streams.get("own-live").is_some());
    assert!(!pub_stream.ended());

    // 2. scan_directory_snapshot via NotificationOverflow must exempt self_clid (is_streaming=Some(false)),
    // while still forgetting a remote client whose directory flag is Some(false).
    client.inject_stream_test_line(1, "notifyclientupdated clid=5 client_is_streaming=0");
    client.trigger_stream_test_overflow();
    wait_for(|| streams.get("remote-5").is_none()).await;
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert!(streams.get("own-live").is_some());
    assert!(!pub_stream.ended());

    let mut stopped_ids = Vec::new();
    while let Ok(ev) = sub.try_recv() {
        if let StreamsEvent::Stopped(stopped) = ev {
            stopped_ids.push((stopped.info.id, stopped.cause));
        }
    }
    assert_eq!(
        stopped_ids,
        vec![("remote-5".to_string(), StreamEndCause::Gone)]
    );

    // 3. Direct forget / expire_gone checks for both self_clid and active state.published entries.
    streams.test_forget(1);
    assert!(!streams.test_has_gone_timer(1));

    let synthetic_pub_info = StreamInfo {
        id: "pub-other-clid".to_string(),
        client_id: 99,
        name: "SyntheticPub".to_string(),
        source: Some(StreamSource::Screen),
        audio: false,
        bitrate: Some(1_500_000),
        accessibility: Some(StreamAccessibility::Public),
        transport: Some(StreamTransport::P2p),
        viewer_limit: Some(0),
        viewers: Some(0),
    };
    let synthetic_pub = streams.test_adopt_published(synthetic_pub_info);
    streams.test_forget(99);
    assert!(!streams.test_has_gone_timer(99));

    // Even if a gone timer entry for 99 existed, expire_gone must not end the active published stream,
    // while still ending a non-published stream belonging to 99.
    client.inject_stream_test_line(
        1,
        "notifystreamstarted id=remote-99 clid=99 name=R99 mode=1",
    );
    wait_for(|| streams.get("remote-99").is_some()).await;
    streams.test_expire_gone_with_timer(99, 777);
    assert!(streams.get("remote-99").is_none());
    assert!(streams.get("pub-other-clid").is_some());
    assert!(!synthetic_pub.ended());
    assert!(streams.test_has_published("pub-other-clid"));
}
