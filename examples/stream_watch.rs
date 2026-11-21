//! TS6 屏幕共享观看端示例：`tsclient_rs::stream` 负责信令，`webrtc` crate 负责媒体。
//!
//! 需要 `stream` feature 与服务端 `STREAM_P2P` 能力扩展。示例观看 `STREAM_ID`
//! 指定的流，未设置时等待并观看第一条出现的流；它打印收到的轨道与 RTP 包计数，
//! 不渲染画面。
//!
//! ```sh
//! TEAMSPEAK_ADDR=127.0.0.1:9987 \
//!   cargo run --example stream_watch --features stream-example
//! ```

use std::sync::Arc;
use std::time::Duration;

use tsclient_rs::stream::{
    IceCandidate, PeerEvent, StreamInfo, Streams, StreamsEvent, StreamsOptions, WatchOptions,
};
use tsclient_rs::{Client, ClientOptions, generateIdentity};

use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCConfigurationBuilder, RTCIceCandidateInit, RTCIceServer, RTCPeerConnectionIceEvent,
    RTCSessionDescription, Registry, register_default_interceptors,
};
use webrtc::rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit};

/// Teamspeak 的官方 STUN 对，ICE 收集更快且不会淹没信令。
const ICE_SERVERS: &[&str] = &[
    "stun:turn.teamspeak.com:3478",
    "stun:turn2.teamspeak.com:3478",
];

#[derive(Clone)]
struct Handler {
    candidates: tokio::sync::mpsc::UnboundedSender<RTCIceCandidateInit>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        if let Ok(init) = event.candidate.to_json() {
            let _ = self.candidates.send(init);
        }
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let kind = track.kind().await;
        println!("[stream] received {kind:?} track");
        tokio::spawn(async move {
            let mut packets = 0u64;
            while let Some(event) = track.poll().await {
                if matches!(event, TrackRemoteEvent::OnRtpPacket(_)) {
                    packets += 1;
                    if packets.is_multiple_of(300) {
                        println!("[stream] {kind:?} RTP packets: {packets}");
                    }
                }
            }
            println!("[stream] {kind:?} track ended after {packets} RTP packets");
        });
    }
}

async fn build_peer() -> Result<
    (
        Arc<dyn PeerConnection>,
        tokio::sync::mpsc::UnboundedReceiver<RTCIceCandidateInit>,
    ),
    Box<dyn std::error::Error>,
> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;
    let registry = register_default_interceptors(Registry::new(), &mut media_engine)?;

    let config = RTCConfigurationBuilder::new()
        .with_ice_servers(vec![RTCIceServer {
            urls: ICE_SERVERS.iter().map(|url| (*url).to_string()).collect(),
            ..Default::default()
        }])
        .build();

    let (candidate_tx, candidate_rx) = tokio::sync::mpsc::unbounded_channel();
    let peer: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_configuration(config)
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .with_handler(Arc::new(Handler {
                candidates: candidate_tx,
            }))
            .with_udp_addrs(vec!["0.0.0.0:0"])
            .build()
            .await?,
    );

    // 观看端只接收视频。发布端的 offer 自带 m=audio，回答会保留该段。
    peer.add_transceiver_from_kind(
        RtpCodecKind::Video,
        Some(RTCRtpTransceiverInit {
            direction: RTCRtpTransceiverDirection::Recvonly,
            ..Default::default()
        }),
    )
    .await?;

    Ok((peer, candidate_rx))
}

async fn wait_for_stream(
    streams: &Streams,
    mut events: tsclient_rs::stream::StreamSubscription<StreamsEvent>,
) -> Option<StreamInfo> {
    if let Ok(id) = std::env::var("STREAM_ID") {
        if let Some(info) = streams.get(&id) {
            return Some(info);
        }
        println!("[stream] waiting for stream {id}");
        while let Some(event) = events.recv().await {
            let info = match event {
                StreamsEvent::Started(info) | StreamsEvent::Updated(info) => info,
                _ => continue,
            };
            if info.id == id {
                return Some(info);
            }
        }
        return None;
    }
    if let Some(info) = streams.list().first() {
        return Some(info.clone());
    }
    println!("[stream] waiting for a stream (Ctrl-C to quit)");
    while let Some(event) = events.recv().await {
        if let StreamsEvent::Started(info) = event {
            return Some(info);
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEAMSPEAK_ADDR").unwrap_or_else(|_| "127.0.0.1:9987".to_string());
    let mut client = Client::new(
        generateIdentity(8),
        address.clone(),
        "stream-watch".to_string(),
        ClientOptions::default(),
    );

    let streams = Streams::new(&client, StreamsOptions::default());
    let events = streams.subscribe();

    println!("[stream] connecting to {address}");
    client.connect().await?;
    client.wait_connected(None).await?;
    client.wait_directory_ready(None).await?;

    if !streams.supported() {
        eprintln!("[stream] server does not advertise STREAM_P2P");
        client.disconnect().await?;
        return Ok(());
    }

    let Some(info) = wait_for_stream(&streams, events).await else {
        eprintln!("[stream] no stream found");
        streams.dispose();
        client.disconnect().await?;
        return Ok(());
    };

    println!("[stream] watching \"{}\" (id {})", info.name, info.id);
    let view = streams
        .watch(
            &info,
            WatchOptions {
                message: "stream-watch example".to_string(),
                ..Default::default()
            },
        )
        .await?;

    let (peer, mut local_candidates) = build_peer().await?;
    peer.set_remote_description(RTCSessionDescription::offer(view.offer().to_string())?)
        .await?;
    let answer = peer.create_answer(None).await?;
    peer.set_local_description(answer.clone()).await?;
    view.answer(&answer.sdp).await?;
    println!("[stream] answered, waiting for media");

    // 回答发出后再 trickle 本地候选，避免对端还没有可应用候选的会话。
    let candidate_view = view.clone();
    tokio::spawn(async move {
        while let Some(init) = local_candidates.recv().await {
            let candidate = IceCandidate {
                candidate: init.candidate,
                sdp_mid: init.sdp_mid,
                sdp_m_line_index: init.sdp_mline_index,
            };
            if let Err(err) = candidate_view.send_candidate(&candidate).await {
                eprintln!("[stream] send candidate failed: {err}");
                break;
            }
        }
    });

    let event_peer = Arc::clone(&peer);
    let event_view = view.clone();
    let mut peer_events = view.subscribe();
    let events_task = tokio::spawn(async move {
        while let Some(event) = peer_events.recv().await {
            match event {
                PeerEvent::Offer(sdp) => {
                    println!("[stream] renegotiation offer: {} bytes", sdp.len());
                    let offer = match RTCSessionDescription::offer(sdp) {
                        Ok(offer) => offer,
                        Err(err) => {
                            eprintln!("[stream] invalid offer: {err}");
                            continue;
                        }
                    };
                    if let Err(err) = event_peer.set_remote_description(offer).await {
                        eprintln!("[stream] set remote description failed: {err}");
                        continue;
                    }
                    if let Ok(answer) = event_peer.create_answer(None).await
                        && let Err(err) = event_view.answer(&answer.sdp).await
                    {
                        eprintln!("[stream] answer failed: {err}");
                    }
                }
                PeerEvent::Candidate(candidate) => {
                    let init = RTCIceCandidateInit {
                        candidate: candidate.candidate,
                        sdp_mid: candidate.sdp_mid,
                        sdp_mline_index: candidate.sdp_m_line_index,
                        ..Default::default()
                    };
                    let _ = event_peer.add_ice_candidate(init).await;
                }
                PeerEvent::Closed(closed) => {
                    println!("[stream] closed: {:?}", closed.cause);
                    break;
                }
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = async {
            while !view.closed() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        } => {}
    }

    events_task.abort();
    let _ = peer.close().await;
    streams.dispose();
    client.disconnect().await?;
    println!("[stream] done");
    Ok(())
}
