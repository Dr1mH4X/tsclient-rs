//! TS6 屏幕共享发布端示例：`tsclient_rs::stream` 负责信令，`webrtc` crate 负责媒体。
//!
//! 需要 `stream` feature 与服务端 `STREAM_P2P` 能力扩展。视频来自一个 VP8 IVF
//! 文件并循环播放；用 ffmpeg 从屏幕采集生成该文件即可推真实画面：
//!
//! ```sh
//! ffmpeg -f avfoundation -framerate 30 -i "1:none" -t 30 \
//!   -c:v libvpx -b:v 5M -f ivf /tmp/screen.ivf
//! STREAM_IVF=/tmp/screen.ivf TEAMSPEAK_ADDR=127.0.0.1:9987 \
//!   cargo run --example stream_publish --features stream-example
//! ```
//!
//! 示例始终添加一路 send-only 的 `m=audio`，即使没有音轨：TeamSpeak 6 客户端
//! 只有在 offer 带音频段时才应答。

use std::io::BufReader;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tsclient_rs::stream::{
    IceCandidate, JoinRequest, PeerEvent, PublishOptions, PublishedStreamEvent, Streams,
    StreamsOptions, ViewerPeer,
};
use tsclient_rs::{Client, ClientOptions, generateIdentity};

use rtc::media::Sample;
use rtc::media::io::ivf_reader::IVFReader;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::media_engine::{MIME_TYPE_OPUS, MIME_TYPE_VP8};
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters,
    RtpCodecKind,
};
use rtc::rtp_transceiver::{PayloadType, RTCRtpTransceiverDirection, RTCRtpTransceiverInit};
use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCConfigurationBuilder, RTCIceCandidateInit, RTCIceServer, RTCPeerConnectionIceEvent,
    RTCSessionDescription, Registry, register_default_interceptors,
};
use webrtc::rtp_transceiver::RtpSender;

/// Teamspeak 的官方 STUN 对。
const ICE_SERVERS: &[&str] = &[
    "stun:turn.teamspeak.com:3478",
    "stun:turn2.teamspeak.com:3478",
];
const VIDEO_PAYLOAD_TYPE: PayloadType = 96;
const AUDIO_PAYLOAD_TYPE: PayloadType = 111;
const MAX_VIEWERS: usize = 4;

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
}

struct VideoSource {
    frames: Arc<Vec<Bytes>>,
    frame_delay: Duration,
}

fn load_ivf(path: &str) -> Result<VideoSource, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let (mut reader, header) = IVFReader::new(BufReader::new(file))?;
    let numerator = u64::from(header.timebase_numerator);
    let denominator = u64::from(header.timebase_denominator).max(1);
    let frame_delay = Duration::from_millis((1000 * numerator / denominator).max(1));

    let mut frames = Vec::new();
    while let Ok((frame, _)) = reader.parse_next_frame() {
        frames.push(frame.freeze());
    }
    if frames.is_empty() {
        return Err(format!("{path} contains no IVF frames").into());
    }
    println!(
        "[publish] loaded {} frames from {path} ({} ms per frame)",
        frames.len(),
        frame_delay.as_millis()
    );
    Ok(VideoSource {
        frames: Arc::new(frames),
        frame_delay,
    })
}

/// 发布者侧的单个观众会话：WebRTC 连接、视频轨与本地 ICE 候选。
struct ViewerSession {
    peer: Arc<dyn PeerConnection>,
    track: Arc<TrackLocalStaticSample>,
    sender: Arc<dyn RtpSender>,
    ssrc: u32,
    candidates: tokio::sync::mpsc::UnboundedReceiver<RTCIceCandidateInit>,
}

fn build_media_engine() -> Result<MediaEngine, Box<dyn std::error::Error>> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_codec(
        RTCRtpCodecParameters {
            rtp_codec: RTCRtpCodec {
                mime_type: MIME_TYPE_VP8.to_string(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            payload_type: VIDEO_PAYLOAD_TYPE,
        },
        RtpCodecKind::Video,
    )?;
    media_engine.register_codec(
        RTCRtpCodecParameters {
            rtp_codec: RTCRtpCodec {
                mime_type: MIME_TYPE_OPUS.to_string(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            payload_type: AUDIO_PAYLOAD_TYPE,
        },
        RtpCodecKind::Audio,
    )?;
    Ok(media_engine)
}

async fn build_viewer_session() -> Result<(ViewerSession, String), Box<dyn std::error::Error>> {
    let mut media_engine = build_media_engine()?;
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

    // 视频必须是 MID 0，音频必须是 MID 1；音频段即使无音轨也要存在。
    let ssrc = rand::random::<u32>();
    let track = Arc::new(TrackLocalStaticSample::new(
        Instant::now(),
        MediaStreamTrack::new(
            format!("tsclient-stream-{ssrc}"),
            format!("tsclient-track-{ssrc}"),
            "screen".to_string(),
            RtpCodecKind::Video,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                codec: RTCRtpCodec {
                    mime_type: MIME_TYPE_VP8.to_string(),
                    clock_rate: 90000,
                    channels: 0,
                    sdp_fmtp_line: String::new(),
                    rtcp_feedback: vec![],
                },
                ..Default::default()
            }],
        ),
    )?);
    let sender = peer
        .add_track(Arc::clone(&track) as Arc<dyn TrackLocal>)
        .await?;
    // send-only 音频段没有音轨可发，但必须给出编码参数，否则 webrtc 拒绝
    // 创建 sendonly transceiver。
    let opus_codec = RTCRtpCodec {
        mime_type: MIME_TYPE_OPUS.to_string(),
        clock_rate: 48000,
        channels: 2,
        sdp_fmtp_line: String::new(),
        rtcp_feedback: vec![],
    };
    peer.add_transceiver_from_kind(
        RtpCodecKind::Audio,
        Some(RTCRtpTransceiverInit {
            direction: RTCRtpTransceiverDirection::Sendonly,
            send_encodings: vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(rand::random::<u32>()),
                    ..Default::default()
                },
                codec: opus_codec,
                ..Default::default()
            }],
            ..Default::default()
        }),
    )
    .await?;

    let offer = peer.create_offer(None).await?;
    peer.set_local_description(offer.clone()).await?;

    Ok((
        ViewerSession {
            peer,
            track,
            sender,
            ssrc,
            candidates: candidate_rx,
        },
        offer.sdp,
    ))
}

async fn pump_frames(
    track: Arc<TrackLocalStaticSample>,
    sender: Arc<dyn RtpSender>,
    ssrc: u32,
    source: Arc<VideoSource>,
    viewer: ViewerPeer,
) {
    // 等待协商完成，拿到对端确认的 payload type。
    let payload_type = loop {
        if viewer.closed() {
            return;
        }
        match sender.get_parameters().await {
            Ok(parameters) => {
                if let Some(codec) = parameters
                    .rtp_parameters
                    .codecs
                    .iter()
                    .find(|codec| codec.rtp_codec.mime_type.eq_ignore_ascii_case("video/VP8"))
                {
                    break codec.payload_type;
                }
            }
            Err(_) => return,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let mut index = 0usize;
    while !viewer.closed() {
        let frame = source.frames[index % source.frames.len()].clone();
        let sample = Sample {
            data: frame,
            duration: source.frame_delay,
            ..Sample::new(Instant::now())
        };
        if let Err(err) = track
            .sample_writer(ssrc, payload_type)
            .write_sample(&sample)
            .await
        {
            eprintln!("[publish] write sample failed: {err}");
            break;
        }
        index += 1;
        tokio::time::sleep(source.frame_delay).await;
    }
}

async fn accept_viewer(
    request: JoinRequest,
    source: Arc<VideoSource>,
) -> Result<(), Box<dyn std::error::Error>> {
    let viewer_clid = request.viewer().client_id;
    let (session, offer) = build_viewer_session().await?;
    let ViewerSession {
        peer,
        track,
        sender,
        ssrc,
        mut candidates,
    } = session;

    // 必须先有 offer 再 accept；accept 会把 offer 发给观众。
    let viewer = request.accept(&offer, "join").await?;
    println!("[publish] accepted viewer {viewer_clid}");

    let candidate_viewer = viewer.clone();
    tokio::spawn(async move {
        while let Some(init) = candidates.recv().await {
            let candidate = IceCandidate {
                candidate: init.candidate,
                sdp_mid: init.sdp_mid,
                sdp_m_line_index: init.sdp_mline_index,
            };
            if let Err(err) = candidate_viewer.send_candidate(&candidate).await {
                eprintln!("[publish] send candidate failed: {err}");
                break;
            }
        }
    });

    let mut events = viewer.subscribe();
    let event_peer = Arc::clone(&peer);
    let pump_track = Arc::clone(&track);
    let pump_sender = Arc::clone(&sender);
    let pump_viewer = viewer.clone();
    tokio::spawn(async move {
        let mut pumping = false;
        while let Some(event) = events.recv().await {
            match event {
                PeerEvent::Answer(sdp) => {
                    println!(
                        "[publish] answer from viewer {viewer_clid}: {} bytes",
                        sdp.len()
                    );
                    match RTCSessionDescription::answer(sdp) {
                        Ok(answer) => {
                            if let Err(err) = event_peer.set_remote_description(answer).await {
                                eprintln!("[publish] set remote description failed: {err}");
                                continue;
                            }
                            if !pumping {
                                pumping = true;
                                tokio::spawn(pump_frames(
                                    Arc::clone(&pump_track),
                                    Arc::clone(&pump_sender),
                                    ssrc,
                                    Arc::clone(&source),
                                    pump_viewer.clone(),
                                ));
                            }
                        }
                        Err(err) => eprintln!("[publish] invalid answer: {err}"),
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
                    println!("[publish] viewer {viewer_clid} closed: {:?}", closed.cause);
                    let _ = event_peer.close().await;
                    break;
                }
                _ => {}
            }
        }
    });

    // 事件任务持有 peer，连接随任务结束关闭；本地句柄可以在这里释放。
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ivf = std::env::var("STREAM_IVF")
        .map_err(|_| "set STREAM_IVF to a VP8 .ivf file (see the module docs)")?;
    let source = Arc::new(load_ivf(&ivf)?);

    let address = std::env::var("TEAMSPEAK_ADDR").unwrap_or_else(|_| "127.0.0.1:9987".to_string());
    let name = std::env::var("STREAM_NAME").unwrap_or_else(|_| "Desktop".to_string());
    let mut client = Client::new(
        generateIdentity(8),
        address.clone(),
        "stream-publish".to_string(),
        ClientOptions::default(),
    );

    let streams = Streams::new(&client, StreamsOptions::default());
    println!("[publish] connecting to {address}");
    client.connect().await?;
    client.wait_connected(None).await?;
    client.wait_directory_ready(None).await?;

    if !streams.supported() {
        eprintln!("[publish] server does not advertise STREAM_P2P");
        client.disconnect().await?;
        return Ok(());
    }

    let published = streams.publish(PublishOptions::new(name)).await?;
    println!("[publish] streaming as {}", published.id());

    let mut requests = published.subscribe();
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("[publish] interrupted");
                break;
            }
            event = requests.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    PublishedStreamEvent::Request(request) => {
                        if published.viewers().len() >= MAX_VIEWERS {
                            let _ = request.deny("stream is full").await;
                            continue;
                        }
                        if let Err(err) = accept_viewer(request, Arc::clone(&source)).await {
                            eprintln!("[publish] cannot accept viewer: {err}");
                        }
                    }
                    PublishedStreamEvent::Ended(ended) => {
                        println!("[publish] stream ended: {:?}", ended.cause);
                        break;
                    }
                }
            }
        }
    }

    published.stop(0).await?;
    streams.dispose();
    client.disconnect().await?;
    println!("[publish] done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn offer_orders_video_before_audio_and_keeps_audio_sendonly() {
        let (session, offer) = build_viewer_session().await.expect("build peer");
        let video = offer.find("m=video").expect("offer has m=video");
        let audio = offer.find("m=audio").expect("offer has m=audio");
        assert!(video < audio, "TS6 expects video MID 0 and audio MID 1");
        let audio_section = &offer[audio..];
        assert!(
            audio_section.contains("a=sendonly"),
            "the audio section must stay send-only even without a track"
        );
        assert!(
            offer.len() < 9_150,
            "the offer must fit the escaped command-line budget"
        );
        let _ = session.peer.close().await;
    }
}
