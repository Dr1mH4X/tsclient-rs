use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::emitter::{EventHub, MAX_KEPT, StreamSubscription};
use super::errors::StreamError;
use super::types::{
    IceCandidate, PeerCloseCause, PeerClosed, PeerEvent, StreamWarning, StreamWarningCode,
};
use super::wire::{
    ParsedSignal, answer_json, candidate_json, encode_signal, has_audio_section, offer_json,
    parse_signal,
};

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) trait PeerHost: Send + Sync {
    fn send_signal_command(
        &self,
        stream_id: String,
        peer_id: i32,
        json: String,
    ) -> BoxFuture<'static, Result<(), StreamError>>;

    fn send_leave_command(
        &self,
        stream_id: String,
        peer_id: i32,
    ) -> Option<BoxFuture<'static, Result<(), StreamError>>>;

    fn on_closed(&self, peer: &Arc<PeerInner>);
    fn on_error(&self, err: StreamError);
    fn on_warning(&self, warning: StreamWarning);
}

/// 当 SDP offer 缺失 `m=audio` 段时生成的警告。
pub(crate) fn no_audio_warning(stream_id: &str, client_id: i32) -> StreamWarning {
    StreamWarning {
        code: StreamWarningCode::OfferWithoutAudio,
        stream_id: stream_id.to_string(),
        client_id,
        message: format!(
            "the offer for client {client_id} has no m=audio section: the TeamSpeak 6 client does not answer an offer without audio, and its Join button then does nothing"
        ),
    }
}

fn peer_event_bucket(event: &PeerEvent) -> Option<usize> {
    Some(match event {
        PeerEvent::Offer(_) => 0,
        PeerEvent::Answer(_) => 1,
        PeerEvent::Candidate(_) => 2,
        PeerEvent::Signal(_) => 3,
        PeerEvent::Closed(_) => 4,
    })
}

fn peer_bucket_name(bucket: usize) -> &'static str {
    match bucket {
        0 => "offer",
        1 => "answer",
        2 => "candidate",
        3 => "signal",
        _ => "closed",
    }
}

pub(crate) struct PeerInner {
    stream_id: String,
    client_id: i32,
    closed: Mutex<bool>,
    overflow_reported: AtomicBool,
    hub: EventHub<PeerEvent>,
    host: Arc<dyn PeerHost>,
}

impl PeerInner {
    pub(crate) fn new(stream_id: String, client_id: i32, host: Arc<dyn PeerHost>) -> Arc<Self> {
        Arc::new(Self {
            stream_id,
            client_id,
            closed: Mutex::new(false),
            overflow_reported: AtomicBool::new(false),
            hub: EventHub::new(5, peer_event_bucket),
            host,
        })
    }

    pub(crate) fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub(crate) fn client_id(&self) -> i32 {
        self.client_id
    }

    pub(crate) fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }

    pub(crate) fn subscribe(&self) -> StreamSubscription<PeerEvent> {
        self.hub.subscribe()
    }

    pub(crate) fn deliver(&self, json: &str) {
        if self.is_closed() {
            return;
        }
        let event = match parse_signal(json) {
            ParsedSignal::Offer(sdp) => PeerEvent::Offer(sdp),
            ParsedSignal::Answer(sdp) => PeerEvent::Answer(sdp),
            ParsedSignal::Candidate(c) => PeerEvent::Candidate(c),
            ParsedSignal::Raw(r) => PeerEvent::Signal(r),
        };
        if let Some(bucket) = self.hub.emit(event)
            && !self.overflow_reported.swap(true, Ordering::AcqRel)
        {
            self.host.on_error(StreamError::Client(crate::Error::Teamspeak(
                format!(
                    "stream {}: dropping the oldest {} from client {}: {} are waiting for a listener",
                    self.stream_id,
                    peer_bucket_name(bucket),
                    self.client_id,
                    MAX_KEPT
                ),
            )));
        }
    }

    pub(crate) fn end(self: &Arc<Self>, closed: PeerClosed) {
        {
            let mut flag = self.closed.lock().unwrap();
            if *flag {
                return;
            }
            *flag = true;
        }
        self.host.on_closed(self);
        let _ = self.hub.emit(PeerEvent::Closed(closed));
    }

    async fn send_json(&self, json: String) -> Result<(), StreamError> {
        if self.is_closed() {
            return Err(StreamError::Client(crate::Error::Teamspeak(
                "the peer is closed".to_string(),
            )));
        }
        self.host
            .send_signal_command(self.stream_id.clone(), self.client_id, json)
            .await
    }

    async fn send_candidate(&self, candidate: &IceCandidate) -> Result<(), StreamError> {
        self.send_json(candidate_json(candidate)).await
    }

    async fn send_signal(&self, cmd: &str, args: serde_json::Value) -> Result<(), StreamError> {
        self.send_json(encode_signal(cmd, &args)).await
    }

    async fn close(self: &Arc<Self>) -> Result<(), StreamError> {
        if self.is_closed() {
            return Ok(());
        }
        self.end(PeerClosed {
            cause: PeerCloseCause::Closed,
            reason: None,
        });
        if let Some(fut) = self
            .host
            .send_leave_command(self.stream_id.clone(), self.client_id)
        {
            fut.await?;
        }
        Ok(())
    }
}

/// 发布者侧对应单一观众的信令句柄。
#[derive(Clone)]
pub struct ViewerPeer {
    pub(crate) inner: Arc<PeerInner>,
}

impl std::fmt::Debug for ViewerPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewerPeer")
            .field("stream_id", &self.stream_id())
            .field("client_id", &self.client_id())
            .field("closed", &self.closed())
            .finish()
    }
}

impl ViewerPeer {
    pub fn stream_id(&self) -> &str {
        self.inner.stream_id()
    }

    pub fn client_id(&self) -> i32 {
        self.inner.client_id()
    }

    pub fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn subscribe(&self) -> StreamSubscription<PeerEvent> {
        self.inner.subscribe()
    }

    /// 发送重协商 SDP offer（缺少 `m=audio` 时会触发 `OfferWithoutAudio` 警告但仍透传）。
    pub async fn send_offer(&self, sdp: &str) -> Result<(), StreamError> {
        if !self.closed() && !has_audio_section(sdp) {
            self.inner
                .host
                .on_warning(no_audio_warning(self.stream_id(), self.client_id()));
        }
        self.inner.send_json(offer_json(sdp)).await
    }

    pub async fn send_candidate(&self, candidate: &IceCandidate) -> Result<(), StreamError> {
        self.inner.send_candidate(candidate).await
    }

    pub async fn send_signal(&self, cmd: &str, args: serde_json::Value) -> Result<(), StreamError> {
        self.inner.send_signal(cmd, args).await
    }

    pub async fn close(&self) -> Result<(), StreamError> {
        self.inner.close().await
    }
}

/// 观看者侧的流观看会话句柄（包含发布者下发的初始 SDP `offer`）。
#[derive(Clone)]
pub struct StreamView {
    offer: String,
    pub(crate) inner: Arc<PeerInner>,
}

impl std::fmt::Debug for StreamView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamView")
            .field("stream_id", &self.stream_id())
            .field("client_id", &self.client_id())
            .field("closed", &self.closed())
            .finish()
    }
}

impl StreamView {
    pub(crate) fn new(offer: String, inner: Arc<PeerInner>) -> Self {
        Self { offer, inner }
    }

    pub fn stream_id(&self) -> &str {
        self.inner.stream_id()
    }

    pub fn client_id(&self) -> i32 {
        self.inner.client_id()
    }

    pub fn offer(&self) -> &str {
        &self.offer
    }

    pub fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn subscribe(&self) -> StreamSubscription<PeerEvent> {
        self.inner.subscribe()
    }

    pub async fn answer(&self, sdp: &str) -> Result<(), StreamError> {
        self.inner.send_json(answer_json(sdp)).await
    }

    pub async fn send_candidate(&self, candidate: &IceCandidate) -> Result<(), StreamError> {
        self.inner.send_candidate(candidate).await
    }

    pub async fn send_signal(&self, cmd: &str, args: serde_json::Value) -> Result<(), StreamError> {
        self.inner.send_signal(cmd, args).await
    }

    /// 离开流观看（向服务端发送 `joinstreamrequest is_remove=1` 并关闭本地会话）。
    pub async fn leave(&self) -> Result<(), StreamError> {
        self.inner.close().await
    }
}
