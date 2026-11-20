use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::emitter::{EventHub, MAX_KEPT, StreamSubscription};
use super::errors::StreamError;
use super::peer::{BoxFuture, PeerHost, PeerInner, ViewerPeer, no_audio_warning};
use super::types::{
    JoinRequestCloseCause, JoinRequestEvent, JoinRequestState, PeerCloseCause, PeerClosed,
    StreamEndCause, StreamEnded, StreamInfo, StreamWarning, ViewerIdentity,
};
use super::wire::{build_respond_command, build_signal_command};

const MAX_CLOSED_VIEWERS: usize = 1024;

pub(crate) enum LookupOutcome {
    Immediate(ViewerIdentity),
    Async(BoxFuture<'static, ViewerIdentity>),
}

pub(crate) trait PublishHost: Send + Sync {
    fn lookup_viewer(&self, client_id: i32) -> LookupOutcome;
    fn report_error(&self, error: StreamError);
    fn warn(&self, warning: StreamWarning);
    fn is_denied(&self, identity: &ViewerIdentity) -> bool;
    fn remember_denied(&self, identity: &ViewerIdentity);
    fn send_command(
        &self,
        cmd: String,
        stream_id: Option<String>,
    ) -> BoxFuture<'static, Result<(), StreamError>>;
    fn stop_stream(
        &self,
        stream_id: String,
        reason: u32,
    ) -> BoxFuture<'static, Result<(), StreamError>>;
    fn stream_info(&self, stream_id: &str) -> Option<StreamInfo>;
    fn mark_stopped(&self, stream_id: &str, reason: u32);
}

/// [`PublishedStream`] 派发的事件。
#[derive(Debug, Clone)]
pub enum PublishedStreamEvent {
    Request(JoinRequest),
    Ended(StreamEnded),
}

fn published_event_bucket(event: &PublishedStreamEvent) -> Option<usize> {
    Some(match event {
        PublishedStreamEvent::Request(_) => 0,
        PublishedStreamEvent::Ended(_) => 1,
    })
}

fn published_bucket_name(bucket: usize) -> &'static str {
    match bucket {
        0 => "request",
        _ => "ended",
    }
}

struct Slot {
    token: u64,
    client_id: i32,
    message: String,
    identity: Option<ViewerIdentity>,
    looking: bool,
}

struct PublishedState {
    ended: bool,
    next_slot_token: u64,
    peers: HashMap<i32, ViewerPeer>,
    pending: HashMap<i32, JoinRequest>,
    resolving: HashMap<i32, u64>,
    slots: VecDeque<Slot>,
    closed_viewers_order: VecDeque<i32>,
    closed_viewers_set: HashSet<i32>,
}

pub(crate) struct PublishedInner {
    id: String,
    initial: StreamInfo,
    host: Arc<dyn PublishHost>,
    state: Mutex<PublishedState>,
    overflow_reported: AtomicBool,
    hub: EventHub<PublishedStreamEvent>,
}

struct ViewerPeerHost {
    client_id: i32,
    published: Weak<PublishedInner>,
    publish_host: Arc<dyn PublishHost>,
}

impl PeerHost for ViewerPeerHost {
    fn send_signal_command(
        &self,
        stream_id: String,
        peer_id: i32,
        json: String,
    ) -> BoxFuture<'static, Result<(), StreamError>> {
        match build_signal_command(&stream_id, peer_id, &json) {
            Ok(cmd) => self.publish_host.send_command(cmd, Some(stream_id)),
            Err(err) => Box::pin(async move { Err(err) }),
        }
    }

    fn send_leave_command(
        &self,
        _stream_id: String,
        _peer_id: i32,
    ) -> Option<BoxFuture<'static, Result<(), StreamError>>> {
        None
    }

    fn on_closed(&self, peer: &Arc<PeerInner>) {
        if let Some(published) = self.published.upgrade() {
            published.remove_closed_viewer(self.client_id, peer);
        }
    }

    fn on_error(&self, err: StreamError) {
        self.publish_host.report_error(err);
    }

    fn on_warning(&self, warning: StreamWarning) {
        self.publish_host.warn(warning);
    }
}

impl PublishedInner {
    pub(crate) fn new(info: StreamInfo, host: Arc<dyn PublishHost>) -> Arc<Self> {
        Arc::new(Self {
            id: info.id.clone(),
            initial: info,
            host,
            state: Mutex::new(PublishedState {
                ended: false,
                next_slot_token: 0,
                peers: HashMap::new(),
                pending: HashMap::new(),
                resolving: HashMap::new(),
                slots: VecDeque::new(),
                closed_viewers_order: VecDeque::new(),
                closed_viewers_set: HashSet::new(),
            }),
            overflow_reported: AtomicBool::new(false),
            hub: EventHub::new(2, published_event_bucket),
        })
    }

    fn remove_closed_viewer(&self, client_id: i32, peer: &Arc<PeerInner>) {
        let mut state = self.state.lock().unwrap();
        let Some(current) = state.peers.get(&client_id) else {
            return;
        };
        if !Arc::ptr_eq(&current.inner, peer) {
            return;
        }
        state.peers.remove(&client_id);
        if state.closed_viewers_set.insert(client_id) {
            state.closed_viewers_order.push_back(client_id);
            if state.closed_viewers_order.len() > MAX_CLOSED_VIEWERS
                && let Some(oldest) = state.closed_viewers_order.pop_front()
            {
                state.closed_viewers_set.remove(&oldest);
            }
        }
    }

    fn open_viewer(self: &Arc<Self>, client_id: i32) -> ViewerPeer {
        let peer_host: Arc<dyn PeerHost> = Arc::new(ViewerPeerHost {
            client_id,
            published: Arc::downgrade(self),
            publish_host: Arc::clone(&self.host),
        });
        let peer = ViewerPeer {
            inner: PeerInner::new(self.id.clone(), client_id, peer_host),
        };
        let mut state = self.state.lock().unwrap();
        state.closed_viewers_set.remove(&client_id);
        state.closed_viewers_order.retain(|id| *id != client_id);
        state.peers.insert(client_id, peer.clone());
        peer
    }

    pub(crate) fn request(self: &Arc<Self>, client_id: i32, message: String) {
        {
            let mut state = self.state.lock().unwrap();
            if state.ended
                || state.peers.contains_key(&client_id)
                || state.resolving.contains_key(&client_id)
                || state
                    .pending
                    .get(&client_id)
                    .is_some_and(|req| req.state() == JoinRequestState::Pending)
            {
                return;
            }
            state.next_slot_token = state.next_slot_token.wrapping_add(1);
            let token = state.next_slot_token;
            state.resolving.insert(client_id, token);
            state.slots.push_back(Slot {
                token,
                client_id,
                message,
                identity: None,
                looking: false,
            });
        }
        self.advance();
    }

    fn fill_slot_identity(&self, token: u64, viewer: ViewerIdentity) {
        let mut state = self.state.lock().unwrap();
        if let Some(slot) = state.slots.iter_mut().find(|slot| slot.token == token) {
            slot.looking = false;
            slot.identity = Some(viewer);
        }
    }

    fn emit_event(&self, event: PublishedStreamEvent) {
        if let Some(bucket) = self.hub.emit(event)
            && !self.overflow_reported.swap(true, Ordering::AcqRel)
        {
            self.host
                .report_error(StreamError::Client(crate::Error::Teamspeak(format!(
                    "stream {}: dropping the oldest {}: {} are waiting for a listener",
                    self.id,
                    published_bucket_name(bucket),
                    MAX_KEPT
                ))));
        }
    }

    fn advance(self: &Arc<Self>) {
        let pending_lookups = {
            let mut state = self.state.lock().unwrap();
            let mut list = Vec::new();
            let resolving = &state.resolving;
            let mut to_mark = Vec::new();
            for slot in &state.slots {
                if slot.identity.is_none()
                    && !slot.looking
                    && resolving.get(&slot.client_id) == Some(&slot.token)
                {
                    to_mark.push((slot.token, slot.client_id));
                }
            }
            for (token, client_id) in to_mark {
                if let Some(slot) = state.slots.iter_mut().find(|s| s.token == token) {
                    slot.looking = true;
                    list.push((token, client_id));
                }
            }
            list
        };

        for (token, client_id) in pending_lookups {
            match self.host.lookup_viewer(client_id) {
                LookupOutcome::Immediate(identity) => {
                    self.fill_slot_identity(token, identity);
                }
                LookupOutcome::Async(fut) => {
                    let weak = Arc::downgrade(self);
                    tokio::spawn(async move {
                        let viewer = fut.await;
                        if let Some(published) = weak.upgrade() {
                            published.fill_slot_identity(token, viewer);
                            published.advance();
                        }
                    });
                }
            }
        }

        let ready = {
            let mut state = self.state.lock().unwrap();
            let mut out = Vec::new();
            while let Some(head) = state.slots.front() {
                let current =
                    !state.ended && state.resolving.get(&head.client_id) == Some(&head.token);
                if current && head.identity.is_none() {
                    break;
                }
                let mut slot = state.slots.pop_front().expect("checked front");
                if current {
                    state.resolving.remove(&slot.client_id);
                    if let Some(viewer) = slot.identity.take() {
                        out.push((viewer, slot.message));
                    }
                }
            }
            out
        };

        for (viewer, message) in ready {
            if self.host.is_denied(&viewer) {
                continue;
            }
            let client_id = viewer.client_id;
            let request = JoinRequest {
                inner: Arc::new(JoinRequestInner {
                    stream: Arc::clone(self),
                    viewer,
                    message,
                    state: Mutex::new(JoinRequestState::Pending),
                    hub: EventHub::new(1, |_| Some(0)),
                }),
            };
            let inserted = {
                let mut state = self.state.lock().unwrap();
                if state.ended {
                    false
                } else {
                    state.pending.insert(client_id, request.clone());
                    true
                }
            };
            if inserted {
                self.emit_event(PublishedStreamEvent::Request(request));
            }
        }
    }

    pub(crate) fn drop_pending(self: &Arc<Self>, client_id: i32) {
        let (removed_req, had_resolving) = {
            let mut state = self.state.lock().unwrap();
            let had_resolving = if let Some(token) = state.resolving.remove(&client_id) {
                if let Some(pos) = state
                    .slots
                    .iter()
                    .position(|slot| slot.token == token && !slot.looking)
                {
                    state.slots.remove(pos);
                }
                true
            } else {
                false
            };
            let req = state.pending.remove(&client_id);
            (req, had_resolving)
        };
        if let Some(req) = removed_req {
            req.inner.close(JoinRequestCloseCause::Withdrawn);
        }
        if had_resolving {
            self.advance();
        }
    }

    pub(crate) fn viewer_gone(self: &Arc<Self>, client_id: i32) {
        self.drop_pending(client_id);
        let peer = self.state.lock().unwrap().peers.get(&client_id).cloned();
        if let Some(peer) = peer {
            peer.inner.end(PeerClosed {
                cause: PeerCloseCause::Left,
                reason: None,
            });
        }
    }

    pub(crate) fn signal(&self, client_id: i32, json: &str) -> bool {
        let (peer, was_closed) = {
            let state = self.state.lock().unwrap();
            (
                state.peers.get(&client_id).cloned(),
                state.closed_viewers_set.contains(&client_id),
            )
        };
        if let Some(peer) = peer {
            peer.inner.deliver(json);
            return true;
        }
        was_closed
    }

    pub(crate) fn finish(&self, ended: StreamEnded) {
        let (requests, peers) = {
            let mut state = self.state.lock().unwrap();
            if state.ended {
                return;
            }
            state.ended = true;
            state.slots.clear();
            state.resolving.clear();
            let requests: Vec<JoinRequest> = state.pending.drain().map(|(_, req)| req).collect();
            let peers: Vec<ViewerPeer> = state.peers.values().cloned().collect();
            (requests, peers)
        };
        for req in requests {
            req.inner.close(JoinRequestCloseCause::Ended);
        }
        let (cause, reason) = match ended.cause {
            StreamEndCause::Disconnected => (PeerCloseCause::Disconnected, None),
            StreamEndCause::Disposed => (PeerCloseCause::Disposed, None),
            StreamEndCause::Stopped | StreamEndCause::Server | StreamEndCause::Gone => {
                (PeerCloseCause::Stopped, Some(ended.reason))
            }
        };
        for peer in peers {
            peer.inner.end(PeerClosed { cause, reason });
        }
        self.emit_event(PublishedStreamEvent::Ended(ended));
    }
}

/// 本端正在发布的流句柄。
#[derive(Clone)]
pub struct PublishedStream {
    pub(crate) inner: Arc<PublishedInner>,
}

impl std::fmt::Debug for PublishedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishedStream")
            .field("id", &self.id())
            .field("ended", &self.ended())
            .finish()
    }
}

impl PublishedStream {
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    pub fn info(&self) -> StreamInfo {
        self.inner
            .host
            .stream_info(&self.inner.id)
            .unwrap_or_else(|| self.inner.initial.clone())
    }

    pub fn ended(&self) -> bool {
        self.inner.state.lock().unwrap().ended
    }

    pub fn subscribe(&self) -> StreamSubscription<PublishedStreamEvent> {
        self.inner.hub.subscribe()
    }

    pub fn viewers(&self) -> Vec<ViewerPeer> {
        self.inner
            .state
            .lock()
            .unwrap()
            .peers
            .values()
            .cloned()
            .collect()
    }

    /// 停止发布当前流（调用 `stopstream`）。
    pub async fn stop(&self, reason: u32) -> Result<(), StreamError> {
        if self.ended() {
            return Ok(());
        }
        self.inner
            .host
            .stop_stream(self.inner.id.clone(), reason)
            .await?;
        self.inner.host.mark_stopped(&self.inner.id, reason);
        Ok(())
    }
}

struct JoinRequestInner {
    stream: Arc<PublishedInner>,
    viewer: ViewerIdentity,
    message: String,
    state: Mutex<JoinRequestState>,
    hub: EventHub<JoinRequestEvent>,
}

impl JoinRequestInner {
    fn state(&self) -> JoinRequestState {
        *self.state.lock().unwrap()
    }

    fn close(&self, cause: JoinRequestCloseCause) {
        let next = match cause {
            JoinRequestCloseCause::Withdrawn => JoinRequestState::Withdrawn,
            JoinRequestCloseCause::Ended => JoinRequestState::Ended,
        };
        {
            let mut state = self.state.lock().unwrap();
            if *state != JoinRequestState::Pending {
                return;
            }
            *state = next;
        }
        let _ = self.hub.emit(JoinRequestEvent::Closed { cause });
    }

    fn assert_pending(&self) -> Result<(), StreamError> {
        let current = self.state();
        if current != JoinRequestState::Pending {
            return Err(StreamError::Client(crate::Error::Teamspeak(format!(
                "the request cannot be answered: it is {:?}",
                current
            ))));
        }
        Ok(())
    }
}

/// 来自观众的观看申请，等待发布者调用 `accept` 或 `deny`。
#[derive(Clone)]
pub struct JoinRequest {
    inner: Arc<JoinRequestInner>,
}

impl std::fmt::Debug for JoinRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinRequest")
            .field("stream_id", &self.stream_id())
            .field("viewer", &self.viewer())
            .field("state", &self.state())
            .finish()
    }
}

impl JoinRequest {
    pub fn stream_id(&self) -> &str {
        &self.inner.stream.id
    }

    pub fn viewer(&self) -> &ViewerIdentity {
        &self.inner.viewer
    }

    pub fn message(&self) -> &str {
        &self.inner.message
    }

    pub fn state(&self) -> JoinRequestState {
        self.inner.state()
    }

    pub fn subscribe(&self) -> StreamSubscription<JoinRequestEvent> {
        self.inner.hub.subscribe()
    }

    /// 接受观看申请，向观众发送初始 SDP `offer` 并返回对应 [`ViewerPeer`]。
    ///
    /// 若 `offer` 中未包含 `m=audio` 段，会触发 `OfferWithoutAudio` 警告但仍允许发送。
    pub async fn accept(&self, offer: &str, message: &str) -> Result<ViewerPeer, StreamError> {
        if offer.is_empty() {
            return Err(StreamError::InvalidOption(
                "the offer must be a non-empty SDP string".to_string(),
            ));
        }
        self.inner.assert_pending()?;
        let cmd = build_respond_command(
            self.stream_id(),
            self.inner.viewer.client_id,
            message,
            offer,
            true,
        )?;
        {
            let mut state = self.inner.state.lock().unwrap();
            if *state != JoinRequestState::Pending {
                return Err(StreamError::Client(crate::Error::Teamspeak(format!(
                    "the request cannot be answered: it is {:?}",
                    *state
                ))));
            }
            *state = JoinRequestState::Accepted;
        }
        if !super::wire::has_audio_section(offer) {
            self.inner.stream.host.warn(no_audio_warning(
                self.stream_id(),
                self.inner.viewer.client_id,
            ));
        }
        let peer = self.inner.stream.open_viewer(self.inner.viewer.client_id);
        let send_res = self
            .inner
            .stream
            .host
            .send_command(cmd, Some(self.stream_id().to_string()))
            .await;
        if let Err(err) = send_res {
            *self.inner.state.lock().unwrap() = JoinRequestState::Failed;
            peer.inner.end(PeerClosed {
                cause: PeerCloseCause::Closed,
                reason: None,
            });
            return Err(err);
        }
        Ok(peer)
    }

    /// 拒绝观看申请（`offer` 显式为空串以满足 TS6 协议要求）。
    pub async fn deny(&self, reason: &str) -> Result<(), StreamError> {
        self.inner.assert_pending()?;
        let cmd = build_respond_command(
            self.stream_id(),
            self.inner.viewer.client_id,
            reason,
            "",
            false,
        )?;
        {
            let mut state = self.inner.state.lock().unwrap();
            if *state != JoinRequestState::Pending {
                return Err(StreamError::Client(crate::Error::Teamspeak(format!(
                    "the request cannot be answered: it is {:?}",
                    *state
                ))));
            }
            *state = JoinRequestState::Denied;
        }
        let send_res = self
            .inner
            .stream
            .host
            .send_command(cmd, Some(self.stream_id().to_string()))
            .await;
        if let Err(err) = send_res {
            *self.inner.state.lock().unwrap() = JoinRequestState::Failed;
            return Err(err);
        }
        self.inner.stream.host.remember_denied(&self.inner.viewer);
        Ok(())
    }
}
