use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::client::{Client, ClientHandle, ClientStatus, ClientWeak};
use crate::{Event, ServerInfo};

use super::emitter::{EventHub, MAX_KEPT, StreamSubscription};
use super::errors::{
    StreamEndedError, StreamError, StreamJoinDeniedError, StreamTransportUnsupportedError,
    StreamsUnsupportedError, is_not_connected_client_error,
};
use super::peer::{BoxFuture, PeerHost, PeerInner, StreamView};
use super::published::{LookupOutcome, PublishHost, PublishedInner, PublishedStream};
use super::types::{
    PeerCloseCause, PeerClosed, PublishOptions, StreamEndCause, StreamEnded, StreamInfo,
    StreamStoppedEvent, StreamTransport, StreamViewerEvent, StreamWarning, StreamsEvent,
    StreamsOptions, ViewerIdentity, WatchOptions, WatchTarget,
};
use super::wire::{
    build_info_command, build_join_command, build_setup_command, build_signal_command,
    build_stop_command, client_of, reason_of, stream_info_from,
};

const DEFAULT_PUBLISH_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_WATCH_TIMEOUT_MS: u64 = 60_000;
const MAX_EARLY_SIGNALS: usize = MAX_KEPT;
const MAX_DENIED: usize = 1024;
const MAX_CLOSED_VIEWS: usize = 1024;
pub(crate) const GONE_GRACE_MS: u64 = 2_000;
pub(crate) const REFRESH_COOLDOWN_MS: u64 = 2_000;
const ANNOUNCE_WINDOW_MS: u64 = 5_000;
const MAX_ORPHANS: usize = 16;
const MAX_STOPPED: usize = 256;

struct BoundedSet<T> {
    max: usize,
    order: VecDeque<T>,
    set: HashSet<T>,
}

impl<T: Eq + std::hash::Hash + Clone> BoundedSet<T> {
    fn new(max: usize) -> Self {
        Self {
            max,
            order: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn contains(&self, item: &T) -> bool {
        self.set.contains(item)
    }

    fn insert(&mut self, item: T) {
        if self.set.contains(&item) {
            self.order.retain(|existing| existing != &item);
        } else {
            self.set.insert(item.clone());
        }
        self.order.push_back(item);
        while self.order.len() > self.max {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
    }

    fn remove(&mut self, item: &T) {
        if self.set.remove(item) {
            self.order.retain(|existing| existing != item);
        }
    }

    fn clear(&mut self) {
        self.order.clear();
        self.set.clear();
    }
}

struct GoneTimerEntry {
    token: u64,
    handle: tokio::task::JoinHandle<()>,
}

struct PublishAttempt {
    tx: Mutex<Option<oneshot::Sender<Result<PublishedStream, StreamError>>>>,
    abandoned: Mutex<bool>,
    reset: Mutex<bool>,
}

impl PublishAttempt {
    fn resolve(&self, value: Result<PublishedStream, StreamError>) {
        if let Some(tx) = self.tx.lock().unwrap().take() {
            let _ = tx.send(value);
        }
    }
}

struct WatchEntry {
    token: u64,
    streamer: i32,
    early: Vec<String>,
    overflowed: bool,
    view: Option<StreamView>,
    settled_tx: Option<oneshot::Sender<Result<StreamView, StreamError>>>,
}

impl WatchEntry {
    fn resolve(&mut self, value: Result<StreamView, StreamError>) {
        if let Some(tx) = self.settled_tx.take() {
            let _ = tx.send(value);
        }
    }
}

struct StreamsState {
    disposed: bool,
    remember_denied: bool,
    infos: HashMap<String, StreamInfo>,
    info_order: Vec<String>,
    asked: HashSet<i32>,
    discover_in_flight: HashMap<i32, usize>,
    last_discover_at: HashMap<i32, tokio::time::Instant>,
    known_client_streaming: HashMap<i32, bool>,
    next_gone_token: u64,
    gone_timers: HashMap<i32, GoneTimerEntry>,
    published: HashMap<String, PublishedStream>,
    next_watch_token: u64,
    watching: HashMap<String, WatchEntry>,
    denied: BoundedSet<String>,
    closed_views: BoundedSet<String>,
    next_attempt_id: u64,
    publishing: HashMap<u64, Arc<PublishAttempt>>,
    orphans: BoundedSet<String>,
    stopped: BoundedSet<String>,
}

impl StreamsState {
    fn new(remember_denied: bool) -> Self {
        Self {
            disposed: false,
            remember_denied,
            infos: HashMap::new(),
            info_order: Vec::new(),
            asked: HashSet::new(),
            discover_in_flight: HashMap::new(),
            last_discover_at: HashMap::new(),
            known_client_streaming: HashMap::new(),
            next_gone_token: 0,
            gone_timers: HashMap::new(),
            published: HashMap::new(),
            next_watch_token: 0,
            watching: HashMap::new(),
            denied: BoundedSet::new(MAX_DENIED),
            closed_views: BoundedSet::new(MAX_CLOSED_VIEWS),
            next_attempt_id: 0,
            publishing: HashMap::new(),
            orphans: BoundedSet::new(MAX_ORPHANS),
            stopped: BoundedSet::new(MAX_STOPPED),
        }
    }

    fn insert_info(&mut self, info: StreamInfo) -> Option<StreamInfo> {
        let prev = self.infos.insert(info.id.clone(), info.clone());
        if prev.is_none() {
            self.info_order.push(info.id);
        }
        prev
    }

    fn remove_info(&mut self, id: &str) -> Option<StreamInfo> {
        let removed = self.infos.remove(id);
        if removed.is_some() {
            self.info_order.retain(|existing| existing != id);
        }
        removed
    }

    fn has_forgettable_streams_for_client(&self, clid: i32) -> bool {
        self.infos
            .values()
            .any(|info| info.client_id == clid && !self.published.contains_key(&info.id))
    }

    fn list_infos(&self) -> Vec<StreamInfo> {
        self.info_order
            .iter()
            .filter_map(|id| self.infos.get(id).cloned())
            .collect()
    }
}

fn streams_event_bucket(event: &StreamsEvent) -> Option<usize> {
    match event {
        StreamsEvent::Error(_) => Some(0),
        StreamsEvent::Warning(_) => Some(1),
        StreamsEvent::Started(_)
        | StreamsEvent::Updated(_)
        | StreamsEvent::Stopped(_)
        | StreamsEvent::ViewerJoined(_)
        | StreamsEvent::ViewerLeft(_) => None,
    }
}

fn server_error_of(err: &crate::Error) -> Option<crate::ServerError> {
    match err {
        crate::Error::ServerError {
            id,
            server_message,
            extra_message,
        } => Some(crate::ServerError {
            id: id.clone(),
            server_message: server_message.clone(),
            extra_message: extra_message.clone(),
        }),
        _ => None,
    }
}

fn translate_stream_error(err: crate::Error, stream_id: Option<&str>) -> StreamError {
    if is_not_connected_client_error(&err) {
        return StreamError::NotConnected;
    }
    if let Some(id) = stream_id
        && let Some(server_err) = server_error_of(&err)
        && server_err.error_code() == Some(crate::ErrorCode::StreamNotFound)
    {
        return StreamEndedError {
            stream_id: id.to_string(),
            cause: Some(server_err),
        }
        .into();
    }
    StreamError::Client(err)
}

pub(crate) struct StreamsInner {
    client: ClientWeak,
    state: Mutex<StreamsState>,
    hub: EventHub<StreamsEvent>,
    gone_grace: Duration,
    refresh_cooldown: Duration,
}

impl Drop for StreamsInner {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        for (_, entry) in state.gone_timers.drain() {
            entry.handle.abort();
        }
    }
}

struct StreamsPublishHost {
    streams: Weak<StreamsInner>,
}

impl PublishHost for StreamsPublishHost {
    fn lookup_viewer(&self, client_id: i32) -> LookupOutcome {
        let Some(streams) = self.streams.upgrade() else {
            return LookupOutcome::Immediate(ViewerIdentity {
                client_id,
                uid: None,
                nickname: None,
            });
        };
        let Some(handle) = streams.client.upgrade() else {
            return LookupOutcome::Immediate(ViewerIdentity {
                client_id,
                uid: None,
                nickname: None,
            });
        };
        if let Some(info) = handle.client_info(client_id) {
            return LookupOutcome::Immediate(ViewerIdentity {
                client_id,
                uid: Some(info.uid),
                nickname: Some(info.nickname),
            });
        }
        LookupOutcome::Async(Box::pin(async move {
            let cmd = format!("clientgetuidfromclid clid={client_id}");
            match handle
                .exec_command_for_stream(&cmd, crate::api::CMD_TIMEOUT_MS, None)
                .await
            {
                Ok(rows) => {
                    if let Some(row) = rows.first() {
                        ViewerIdentity {
                            client_id,
                            uid: row.get("cluid").cloned(),
                            nickname: row.get("nickname").cloned(),
                        }
                    } else {
                        ViewerIdentity {
                            client_id,
                            uid: None,
                            nickname: None,
                        }
                    }
                }
                Err(_) => ViewerIdentity {
                    client_id,
                    uid: None,
                    nickname: None,
                },
            }
        }))
    }

    fn report_error(&self, error: StreamError) {
        if let Some(streams) = self.streams.upgrade() {
            streams.report(error);
        }
    }

    fn warn(&self, warning: StreamWarning) {
        if let Some(streams) = self.streams.upgrade() {
            let _ = streams.hub.emit(StreamsEvent::Warning(warning));
        }
    }

    fn is_denied(&self, identity: &ViewerIdentity) -> bool {
        let Some(streams) = self.streams.upgrade() else {
            return false;
        };
        let Some(uid) = identity.uid.as_ref() else {
            return false;
        };
        let state = streams.state.lock().unwrap();
        state.remember_denied && state.denied.contains(uid)
    }

    fn remember_denied(&self, identity: &ViewerIdentity) {
        let Some(streams) = self.streams.upgrade() else {
            return;
        };
        let Some(uid) = identity.uid.as_ref() else {
            return;
        };
        let mut state = streams.state.lock().unwrap();
        if state.remember_denied {
            state.denied.insert(uid.clone());
        }
    }

    fn send_command(
        &self,
        cmd: String,
        stream_id: Option<String>,
    ) -> BoxFuture<'static, Result<(), StreamError>> {
        let streams = self.streams.upgrade();
        Box::pin(async move {
            let Some(streams) = streams else {
                return Err(StreamError::Disposed);
            };
            streams
                .send_checked_command(&cmd, stream_id.as_deref(), DEFAULT_PUBLISH_TIMEOUT_MS, None)
                .await
        })
    }

    fn stop_stream(
        &self,
        stream_id: String,
        reason: u32,
    ) -> BoxFuture<'static, Result<(), StreamError>> {
        let streams = self.streams.upgrade();
        Box::pin(async move {
            let Some(streams) = streams else {
                return Err(StreamError::Disposed);
            };
            streams.stop_on_server(&stream_id, reason).await
        })
    }

    fn stream_info(&self, stream_id: &str) -> Option<StreamInfo> {
        let streams = self.streams.upgrade()?;
        let state = streams.state.lock().unwrap();
        state.infos.get(stream_id).cloned()
    }

    fn mark_stopped(&self, stream_id: &str, reason: u32) {
        if let Some(streams) = self.streams.upgrade() {
            streams.end_stream(stream_id, reason, StreamEndCause::Stopped);
        }
    }
}

struct StreamViewHost {
    stream_id: String,
    streams: Weak<StreamsInner>,
}

impl PeerHost for StreamViewHost {
    fn send_signal_command(
        &self,
        stream_id: String,
        peer_id: i32,
        json: String,
    ) -> BoxFuture<'static, Result<(), StreamError>> {
        let streams = self.streams.upgrade();
        Box::pin(async move {
            let Some(streams) = streams else {
                return Err(StreamError::Disposed);
            };
            let cmd = build_signal_command(&stream_id, peer_id, &json)?;
            streams
                .send_checked_command(&cmd, Some(&stream_id), crate::api::CMD_TIMEOUT_MS, None)
                .await
        })
    }

    fn send_leave_command(
        &self,
        stream_id: String,
        peer_id: i32,
    ) -> Option<BoxFuture<'static, Result<(), StreamError>>> {
        let streams = self.streams.upgrade();
        Some(Box::pin(async move {
            let Some(streams) = streams else {
                return Err(StreamError::Disposed);
            };
            streams.leave_on_server(&stream_id, peer_id).await
        }))
    }

    fn on_closed(&self, peer: &Arc<PeerInner>) {
        if let Some(streams) = self.streams.upgrade() {
            let mut state = streams.state.lock().unwrap();
            if let Some(entry) = state.watching.get(&self.stream_id)
                && entry
                    .view
                    .as_ref()
                    .is_some_and(|view| Arc::ptr_eq(&view.inner, peer))
            {
                state.watching.remove(&self.stream_id);
            }
            state.closed_views.insert(self.stream_id.clone());
        }
    }

    fn on_error(&self, err: StreamError) {
        if let Some(streams) = self.streams.upgrade() {
            streams.report(err);
        }
    }

    fn on_warning(&self, warning: StreamWarning) {
        if let Some(streams) = self.streams.upgrade() {
            let _ = streams.hub.emit(StreamsEvent::Warning(warning));
        }
    }
}

impl StreamsInner {
    fn report(&self, error: StreamError) {
        let _ = self.hub.emit(StreamsEvent::Error(error));
    }

    fn server_info(&self) -> Option<ServerInfo> {
        self.client.upgrade()?.server_info()
    }

    fn supported(&self) -> bool {
        self.server_info()
            .is_some_and(|info| info.supports_stream_p2p())
    }

    fn assert_usable(&self) -> Result<(), StreamError> {
        if self.state.lock().unwrap().disposed {
            return Err(StreamError::Disposed);
        }
        let server_info = self.server_info();
        match server_info {
            Some(info) if info.supports_stream_p2p() => Ok(()),
            Some(info) => Err(StreamsUnsupportedError {
                version: info.version,
                extensions: info.extensions,
            }
            .into()),
            None => Err(StreamsUnsupportedError {
                version: String::new(),
                extensions: Vec::new(),
            }
            .into()),
        }
    }

    async fn send_checked_command(
        &self,
        cmd: &str,
        stream_id: Option<&str>,
        timeout_ms: u64,
        sent_flag: Option<&AtomicBool>,
    ) -> Result<(), StreamError> {
        let Some(handle) = self.client.upgrade() else {
            return Err(StreamError::NotConnected);
        };
        handle
            .exec_command_for_stream(cmd, timeout_ms, sent_flag)
            .await
            .map(|_| ())
            .map_err(|err| translate_stream_error(err, stream_id))
    }

    async fn stop_on_server(&self, stream_id: &str, reason: u32) -> Result<(), StreamError> {
        let Some(handle) = self.client.upgrade() else {
            return Err(StreamError::NotConnected);
        };
        let cmd = build_stop_command(stream_id, reason)?;
        match handle
            .exec_command_for_stream(&cmd, crate::api::CMD_TIMEOUT_MS, None)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                if is_not_connected_client_error(&err) {
                    return Err(StreamError::NotConnected);
                }
                if let Some(server_err) = server_error_of(&err)
                    && server_err.error_code() == Some(crate::ErrorCode::StreamNotFound)
                {
                    return Ok(());
                }
                Err(StreamError::Client(err))
            }
        }
    }

    async fn leave_on_server(&self, stream_id: &str, streamer: i32) -> Result<(), StreamError> {
        let cmd = build_join_command(stream_id, streamer, "", true)?;
        match self
            .send_checked_command(&cmd, Some(stream_id), crate::api::CMD_TIMEOUT_MS, None)
            .await
        {
            Ok(()) | Err(StreamError::Ended(_)) | Err(StreamError::NotConnected) => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn spawn_best_effort<F>(self: &Arc<Self>, fut: F)
    where
        F: std::future::Future<Output = Result<(), StreamError>> + Send + 'static,
    {
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            self.spawn_best_effort_on(&rt, fut);
        }
    }

    fn spawn_best_effort_on<F>(self: &Arc<Self>, rt: &tokio::runtime::Handle, fut: F)
    where
        F: std::future::Future<Output = Result<(), StreamError>> + Send + 'static,
    {
        let weak = Arc::downgrade(self);
        rt.spawn(async move {
            if let Err(err) = fut.await
                && !err.is_not_connected()
                && let Some(streams) = weak.upgrade()
            {
                streams.report(err);
            }
        });
    }

    fn can_query_client(&self, clid: i32) -> Option<ClientHandle> {
        let handle = self.client.upgrade()?;
        if handle.status() != ClientStatus::Connected || clid == 0 || clid == handle.client_id() {
            return None;
        }
        if !self.supported() {
            return None;
        }
        Some(handle)
    }

    fn record_query_start(state: &mut StreamsState, clid: i32, cooldown: Duration) {
        let now = tokio::time::Instant::now();
        *state.discover_in_flight.entry(clid).or_insert(0) += 1;
        state.last_discover_at.insert(clid, now);
        if state.last_discover_at.len() > 1024 {
            state
                .last_discover_at
                .retain(|_, ts| now.saturating_duration_since(*ts) < cooldown);
        }
    }

    fn finish_query_in_flight(state: &mut StreamsState, clid: i32) {
        if let Some(count) = state.discover_in_flight.get_mut(&clid) {
            if *count <= 1 {
                state.discover_in_flight.remove(&clid);
            } else {
                *count -= 1;
            }
        }
    }

    fn send_info_query(
        self: &Arc<Self>,
        handle: ClientHandle,
        clid: i32,
        clear_asked_on_err: bool,
    ) {
        let Ok(cmd) = build_info_command(clid) else {
            let mut state = self.state.lock().unwrap();
            Self::finish_query_in_flight(&mut state, clid);
            if clear_asked_on_err {
                state.asked.remove(&clid);
            }
            return;
        };
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let res = handle
                .exec_command_for_stream(&cmd, crate::api::CMD_TIMEOUT_MS, None)
                .await;
            if let Some(streams) = weak.upgrade() {
                let mut report_err = None;
                {
                    let mut state = streams.state.lock().unwrap();
                    Self::finish_query_in_flight(&mut state, clid);
                    if let Err(err) = res {
                        if clear_asked_on_err {
                            state.asked.remove(&clid);
                        }
                        if !is_not_connected_client_error(&err) {
                            report_err = Some(StreamError::Client(err));
                        }
                    }
                }
                if let Some(err) = report_err {
                    streams.report(err);
                }
            }
        });
    }

    fn discover(self: &Arc<Self>, clid: i32) {
        let Some(handle) = self.can_query_client(clid) else {
            return;
        };
        {
            let mut state = self.state.lock().unwrap();
            if state.disposed || !state.asked.insert(clid) {
                return;
            }
            Self::record_query_start(&mut state, clid, self.refresh_cooldown);
        }
        self.send_info_query(handle, clid, true);
    }

    fn refresh_client(self: &Arc<Self>, clid: i32) {
        let Some(handle) = self.can_query_client(clid) else {
            return;
        };
        {
            let mut state = self.state.lock().unwrap();
            if state.disposed || state.discover_in_flight.contains_key(&clid) {
                return;
            }
            let now = tokio::time::Instant::now();
            if state
                .last_discover_at
                .get(&clid)
                .is_some_and(|prev| now.saturating_duration_since(*prev) < self.refresh_cooldown)
            {
                return;
            }
            Self::record_query_start(&mut state, clid, self.refresh_cooldown);
        }
        self.send_info_query(handle, clid, false);
    }

    fn scan_directory_snapshot(self: &Arc<Self>, reset_asked: bool) {
        let Some(handle) = self.client.upgrade() else {
            return;
        };
        if handle.status() != ClientStatus::Connected || !self.supported() {
            return;
        }
        let Some(snapshot) = handle.directory_snapshot() else {
            return;
        };
        let self_clid = handle.client_id();
        let streaming_ids: Vec<i32> = snapshot
            .clients
            .iter()
            .filter(|c| c.is_streaming == Some(true) && c.id != self_clid)
            .map(|c| c.id)
            .collect();
        let stopped_ids: Vec<i32> = snapshot
            .clients
            .iter()
            .filter(|c| c.is_streaming == Some(false) && c.id != self_clid)
            .map(|c| c.id)
            .collect();

        if reset_asked {
            let mut state = self.state.lock().unwrap();
            for clid in &streaming_ids {
                state.asked.remove(clid);
            }
        }
        {
            let mut state = self.state.lock().unwrap();
            for c in &snapshot.clients {
                if c.id != self_clid
                    && let Some(streaming) = c.is_streaming
                {
                    state.known_client_streaming.insert(c.id, streaming);
                }
            }
        }
        for clid in streaming_ids {
            self.cancel_gone(clid);
            self.discover(clid);
        }
        if reset_asked {
            for clid in stopped_ids {
                self.forget(clid);
            }
        }
    }

    fn forget(self: &Arc<Self>, clid: i32) {
        let self_clid = self.client.upgrade().map_or(0, |h| h.client_id());
        if clid == 0 || (self_clid != 0 && clid == self_clid) {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.asked.remove(&clid);
        if state.disposed
            || state.gone_timers.contains_key(&clid)
            || !state.has_forgettable_streams_for_client(clid)
        {
            return;
        }
        state.next_gone_token = state.next_gone_token.wrapping_add(1);
        let token = state.next_gone_token;
        let weak = Arc::downgrade(self);
        let grace = self.gone_grace;
        let join = tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            if let Some(streams) = weak.upgrade() {
                streams.expire_gone(clid, token);
            }
        });
        state.gone_timers.insert(
            clid,
            GoneTimerEntry {
                token,
                handle: join,
            },
        );
    }

    fn expire_gone(&self, clid: i32, token: u64) {
        let self_clid = self.client.upgrade().map_or(0, |h| h.client_id());
        let stream_ids: Vec<String> = {
            let mut state = self.state.lock().unwrap();
            if state
                .gone_timers
                .get(&clid)
                .is_none_or(|entry| entry.token != token)
            {
                return;
            }
            state.gone_timers.remove(&clid);
            if clid == 0 || (self_clid != 0 && clid == self_clid) {
                return;
            }
            state
                .infos
                .values()
                .filter(|info| info.client_id == clid && !state.published.contains_key(&info.id))
                .map(|info| info.id.clone())
                .collect()
        };
        for id in stream_ids {
            self.end_stream(&id, 0, StreamEndCause::Gone);
        }
    }

    fn cancel_gone(&self, clid: i32) {
        let timer = self.state.lock().unwrap().gone_timers.remove(&clid);
        if let Some(entry) = timer {
            entry.handle.abort();
        }
    }

    fn on_raw_notification(self: &Arc<Self>, name: &str, items: &[HashMap<String, String>]) {
        if self.state.lock().unwrap().disposed {
            return;
        }
        match name {
            "notifystreamstarted" | "notifystreaminfo" => {
                for params in items {
                    self.on_info(params);
                }
            }
            "notifyjoinstreamrequest" => {
                for params in items {
                    let id = params.get("id").map(String::as_str).unwrap_or("");
                    let clid = client_of(params, "clid");
                    if clid == 0 {
                        continue;
                    }
                    let published = self.state.lock().unwrap().published.get(id).cloned();
                    if let Some(published) = published {
                        if params.get("is_remove").map(String::as_str) == Some("1") {
                            published.inner.viewer_gone(clid);
                        } else {
                            let msg = params.get("msg").cloned().unwrap_or_default();
                            published.inner.request(clid, msg);
                        }
                    }
                }
            }
            "notifyrespondjoinstreamrequest" => {
                for params in items {
                    self.on_respond(params);
                }
            }
            "notifystreamsignaling" => {
                for params in items {
                    self.on_signaling(params);
                }
            }
            "notifystreamclientjoined" => {
                for params in items {
                    let id = params.get("id").map(String::as_str).unwrap_or("");
                    let clid = client_of(params, "clid");
                    if clid == 0 {
                        continue;
                    }
                    let info = self.state.lock().unwrap().infos.get(id).cloned();
                    if let Some(info) = info {
                        let _ = self.hub.emit(StreamsEvent::ViewerJoined(StreamViewerEvent {
                            info,
                            client_id: clid,
                        }));
                    }
                }
            }
            "notifystreamclientleft" => {
                for params in items {
                    self.on_client_left(params);
                }
            }
            "notifystreamstopped" => {
                for params in items {
                    let id = params.get("id").map(String::as_str).unwrap_or("");
                    if !id.is_empty() {
                        self.end_stream(id, reason_of(params), StreamEndCause::Server);
                        self.state.lock().unwrap().orphans.remove(&id.to_string());
                    }
                }
            }
            _ => {}
        }
    }

    fn on_info(&self, params: &HashMap<String, String>) {
        let Some(id) = params.get("id").filter(|s| !s.is_empty()) else {
            return;
        };
        let known = self.state.lock().unwrap().infos.get(id).cloned();
        if let Some(info) = stream_info_from(params, known.as_ref()) {
            self.learn(info);
        }
    }

    fn learn(&self, info: StreamInfo) {
        let event = {
            let mut state = self.state.lock().unwrap();
            if state.orphans.contains(&info.id) {
                return;
            }
            match state.insert_info(info.clone()) {
                None => Some(StreamsEvent::Started(info)),
                Some(prev) if prev != info => Some(StreamsEvent::Updated(info)),
                Some(_) => None,
            }
        };
        if let Some(event) = event {
            let _ = self.hub.emit(event);
        }
    }

    fn on_respond(self: &Arc<Self>, params: &HashMap<String, String>) {
        let id = params.get("id").map(String::as_str).unwrap_or("");
        if id.is_empty() {
            return;
        }
        let message = params.get("msg").cloned().unwrap_or_default();
        let accepted = params.get("decision").map(String::as_str) == Some("1");
        let offer = params.get("offer").cloned().unwrap_or_default();

        let rejected = {
            let mut state = self.state.lock().unwrap();
            let Some(entry) = state.watching.get(id) else {
                return;
            };
            if entry.view.is_some() {
                return;
            }
            if !accepted {
                let entry = state.watching.remove(id).expect("checked present");
                Some((
                    entry,
                    StreamJoinDeniedError {
                        stream_id: id.to_string(),
                        server_message: message,
                    }
                    .into(),
                    None,
                ))
            } else if offer.is_empty() {
                let entry = state.watching.remove(id).expect("checked present");
                let streamer = entry.streamer;
                Some((
                    entry,
                    StreamError::Client(crate::Error::Teamspeak(
                        "the publisher accepted the request without an offer".to_string(),
                    )),
                    Some(streamer),
                ))
            } else {
                let entry = state.watching.get_mut(id).expect("checked present");
                let streamer = entry.streamer;
                let host: Arc<dyn PeerHost> = Arc::new(StreamViewHost {
                    stream_id: id.to_string(),
                    streams: Arc::downgrade(self),
                });
                let peer_inner = PeerInner::new(id.to_string(), streamer, host);
                let early = std::mem::take(&mut entry.early);
                for json in early {
                    peer_inner.deliver(&json);
                }
                let view = StreamView::new(offer, peer_inner);
                entry.view = Some(view.clone());
                entry.resolve(Ok(view));
                None
            }
        };

        if let Some((mut entry, err, leave_streamer)) = rejected {
            entry.resolve(Err(err));
            if let Some(streamer) = leave_streamer {
                let owned_id = id.to_string();
                let streams = Arc::clone(self);
                self.spawn_best_effort(async move {
                    streams.leave_on_server(&owned_id, streamer).await
                });
            }
        }
    }

    fn on_signaling(&self, params: &HashMap<String, String>) {
        let id = params.get("id").map(String::as_str).unwrap_or("");
        let clid = client_of(params, "clid");
        let json = params.get("json").map(String::as_str).unwrap_or("");

        let published = self.state.lock().unwrap().published.get(id).cloned();
        if let Some(published) = published
            && published.inner.signal(clid, json)
        {
            return;
        }

        enum SignalResult {
            Delivered(StreamView),
            EarlyOverflow,
            Ignore,
            ReportUnknown,
        }

        let action = {
            let mut state = self.state.lock().unwrap();
            if let Some(watch) = state.watching.get_mut(id) {
                if let Some(view) = watch.view.clone() {
                    SignalResult::Delivered(view)
                } else {
                    watch.early.push(json.to_string());
                    if watch.early.len() > MAX_EARLY_SIGNALS {
                        watch.early.remove(0);
                        if !watch.overflowed {
                            watch.overflowed = true;
                            SignalResult::EarlyOverflow
                        } else {
                            SignalResult::Ignore
                        }
                    } else {
                        SignalResult::Ignore
                    }
                }
            } else if state.closed_views.contains(&id.to_string()) {
                SignalResult::Ignore
            } else {
                SignalResult::ReportUnknown
            }
        };

        match action {
            SignalResult::Delivered(view) => view.inner.deliver(json),
            SignalResult::EarlyOverflow => {
                self.report(StreamError::Client(crate::Error::Teamspeak(format!(
                    "stream {id}: dropping the oldest signalling message: {MAX_EARLY_SIGNALS} are waiting for the publisher's answer"
                ))));
            }
            SignalResult::ReportUnknown => {
                self.report(StreamError::Client(crate::Error::Teamspeak(format!(
                    "signalling for stream {id} from client {clid}, which has no session"
                ))));
            }
            SignalResult::Ignore => {}
        }
    }

    fn on_client_left(&self, params: &HashMap<String, String>) {
        let id = params.get("id").map(String::as_str).unwrap_or("");
        let clid = client_of(params, "clid");
        if clid == 0 {
            return;
        }
        let self_clid = self.client.upgrade().map_or(0, |h| h.client_id());
        let (published, view, info) = {
            let state = self.state.lock().unwrap();
            let published = state.published.get(id).cloned();
            let view = state.watching.get(id).and_then(|w| w.view.clone());
            let info = state.infos.get(id).cloned();
            (published, view, info)
        };
        if let Some(published) = published {
            published.inner.viewer_gone(clid);
        }
        if let Some(view) = view
            && clid == self_clid
        {
            view.inner.end(PeerClosed {
                cause: PeerCloseCause::Left,
                reason: None,
            });
        }
        if let Some(info) = info {
            let _ = self.hub.emit(StreamsEvent::ViewerLeft(StreamViewerEvent {
                info,
                client_id: clid,
            }));
        }
    }

    fn adopt(self: &Arc<Self>, state: &mut StreamsState, info: StreamInfo) -> PublishedStream {
        if !state.orphans.contains(&info.id) {
            let event = match state.insert_info(info.clone()) {
                None => Some(StreamsEvent::Started(info.clone())),
                Some(prev) if prev != info => Some(StreamsEvent::Updated(info.clone())),
                Some(_) => None,
            };
            if let Some(ev) = event {
                let _ = self.hub.emit(ev);
            }
        }
        if let Some(existing) = state.published.get(&info.id) {
            return existing.clone();
        }
        let merged_info = state.infos.get(&info.id).cloned().unwrap_or(info);
        let host: Arc<dyn PublishHost> = Arc::new(StreamsPublishHost {
            streams: Arc::downgrade(self),
        });
        let published = PublishedStream {
            inner: PublishedInner::new(merged_info, host),
        };
        state
            .published
            .insert(published.id().to_string(), published.clone());
        published
    }

    fn end_stream(&self, id: &str, reason: u32, cause: StreamEndCause) {
        let (info, published, mut watch, cancel_timer) = {
            let mut state = self.state.lock().unwrap();
            if cause != StreamEndCause::Gone {
                state.stopped.insert(id.to_string());
            }
            let info = state.remove_info(id);
            let cancel_timer = info
                .as_ref()
                .filter(|removed| !state.has_forgettable_streams_for_client(removed.client_id))
                .and_then(|removed| state.gone_timers.remove(&removed.client_id));
            let published = state.published.remove(id);
            let watch = state.watching.remove(id);
            (info, published, watch, cancel_timer)
        };
        if let Some(entry) = cancel_timer {
            entry.handle.abort();
        }
        if let Some(published) = published {
            published.inner.finish(StreamEnded { cause, reason });
        }
        if let Some(ref mut watch) = watch {
            if let Some(view) = watch.view.take() {
                view.inner.end(PeerClosed {
                    cause: PeerCloseCause::Stopped,
                    reason: Some(reason),
                });
            } else {
                watch.resolve(Err(StreamEndedError {
                    stream_id: id.to_string(),
                    cause: None,
                }
                .into()));
            }
        }
        if let Some(info) = info {
            let _ = self.hub.emit(StreamsEvent::Stopped(StreamStoppedEvent {
                info,
                cause,
                reason,
            }));
        }
    }

    fn reset(self: &Arc<Self>, cause: StreamEndCause) {
        let connected = self
            .client
            .upgrade()
            .is_some_and(|handle| handle.status() == ClientStatus::Connected);

        let (attempts, published, watching, gone_timers) = {
            let mut state = self.state.lock().unwrap();
            if cause == StreamEndCause::Disposed {
                state.disposed = true;
                state.denied.clear();
            }
            let attempts: Vec<Arc<PublishAttempt>> =
                state.publishing.drain().map(|(_, a)| a).collect();
            for attempt in &attempts {
                *attempt.reset.lock().unwrap() = true;
            }
            state.orphans.clear();
            state.stopped.clear();
            let published: Vec<PublishedStream> = state.published.drain().map(|(_, p)| p).collect();
            let watching: Vec<(String, WatchEntry)> = state.watching.drain().collect();
            let gone_timers: Vec<GoneTimerEntry> =
                state.gone_timers.drain().map(|(_, t)| t).collect();
            state.infos.clear();
            state.info_order.clear();
            state.asked.clear();
            state.discover_in_flight.clear();
            state.last_discover_at.clear();
            state.known_client_streaming.clear();
            state.closed_views.clear();
            (attempts, published, watching, gone_timers)
        };

        let gone_error = || match cause {
            StreamEndCause::Disconnected => StreamError::NotConnected,
            _ => StreamError::Disposed,
        };
        for attempt in attempts {
            attempt.resolve(Err(gone_error()));
        }

        if connected {
            for stream in &published {
                let streams = Arc::clone(self);
                let id = stream.id().to_string();
                self.spawn_best_effort(async move { streams.stop_on_server(&id, 0).await });
            }
            for (id, watch) in &watching {
                let streams = Arc::clone(self);
                let id = id.clone();
                let streamer = watch.streamer;
                self.spawn_best_effort(async move { streams.leave_on_server(&id, streamer).await });
            }
        }

        for entry in gone_timers {
            entry.handle.abort();
        }
        let ended = StreamEnded { cause, reason: 0 };
        for stream in published {
            stream.inner.finish(ended);
        }
        let peer_cause = match cause {
            StreamEndCause::Disconnected => PeerCloseCause::Disconnected,
            _ => PeerCloseCause::Disposed,
        };
        for (_, mut watch) in watching {
            if let Some(view) = watch.view.take() {
                view.inner.end(PeerClosed {
                    cause: peer_cause,
                    reason: None,
                });
            } else {
                watch.resolve(Err(gone_error()));
            }
        }
    }

    async fn publish(
        self: &Arc<Self>,
        options: PublishOptions,
    ) -> Result<PublishedStream, StreamError> {
        self.assert_usable()?;
        let cmd = build_setup_command(&options)?;
        if options
            .abort
            .as_ref()
            .is_some_and(crate::AbortSignal::is_aborted)
        {
            return Err(StreamError::Aborted);
        }
        self.assert_usable()?;

        let timeout_ms = options.timeout_ms.unwrap_or(DEFAULT_PUBLISH_TIMEOUT_MS);
        let (tx, rx) = oneshot::channel();
        let attempt = Arc::new(PublishAttempt {
            tx: Mutex::new(Some(tx)),
            abandoned: Mutex::new(false),
            reset: Mutex::new(false),
        });
        let attempt_id = {
            let mut state = self.state.lock().unwrap();
            state.next_attempt_id = state.next_attempt_id.wrapping_add(1);
            let id = state.next_attempt_id;
            state.publishing.insert(id, Arc::clone(&attempt));
            id
        };

        let streams = Arc::clone(self);
        let setup_attempt = Arc::clone(&attempt);
        let setup_timeout = timeout_ms.saturating_add(ANNOUNCE_WINDOW_MS);
        tokio::spawn(async move {
            streams
                .run_setup(attempt_id, setup_attempt, &cmd, setup_timeout)
                .await;
        });

        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        let wait_outcome = run_with_timeout_and_abort(
            async { rx.await.unwrap_or_else(|_| Err(StreamError::NotConnected)) },
            deadline,
            options.abort.as_ref(),
            "setupstream",
        )
        .await;

        match wait_outcome {
            Ok(published) => Ok(published),
            Err(err) => {
                *attempt.abandoned.lock().unwrap() = true;
                Err(err)
            }
        }
    }

    async fn run_setup(
        self: Arc<Self>,
        attempt_id: u64,
        attempt: Arc<PublishAttempt>,
        cmd: &str,
        timeout_ms: u64,
    ) {
        let Some(handle) = self.client.upgrade() else {
            self.state.lock().unwrap().publishing.remove(&attempt_id);
            attempt.resolve(Err(StreamError::NotConnected));
            return;
        };
        let rows = match handle.exec_command_for_stream(cmd, timeout_ms, None).await {
            Ok(rows) => rows,
            Err(err) => {
                self.state.lock().unwrap().publishing.remove(&attempt_id);
                attempt.resolve(Err(translate_stream_error(err, None)));
                return;
            }
        };

        let mut orphan_to_end: Option<String> = None;
        let resolved: Option<Result<PublishedStream, StreamError>> = {
            let mut state = self.state.lock().unwrap();
            state.publishing.remove(&attempt_id);

            let mut found: Option<StreamInfo> = None;
            for row in &rows {
                let existing = row.get("id").and_then(|id| state.infos.get(id));
                if let Some(info) = stream_info_from(row, existing) {
                    found = Some(info);
                }
            }

            if *attempt.reset.lock().unwrap() {
                if let Some(info) = found {
                    if handle.status() == ClientStatus::Connected {
                        let streams = Arc::clone(&self);
                        self.spawn_best_effort(
                            async move { streams.stop_on_server(&info.id, 0).await },
                        );
                    }
                }
                None
            } else if let Some(info) = found {
                if *attempt.abandoned.lock().unwrap() {
                    state.orphans.insert(info.id.clone());
                    orphan_to_end = Some(info.id);
                    None
                } else if state.stopped.contains(&info.id) {
                    Some(Err(StreamEndedError {
                        stream_id: info.id,
                        cause: None,
                    }
                    .into()))
                } else {
                    Some(Ok(self.adopt(&mut state, info)))
                }
            } else {
                Some(Err(StreamError::Client(crate::Error::Teamspeak(
                    "the server answered setupstream with no stream".to_string(),
                ))))
            }
        };

        if let Some(id) = orphan_to_end {
            self.end_stream(&id, 0, StreamEndCause::Stopped);
            let streams = Arc::clone(&self);
            self.spawn_best_effort(async move { streams.stop_on_server(&id, 0).await });
        }
        if let Some(res) = resolved {
            attempt.resolve(res);
        }
    }

    async fn watch(
        self: &Arc<Self>,
        target: WatchTarget<'_>,
        options: WatchOptions,
    ) -> Result<StreamView, StreamError> {
        self.assert_usable()?;
        let (id, known) = match target {
            WatchTarget::Id(id) => (
                id.to_string(),
                self.state.lock().unwrap().infos.get(id).cloned(),
            ),
            WatchTarget::Info(info) => (info.id.clone(), Some(info.clone())),
        };
        let Some(known) = known else {
            return Err(StreamEndedError {
                stream_id: id,
                cause: None,
            }
            .into());
        };
        if let Some(StreamTransport::Unknown(mode)) = known.transport {
            return Err(StreamTransportUnsupportedError {
                stream_id: id,
                transport: mode,
            }
            .into());
        }
        if options
            .abort
            .as_ref()
            .is_some_and(crate::AbortSignal::is_aborted)
        {
            return Err(StreamError::Aborted);
        }

        let streamer = known.client_id;
        let cmd = build_join_command(&id, streamer, &options.message, false)?;
        let (tx, mut rx) = oneshot::channel();
        let token = {
            let mut state = self.state.lock().unwrap();
            if state.watching.contains_key(&id) {
                return Err(StreamError::Client(crate::Error::Teamspeak(format!(
                    "already watching stream {id}"
                ))));
            }
            state.closed_views.remove(&id);
            state.next_watch_token = state.next_watch_token.wrapping_add(1);
            let token = state.next_watch_token;
            state.watching.insert(
                id.clone(),
                WatchEntry {
                    token,
                    streamer,
                    early: Vec::new(),
                    overflowed: false,
                    view: None,
                    settled_tx: Some(tx),
                },
            );
            token
        };

        let timeout_ms = options.timeout_ms.unwrap_or(DEFAULT_WATCH_TIMEOUT_MS);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        let sent_flag = Arc::new(AtomicBool::new(false));
        let mut guard = WatchGuard {
            streams: Arc::clone(self),
            id: id.clone(),
            streamer,
            token,
            sent_flag: Arc::clone(&sent_flag),
            rt: tokio::runtime::Handle::try_current().ok(),
            armed: true,
        };

        let send_outcome = run_with_timeout_and_abort(
            self.send_checked_command(&cmd, Some(&id), timeout_ms, Some(&sent_flag)),
            deadline,
            options.abort.as_ref(),
            "joinstreamrequest",
        )
        .await;

        let final_outcome = match send_outcome {
            Ok(()) => {
                run_with_timeout_and_abort(
                    async {
                        (&mut rx)
                            .await
                            .unwrap_or_else(|_| Err(StreamError::NotConnected))
                    },
                    deadline,
                    options.abort.as_ref(),
                    "joinstreamrequest",
                )
                .await
            }
            Err(err) => Err(err),
        };

        match final_outcome {
            Ok(view) => {
                guard.disarm();
                Ok(view)
            }
            Err(err) => {
                let existing_view = self
                    .state
                    .lock()
                    .unwrap()
                    .watching
                    .get(&id)
                    .filter(|w| w.token == token)
                    .and_then(|w| w.view.clone());
                if let Some(view) = existing_view {
                    guard.disarm();
                    return Ok(view);
                }
                if !matches!(err, StreamError::Ended(_))
                    && let Ok(settled) = rx.try_recv()
                {
                    guard.disarm();
                    return settled;
                }

                let was_pending = {
                    let mut state = self.state.lock().unwrap();
                    if state
                        .watching
                        .get(&id)
                        .is_some_and(|entry| entry.token == token)
                    {
                        state.watching.remove(&id);
                        true
                    } else {
                        false
                    }
                };
                guard.disarm();
                if let StreamError::Ended(_) = &err {
                    if was_pending {
                        self.end_stream(&id, 0, StreamEndCause::Gone);
                    }
                } else {
                    let answered = matches!(
                        err,
                        StreamError::JoinDenied(_)
                            | StreamError::NotConnected
                            | StreamError::Client(crate::Error::ServerError { .. })
                    );
                    if was_pending && sent_flag.load(Ordering::Acquire) && !answered {
                        let streams = Arc::clone(self);
                        let leave_id = id.clone();
                        self.spawn_best_effort(async move {
                            streams.leave_on_server(&leave_id, streamer).await
                        });
                    }
                }
                Err(err)
            }
        }
    }
}

struct WatchGuard {
    streams: Arc<StreamsInner>,
    id: String,
    streamer: i32,
    token: u64,
    sent_flag: Arc<AtomicBool>,
    rt: Option<tokio::runtime::Handle>,
    armed: bool,
}

impl WatchGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (removed_pending, unreturned_view) = {
            let Ok(mut state) = self.streams.state.lock() else {
                return;
            };
            let Some(entry) = state.watching.get(&self.id) else {
                return;
            };
            if entry.token != self.token {
                return;
            }
            let mut entry = state.watching.remove(&self.id).expect("checked present");
            if let Some(view) = entry.view.take() {
                state.closed_views.insert(self.id.clone());
                (false, Some(view))
            } else {
                (true, None)
            }
        };
        let has_unreturned_view = unreturned_view.is_some();
        if let Some(view) = unreturned_view {
            view.inner.end(PeerClosed {
                cause: PeerCloseCause::Closed,
                reason: None,
            });
        }
        if has_unreturned_view || (removed_pending && self.sent_flag.load(Ordering::Acquire)) {
            let streams = Arc::clone(&self.streams);
            let leave_id = self.id.clone();
            let streamer = self.streamer;
            let fut = async move { streams.leave_on_server(&leave_id, streamer).await };
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                self.streams.spawn_best_effort_on(&rt, fut);
            } else if let Some(rt) = self.rt.as_ref() {
                self.streams.spawn_best_effort_on(rt, fut);
            }
        }
    }
}

async fn run_with_timeout_and_abort<F, T>(
    fut: F,
    deadline: tokio::time::Instant,
    abort: Option<&crate::AbortSignal>,
    command: &str,
) -> Result<T, StreamError>
where
    F: std::future::Future<Output = Result<T, StreamError>>,
{
    match abort {
        Some(signal) => {
            tokio::select! {
                res = fut => res,
                _ = tokio::time::sleep_until(deadline) => Err(StreamError::Client(crate::Error::CommandTimeout {
                    command: command.to_string(),
                })),
                _ = signal.wait_for_abort() => Err(StreamError::Aborted),
            }
        }
        None => {
            tokio::select! {
                res = fut => res,
                _ = tokio::time::sleep_until(deadline) => Err(StreamError::Client(crate::Error::CommandTimeout {
                    command: command.to_string(),
                })),
            }
        }
    }
}

/// TeamSpeak 6 屏幕共享控制面与信令管理器，独立挂载到 [`Client`]。
#[derive(Clone)]
pub struct Streams {
    inner: Arc<StreamsInner>,
}

impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("supported", &self.supported())
            .finish()
    }
}

impl Streams {
    /// 挂载到指定 [`Client`] 并注册自动发现与通知处理回调。
    ///
    /// 若调用时客户端已处于连接状态且目录快照已就绪，会立即扫描其中 `is_streaming == true`
    /// 的成员并触发 `requeststreaminfo`。
    pub fn new(client: &Client, options: StreamsOptions) -> Self {
        Self::build(
            client,
            options,
            Duration::from_millis(GONE_GRACE_MS),
            Duration::from_millis(REFRESH_COOLDOWN_MS),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_grace(
        client: &Client,
        options: StreamsOptions,
        gone_grace: Duration,
    ) -> Self {
        Self::build(
            client,
            options,
            gone_grace,
            Duration::from_millis(REFRESH_COOLDOWN_MS),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_refresh_cooldown(
        client: &Client,
        options: StreamsOptions,
        refresh_cooldown: Duration,
    ) -> Self {
        Self::build(
            client,
            options,
            Duration::from_millis(GONE_GRACE_MS),
            refresh_cooldown,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_forget(&self, clid: i32) {
        self.inner.forget(clid);
    }

    #[cfg(test)]
    pub(crate) fn test_has_gone_timer(&self, clid: i32) -> bool {
        self.inner
            .state
            .lock()
            .unwrap()
            .gone_timers
            .contains_key(&clid)
    }

    #[cfg(test)]
    pub(crate) fn test_adopt_published(&self, info: StreamInfo) -> PublishedStream {
        let mut state = self.inner.state.lock().unwrap();
        self.inner.adopt(&mut state, info)
    }

    #[cfg(test)]
    pub(crate) fn test_has_published(&self, id: &str) -> bool {
        self.inner.state.lock().unwrap().published.contains_key(id)
    }

    #[cfg(test)]
    pub(crate) fn test_expire_gone_with_timer(&self, clid: i32, token: u64) {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.gone_timers.insert(
                clid,
                GoneTimerEntry {
                    token,
                    handle: tokio::spawn(async {}),
                },
            );
        }
        self.inner.expire_gone(clid, token);
    }

    fn build(
        client: &Client,
        options: StreamsOptions,
        gone_grace: Duration,
        refresh_cooldown: Duration,
    ) -> Self {
        let inner = Arc::new(StreamsInner {
            client: client.weak_handle(),
            state: Mutex::new(StreamsState::new(options.remember_denied)),
            hub: EventHub::new(2, streams_event_bucket),
            gone_grace,
            refresh_cooldown,
        });
        Self::register_client_hooks(client, &inner);
        if client.status() == ClientStatus::Connected {
            inner.scan_directory_snapshot(false);
        }
        Self { inner }
    }

    fn register_client_hooks(client: &Client, inner: &Arc<StreamsInner>) {
        let weak = Arc::downgrade(inner);
        client.on_connected(Arc::new(move || {
            if let Some(streams) = weak.upgrade() {
                streams.scan_directory_snapshot(false);
            }
        }));

        let weak = Arc::downgrade(inner);
        client.on_directory_snapshot(Arc::new(move |event| {
            let Some(streams) = weak.upgrade() else {
                return;
            };
            if let Event::DirectorySnapshot(snapshot) = event {
                let self_clid = streams.client.upgrade().map_or(0, |h| h.client_id());
                {
                    let mut state = streams.state.lock().unwrap();
                    for client_info in &snapshot.clients {
                        if client_info.id != self_clid
                            && let Some(streaming) = client_info.is_streaming
                        {
                            state
                                .known_client_streaming
                                .insert(client_info.id, streaming);
                        }
                    }
                }
                for client_info in &snapshot.clients {
                    if client_info.is_streaming == Some(true) && client_info.id != self_clid {
                        streams.cancel_gone(client_info.id);
                        streams.discover(client_info.id);
                    }
                }
            }
        }));

        let weak = Arc::downgrade(inner);
        client.on_client_enter(Arc::new(move |event| {
            let Some(streams) = weak.upgrade() else {
                return;
            };
            if let Event::ClientEnter(info) = event {
                let self_clid = streams.client.upgrade().map_or(0, |h| h.client_id());
                if self_clid != 0 && info.id == self_clid {
                    return;
                }
                if let Some(streaming) = info.is_streaming {
                    streams
                        .state
                        .lock()
                        .unwrap()
                        .known_client_streaming
                        .insert(info.id, streaming);
                    if streaming {
                        streams.cancel_gone(info.id);
                        streams.discover(info.id);
                    } else {
                        streams.forget(info.id);
                    }
                }
            }
        }));

        let weak = Arc::downgrade(inner);
        client.on_client_updated(Arc::new(move |event| {
            let Some(streams) = weak.upgrade() else {
                return;
            };
            if let Event::ClientUpdated(info) = event {
                let self_clid = streams.client.upgrade().map_or(0, |h| h.client_id());
                if self_clid != 0 && info.id == self_clid {
                    return;
                }
                if let Some(streaming) = info.is_streaming {
                    let prev = streams
                        .state
                        .lock()
                        .unwrap()
                        .known_client_streaming
                        .insert(info.id, streaming);
                    if streaming {
                        streams.cancel_gone(info.id);
                        streams.discover(info.id);
                    } else if prev != Some(false) {
                        streams.forget(info.id);
                    }
                }
            }
        }));

        let weak = Arc::downgrade(inner);
        client.on_client_leave(Arc::new(move |event| {
            let Some(streams) = weak.upgrade() else {
                return;
            };
            if let Event::ClientLeave(leave) = event {
                let published: Vec<PublishedStream> = {
                    let mut state = streams.state.lock().unwrap();
                    state.asked.remove(&leave.id);
                    state.last_discover_at.remove(&leave.id);
                    state.known_client_streaming.remove(&leave.id);
                    state.published.values().cloned().collect()
                };
                for stream in published {
                    stream.inner.drop_pending(leave.id);
                }
            }
        }));

        let weak = Arc::downgrade(inner);
        client.on_raw_notification(Arc::new(move |event| {
            let Some(streams) = weak.upgrade() else {
                return;
            };
            match event {
                Event::RawNotification { name, items } => {
                    streams.on_raw_notification(&name, &items);
                }
                Event::NotificationOverflow { .. } => {
                    streams.scan_directory_snapshot(true);
                }
                _ => {}
            }
        }));

        let weak = Arc::downgrade(inner);
        client.on_disconnected(Arc::new(move |_| {
            if let Some(streams) = weak.upgrade() {
                streams.reset(StreamEndCause::Disconnected);
            }
        }));
    }

    /// 检查当前连接服务端是否声明 `STREAM_P2P` 能力扩展。
    pub fn supported(&self) -> bool {
        self.inner.supported()
    }

    /// 返回当前已知的活动流列表（按发现先后排序）。
    pub fn list(&self) -> Vec<StreamInfo> {
        self.inner.state.lock().unwrap().list_infos()
    }

    /// 按流 ID 查询当前已知的流描述。
    pub fn get(&self, id: &str) -> Option<StreamInfo> {
        self.inner.state.lock().unwrap().infos.get(id).cloned()
    }

    /// 主动查询指定客户端的活动流信息（带在途去重与冷却限流保护的 `requeststreaminfo`）。
    pub fn refresh_client(&self, client_id: u16) {
        self.inner.refresh_client(i32::from(client_id));
    }

    /// 订阅 [`StreamsEvent`] 事件流（`Error` 与 `Warning` 在首个订阅者接入前保留最多 256 条）。
    pub fn subscribe(&self) -> StreamSubscription<StreamsEvent> {
        self.inner.hub.subscribe()
    }

    /// 发起新的屏幕/窗口/摄像头共享流。
    pub async fn publish(&self, options: PublishOptions) -> Result<PublishedStream, StreamError> {
        self.inner.publish(options).await
    }

    /// 申请观看指定流，并在发布者接受后返回携带初始 SDP `offer` 的 [`StreamView`]。
    pub async fn watch<'a, T: Into<WatchTarget<'a>>>(
        &self,
        target: T,
        options: WatchOptions,
    ) -> Result<StreamView, StreamError> {
        self.inner.watch(target.into(), options).await
    }

    /// 停止监听客户端并清理所有本地流会话；若仍已连接，会尽力向服务端发送
    /// `stopstream` 与 `joinstreamrequest is_remove=1`。
    pub fn dispose(&self) {
        if self.inner.state.lock().unwrap().disposed {
            return;
        }
        self.inner.reset(StreamEndCause::Disposed);
    }
}
