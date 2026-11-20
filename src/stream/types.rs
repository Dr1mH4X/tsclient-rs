use crate::AbortSignal;

use super::errors::StreamError;

/// 流采集源（`setupstream` 中的 `type` 字段）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamSource {
    None,
    Camera,
    Screen,
    Window,
    Session,
    Unknown(u32),
}

impl StreamSource {
    /// 从线缆整数值解码采集源。
    pub const fn from_wire(value: u32) -> Self {
        match value {
            0 => Self::None,
            1 => Self::Camera,
            2 => Self::Screen,
            3 => Self::Window,
            4 => Self::Session,
            other => Self::Unknown(other),
        }
    }

    /// 当为已知命名值时返回对应线缆整数（用于 `setupstream` 参数校验）。
    pub const fn known_wire_code(self) -> Option<u32> {
        match self {
            Self::None => Some(0),
            Self::Camera => Some(1),
            Self::Screen => Some(2),
            Self::Window => Some(3),
            Self::Session => Some(4),
            Self::Unknown(_) => None,
        }
    }
}

/// 流可见性（`accessibility` / `access` 字段）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamAccessibility {
    None,
    Public,
    Contacts,
    Private,
    Unknown(u32),
}

impl StreamAccessibility {
    /// 从线缆整数值解码可见性。
    pub const fn from_wire(value: u32) -> Self {
        match value {
            0 => Self::None,
            1 => Self::Public,
            2 => Self::Contacts,
            3 => Self::Private,
            other => Self::Unknown(other),
        }
    }

    /// 当为已知命名值时返回对应线缆整数（用于 `setupstream` 参数校验）。
    pub const fn known_wire_code(self) -> Option<u32> {
        match self {
            Self::None => Some(0),
            Self::Public => Some(1),
            Self::Contacts => Some(2),
            Self::Private => Some(3),
            Self::Unknown(_) => None,
        }
    }
}

/// 媒体传输模式（`mode` 字段）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamTransport {
    P2p,
    Unknown(u32),
}

impl StreamTransport {
    /// 从线缆整数值解码传输模式；`0` 表示服务端未指定。
    pub const fn from_wire(value: u32) -> Option<Self> {
        match value {
            0 => None,
            1 => Some(Self::P2p),
            other => Some(Self::Unknown(other)),
        }
    }
}

/// 已知流描述（整合自 `notifystreamstarted` 与 `notifystreaminfo`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub id: String,
    pub client_id: i32,
    pub name: String,
    pub source: Option<StreamSource>,
    pub audio: bool,
    pub bitrate: Option<i64>,
    pub accessibility: Option<StreamAccessibility>,
    pub transport: Option<StreamTransport>,
    pub viewer_limit: Option<i64>,
    pub viewers: Option<i64>,
}

/// 请求观看流的用户身份信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewerIdentity {
    pub client_id: i32,
    pub uid: Option<String>,
    pub nickname: Option<String>,
}

/// 透传给远程端的 ICE Candidate 载荷。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IceCandidate {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_m_line_index: Option<u16>,
}

/// 未知或自定义形状的信令 JSON 消息（原样透传）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSignal {
    pub cmd: String,
    pub args: Option<serde_json::Value>,
    pub json: String,
}

/// `Streams::new` 构造配置。
#[derive(Debug, Clone, Default)]
pub struct StreamsOptions {
    /// 拒绝过的观众按 UID 记住（上限 1024），再次申请时直接忽略不再派发请求。
    pub remember_denied: bool,
}

/// 发布新流的配置（`Streams::publish`）。
#[derive(Debug, Clone)]
pub struct PublishOptions {
    pub name: String,
    pub source: StreamSource,
    pub audio: bool,
    pub bitrate: u32,
    pub accessibility: StreamAccessibility,
    pub viewer_limit: u16,
    pub timeout_ms: Option<u64>,
    pub abort: Option<AbortSignal>,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            name: String::new(),
            source: StreamSource::Screen,
            audio: false,
            bitrate: 1_500_000,
            accessibility: StreamAccessibility::Public,
            viewer_limit: 0,
            timeout_ms: None,
            abort: None,
        }
    }
}

impl PublishOptions {
    /// 以指定流名称构造默认发布参数（`screen` / `public` / `1_500_000` / 无人数上限）。
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }
}

/// 请求观看流的配置（`Streams::watch`）。
#[derive(Debug, Clone, Default)]
pub struct WatchOptions {
    pub message: String,
    pub timeout_ms: Option<u64>,
    pub abort: Option<AbortSignal>,
}

/// `Streams::watch` 的目标，支持流 ID 或已知 [`StreamInfo`]。
#[derive(Debug, Clone, Copy)]
pub enum WatchTarget<'a> {
    Id(&'a str),
    Info(&'a StreamInfo),
}

impl<'a> From<&'a str> for WatchTarget<'a> {
    fn from(value: &'a str) -> Self {
        Self::Id(value)
    }
}

impl<'a> From<&'a String> for WatchTarget<'a> {
    fn from(value: &'a String) -> Self {
        Self::Id(value.as_str())
    }
}

impl<'a> From<&'a StreamInfo> for WatchTarget<'a> {
    fn from(value: &'a StreamInfo) -> Self {
        Self::Info(value)
    }
}

/// 流结束原因分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamEndCause {
    Stopped,
    Server,
    Gone,
    Disconnected,
    Disposed,
}

/// `PublishedStream` 结束事件载荷。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEnded {
    pub cause: StreamEndCause,
    pub reason: u32,
}

/// 信令对端句柄（[`crate::stream::ViewerPeer`] / [`crate::stream::StreamView`]）关闭原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerCloseCause {
    Closed,
    Left,
    Stopped,
    Disconnected,
    Disposed,
}

/// 信令对端关闭事件载荷。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerClosed {
    pub cause: PeerCloseCause,
    pub reason: Option<u32>,
}

/// `JoinRequest` 当前状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinRequestState {
    Pending,
    Accepted,
    Denied,
    Withdrawn,
    Ended,
    Failed,
}

/// `JoinRequest` 被提前关闭的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinRequestCloseCause {
    Withdrawn,
    Ended,
}

/// 静态检查告警代码。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamWarningCode {
    OfferWithoutAudio,
}

/// `Streams` 发出的非致命警告。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamWarning {
    pub code: StreamWarningCode,
    pub stream_id: String,
    pub client_id: i32,
    pub message: String,
}

/// 观众进入/离开流的通知事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamViewerEvent {
    pub info: StreamInfo,
    pub client_id: i32,
}

/// 流停止事件（在 `Streams` 总线上派发）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamStoppedEvent {
    pub info: StreamInfo,
    pub cause: StreamEndCause,
    pub reason: u32,
}

/// `Streams` 实例公开事件。
#[derive(Debug, Clone)]
pub enum StreamsEvent {
    Started(StreamInfo),
    Updated(StreamInfo),
    Stopped(StreamStoppedEvent),
    ViewerJoined(StreamViewerEvent),
    ViewerLeft(StreamViewerEvent),
    Error(StreamError),
    Warning(StreamWarning),
}

/// `ViewerPeer` / `StreamView` 收到的信令与关闭事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerEvent {
    Offer(String),
    Answer(String),
    Candidate(IceCandidate),
    Signal(RawSignal),
    Closed(PeerClosed),
}

/// `JoinRequest` 生命周期事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinRequestEvent {
    Closed { cause: JoinRequestCloseCause },
}
