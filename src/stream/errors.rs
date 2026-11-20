use std::fmt;

use crate::{Error, ErrorCode, ServerError};

/// TeamSpeak 6 服务端可接收的单条 stream 命令行最大字节数（不含随后附加的
/// ` return_code=<N>` 后缀；按转义后的 UTF-8 字节长度计算）。
pub const MAX_COMMAND_LINE_BYTES: usize = 9_150;

/// 当前服务器未在 `virtualserver_capability_extensions` 中声明 `STREAM_P2P`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsUnsupportedError {
    pub version: String,
    pub extensions: Vec<String>,
}

impl fmt::Display for StreamsUnsupportedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let named = if self.extensions.is_empty() {
            "none".to_string()
        } else {
            self.extensions.join(", ")
        };
        if self.version.is_empty() {
            write!(
                f,
                "streams need a server that names STREAM_P2P among its extensions; current server names {named}"
            )
        } else {
            write!(
                f,
                "streams need a server that names STREAM_P2P among its extensions; server ({}) names {named}",
                self.version
            )
        }
    }
}

impl std::error::Error for StreamsUnsupportedError {}

/// 流发布者拒绝了加入请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamJoinDeniedError {
    pub stream_id: String,
    pub server_message: String,
}

impl fmt::Display for StreamJoinDeniedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.server_message.is_empty() {
            write!(f, "the stream's owner denied the request to watch it")
        } else {
            write!(
                f,
                "the stream's owner denied the request to watch it: {}",
                self.server_message
            )
        }
    }
}

impl std::error::Error for StreamJoinDeniedError {}

/// 转义后的 stream 命令行超过 [`MAX_COMMAND_LINE_BYTES`]，在发送前即拒绝。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPayloadTooLargeError {
    pub command: String,
    pub size: usize,
    pub limit: usize,
}

impl fmt::Display for StreamPayloadTooLargeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} is {} bytes escaped; the server takes at most {}",
            self.command, self.size, self.limit
        )
    }
}

impl std::error::Error for StreamPayloadTooLargeError {}

/// 流已结束、未知或服务端返回 `5635` (`StreamNotFound`)。
#[derive(Debug, Clone)]
pub struct StreamEndedError {
    pub stream_id: String,
    pub cause: Option<ServerError>,
}

impl fmt::Display for StreamEndedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream {} has ended, or is not known", self.stream_id)
    }
}

impl std::error::Error for StreamEndedError {}

/// 流使用的传输模式不是当前库支持的点对点 (`mode=1`) 模式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTransportUnsupportedError {
    pub stream_id: String,
    pub transport: u32,
}

impl fmt::Display for StreamTransportUnsupportedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "stream {} is in mode {}; only peer to peer streams can be watched",
            self.stream_id, self.transport
        )
    }
}

impl std::error::Error for StreamTransportUnsupportedError {}

/// `stream` 模块的统一错误枚举。
#[derive(Debug, Clone, thiserror::Error)]
pub enum StreamError {
    #[error(transparent)]
    Unsupported(#[from] StreamsUnsupportedError),

    #[error(transparent)]
    JoinDenied(#[from] StreamJoinDeniedError),

    #[error(transparent)]
    PayloadTooLarge(#[from] StreamPayloadTooLargeError),

    #[error(transparent)]
    Ended(#[from] StreamEndedError),

    #[error(transparent)]
    TransportUnsupported(#[from] StreamTransportUnsupportedError),

    #[error("not connected")]
    NotConnected,

    #[error("these Streams were disposed")]
    Disposed,

    #[error("operation aborted")]
    Aborted,

    #[error("{0}")]
    InvalidOption(String),

    #[error("{0}")]
    Client(Error),
}

impl StreamError {
    /// 判断是否为断线类错误（用于 best-effort 清理时静默忽略）。
    pub fn is_not_connected(&self) -> bool {
        matches!(self, Self::NotConnected)
    }

    /// 如果是服务端返回的错误，尝试解析其数字错误码。
    pub fn server_error_code(&self) -> Option<u32> {
        match self {
            Self::Ended(e) => e.cause.as_ref().and_then(ServerError::code),
            Self::Client(err) => err.server_error_code(),
            _ => None,
        }
    }

    /// 检查是否匹配指定的 [`ErrorCode`]。
    pub fn is_server_error_code(&self, code: ErrorCode) -> bool {
        self.server_error_code() == Some(code.code())
    }
}

pub(crate) fn is_not_connected_client_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Teamspeak(msg)
            if msg == "not connected"
                || msg == "command channel closed"
                || msg == "connection epoch changed before command send"
    )
}

impl From<Error> for StreamError {
    fn from(err: Error) -> Self {
        if is_not_connected_client_error(&err) {
            Self::NotConnected
        } else {
            Self::Client(err)
        }
    }
}
