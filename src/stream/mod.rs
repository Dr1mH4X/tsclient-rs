//! TeamSpeak 6 原生屏幕共享控制面与信令支持（需启用 `stream` feature）。
//!
//! 本模块严格负责 TS6 控制命令与 JSON 信令透传（不引入任何 WebRTC 或音视频媒体依赖）。

mod emitter;
mod errors;
mod peer;
mod published;
mod streams;
mod types;
mod wire;

pub use emitter::StreamSubscription;
pub use errors::{
    MAX_COMMAND_LINE_BYTES, StreamEndedError, StreamError, StreamJoinDeniedError,
    StreamPayloadTooLargeError, StreamTransportUnsupportedError, StreamsUnsupportedError,
};
pub use peer::{StreamView, ViewerPeer};
pub use published::{JoinRequest, PublishedStream, PublishedStreamEvent};
pub use streams::Streams;
pub use types::{
    IceCandidate, JoinRequestCloseCause, JoinRequestEvent, JoinRequestState, PeerCloseCause,
    PeerClosed, PeerEvent, PublishOptions, RawSignal, StreamAccessibility, StreamEndCause,
    StreamEnded, StreamInfo, StreamSource, StreamStoppedEvent, StreamTransport, StreamViewerEvent,
    StreamWarning, StreamWarningCode, StreamsEvent, StreamsOptions, ViewerIdentity, WatchOptions,
    WatchTarget,
};
pub use wire::{ParsedSignal, has_audio_section, parse_signal};

#[cfg(test)]
mod tests;
