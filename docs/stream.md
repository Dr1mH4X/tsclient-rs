# TS6 屏幕共享（`stream` feature）

`tsclient_rs::stream` 是 TeamSpeak 6 屏幕共享的控制面封装：负责 `setupstream`、观看申请、SDP/ICE 信令透传与生命周期清理。模块不包含 WebRTC、编解码器或任何媒体实现，`offer` / `answer` / `candidate` 仍需交给调用方的 WebRTC 端点。

完整可运行代码见 [`examples/stream_publish.rs`](../examples/stream_publish.rs) 与 [`examples/stream_watch.rs`](../examples/stream_watch.rs)。

## 启用与前提

`stream` 是可选 feature，默认关闭，只在该 feature 下拉入 `serde` / `serde_json`：

```toml
[dependencies.tsclient-rs]
version = "<latest>"
features = ["stream"]
```

服务端必须在 `virtualserver_capability_extensions` 中声明 `STREAM_P2P`（常量 `tsclient_rs::STREAM_P2P_EXTENSION`）。`Streams::supported()` 以缓存的 `ServerInfo` 为准：`ServerInfo` 尚未就绪或缺少该扩展时返回 `false`，此时 `publish` / `watch` 返回 `StreamError::Unsupported`。断线后缓存可能仍在，但命令发送会以 `NotConnected` 失败。

核心层无需 feature 即可读取直播状态：

- `ClientInfo::is_streaming: Option<bool>` 在欢迎行、`clientlist`（`list_clients`）、`notifycliententerview` 与 `notifyclientupdated` 中解析 `client_is_streaming`。`None` 表示服务端未上报，`Some(false)` 才是明确的“未在直播”。
- `ServerInfo::supports_stream_p2p()` 检查能力扩展；`ServerInfo::capability_extensions()` 返回去重后的扩展列表。

## Rust 可运行示例

库本身不包含 WebRTC。仓库里的两个示例用 [`webrtc`](https://crates.io/crates/webrtc) crate 处理媒体，通过 `stream-example` feature 启用；该 feature 只服务于示例，启用它才会拉入 `webrtc` / `rtc`。仅启用 `stream` 时库仍然只有 `serde` / `serde_json`：

- [`examples/stream_watch.rs`](../examples/stream_watch.rs)：观看第一条流，或 `STREAM_ID` 指定的流；打印收到的轨道与 RTP 包计数，不渲染画面。
- [`examples/stream_publish.rs`](../examples/stream_publish.rs)：把 VP8 IVF 文件循环发布为屏幕共享；处理观看申请、SDP answer、ICE 与断流。

```sh
# 终端 1：从 macOS 屏幕采集 VP8 IVF（Linux/Windows 换成对应采集器）
ffmpeg -f avfoundation -framerate 30 -i "1:none" -t 30 \
  -c:v libvpx -b:v 5M -f ivf /tmp/screen.ivf

# 终端 2：发布
STREAM_IVF=/tmp/screen.ivf TEAMSPEAK_ADDR=127.0.0.1:9987 \
  cargo run --example stream_publish --features stream-example

# 终端 3：观看
TEAMSPEAK_ADDR=127.0.0.1:9987 \
  cargo run --example stream_watch --features stream-example
```

发布示例会先加载整个 IVF 文件再循环播放，适合一段可重复的录屏。需要长时间直播时，把文件读取换成持续的采集 / 编码输出即可；控制面与 WebRTC 协商部分不用改。

Flutter 端可以用 `flutter_webrtc` 直接照搬同一套流程：控制面调用与 Rust 示例一致，媒体实现可参考 [FlSpeak](https://github.com/Dr1mH4X/FlSpeak) 的 `lib/core/stream_publisher.dart` 与 `lib/core/stream_viewer.dart`。

### 媒体层约束

发布端：

- 先创建 WebRTC offer，再调用 `JoinRequest::accept(offer, message)`；`accept` 会把 offer 转发给观众。
- 视频轨必须第一个加入（MID 0），音频段第二个（MID 1）。没有音轨时也要加一个 send-only 的 `m=audio`；TeamSpeak 6 客户端不会应答缺少音频段的 offer。用 webrtc-rs 的 `add_transceiver_from_kind(Audio, Sendonly)` 时必须提供 `send_encodings`，否则返回 `ErrInvalidDirection`。
- 只注册 VP8 + Opus 通常足以把 offer 控制在几 KB，远低于 9 150 字节上限。需要 H.264 时同时注册 H.264 并调整编码优先级。
- 本地 ICE candidate 缓存在 `accept` 之前，拿到 `ViewerPeer` 后再 trickle；观众 answer 到达前不要发送。
- 处理 `PeerEvent::Answer`（`set_remote_description`）、`Candidate`（`add_ice_candidate`）与 `Closed`（关闭 peer）。每个观众一个独立 peer connection。

观看端：

- 先加 recvonly 视频 transceiver，再把发布者 offer 交给 `set_remote_description`；offer 自带的音频段会创建对应 transceiver。
- 设置 remote description 后先应用已缓存的远端 candidate，再 `create_answer`、`set_local_description`，最后调用 `StreamView::answer`。
- 本地 candidate 在 answer 发出后再 trickle；远端 candidate 在 remote description 就绪前先缓存。
- 重协商通过 `PeerEvent::Offer` 到达，回答仍用 `StreamView::answer`。

两端都使用 Teamspeak 的 STUN 对（`stun:turn.teamspeak.com:3478`、`stun:turn2.teamspeak.com:3478`）。SDP 只有在转义后超过 9 150 字节时才需要裁剪；优先保留 VP8/H264 与 Opus，并保留 `m=audio` 与 MID 顺序。

这些约束来自与 TeamSpeak 6 客户端的互操作实践，可参考 [FlSpeak](https://github.com/Dr1mH4X/FlSpeak) 的 `lib/core/stream_publisher.dart` / `lib/core/stream_viewer.dart`，以及上游 [teamspeak-js Streams 文档](https://github.com/honeybbq/teamspeak-js/blob/main/docs/streams.md)。

## 发布流

```rust
use tsclient_rs::stream::{
    PeerEvent, PublishOptions, PublishedStreamEvent, Streams, StreamsOptions,
};

let streams = Streams::new(&client, StreamsOptions::default());
// offer_sdp 由调用方的 WebRTC 端点生成。
let published = streams.publish(PublishOptions::new("Desktop")).await?;
let mut requests = published.subscribe();

while let Some(PublishedStreamEvent::Request(request)) = requests.recv().await {
    let peer = request.accept(offer_sdp, "join").await?;
    let mut signals = peer.subscribe();
    while let Some(event) = signals.recv().await {
        if let PeerEvent::Candidate(candidate) = event {
            // 交给 WebRTC 端点
        }
    }
}

published.stop(0).await?;
```

`Streams::new` 注册客户端事件回调。若挂载时客户端已连接且目录快照就绪，会立即扫描 `is_streaming == Some(true)` 的成员并发送 `requeststreaminfo`。

`PublishOptions` 的默认值：

| 字段 | 默认值 | 约束 |
|------|--------|------|
| `name` | 空字符串 | — |
| `source` | `StreamSource::Screen` | `Unknown` 在发送前返回 `InvalidOption` |
| `audio` | `false` | 映射为 `audio=0/1` |
| `bitrate` | `1_500_000` | 不得超过 `i32::MAX` |
| `accessibility` | `StreamAccessibility::Public` | `Unknown` 在发送前返回 `InvalidOption` |
| `viewer_limit` | `0` | `0` 表示不限 |
| `timeout_ms` | `10_000` | 覆盖默认等待时长 |
| `abort` | `None` | 中止后返回 `StreamError::Aborted` |

传输模式固定为 `mode=1`（P2P）。`publish` 等待 `setupstream` 与带匹配 `return_code` 的 `notifystreamstarted`；超时返回 `StreamError::Client`，断线返回 `NotConnected`，已 `dispose` 返回 `Disposed`。

### 处理观看申请

`PublishedStreamEvent::Request(JoinRequest)` 携带 `viewer()`（`ViewerIdentity { client_id, uid, nickname }`）、`message()` 与 `state()`。

- `accept(offer, message)`：`offer` 必须非空，发送 `respondjoinstreamrequest decision=1`，返回对应的 `ViewerPeer`。发送失败时请求状态变为 `Failed`，同时该 peer 以 `PeerCloseCause::Closed` 结束。
- `deny(reason)`：发送 `decision=0`，协议要求 `offer` 为空串。启用 `remember_denied` 后按观众 UID 记录拒绝（上限 1024），同一 UID 后续申请不再派发 `Request`。
- 同一 `viewer_clid` 重新申请时，旧的 `ViewerPeer` 先收到 `PeerEvent::Closed { cause: PeerCloseCause::Left }`，随后新申请照常派发。旧句柄的 `closed()` 变为 `true`，后续发送返回错误。
- `deny` 与 `accept` 只能调用一次；状态已不是 `Pending` 时返回 `StreamError::Client`。

`ViewerPeer` 是发布者侧对单个观众的信令句柄：

- `send_offer(sdp)`：重协商，缺少 `m=audio` 时发出 `StreamWarningCode::OfferWithoutAudio` 警告但仍发送。
- `send_candidate(&IceCandidate)`、`send_signal(cmd, args)`：透传 ICE 或自定义 JSON。
- `close()`：只关闭本地会话，不发送服务端命令。
- `kick(reason)`：发送 `removeclientfromstream` 并完成本地关闭；服务端返回 `StreamNotFound`、`StreamNotParticipating`、`CommandNotFound` 时视为非致命错误静默忽略。
- `subscribe()` 接收 `PeerEvent::{Offer, Answer, Candidate, Signal, Closed}`。

`PublishedStream::stop(reason)` 发送 `stopstream`。即使服务端返回非 `5635` 错误或超时，也会清理本地发布流与全部观众会话，再把错误返回给调用方；已经结束时直接返回 `Ok(())`。

## 观看流

```rust
use tsclient_rs::stream::{Streams, StreamsOptions, WatchOptions};

let streams = Streams::new(&client, StreamsOptions::default());
if let Some(info) = streams.list().first() {
    let view = streams.watch(info, WatchOptions::default()).await?;
    view.answer(answer_sdp).await?;
    view.send_candidate(&ice_candidate).await?;
    view.leave().await?;
}
```

- `list()` 返回按发现顺序排列的 `StreamInfo`，`get(id)` 按 ID 查询。
- `refresh_client(client_id)` 主动发送 `requeststreaminfo`，用于目录里刚出现或信息过期的客户端。请求有在途去重与 2 秒冷却；自身、`clid == 0`、未连接或服务端不支持时静默忽略。结果通过 `StreamsEvent` 与 `list()` 更新，不返回 `Result`。
- `watch(target, options)` 接受流 ID（`&str` / `&String`）或 `&StreamInfo`。未知 ID 返回 `StreamError::Ended`；`StreamTransport::Unknown(mode)` 返回 `TransportUnsupported`；同一流重复观看返回 `StreamError::Client`。
- 默认等待发布者决定 60 秒，可用 `WatchOptions { message, timeout_ms, abort }` 覆盖。
- 发布者拒绝时返回 `StreamError::JoinDenied { stream_id, server_message }`；接受但 `offer` 为空时返回 `Client` 错误并尽力发送离开命令。
- 发布者决定前到达的信令按流缓存，最多 256 条；溢出时丢弃最旧一条并通过 `StreamsEvent::Error` 报告。

`StreamView` 是观看者侧句柄：

- `offer()` 返回发布者的初始 SDP；它不会作为 `PeerEvent::Offer` 再派发一次。
- `answer(sdp)`、`send_candidate(&IceCandidate)`、`send_signal(cmd, args)`、`leave()`。`leave()` 发送 `joinstreamrequest is_remove=1` 并关闭本地会话。
- `subscribe()` 接收后续 `PeerEvent`；`closed()` 与 `stream_id()` / `client_id()` 查询会话状态。

## 事件与生命周期

`StreamsEvent`：

| 事件 | 触发 |
|------|------|
| `Started(StreamInfo)` / `Updated(StreamInfo)` | 发现新流或流描述变化 |
| `Stopped(StreamStoppedEvent)` | 本端 `stop()`、服务端 `notifystreamstopped`、停止失败后的本地清理，或 2 秒宽限到期；`cause` 区分 `Stopped` / `Server` / `Gone` |
| `ViewerJoined` / `ViewerLeft` | `notifystreamclientjoined` / `notifystreamclientleft` |
| `Warning(StreamWarning)` | `OfferWithoutAudio` 等非致命检查 |
| `Error(StreamError)` | 后台发现、信令缓存溢出等异步错误 |

`StreamEndCause` 区分 `Stopped`、`Server`、`Gone`、`Disconnected`、`Disposed`。`PeerEvent::Closed` 的 `PeerCloseCause` 区分 `Closed`、`Left`、`Stopped`、`Disconnected`、`Disposed`。断线与 `dispose()` 不逐条派发 `StreamsEvent::Stopped`；它们以 `Disconnected` / `Disposed` 结束 `PublishedStream` 与 `StreamView` 句柄。

自动发现覆盖以下入口：

- `Streams::new` 挂载时的目录快照。
- `DirectorySnapshot` 事件。
- `ClientEnter` 与 `ClientUpdated`。自身 `clid` 被忽略；`is_streaming == Some(true)` 会取消待执行的清理并查询该客户端。
- `NotificationOverflow` 会重新扫描完整目录快照。

### 2 秒宽限期

`client_is_streaming` 变为 `false` 时，客户端不会立刻删除该发布者的流，而是等待 `notifystreamstopped`：

- 2 秒内收到 `notifystreamstopped`：按 `StreamEndCause::Server` 立即结束，携带服务端 `reason`。
- 2 秒内没有收到：宽限期到期，按 `StreamEndCause::Gone` 清理该客户端已知的流。
- 新的 `notifystreamstarted` / `is_streaming == Some(true)` 会取消计时器。
- 本端 `clid` 与本地正在发布的流（`published` 中的 ID）不受 `Gone` 清理影响。

`dispose()` 停止监听并清理所有本地会话；仍连接时会尽力发送 `stopstream` 与 `joinstreamrequest is_remove=1`，并清空拒绝记忆。仅丢弃 `Streams` 不会主动通知服务端。

`StreamsEvent::Error` 与 `Warning` 在首个订阅者接入前各保留最多 256 条；`PublishedStreamEvent` 的 `Request` / `Ended` 同样各保留最多 256 条。`JoinRequestEvent::Closed { cause }` 使用 `JoinRequestCloseCause::{Withdrawn, Ended}`。

## 线缆约束

| 命令 | 方向 | 用途 |
|------|------|------|
| `setupstream` | C→S | 创建发布，固定 `mode=1` |
| `joinstreamrequest` | C→S | 申请观看；`is_remove=1` 离开或撤回 |
| `respondjoinstreamrequest` | C→S | 接受 / 拒绝观看申请 |
| `streamsignaling` | C→S | 透传 SDP / ICE JSON |
| `stopstream` | C→S | 停止发布 |
| `removeclientfromstream` | C→S | 将观众移出流 |
| `requeststreaminfo` | C→S | 查询指定客户端的活动流 |
| `notifystream*` | S→C | 流状态与信令通知 |

所有命令通过带 `return_code` 与限流重试的 `exec_command` 通道发送。发送前按转义后的 UTF-8 字节数检查 `MAX_COMMAND_LINE_BYTES = 9_150`（不含随后追加的 `return_code` 后缀）；超限返回 `StreamError::PayloadTooLarge`，不会发出截断命令。

`has_audio_section(sdp)` 在 SDP 缺少 `m=audio` 时产生 `StreamWarningCode::OfferWithoutAudio`。该检查只告警，不拒绝发送。

`parse_signal` 对 TS6 客户端的实际载荷做宽松解析：

- `offer` / `reconnectOffer`：`args` 可以是 SDP 字符串，也可以是含 `offer` 或 `sdp` 键的对象。
- `answer`：读取 `answer`，回退到 `sdp`。
- `iceCandidate`：候选文本读取 `sdp`，回退到 `candidate`；`mid` / `sdpMid` 接受字符串或数字；`mLine` / `sdpMLineIndex` 接受数字或纯数字字符串，并限制在 `u16` 范围。
- 非 JSON、缺少 `cmd` 或形状不匹配的载荷作为 `ParsedSignal::Raw(RawSignal)` 原样透传。

发送 candidate 时，`sdp_mid` 与 `sdp_m_line_index` 为 `None` 的字段从 JSON 中省略，避免发送 `null` 触发对端解析失败。

## 错误

`StreamError` 的变体与定位方式：

| 变体 | 含义 |
|------|------|
| `Unsupported(StreamsUnsupportedError)` | 服务端未声明 `STREAM_P2P` 或连接信息不可用 |
| `JoinDenied(StreamJoinDeniedError)` | 发布者拒绝观看 |
| `PayloadTooLarge(StreamPayloadTooLargeError)` | 转义后命令行超过 9 150 字节 |
| `Ended(StreamEndedError)` | 流已结束、未知或服务端返回 `5635` |
| `TransportUnsupported(StreamTransportUnsupportedError)` | 非 P2P 传输模式 |
| `NotConnected` | 断线 |
| `Disposed` | `Streams` 已 `dispose` |
| `Aborted` | `AbortSignal` 已触发 |
| `InvalidOption(String)` | 发布参数不合法 |
| `Client(Error)` | 其他底层客户端 / 服务端错误 |

`StreamError::server_error_code()` 返回底层数字错误码，`is_server_error_code(code)` 用于匹配 `ErrorCode`：

| `ErrorCode` | 值 | 说明 |
|-------------|----|------|
| `CommandNotFound` | `0x0100` | 服务端不支持该命令 |
| `StreamNotFound` | `0x1603` | 流不存在或已结束 |
| `StreamNotParticipating` | `0x1604` | 客户端未参与该流 |
| `StreamAlreadyJoined` | `0x1606` | 已经加入 |
| `StreamJoinRequestRedacted` | `0x1607` | 观看申请被隐藏 |

## 完整示例

- [`examples/stream_publish.rs`](../examples/stream_publish.rs)：发布、处理申请、拒绝、ICE、断流与清理。
- [`examples/stream_watch.rs`](../examples/stream_watch.rs)：发现流、观看、answer、trickle ICE 与 RTP 统计。

两个示例都需要 `stream-example` feature 与一个支持 `STREAM_P2P` 的 TeamSpeak 6 服务端；运行命令见上文。
