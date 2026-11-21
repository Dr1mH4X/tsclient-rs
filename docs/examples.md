# Examples

## Quick Start

```rust
use std::sync::Arc;
use tsclient_rs::*;

#[tokio::main]
async fn main() -> Result<(), tsclient_rs::Error> {
    let identity = generateIdentity(8);

    let client = Client::new(
        identity,
        "127.0.0.1:9987".to_string(),
        "MyBot".to_string(),
        ClientOptions::default(),
    );

    client.on_text_message(Arc::new(|event| {
        if let Event::TextMessage(ref msg) = event {
            println!("Received: {}", msg.message);
        }
    }));

    // Per-client state changes (mute, away, groups) arrive as events, so a
    // consumer never has to poll the directory.
    client.on_client_updated(Arc::new(|event| {
        if let Event::ClientUpdated(info) = event {
            if info.input_muted == Some(true) {
                println!("{} muted their microphone", info.nickname);
            }
        }
    }));

    client.connect().await?;
    client.wait_connected(None).await?;
    client.wait_directory_ready(None).await?;
    println!("Connected, CLID: {}", client.client_id());

    // The welcome stream is available even when the server denies an explicit
    // channellist or clientlist command to a guest.
    let directory = client.directory_snapshot().expect("directory is ready");
    println!(
        "{} channels, {} visible clients",
        directory.channels.len(),
        directory.clients.len()
    );

    // Live client state: `None` means the server has not reported the flag
    // yet, `Some(false)` means it reported the flag as off.
    for client in &directory.clients {
        match client.input_muted {
            Some(true) => println!("{} has their microphone muted", client.nickname),
            Some(false) => println!("{} can speak", client.nickname),
            None => {}
        }
    }

    client.disconnect().await?;

    Ok(())
}
```

## Voice I/O

```rust
use std::sync::Arc;
use tsclient_rs::*;

#[tokio::main]
async fn main() -> Result<(), tsclient_rs::Error> {
    let identity = generateIdentity(8);
    let client = Client::new(
        identity,
        "127.0.0.1:9987".to_string(),
        "MyBot".to_string(),
        ClientOptions::default(),
    );

    // Receive voice data
    client.on_voice_data(Arc::new(|event| {
        if let Event::VoiceData(ref vd) = event {
            println!(
                "Voice from client {}, seq: {}, codec: {}, whisper: {}, bytes: {}, at: {:?}",
                vd.client_id,
                vd.seq,
                vd.codec,
                vd.whisper,
                vd.data.len(),
                vd.received_at,
            );
            // vd.data contains Opus-encoded audio frames (empty on end-of-speech tail frame)
        }
    }));

    client.connect().await?;
    client.wait_connected(None).await?;

    // Send voice data (Opus encoded)
    let opus_frame: Vec<u8> = vec![/* Opus-encoded audio data */];
    client.send_voice(opus_frame, 4); // codec 4 = Speex, 5 = Opus

    client.disconnect().await?;
    Ok(())
}
```

## Identity

```rust
use tsclient_rs::*;

// Generate a new identity with security level 8
let identity = generateIdentity(8);

// Export to string for persistent storage
let exported = identity.export_string();

// Import from a previously exported string
let restored = identityFromString(&exported);

// Get UID from public key
let uid = getUidFromPublicKey(&identity.public_key);
```

## Command Examples

```rust
use tsclient_rs::*;

// Send text message (target_mode: 1=private, 2=channel, 3=server)
sendTextMessage(&client, 1, target_client_id, "Hello!").await?;

// Move client to a channel
clientMove(&client, clid, channel_id, "").await?;

// Poke a client
poke(&client, clid, "Wake up!").await?;

// Kick a client
clientKick(&client, clid, KickReason::Server, "Goodbye").await?;

// Ban a client (time_secs=0 for permanent)
banClient(&client, clid, 3600, "Spamming").await?;

// List all channels
let channels = listChannels(&client).await?;

// Get detailed channel configuration (includes description)
let channel_info = getChannelInfo(&client, channel_id).await?;

// Fetch only a channel's description via channelgetdescription
let description = getChannelDescription(&client, channel_id).await?;

// List all connected clients
let clients = listClients(&client).await?;

// Get detailed client info
let info = getClientInfo(&client, clid).await?;

// Execute raw command with response
let response = client.exec_command_with_response("serverconnectinfo", 5000).await?;

// Build and execute custom protocol command with automatic parameter escaping
client.send_raw_command("requeststreaminfo", &[("clid", "42")]).await?;

// Fire-and-forget command
client.send_command_no_wait("clientupdate").await?;

// Send plugin command (target: CurrentChannel=0, Server=1, Client=2)
sendPluginCommand(
    &client,
    "my_plugin",
    "payload_data",
    PluginCommandTarget::CurrentChannel,
    None,
).await?;

// Send private plugin command to a specific client
sendPluginCommand(
    &client,
    "my_plugin",
    "private_data",
    PluginCommandTarget::Client,
    Some(clid),
).await?;
```

## Plugin Command Listening

```rust
use std::sync::Arc;
use tsclient_rs::*;

client.on_plugin_command(Arc::new(|event| {
    if let Event::PluginCommand(ref cmd) = event {
        println!(
            "Plugin command '{}' from {} (clid={}, uid={}): {}",
            cmd.name, cmd.invoker_name, cmd.invoker_id, cmd.invoker_uid, cmd.data
        );
    }
}));
```

## OpenProtocol（`open-protocol` feature）

运行完整示例：`cargo run --example open_protocol_bot --features open-protocol`

```rust
use tsclient_rs::open_protocol::{
    BotCommand, BotCommandList, ClientKind, OpenProtocolConfig, OpenProtocolHandle, PeerIdentity,
};
use tsclient_rs::{Client, ClientOptions, generateIdentity};

let mut client = Client::new(
    generateIdentity(8),
    "127.0.0.1:9987".to_string(),
    "OpenProtocolBot".to_string(),
    ClientOptions::default(),
);

let config = OpenProtocolConfig::new(PeerIdentity::new(
    ClientKind::Bot,
    "ExampleBot",
    env!("CARGO_PKG_VERSION"),
))
.with_commands(
    BotCommandList::new(vec![BotCommand::new("ping", "Ping bot")]).with_prefix("!"),
);

let open_protocol = OpenProtocolHandle::attach(&client, config);
client.connect().await?;
client.wait_connected(None).await?;
```

## File Transfer Example

```rust
use std::io::Cursor;

// Initialize file upload
let upload_info = client.file_transfer_init_upload(
    channel_id,      // Channel ID
    "/path/to/file", // Server path
    "",              // Password (empty if none)
    file_size,       // File size
    true,            // Overwrite existing file
).await?;

// Upload file data
let file_data = std::fs::read("local_file.txt")?;
let cursor = Cursor::new(file_data);
uploadFileData("127.0.0.1", &upload_info, cursor).await?;

// Initialize file download
let download_info = client.file_transfer_init_download(
    channel_id,
    "/path/to/file",
    "",
).await?;

// Download file data
let mut output = Vec::new();
downloadFileData("127.0.0.1", &download_info, &mut output).await?;

// Delete server file
fileTransferDeleteFile(&client, channel_id, "/path/to/file", "").await?;
```

## Options

```rust
let client = Client::new(
    identity,
    "ts.example.com".to_string(),
    "MyBot".to_string(),
    ClientOptions {
        server_password: Some("secret".into()),
        default_channel: Some("Lobby".into()),
        ..Default::default()
    },
);
```

## Middleware

```rust
use tsclient_rs::*;

struct LogMiddleware;

impl CommandMiddleware for LogMiddleware {
    fn wrap(&self, next: CommandHandler) -> CommandHandler {
        Arc::new(move |cmd: String| {
            println!(">> {cmd}");
            let next = next.clone();
            Box::pin(async move { next(cmd).await })
        })
    }
}

client.use_command_middleware(vec![Box::new(LogMiddleware)]);
```

## 只读频道图标调试

`examples/channel_icons.rs` 从欢迎目录读取频道图标 id，逐个下载唯一的自定义图标；它不上传文件或修改服务器。服务器地址和输出目录从环境变量传入，不会保存身份私钥。

```sh
TS_ICON_SERVER=127.0.0.1:9987 \
TS_ICON_OUTPUT_DIR=/tmp/channel-icons \
cargo run --example channel_icons
```

文件传输使用与服务器相同的主机；如果连接地址通过 SRV 指向其他主机，可用 `TS_ICON_HOST` 显式指定实际传输主机。下载调用的 `max_bytes` 是应用自身的限制，示例限制每个图标为 1 MiB。

## 读取服务器报告的权限

先通过 `client.exec_command_with_response("permissionlist", 5000)` 读取服务器的权限名称表，再使用对应 id 调用 `client.permission_value(id)`。返回 `None` 表示未知或已撤销，界面应禁用相应管理操作；最终操作仍由服务器校验。

`Error::ServerError` 的 `extra_message` 可读取服务端附加说明。命令执行器自动处理带等待时间的防刷拒绝；超时或没有等待时间的拒绝直接返回，不重发已执行结果未知的请求。

## TS6 屏幕共享控制面（`stream` feature）

库负责控制命令与 SDP/ICE 透传，媒体由调用方的 WebRTC 栈处理。仓库提供两个可运行示例，使用 `webrtc` crate 并需要 `stream-example` feature：

- [`examples/stream_publish.rs`](../examples/stream_publish.rs)：VP8 IVF 发布端，处理观看申请、answer、ICE 与断流。
- [`examples/stream_watch.rs`](../examples/stream_watch.rs)：观看端，打印轨道与 RTP 包计数。

```sh
ffmpeg -f avfoundation -framerate 30 -i "1:none" -t 30 \
  -c:v libvpx -b:v 5M -f ivf /tmp/screen.ivf
STREAM_IVF=/tmp/screen.ivf TEAMSPEAK_ADDR=127.0.0.1:9987 \
  cargo run --example stream_publish --features stream-example
TEAMSPEAK_ADDR=127.0.0.1:9987 \
  cargo run --example stream_watch --features stream-example
```

发布端先生成 offer 再接受申请，视频 MID 0、音频 MID 1，且始终保留 `m=audio`：

```rust
use tsclient_rs::stream::{PeerEvent, PublishOptions, PublishedStreamEvent};

# async fn demo(streams: tsclient_rs::stream::Streams, offer_sdp: String)
#     -> Result<(), tsclient_rs::stream::StreamError> {
let published = streams.publish(PublishOptions::new("Desktop")).await?;
let mut requests = published.subscribe();
if let Some(PublishedStreamEvent::Request(request)) = requests.recv().await {
    let peer = request.accept(&offer_sdp, "join").await?;
    let mut events = peer.subscribe();
    while let Some(event) = events.recv().await {
        match event {
            PeerEvent::Answer(sdp) => { /* set_remote_description */ }
            PeerEvent::Candidate(candidate) => { /* add_ice_candidate */ }
            PeerEvent::Closed(_) => break,
            _ => {}
        }
    }
}
# Ok(())
# }
```

观看端的初始 offer 用 `view.offer()` 读取，answer 生成后交给 `view.answer()`，本地 ICE 在 answer 发出后再 trickle。

发送前按转义后 UTF-8 字节检查 9 150 字节上限，超出返回 `StreamPayloadTooLargeError`；缺少 `m=audio` 时发出 `OfferWithoutAudio` 警告但仍发送。`client_is_streaming` 清除后等待 2 秒 `notifystreamstopped`，期间到达的新流会取消清理；本端 `clid` 与本地发布流不清。完整契约与媒体层约束见 [TS6 屏幕共享](stream.md)。
