# tsclient-rs

Rust implementation of the TeamSpeak 3 client protocol (UDP), ported from [teamspeak-js](https://github.com/teamspeak-js).

TeamSpeak 3 客户端协议的 Rust 实现（UDP），从 [teamspeak-js](https://github.com/teamspeak-js) 移植。

---

## Example / 完整示例

```rust
use std::sync::Arc;
use tsclient_rs::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt().init();

    // Generate identity / 生成身份
    let identity = generateIdentity(8);

    // Create client / 创建客户端
    let mut client = Client::new(
        identity,
        "127.0.0.1:9987".to_string(),
        "MyBot".to_string(),
        ClientOptions {
            logger: Arc::new(noopLogger),
            ..Default::default()
        },
    );

    // Register event handlers / 注册事件
    client.on_connected(Arc::new(|| println!("connected")));
    client.on_disconnected(Arc::new(|ev| println!("disconnected: {:?}", ev)));

    // Connect and wait / 连接并等待完成
    client.connect().await?;
    client.wait_connected(None).await?;
    println!("CLID: {}", client.client_id());

    // List channels and clients / 获取频道和客户端列表
    let channels = listChannels(&client).await?;
    let clients = listClients(&client).await?;

    // Clean disconnect / 优雅断开
    client.disconnect().await?;

    Ok(())
}
```

---

## Identity / 身份管理

```rust
// Generate a new identity (security level) / 生成新身份
let identity = generateIdentity(8);

// Restore from string / 从字符串恢复
let identity = identityFromString("AQAFAAEAb...")?;

// Get UID from public key / 从公钥获取 UID
let uid = getUidFromPublicKey(&identity.public_key);
```

---

## Connect & Disconnect / 连接与断开

```rust
let mut client = Client::new(
    identity,
    "server.com:9987".into(),
    "BotName".into(),
    ClientOptions::default(),
);

// Connect (non-blocking, returns once handshake starts)
// 连接（非阻塞，握手启动即返回）
client.connect().await?;

// Wait until fully connected / 等待连接完成
client.wait_connected(None).await?;

// With abort signal / 使用中止信号
let signal = AbortSignal::new();
tokio::spawn(async move {
    tokio::time::sleep(Duration::from_secs(5)).await;
    signal.abort();
});
client.wait_connected(Some(&signal)).await?;

// Check status / 检查状态
println!("{:?}", client.status()); // ClientStatus::Connected

// Get IDs / 获取 ID
let clid = client.client_id();   // client ID on server
let cid  = client.channel_id();  // current channel ID

// Graceful disconnect (joins all background tasks)
// 优雅断开（等待所有后台任务结束）
client.disconnect().await?;
```

---

## Events / 事件

```rust
// Connected / 已连接
client.on_connected(Arc::new(|| {
    println!("connected to server");
}));

// Disconnected / 已断开
client.on_disconnected(Arc::new(|ev| {
    // Disconnected(None)       — clean disconnect / 正常断开
    // Disconnected(Some(err))  — connection lost / 连接丢失
    println!("disconnected: {:?}", ev);
}));

// Text message / 文本消息
client.on_text_message(Arc::new(|ev| {
    if let Event::TextMessage(msg) = ev {
        println!("[{}] {}", msg.invoker_name, msg.message);
    }
}));

// Client joined / 客户端加入
client.on_client_enter(Arc::new(|ev| {
    if let Event::ClientEnter(info) = ev {
        println!("{} joined", info.nickname);
    }
}));

// Client left / 客户端离开
client.on_client_leave(Arc::new(|ev| {
    if let Event::ClientLeave(evt) = ev {
        println!("{} left (reason={})", evt.id, evt.reason_id);
    }
}));

// Client moved / 客户端移动
client.on_client_moved(Arc::new(|ev| {
    if let Event::ClientMoved(evt) = ev {
        println!("{} moved to channel {}", evt.id, evt.target_channel_id);
    }
}));

// Poked / 被戳
client.on_poked(Arc::new(|ev| {
    if let Event::Poked(poke) = ev {
        println!("poked by {}: {}", poke.invoker_name, poke.message);
    }
}));

// Kicked / 被踢
client.on_kicked(Arc::new(|ev| {
    if let Event::Kicked(msg) = ev {
        println!("kicked: {}", msg);
    }
}));

// Voice data / 语音数据
client.on_voice_data(Arc::new(|ev| {
    if let Event::VoiceData(vd) = ev {
        // vd.client_id, vd.codec, vd.data
    }
}));
```

---

## Commands / 命令

```rust
use tsclient_rs::*;

// Send text message / 发送文本消息
sendTextMessage(&client, 3, 0, "Hello everyone!").await?;  // server chat
sendTextMessage(&client, 1, clid, "Hi").await?;            // to client
sendTextMessage(&client, 2, cid, "Hello channel").await?;  // to channel

// Move client / 移动客户端
clientMove(&client, clid, target_channel_id, "").await?;

// Poke / 戳
poke(&client, clid, "Hey!").await?;

// Get client info / 获取客户端信息
let info = getClientInfo(&client, clid).await?;

// Raw commands / 原始命令
client.send_command_no_wait("servernotifyregister event=server").await?;
let data = client.exec_command_with_response("serverinfo", 5_000).await?;
```

---

## List Channels & Clients / 列出频道与客户端

```rust
// All channels / 所有频道
let channels = listChannels(&client).await?;
for ch in &channels {
    println!("[{}] {} (parent: {:?})", ch.id, ch.name, ch.parent_id);
}

// All clients / 所有客户端
let clients = listClients(&client).await?;
for c in &clients {
    println!("[{}] {} (UID: {})", c.id, c.nickname, c.uid);
}
```

---

## File Transfer / 文件传输

```rust
// Initiate upload / 发起上传
let info = client.file_transfer_init_upload(
    channel_id, "/remote/path.txt", "", file_size, false,
).await?;

// Upload data / 上传数据
let file = tokio::fs::File::open("local.txt").await?;
uploadFileData("filehost.example.com", &info, file).await?;

// Initiate download / 发起下载
let info = client.file_transfer_init_download(
    channel_id, "/remote/path.txt", "",
).await?;

// Download data / 下载数据
let dest = tokio::fs::File::create("downloaded.txt").await?;
downloadFileData("filehost.example.com", &info, dest).await?;

// Delete file / 删除文件
fileTransferDeleteFile(&client, channel_id, &["/remote/path.txt".into()]).await?;
```

---

## Voice Data / 语音数据

```rust
// Send raw Opus voice data / 发送原始 Opus 语音数据
client.send_voice(opus_data, 5 /* Opus Voice */);
```

---

## Middleware / 中间件

```rust
use tsclient_rs::*;

// Command middleware / 命令中间件
struct LogMiddleware;

impl CommandMiddleware for LogMiddleware {
    fn wrap(&self, next: CommandHandler) -> CommandHandler {
        Arc::new(move |cmd: String| {
            println!(">> send: {}", cmd);
            let next = next.clone();
            Box::pin(async move { next(cmd).await })
        })
    }
}

client.use_command_middleware(vec![Box::new(LogMiddleware)]);

// Event middleware / 事件中间件
struct DropPrivateMessages;

impl EventMiddleware for DropPrivateMessages {
    fn wrap(&self, next: EventHandler) -> EventHandler {
        Arc::new(move |ev: Event| {
            if let Event::TextMessage(ref msg) = ev {
                if msg.target_mode == 1 { return; } // drop DMs
            }
            next(ev)
        })
    }
}

client.use_event_middleware(vec![Box::new(DropPrivateMessages)]);
```

---

## Logger / 日志

```rust
// Built-in / 内置
let logger: Arc<dyn Logger> = Arc::new(consoleLogger); // tracing
let logger: Arc<dyn Logger> = Arc::new(noopLogger);    // silent

// Custom / 自定义
struct MyLogger;
impl Logger for MyLogger {
    fn debug(&self, msg: &str, _args: &[&dyn Display]) { eprintln!("[DEBUG] {msg}"); }
    fn info(&self, msg: &str, _args: &[&dyn Display])  { eprintln!("[INFO] {msg}"); }
    fn warn(&self, msg: &str, _args: &[&dyn Display])  { eprintln!("[WARN] {msg}"); }
    fn error(&self, msg: &str, _args: &[&dyn Display]) { eprintln!("[ERROR] {msg}"); }
}

Client::new(identity, addr, nickname, ClientOptions {
    logger: Arc::new(MyLogger),
    ..Default::default()
});
```

---

## Error Handling / 错误处理

```rust
match client.connect().await {
    Err(Error::AlreadyConnected)     => eprintln!("already connected"),
    Err(Error::CommandTimeout { .. }) => eprintln!("command timed out"),
    Err(Error::ServerError(id, msg)) => eprintln!("server error {id}: {msg}"),
    Err(Error::Teamspeak(msg))       => eprintln!("error: {msg}"),
    Ok(()) => {},
}
```

---

## Dependencies / 依赖配置

```toml
[dependencies]
tsclient-rs = { git = "https://github.com/your-org/tsclient-rs" }
tokio = { version = "1", features = ["rt", "macros", "net", "time"] }
tracing = "0.1"
```

For `current_thread` runtime (no `rt-multi-thread` feature), `disconnect()` awaits all background tasks before returning, ensuring clean shutdown.

使用 `current_thread` runtime（不启用 `rt-multi-thread`）时，`disconnect()` 会等待所有后台任务结束后再返回，确保干净退出。
