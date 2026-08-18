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

    client.connect().await?;
    client.wait_connected(None).await?;
    println!("Connected, CLID: {}", client.client_id());

    let channels = listChannels(&client).await?;
    let clients = listClients(&client).await?;

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
            println!("Voice from client {}, codec: {}", vd.client_id, vd.codec);
            // vd.data contains Opus-encoded audio frames
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

// List all connected clients
let clients = listClients(&client).await?;

// Get detailed client info
let info = getClientInfo(&client, clid).await?;

// Execute raw command with response
let response = client.exec_command_with_response("serverconnectinfo", 5000).await?;

// Fire-and-forget command
client.send_command_no_wait("clientupdate").await?;
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
uploadFileData(&upload_info.server_password, &upload_info, cursor).await?;

// Initialize file download
let download_info = client.file_transfer_init_download(
    channel_id,
    "/path/to/file",
    "",
).await?;

// Download file data
let mut output = Vec::new();
downloadFileData(&download_info.server_password, &download_info, &mut output).await?;

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
