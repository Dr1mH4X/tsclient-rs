use std::collections::HashSet;
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use tsclient_rs::{Client, ClientOptions, downloadIcon, generateIdentity};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let address = std::env::var("TS_ICON_SERVER")?;
    let host = std::env::var("TS_ICON_HOST").unwrap_or_else(|_| {
        address
            .parse::<std::net::SocketAddr>()
            .map(|addr| addr.ip().to_string())
            .or_else(|_| {
                address
                    .parse::<std::net::IpAddr>()
                    .map(|addr| addr.to_string())
            })
            .unwrap_or_else(|_| {
                address
                    .rsplit_once(':')
                    .map_or(address.as_str(), |(host, _)| host)
                    .to_string()
            })
    });
    let output = PathBuf::from(std::env::var("TS_ICON_OUTPUT_DIR")?);
    let mut client = Client::new(
        generateIdentity(8),
        address,
        "icon-read-only-probe".into(),
        ClientOptions::default(),
    );
    client.connect().await?;
    let result: Result<(), Box<dyn Error>> = async {
        tokio::time::timeout(Duration::from_secs(30), client.wait_connected(None)).await??;
        tokio::time::timeout(Duration::from_secs(30), client.wait_directory_ready(None)).await??;
        let directory = client
            .directory_snapshot()
            .ok_or("directory is not ready")?;
        println!("channels={}", directory.channels.len());
        let mut seen = HashSet::new();
        let mut failures = 0;
        std::fs::create_dir_all(&output)?;
        for channel in directory.channels {
            let Some(id) = channel.icon_id.filter(|id| *id != 0) else {
                continue;
            };
            if !seen.insert(id) {
                continue;
            }
            match downloadIcon(&client, &host, id, 1024 * 1024).await {
                Ok(bytes) => {
                    let extension = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
                        "png"
                    } else if bytes.starts_with(b"GIF8") {
                        "gif"
                    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
                        "jpg"
                    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
                        "webp"
                    } else {
                        "bin"
                    };
                    let path = output.join(format!("icon_{id}.{extension}"));
                    std::fs::write(&path, &bytes)?;
                    println!(
                        "channel_id={} icon_id={id} bytes={} path={}",
                        channel.id,
                        bytes.len(),
                        path.display()
                    );
                }
                Err(error) => {
                    failures += 1;
                    println!("channel_id={} icon_id={id} error={error}", channel.id);
                }
            }
        }
        println!("unique_custom_icons={} failed={failures}", seen.len());
        if failures != 0 {
            return Err(format!("{failures} icon downloads failed").into());
        }
        Ok(())
    }
    .await;
    client.disconnect().await?;
    result
}
