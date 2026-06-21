//! File transfer — mirrors `teamspeak-js/src/transfer.ts`

use std::collections::HashMap;

use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

use crate::command::build_command;
use crate::types::*;

#[derive(Debug, Clone)]
pub enum FtNotification {
    Upload(FileUploadInfo),
    Download(FileDownloadInfo),
    Status(FileTransferStatusInfo),
}

pub struct FileTransferTracker {
    pending: HashMap<i32, oneshot::Sender<FtNotification>>,
    next_id: i32,
}

impl FileTransferTracker {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn register(&mut self) -> (i32, oneshot::Receiver<FtNotification>) {
        self.next_id += 1;
        if self.next_id > 65535 {
            self.next_id = 1;
        }
        let cftid = self.next_id;
        let (tx, rx) = oneshot::channel();
        self.pending.insert(cftid, tx);
        (cftid, rx)
    }

    pub fn unregister(&mut self, cftid: i32) {
        self.pending.remove(&cftid);
    }

    pub fn notify(&mut self, cftid: i32, value: FtNotification) {
        if let Some(sender) = self.pending.remove(&cftid) {
            let _ = sender.send(value);
        }
    }

    pub fn reset(&mut self) {
        self.pending.clear();
        self.next_id = 0;
    }
}

pub async fn dial_file_transfer(
    host: &str,
    port: u16,
    key: &str,
) -> Result<tokio::net::TcpStream, crate::Error> {
    let addr = format!("{host}:{port}");
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| crate::Error::FileTransfer("connection timeout".into()))?
    .map_err(|e| crate::Error::FileTransfer(format!("failed to connect: {e}")))?;

    stream.write_all(key.as_bytes()).await
        .map_err(|e| crate::Error::FileTransfer(format!("failed to send transfer key: {e}")))?;

    Ok(stream)
}

pub async fn upload_file_data(
    host: &str,
    info: &FileUploadInfo,
    data: Vec<u8>,
) -> Result<(), crate::Error> {
    let mut stream = dial_file_transfer(host, info.port as u16, &info.file_transfer_key).await?;
    stream.write_all(&data).await
        .map_err(|e| crate::Error::FileTransfer(format!("upload failed: {e}")))?;
    stream.shutdown().await
        .map_err(|e| crate::Error::FileTransfer(format!("upload shutdown failed: {e}")))?;
    Ok(())
}

pub async fn download_file_data(
    host: &str,
    info: &FileDownloadInfo,
) -> Result<Vec<u8>, crate::Error> {
    use tokio::io::AsyncReadExt;
    let mut stream = dial_file_transfer(host, info.port as u16, &info.file_transfer_key).await?;
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await
        .map_err(|e| crate::Error::FileTransfer(format!("download failed: {e}")))?;
    Ok(data)
}

pub fn build_ft_init_upload(
    channel_id: u64,
    path: &str,
    password: &str,
    size: u64,
    cftid: i32,
    overwrite: bool,
) -> String {
    let target_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let mut params = HashMap::new();
    params.insert("cid".to_string(), channel_id.to_string());
    params.insert("name".to_string(), target_path);
    params.insert("cpw".to_string(), password.to_string());
    params.insert("size".to_string(), size.to_string());
    params.insert("clientftfid".to_string(), cftid.to_string());
    params.insert("overwrite".to_string(), if overwrite { "1" } else { "0" }.to_string());
    params.insert("resume".to_string(), "0".to_string());
    build_command("ftinitupload", params)
}

pub fn build_ft_init_download(
    channel_id: u64,
    path: &str,
    password: &str,
    cftid: i32,
) -> String {
    let target_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let mut params = HashMap::new();
    params.insert("cid".to_string(), channel_id.to_string());
    params.insert("name".to_string(), target_path);
    params.insert("cpw".to_string(), password.to_string());
    params.insert("clientftfid".to_string(), cftid.to_string());
    params.insert("seekpos".to_string(), "0".to_string());
    build_command("ftinitdownload", params)
}
