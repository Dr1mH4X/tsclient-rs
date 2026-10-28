//! File transfer — mirrors `teamspeak-js/src/transfer.ts`
//!
//! TeamSpeak 文件传输的 TCP 密钥和文件内容均为明文，不提供传输加密。
//! 敏感文件应由调用方通过可信网络或加密隧道传输。

use std::collections::HashMap;

use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
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

/// 打开文件传输 TCP 连接；此低层入口不发送传输密钥。
pub async fn dial_file_transfer(
    host: &str,
    port: u16,
) -> Result<tokio::net::TcpStream, crate::Error> {
    if port == 0 {
        return Err(crate::Error::FileTransfer("invalid file transfer port: 0".into()));
    }
    tokio::time::timeout(Duration::from_secs(10), tokio::net::TcpStream::connect((host, port)))
        .await
        .map_err(|_| crate::Error::FileTransfer("connection timeout".into()))?
        .map_err(|e| crate::Error::FileTransfer(format!("failed to connect: {e}")))
}

async fn open_transfer(
    host: &str,
    port: i32,
    key: &str,
) -> Result<tokio::net::TcpStream, crate::Error> {
    let port = u16::try_from(port)
        .map_err(|_| crate::Error::FileTransfer(format!("invalid file transfer port: {port}")))?;
    let mut stream = dial_file_transfer(host, port).await?;
    tokio::time::timeout(Duration::from_secs(30), stream.write_all(key.as_bytes()))
        .await
        .map_err(|_| crate::Error::FileTransfer("transfer key write timeout".into()))?
        .map_err(|e| crate::Error::FileTransfer(format!("failed to send transfer key: {e}")))?;
    Ok(stream)
}

/// 按 JS/Go 参考实现通过明文 TCP 发送 ftkey 和原始文件字节。
/// 调用方数据源的读取不设超时；需要期限时应包裹整个调用。
pub async fn upload_file_data<R: AsyncRead + Unpin>(
    host: &str,
    info: &FileUploadInfo,
    mut data: R,
) -> Result<(), crate::Error> {
    let mut stream = open_transfer(host, info.port, &info.file_transfer_key).await?;
    let mut buffer = [0u8; 32 * 1024];
    loop {
        let length = data.read(&mut buffer).await
            .map_err(|e| crate::Error::FileTransfer(format!("read failed: {e}")))?;
        if length == 0 {
            break;
        }
        tokio::time::timeout(Duration::from_secs(30), stream.write_all(&buffer[..length]))
            .await
            .map_err(|_| crate::Error::FileTransfer("upload write timeout".into()))?
            .map_err(|e| crate::Error::FileTransfer(format!("write failed: {e}")))?;
    }
    stream.shutdown().await
        .map_err(|e| crate::Error::FileTransfer(format!("shutdown failed: {e}")))
}

/// 通过明文 TCP 发送 ftkey，接收准确的声明字节数并等待服务器关闭连接。
/// 调用方目标的写入和刷新不设超时；需要期限时应包裹整个调用。
pub async fn download_file_data<W: AsyncWrite + Unpin>(
    host: &str,
    info: &FileDownloadInfo,
    mut dest: W,
) -> Result<(), crate::Error> {
    let mut stream = open_transfer(host, info.port, &info.file_transfer_key).await?;
    let mut remaining = info.size;
    let mut buffer = [0u8; 32 * 1024];
    loop {
        // 声明长度之外最多读取一个字节，发现超量时不把它交给调用者。
        let limit = remaining.saturating_add(1).min(buffer.len() as u64) as usize;
        let length = tokio::time::timeout(Duration::from_secs(30), stream.read(&mut buffer[..limit]))
            .await
            .map_err(|_| crate::Error::FileTransfer("download read timeout".into()))?
            .map_err(|e| crate::Error::FileTransfer(format!("read failed: {e}")))?;
        if length == 0 {
            if remaining != 0 {
                return Err(crate::Error::FileTransfer(format!("download ended with {remaining} bytes missing")));
            }
            break;
        }
        if length as u64 > remaining {
            return Err(crate::Error::FileTransfer("download exceeds announced size".into()));
        }
        dest.write_all(&buffer[..length]).await
            .map_err(|e| crate::Error::FileTransfer(format!("write failed: {e}")))?;
        remaining -= length as u64;
    }
    dest.flush().await
        .map_err(|e| crate::Error::FileTransfer(format!("flush failed: {e}")))
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn download_info(port: u16, size: u64) -> FileDownloadInfo {
        FileDownloadInfo {
            file_transfer_key: "test-key".into(),
            size,
            client_file_transfer_id: 1,
            server_file_transfer_id: 2,
            port: i32::from(port),
        }
    }

    #[tokio::test]
    async fn download_sends_raw_key_and_preserves_non_block_sized_icon_bytes() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let data = b"GIF89a-icon-data-tail";
        assert_ne!(data.len() % 16, 0);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut key = [0u8; 8];
            socket.read_exact(&mut key).await.unwrap();
            assert_eq!(&key, b"test-key");
            socket.write_all(data).await.unwrap();
        });
        let mut output = Vec::new();
        download_file_data("127.0.0.1", &download_info(port, data.len() as u64), &mut output).await.unwrap();
        assert_eq!(output, data);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn download_rejects_truncated_or_oversized_content() {
        for (data, size) in [(b"short".as_slice(), 6), (b"too-long".as_slice(), 4)] {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut key = [0u8; 8];
                socket.read_exact(&mut key).await.unwrap();
                socket.write_all(data).await.unwrap();
            });
            let mut output = Vec::new();
            assert!(download_file_data("127.0.0.1", &download_info(port, size), &mut output).await.is_err());
            assert!(output.len() as u64 <= size);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn upload_sends_key_and_the_complete_unencrypted_payload() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            socket.read_to_end(&mut data).await.unwrap();
            data
        });
        let info = FileUploadInfo {
            file_transfer_key: "test-key".into(),
            seek_position: 0,
            client_file_transfer_id: 1,
            server_file_transfer_id: 2,
            port: i32::from(port),
        };
        upload_file_data("127.0.0.1", &info, b"unaligned-file".as_slice()).await.unwrap();
        assert_eq!(server.await.unwrap(), b"test-keyunaligned-file");
    }

    #[tokio::test]
    async fn transfer_ports_are_validated_without_wrapping() {
        for port in [-1, 0, 65536] {
            let info = FileDownloadInfo { port, ..download_info(1, 1) };
            assert!(download_file_data("127.0.0.1", &info, &mut Vec::new()).await.is_err());
        }
    }
}
