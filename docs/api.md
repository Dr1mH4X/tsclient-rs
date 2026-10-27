# API Reference

## Client Lifecycle

| Method                                     | Description                              |
| ------------------------------------------ | ---------------------------------------- |
| `Client::new(identity, addr, name, opts?)` | Create a new client                      |
| `client.connect()`                         | Initiate connection to the server        |
| `client.wait_connected(signal?)`           | Block until the handshake completes      |
| `client.wait_directory_ready(signal?)`     | Block until the welcome directory is complete |
| `client.directory_ready()`                 | Whether the welcome directory is complete |
| `client.disconnect()`                      | Gracefully disconnect (awaits bg tasks)  |
| `client.status()`                          | Get current `ClientStatus`               |
| `client.client_id()`                       | Get assigned client ID on the server     |
| `client.channel_id()`                      | Get current channel ID                   |

## Events

| Method                                    | Description                        |
| ----------------------------------------- | ---------------------------------- |
| `client.on_connected(handler)`            | Fires when fully connected         |
| `client.on_disconnected(handler)`         | Fires on disconnect                |
| `client.on_text_message(handler)`         | Fires on text messages             |
| `client.on_client_enter(handler)`         | Fires when a client joins          |
| `client.on_client_leave(handler)`         | Fires when a client leaves         |
| `client.on_client_moved(handler)`         | Fires when a client moves channels |
| `client.on_client_updated(handler)`       | Fires when a known client's state changes |
| `client.on_directory_snapshot(handler)`   | Fires with the live channel/client directory |
| `client.on_kicked(handler)`               | Fires when the bot is kicked       |
| `client.on_poked(handler)`                | Fires when poked by a client       |
| `client.on_voice_data(handler)`           | Fires on incoming voice data       |

Directory snapshots are coalesced and delivered serially per handler. A slow
handler retains only the newest pending state. The live directory cache is
bounded in both entry count and total text storage. `DirectorySnapshot.truncated`
is true when a row was rejected by those limits. `directory_snapshot()` returns
`None` until the welcome stream finishes; use `wait_directory_ready()` first to
wait for a complete snapshot.

`ClientInfo` carries live client state alongside identity: `away`,
`away_message`, `input_muted`, `output_muted` and `channel_commander`.
Each is `Option`-typed: `None` means the server has not reported the flag
yet, which is distinct from a reported `Some(false)`. Mute and away toggles
arrive through `notifyclientupdated`, are reflected in later snapshots, and
dispatch `client_updated` events carrying the fresh record.
Welcome traffic is reconciled against live events: updates that arrive
before a client's row are replayed once the row inserts the client, and
clients an enterview introduced or a left view removed are never replaced
or resurrected by a late row.

## Commands

| Function                                               | Description                                |
| ------------------------------------------------------ | ------------------------------------------ |
| `sendTextMessage(client, targetMode, targetId, msg)`   | Send a text message                        |
| `clientMove(client, clid, channelId, password?)`       | Move a client to a channel                 |
| `poke(client, clid, message)`                          | Poke a client                              |
| `client.send_voice(data, codec)`                       | Send Opus voice data                       |
| `listChannels(client)`                                 | List all channels                          |
| `listClients(client)`                                  | List all connected clients                 |
| `client.directory_snapshot()`                          | Read the complete channel/client directory |
| `getClientInfo(client, clid)`                          | Get detailed client information            |
| `client.exec_command(cmd, timeout?)`                   | Execute a raw command                      |
| `client.exec_command_with_response(cmd, timeout?)`     | Execute a command and return response data |
| `client.send_command_no_wait(cmd)`                     | Fire-and-forget command                    |

## File Transfers

| Function                                  | Description                       |
| ----------------------------------------- | --------------------------------- |
| `client.file_transfer_init_upload(...)`   | Initialize a file upload          |
| `client.file_transfer_init_download(...)` | Initialize a file download        |
| `fileTransferDeleteFile(client, ...)`     | Delete files on the server        |
| `uploadFileData(host, info, reader)`      | Transfer file data to the server  |
| `downloadFileData(host, info, writer)`    | Receive file data from the server |

## 频道图标

`ChannelInfo.icon_id` 保存 `channel_icon_id`，类型为 `Option<u32>`。`None` 表示服务器未报告该字段，`Some(0)` 表示没有自定义图标；协议中的有符号 32 位 id 会按同一组位转换成无符号值。频道创建和编辑通知会更新后续目录快照，未携带图标字段的通知保留已有值。

| API | 用途 |
| --- | --- |
| `icon_file_name(id)` / `iconFileName(id)` | 返回频道 0 文件区中的 `/icon_<unsigned id>` |
| `client.download_icon(host, id, max_bytes)` | 下载自定义图标，拒绝 0 id、空文件和超过限制的声明长度 |
| `download_icon(&client, host, id, max_bytes)` / `downloadIcon(...)` | 上一接口的独立函数形式 |

需要控制协议请求与 TCP 传输时，可先调用 `client.file_transfer_init_download(0, &icon_file_name(id), "")`，校验返回的 `size` 后调用 `downloadFileData(host, &info, writer)`。`host` 由调用者提供，不取第三方主机；应用自身负责访问策略和缓存。

文件传输先向 TCP 连接写入原始 `ftkey`，之后传输原始文件字节，与 JS `src/client/transfer.ts` 和 Go `transfer.go` 一致。下载严格校验声明长度，提前关闭或多传字节都会失败；连接超时为 10 秒，传输密钥及每次网络读写的无进展超时为 30 秒。低层兼容入口 `dialFileTransfer(host, port)` 只建立连接，密钥由上传和下载函数发送。
