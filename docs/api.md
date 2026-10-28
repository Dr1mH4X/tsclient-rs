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

`ChannelInfo.icon_id` 保存 `channel_icon_id`，类型为 `Option<u32>`。`None` 表示服务器未报告该字段，`Some(0)` 表示没有自定义图标；协议中的有符号 32 位 id 以及零扩展到 64 位的无符号形式都会按同一组位转换成无符号值。频道创建和编辑通知会更新后续目录快照，未携带图标字段的通知保留已有值。`listChannels` 请求 `channellist -icon`，因此该接口返回的频道也带有图标 id。

| API | 用途 |
| --- | --- |
| `icon_file_name(id)` / `iconFileName(id)` | 返回频道 0 文件区中的 `/icon_<unsigned id>` |
| `client.download_icon(host, id, max_bytes)` | 下载自定义图标，拒绝 0 id、空文件和超过限制的声明长度 |
| `download_icon(&client, host, id, max_bytes)` / `downloadIcon(...)` | 上一接口的独立函数形式 |

需要控制协议请求与 TCP 传输时，可先调用 `client.file_transfer_init_download(0, &icon_file_name(id), "")`，校验返回的 `size` 后调用 `downloadFileData(host, &info, writer)`。`host` 由调用者提供，不取第三方主机；应用自身负责访问策略和缓存。

文件传输先向 TCP 连接写入原始 `ftkey`，之后传输原始文件字节，与 JS `src/client/transfer.ts` 和 Go `transfer.go` 一致。下载严格校验声明长度，提前关闭或多传字节都会失败；连接超时为 10 秒，传输密钥及每次网络读写的无进展超时为 30 秒。低层兼容入口 `dialFileTransfer(host, port)` 只建立连接，密钥由上传和下载函数发送。

**明文传输：** TeamSpeak 文件传输协议通过普通 TCP 发送 `ftkey` 和文件内容，库不提供传输加密。敏感文件应通过可信网络或由调用方建立的加密隧道传输。

上述超时仅覆盖网络操作；调用方提供的 `AsyncRead` 读取、`AsyncWrite` 写入和刷新均不设超时。需要统一期限时，可使用 `tokio::time::timeout` 包裹整个上传或下载调用。

## 查询通知与有效权限

`exec_command_with_response` 收集当前命令对应的权限组、权限列表及频道文件列表通知，返回其中的数据行；普通成员状态通知不会混入查询。通知仍正常更新客户端状态。命令串行发送时，通知行归属于随后返回确认的查询，与 JS 参考实现的内联响应规则一致。

`client.permission_value(permid)` 返回服务器通过 `notifyclientneededpermissions` 最近推送的本端有效权限；未报告或撤销时返回 `None`，断开连接后清空。`on_own_permissions_changed` 收到 `Event::OwnPermissionsChanged` 后，调用方可重新读取受影响的操作权限。权限 id 由服务器的 `permissionlist` 数据确定，不能跨服务器版本写死。

文件传输协商中的 `notifystatusfiletransfer` 拒绝结果以 `Error::ServerError` 返回，保留数字错误码与服务器文案，调用方可以区分频道密码错误和权限不足。

命令错误保留服务端 `extra_msg`。只有错误 524 携带明确等待时间时才在原调用时限内重试，最多三次；节流等待计入调用超时。
