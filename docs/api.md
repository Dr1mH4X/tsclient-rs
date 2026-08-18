# API Reference

## Client Lifecycle

| Method                                     | Description                              |
| ------------------------------------------ | ---------------------------------------- |
| `Client::new(identity, addr, name, opts?)` | Create a new client                      |
| `client.connect()`                         | Initiate connection to the server        |
| `client.wait_connected(signal?)`           | Block until the handshake completes      |
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
| `client.on_kicked(handler)`               | Fires when the bot is kicked       |
| `client.on_poked(handler)`                | Fires when poked by a client       |
| `client.on_voice_data(handler)`           | Fires on incoming voice data       |

## Commands

| Function                                               | Description                                |
| ------------------------------------------------------ | ------------------------------------------ |
| `sendTextMessage(client, targetMode, targetId, msg)`   | Send a text message                        |
| `clientMove(client, clid, channelId, password?)`       | Move a client to a channel                 |
| `poke(client, clid, message)`                          | Poke a client                              |
| `client.send_voice(data, codec)`                       | Send Opus voice data                       |
| `listChannels(client)`                                 | List all channels                          |
| `listClients(client)`                                  | List all connected clients                 |
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
