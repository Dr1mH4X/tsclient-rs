<div align="center">

# tsclient-rs

**A clean-room TeamSpeak 3 client protocol library written in Rust.**

Compatible with TeamSpeak 3, 5 & 6. Ported from [teamspeak-js](https://github.com/honeybbq/teamspeak-js).

[![CI](https://github.com/Dr1mH4X/tsclient-rs/actions/workflows/publish.yml/badge.svg)](https://github.com/Dr1mH4X/tsclient-rs/actions/workflows/publish.yml)
[![Rust](https://img.shields.io/badge/Rust-2024-orange?logo=rust&logoColor=white)](https://crates.io/crates/tsclient-rs)
[![TeamSpeak](https://img.shields.io/badge/TeamSpeak-3%2F5%2F6-blue?logo=teamspeak&logoColor=white)](https://teamspeak.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/Dr1mH4X/Dr1mH4X/main/used_by/tsclient-rs/showcase-dark.svg">
  <img width="100%" alt="Projects using tsclient-rs" src="https://raw.githubusercontent.com/Dr1mH4X/Dr1mH4X/main/used_by/tsclient-rs/showcase-light.svg">
</picture>

## Features

- **Full protocol handshake** — ECDH key exchange, RSA puzzle, EAX-encrypted transport
- **Command & notification system** — Send commands, receive server events
- **Event-driven API** — Register handlers for text messages, client enter/leave, channel moves, kicks, etc.
- **Voice data** — Send Opus voice packets (codec 4 & 5)
- **File transfers** — Upload, download, and delete files on the server
- **Address resolution** — SRV records, TSDNS, and direct address support
- **Middleware** — Pluggable command and event middleware chains
- **Built-in rate limiter** — Token-bucket throttling to prevent server-side flood kicks
- **Identity management** — Generate, import/export identities
- **Zero unsafe code** — Pure safe Rust, no `unsafe` blocks
- **Async-native** — Built on `tokio` with proper async cancellation and graceful shutdown

## Documentation

- [API Reference](docs/api.md)
- [Examples](docs/examples.md)

## Known Dead Code

The following items are unused but kept for API completeness
(they match re-exports in the JS reference and may be used by external consumers):

| Module | Item | Reason |
|--------|------|--------|
| `crypto` | `KeyNonce` (re-export) | Exported type, no internal consumer |
| `crypto` | `EAX`, `aes_cmac` (re-exports) | Exported for downstream use |
| `crypto` | `clamp_scalar`, `generate_temporary_key`, `get_shared_secret2`, `sign`, `verify_sign` (re-exports) | Primitives exported for downstream |
| `handshake` | `INIT_VERSION` (re-export) | Published constant |
| `handshake` | `LicenseChain`, `parse_licenses` (re-exports) | Published types/functions |
| `transport` | Packet function camelCase aliases (re-exports) | JS-compatible naming exports |
| `transport` | `GenerationWindow`, `Qlz`, `OnClose`, `OnPacket` (re-exports) | Published types |
| `crypto/crypt` | `KeyNonce::gen` | Stored but not read; same as JS |
| `handshake/license` | `not_valid_before`, `not_valid_after` | Parsed but not read after construction; same as JS |
| `transport/packet` | `parse_c2s_header` | Never called; same as JS |
| `transport/generation_window` | `generation()`, `sync_to()`, `is_future_packet()`, `reset()` | Unused convenience methods; same as JS |

Items marked `#[allow(dead_code)]` or `#[allow(unused_imports)]` in the source.

## Related

- **[teamspeak-js](https://github.com/honeybbq/teamspeak-js)** — The TypeScript reference implementation this library is ported from
- **[teamspeak-go](https://github.com/honeybbq/teamspeak-go)** — The original Go implementation

## Acknowledgments

Protocol knowledge was primarily informed by the [TSLib](https://github.com/Splamy/TS3AudioBot) implementation in [TS3AudioBot](https://github.com/Splamy/TS3AudioBot) by Splamy. Huge thanks to the TS3AudioBot project and its contributors.

## Disclaimer

TeamSpeak is a registered trademark of [TeamSpeak Systems GmbH](https://teamspeak.com/). This project is not affiliated with, endorsed by, or associated with TeamSpeak Systems GmbH in any way.

This library is a **clean-room implementation** developed from publicly available documentation, protocol analysis of network traffic, and independent research. No proprietary TeamSpeak SDK code, headers, or libraries were used in its creation.

## License

[MIT](LICENSE)
