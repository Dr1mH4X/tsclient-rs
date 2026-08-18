# AGENTS.md

tsclient-rs 是 [teamspeak-js](https://github.com/honeybbq/teamspeak-js) 的 Rust 移植版（clean-room TeamSpeak 3/5/6 协议库，纯 safe Rust）。

## 参考实现（最高优先级上下文）

- 参考实现从上游远程仓库拉取：
  - JS（主参考）：https://github.com/HoneyBBQ/teamspeak-js.git
  - Go（原始实现）：https://github.com/HoneyBBQ/teamspeak-go.git
- `src/` 与 JS 参考的 `src/` 目录结构一一对应（api、client、command、crypto、discovery、handshake、notifications、throttle、transfer、transport、types…）。
- 需要对照时临时 clone 到临时目录，用后清理。
- 移植代码必须与参考对齐：行为/命名有疑问先读参考对应文件，再改 Rust。
- 公共 API 刻意保留 JS 兼容的 camelCase 别名（`sendTextMessage`、`clientMove` 等，见 `src/lib.rs` re-export），这是兼容导出，不是风格问题，不要改名或删除。

## 构建与验证（CI 顺序，见 `.github/workflows/publish.yml`）

```sh
cargo check --all-targets
cargo test
cargo clippy -- -D warnings   # 必须零警告
```

- 测试全部是模块内 `#[cfg(test)]` 单测（18 个），无集成测试、不需要服务器/网络。跑单个：`cargo test <名称>`。
- `src/lib.rs` 顶部有 crate 级 clippy allow 列表（JS 移植遗留风格问题）：不新增条目，也不删除。
- 版本管理：Cargo.toml 固定 `0.0.0`，发版时由 CI 从 `v*` tag 用 sed 重写版本号再 `cargo publish`。不要手动 bump 版本号，发版走打 tag。默认分支是 `master`。

## 刻意保留的 dead code

README 的 "Known Dead Code" 表列出了与 JS 参考对齐、刻意保留的 dead code 与 re-export（源码带 `#[allow(dead_code)]` / `#[allow(unused_imports)]`）。这些不是垃圾代码，清理候选先查该表。

## 文档

- `docs/api.md`、`docs/examples.md` 是公共 API 文档，公共 API 变更需同步更新。

## 规范来源

完整编码规范在`.github/copilot-instructions.md`，冲突时按全局 AGENTS.md 执行。
