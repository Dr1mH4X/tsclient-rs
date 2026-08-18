# AGENTS.md

tsclient-rs 是 [teamspeak-js](https://github.com/honeybbq/teamspeak-js) 的 Rust 移植版（clean-room TeamSpeak 3/5/6 协议库，纯 safe Rust）。

## 参考实现（最高优先级上下文）

- 仓库根目录的 `teamspeak-js/`、`teamspeak-go/` 是 git submodule，指向上游 HoneyBBQ 的参考实现。clone 后先跑 `git submodule update --init`。子模块钉在 commit，需要最新代码时在子模块内 `git pull`（或 `git submodule update --remote`）。`src/` 与 `teamspeak-js/src` 目录结构一一对应（api、client、command、crypto、discovery、handshake、notifications、throttle、transfer、transport、types…）。
- 注意：子模块工作区有本地未提交改动（handshake 调试打印），不要执行 `git submodule update` / `git checkout -f` 之类的重置操作，会丢失这些改动。
- 移植代码必须与 JS 参考对齐：行为/命名有疑问先读 `teamspeak-js/src` 对应文件，再改 Rust。
- 公共 API 刻意保留 JS 兼容的 camelCase 别名（`sendTextMessage`、`clientMove` 等，见 `src/lib.rs` re-export），这是兼容导出，不是风格问题，不要改名或删除。

## 构建与验证（CI 顺序，见 `.github/workflows/publish.yml`）

```sh
cargo check --all-targets
cargo test
cargo clippy -- -D warnings   # 必须零警告，这是 CI 硬性门槛
```

- 测试全部是模块内 `#[cfg(test)]` 单测（18 个），无集成测试、不需要服务器/网络。跑单个：`cargo test <名称>`。
- `src/lib.rs` 顶部有 crate 级 clippy allow 列表（JS 移植遗留风格问题）：不新增条目，也不删除。
- 版本管理：Cargo.toml 固定 `0.0.0`，发版时由 CI 从 `v*` tag 用 sed 重写版本号再 `cargo publish`。不要手动 bump 版本号，发版走打 tag。默认分支是 `master`。

## 刻意保留的 dead code

README 的 "Known Dead Code" 表列出了与 JS 参考对齐、刻意保留的 dead code 与 re-export（源码带 `#[allow(dead_code)]` / `#[allow(unused_imports)]`）。这些不是垃圾代码，清理候选先查该表。

## 文档

- `docs/api.md`、`docs/examples.md` 是手工维护的公共 API 文档，公共 API 变更需同步更新。
- README 中的 showcase 图由个人仓库 [Dr1mH4X/Dr1mH4X](https://github.com/Dr1mH4X/Dr1mH4X) 的 `scripts/generate_used_by.py` 生成（该仓库 workflow 手动触发），产物在 `used_by/tsclient-rs/`，不要手改。

## 规范来源

完整编码规范（FAILFAST/YAGNI、中文注释、子代理分流、Conventional Commits 等）在全局 AGENTS.md 与 `.github/copilot-instructions.md`，冲突时按全局 AGENTS.md 执行。
