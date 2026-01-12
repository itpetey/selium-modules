# Repository Guidelines

## Project Structure & Module Organization
This repo contains multiple Rust workspaces, each module living at the top level: `atlas/`, `switchboard/`, and `remote-client/`. Each workspace contains focused crates such as `client/`, `server/`, and `protocol/` (plus `core/` for switchboard and `cli/` for remote-client). Protocol schemas live in `*/protocol/schemas/*.fbs`, while generated FlatBuffers code is checked into `*/protocol/src/fbs/`. Integration tests and fixtures live under `tests/request-reply/`.

## Build, Test, and Development Commands
Run commands from the repo root and target a workspace with `--manifest-path`:
- `cargo check --manifest-path atlas/Cargo.toml --workspace --all-targets` (type-checks all targets in a module)
- `cargo fmt --manifest-path switchboard/Cargo.toml --all` (format check; CI requires rustfmt)
- `cargo clippy --manifest-path remote-client/Cargo.toml -- -D warnings` (lint; warnings are errors)
- `cargo test --manifest-path atlas/Cargo.toml --workspace --all-targets` (unit + integration tests per module)
- `cargo test --manifest-path tests/request-reply/Cargo.toml` (end-to-end test; requires `selium-runtime` in PATH and the `wasm32-unknown-unknown` target)

For WASM modules, build the server crate, e.g. `cargo build --release --target wasm32-unknown-unknown -p selium-switchboard-server`.

## Coding Style & Naming Conventions
Rust 2024 edition is used across workspaces; prefer default rustfmt output and keep clippy clean. Use standard Rust naming (`snake_case` for functions/modules, `CamelCase` for types). Workspace members follow `selium-<module>-<component>` naming. Avoid editing generated files under `*/protocol/src/fbs/`; update the `.fbs` schemas and regenerate via the module build scripts.

## Testing Guidelines
Unit tests live with each crate; the repository-level integration suite is in `tests/request-reply/tests/request_reply.rs`. If you change protocol schemas or wire formats, update both schema files and generated code and ensure the request-reply test still passes. The integration test expects local runtime tooling, so document any environment setup in your PR.

## Commit & Pull Request Guidelines
Recent commits use short, descriptive, imperative summaries (optionally with a scope). Keep commit messages concise and focused on one change set. For PRs, include a clear summary, rationale, and the exact `cargo` commands you ran; call out affected modules and any runtime requirements (e.g., wasm builds or `selium-runtime` setup).
