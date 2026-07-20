# AGENTS.md

## Project

This Rust service reads GIWA market/event state from PostgreSQL, Redis, and chain providers and publishes realtime data through an Axum WebSocket server.

- Runtime: Rust 2021, Tokio, Axum, SQLx, Redis, Alloy, and Prometheus.
- Current GIWA terminology uses WETH for wrapped native value.
- Current market wire/database values are `NADFUN` and `UNISWAPV3`.

## Structure

- `src/main.rs` starts shared clients, stream tasks, metrics, and the WebSocket server.
- `src/server/` owns connections and wire-facing request/response behavior.
- `src/stream/` coordinates realtime publication; `src/event/` handles event shapes.
- `src/client/` contains provider clients and block update behavior.
- `src/db/` reads PostgreSQL/Redis state; tracked `.sqlx/` files support offline builds.
- `src/types/` defines public/internal payloads; `src/metrics/` provides observability.
- `abi/v1/` and `abi/v2/` contain contract interfaces used by the active streams.
- `bin/stress_test.rs` is a load generator; `scripts/ws-test/` contains manual WebSocket probes.

## Commands

```bash
cargo fmt --all -- --check
cargo test
SQLX_OFFLINE=true cargo build --release
cargo run --release
```

`cargo run --bin stress-test` generates load and is not a routine validation command; use it only against an explicitly approved target.

## Project-Specific Rules

- Preserve published payload field names, types, nullability, and subscription semantics; coordinate intentional wire changes with consumers.
- Keep `NADFUN`/`UNISWAPV3` and WETH terminology aligned with observer writes and API responses.
- Maintain canonical block, transaction, and log ordering when merging database, cache, and chain updates.
- Reconnects and duplicate upstream events must not emit corrupt state or advance internal progress incorrectly.
- Keep slow-client handling bounded so one connection cannot block broadcast progress or grow memory without limit.
- ABI changes must update the correct versioned ABI, decoder/event types, and downstream payload tests together.
- SQLx query changes require the matching schema contract and refreshed tracked offline metadata.
- Do not infer current behavior from old branch documents when source, types, and recent history differ.

## Security and Sensitive Operations

- Never expose provider, PostgreSQL, or Redis credentials through logs, metrics, close reasons, or WebSocket payloads.
- Treat subscription inputs and connection metadata as untrusted; keep parsing, size, and rate limits intact.
- Do not run deployment, debug, stress, or long-lived probe scripts against shared infrastructure without explicit authorization.
- Validate chain identity and contract addresses before enabling event-backed streams.

## Validation

- Run formatting, unit tests, and the SQLx offline release build.
- For payload changes, test serialization plus subscribe, update, reconnect, and disconnect behavior.
- For database/cache changes, validate against disposable PostgreSQL and Redis instances when available.
- Report any live WebSocket, provider failover, load, or integration checks that were not run.
