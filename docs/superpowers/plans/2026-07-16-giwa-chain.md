# giwa chain websocket-server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** giwa chain(Arbitrum 기반 L2) 배포용으로 websocket-server를 v2 bonding curve + v1 dex(Uniswap V3) 조합으로 재구성한다.

**Architecture:** 기존 4개 스트림(v1 curve, v1 dex, v2 curve, v2 pair) 중 v2 curve와 v1 dex만 남긴다. Monad 전용 구독(`monadLogs`/`CommitState`)을 표준 `eth_subscribe("logs")`로 전환하고, Pyth 질의 정렬을 블록번호 modulo에서 timestamp 버킷팅으로 바꾼다. wire format의 market_type은 `CURVE`/`DEX` 2종으로 축소한다. 디렉토리 구조는 rename하지 않는다 (접근 A — 수술적 수정).

**Tech Stack:** Rust, tokio, alloy, sqlx(PostgreSQL), redis, Pyth Hermes API

스펙: `docs/superpowers/specs/2026-07-16-giwa-chain-design.md`

## Global Constraints

- 브랜치: `giwa` (base `v2`). 커밋 메시지는 English.
- `*.md`는 이 repo `.gitignore`에 걸려 있음 — 플랜/스펙 문서는 커밋 불가(로컬 유지). `branches/giwa.md`도 마찬가지.
- 디렉토리/모듈 경로는 rename 금지 (`stream/v2/curve`, `stream/v1/dex` 유지).
- env 변수명은 rename 금지 (`V2_BONDING_CURVE`, `WMON`, `V1_DEX_FACTORY` 유지). `V1_BONDING_CURVE` 요구만 제거.
- `TokenVersion` enum(V1/V2)은 유지한다 (DB `token.version` 컬럼 대응).
- ABI 파일(`abi/v1/BondingCurve.json` 등)은 삭제하지 않는다 (미사용이어도 무해).
- 각 태스크 완료 시: `cargo build` 성공 + 해당 스코프 `cargo test` 통과 + 커밋.
- 검증 명령의 기대 출력이 다르면 멈추고 원인 파악 (fail loud).

---

### Task 1: v1 curve 모듈 제거 + `fetch_token_metadata` 이동

v1 bonding curve는 giwa에 없다. 모듈을 삭제하되, v2 curve가 재사용하던 `fetch_token_metadata`/`fetch_metadata`를 먼저 v2로 옮긴다.

**Files:**
- Modify: `src/stream/v2/curve/stream.rs` (import + 함수 3개 이식)
- Modify: `src/stream/v1/mod.rs`
- Modify: `src/main.rs`
- Modify: `src/config.rs`
- Modify: `src/db/cache/mod.rs:900`
- Modify: `.env.example`
- Delete: `src/stream/v1/curve/` (디렉토리 전체)

**Interfaces:**
- Produces: `crate::stream::v2::curve::stream::fetch_token_metadata(token_uri: &str) -> Result<TokenMetadata>` (v2 curve 내부에서만 사용)

- [ ] **Step 1: v2/curve/stream.rs로 메타데이터 함수 이식**

`src/stream/v1/curve/stream.rs`에서 아래 3개 항목을 **verbatim으로** 잘라내 `src/stream/v2/curve/stream.rs` 파일 끝(`#[cfg(test)] mod tests` 바로 앞)에 붙여넣는다:
- `const REQUEST_TIMEOUT_SECS: u64 = 10;` (line 736)
- `pub async fn fetch_token_metadata(token_uri: &str) -> Result<TokenMetadata>` (lines 738~811)
- `async fn fetch_metadata(client: &reqwest::Client, url: &str) -> Result<TokenMetadata>` (lines 813~897)

`src/stream/v2/curve/stream.rs` import 수정 3곳:

```rust
// OLD
use std::{sync::Arc, time::Duration};
// NEW (fetch_metadata의 err.source() 용)
use std::{error::Error, sync::Arc, time::Duration};
```

```rust
// OLD
use anyhow::Result;
// NEW (fetch_metadata의 .context() 용)
use anyhow::{Context, Result};
```

```rust
// OLD
    types::stream::{
        Buy, CreateCurve, CurveChartUpdate, CurveEventType, CurveSync, EventType, Graduate, Sell,
    },
// NEW
    types::stream::{
        Buy, CreateCurve, CurveChartUpdate, CurveEventType, CurveSync, EventType, Graduate, Sell,
        TokenMetadata,
    },
```

그리고 기존 재사용 import 2줄 삭제:

```rust
// DELETE
// V1에서 fetch_token_metadata 재사용
use crate::stream::v1::curve::stream::fetch_token_metadata;
```

- [ ] **Step 2: v1 curve 모듈 삭제**

```bash
git rm -r src/stream/v1/curve
```

`src/stream/v1/mod.rs` 전체를 다음으로 교체:

```rust
pub mod dex;
```

- [ ] **Step 3: main.rs에서 v1 curve spawn 제거**

`src/main.rs:10` import 수정:

```rust
// OLD
    stream::{v1::curve, v1::dex, v2::curve as v2_curve, v2::pair as v2_pair, handler::run_event_handler, price},
// NEW
    stream::{v1::dex, v2::curve as v2_curve, v2::pair as v2_pair, handler::run_event_handler, price},
```

spawn 블록에서 삭제 (main.rs:71-73):

```rust
// DELETE
    set.spawn(run_event_handler::<curve::CurveEventHandler>(
        EventType::Curve,
    ));
```

- [ ] **Step 4: config.rs에서 V1_BONDING_CURVE 제거**

`src/config.rs:48-54`:

```rust
// OLD
lazy_static! {
    pub static ref V1_DEX_FACTORY_ADDRESS: String =
        env::var("V1_DEX_FACTORY").expect("V1_DEX_FACTORY must be set");
    pub static ref V1_BONDING_CURVE_ADDRESS: String =
        env::var("V1_BONDING_CURVE").expect("V1_BONDING_CURVE must be set");
    pub static ref WMON_ADDRESS: String = env::var("WMON").expect("WMON must be set");
}
// NEW
lazy_static! {
    pub static ref V1_DEX_FACTORY_ADDRESS: String =
        env::var("V1_DEX_FACTORY").expect("V1_DEX_FACTORY must be set");
    pub static ref WMON_ADDRESS: String = env::var("WMON").expect("WMON must be set");
}
```

`src/db/cache/mod.rs:898-902` (빈 market_id fallback — giwa에서 커브 마켓은 v2 bonding curve 주소):

```rust
// OLD
                    let market_id = if row.market_id.is_empty() {
                        crate::config::V1_BONDING_CURVE_ADDRESS.clone()
                    } else {
                        row.market_id
                    };
// NEW
                    let market_id = if row.market_id.is_empty() {
                        crate::config::V2_BONDING_CURVE_ADDRESS.clone()
                    } else {
                        row.market_id
                    };
```

`.env.example:13` 수정:

```bash
# OLD
V1_BONDING_CURVE=0x67aD6EA566BA6B0fC52e97Bc25CE46120fdAc04c
# NEW (V2_BONDING_CURVE 항목이 없으므로 교체)
V2_BONDING_CURVE=
```

- [ ] **Step 5: 빌드 및 테스트**

Run: `cargo build 2>&1 | tail -5`
Expected: `Finished` (에러 0)

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 전부 pass (v1 curve 테스트는 모듈과 함께 삭제됨)

- [ ] **Step 6: Commit**

```bash
git add -A src .env.example
git commit -m "refactor: remove v1 curve stream for giwa (v2 bonding curve only)"
```

---

### Task 2: v2 pair 모듈 제거

giwa에서 graduate 대상은 v1 dex(Uniswap V3)이므로 NadFunPair 스트림은 불필요.

**Files:**
- Delete: `src/stream/v2/pair/` (디렉토리 전체)
- Modify: `src/stream/v2/mod.rs`
- Modify: `src/main.rs`
- Modify: `src/metrics/stream_metrics.rs`

- [ ] **Step 1: 모듈 삭제**

```bash
git rm -r src/stream/v2/pair
```

`src/stream/v2/mod.rs` 전체를 다음으로 교체:

```rust
pub mod curve;
```

- [ ] **Step 2: main.rs에서 v2 pair spawn 제거**

import 수정:

```rust
// OLD
    stream::{v1::dex, v2::curve as v2_curve, v2::pair as v2_pair, handler::run_event_handler, price},
// NEW
    stream::{v1::dex, v2::curve as v2_curve, handler::run_event_handler, price},
```

spawn 블록에서 삭제:

```rust
// DELETE
    set.spawn(run_event_handler::<v2_pair::V2PairEventHandler>(
        EventType::V2Pair,
    ));
```

- [ ] **Step 3: v2 pair 메트릭 제거**

`src/metrics/stream_metrics.rs`에서 삭제:
- 필드 `last_v2_pair_event_time`, `v2_pair_events_total` (선언 + `new()` 초기화 2줄)
- 메서드 `record_v2_pair_event()`, `is_v2_pair_stream_healthy()`, `get_v2_pair_values()`

(호출자 없음 — `record_v2_pair_event`는 방금 삭제한 pair 스트림에서만 호출됐고, getter는 미사용이었음)

- [ ] **Step 4: 빌드 및 테스트**

Run: `cargo build 2>&1 | tail -5` → `Finished`
Run: `cargo test --lib 2>&1 | tail -5` → 전부 pass

- [ ] **Step 5: Commit**

```bash
git add -A src
git commit -m "refactor: remove v2 pair stream for giwa (graduate targets v1 dex)"
```

---

### Task 3: EventType 축소 + v2 curve를 Curve로 재배선 + 메트릭 통합

남은 스트림은 2개: curve(= v2 bonding curve 코드)와 dex. EventType을 2개로 줄이고 v2 curve 스트림이 curve 메트릭을 쓰게 한다.

**Files:**
- Modify: `src/types/stream.rs:9-30`
- Modify: `src/main.rs`
- Modify: `src/stream/v2/curve/stream.rs:202`
- Modify: `src/metrics/stream_metrics.rs`

**Interfaces:**
- Produces: `EventType { Curve, Dex }` — `as_str()` → `"curve"` / `"dex"`, `all() -> [EventType; 2]`

- [ ] **Step 1: EventType 축소**

`src/types/stream.rs:9-30`:

```rust
// OLD
pub enum EventType {
    Curve,
    Dex,
    V2Curve,
    V2Pair,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Curve => "curve",
            EventType::Dex => "dex",
            EventType::V2Curve => "v2_curve",
            EventType::V2Pair => "v2_pair",
        }
    }

    pub fn all() -> [EventType; 4] {
        [EventType::Curve, EventType::Dex, EventType::V2Curve, EventType::V2Pair]
    }
}
// NEW
pub enum EventType {
    Curve,
    Dex,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Curve => "curve",
            EventType::Dex => "dex",
        }
    }

    pub fn all() -> [EventType; 2] {
        [EventType::Curve, EventType::Dex]
    }
}
```

- [ ] **Step 2: main.rs 재배선**

```rust
// OLD
    set.spawn(run_event_handler::<v2_curve::V2CurveEventHandler>(
        EventType::V2Curve,
    ));
// NEW
    set.spawn(run_event_handler::<v2_curve::V2CurveEventHandler>(
        EventType::Curve,
    ));
```

최종 spawn 구성 확인 (metrics/health check spawn 2개는 기존 그대로 유지):

```rust
    let mut set = JoinSet::new();
    set.spawn(run_event_handler::<v2_curve::V2CurveEventHandler>(
        EventType::Curve,
    ));
    set.spawn(run_event_handler::<dex::DexEventHandler>(EventType::Dex));
    set.spawn(server::main());
    set.spawn(event::main());

    set.spawn(metrics::run_metrics_logging());
    set.spawn(client::RpcClient::start_health_check_loop());
```

- [ ] **Step 3: v2 curve 스트림이 curve 메트릭 기록**

`src/stream/v2/curve/stream.rs:202`:

```rust
// OLD
                    crate::metrics::METRICS.stream.record_v2_curve_event();
// NEW
                    crate::metrics::METRICS.stream.record_curve_event();
```

- [ ] **Step 4: v2_curve 메트릭 제거**

`src/metrics/stream_metrics.rs`에서 삭제:
- 필드 `last_v2_curve_event_time`, `v2_curve_events_total` (선언 + `new()` 초기화 2줄)
- 메서드 `record_v2_curve_event()`, `is_v2_curve_stream_healthy()`, `get_v2_curve_values()`

주석 정리: `/// 마지막 V1 curve 이벤트 수신 시간` → `/// 마지막 curve 이벤트 수신 시간` (dex도 동일하게 `V1` 표기 제거). `record_curve_event`/`record_dex_event`의 doc comment도 같은 방식.

- [ ] **Step 5: 빌드 및 테스트**

Run: `cargo build 2>&1 | tail -5` → `Finished`
Run: `cargo test --lib 2>&1 | tail -5` → 전부 pass

- [ ] **Step 6: Commit**

```bash
git add -A src
git commit -m "refactor: reduce EventType to Curve/Dex and unify stream metrics"
```

---

### Task 4: MarketType 축소 — wire format `CURVE`/`DEX`

observer giwa 브랜치와 맞추는 핵심 변경. `V2_CURVE`/`V2_DEX` 값을 제거한다.

**Files:**
- Modify: `src/types/mod.rs:51-65` (+ 파일 끝에 테스트 추가)
- Modify: `src/stream/v2/curve/stream.rs` (5곳: 427 주석+438, 587, 648, 1054, 1178)
- Modify: `src/db/cache/mod.rs` (747-750, 784-787, 1064-1069, 1753-1758, 2128-2158)

**Interfaces:**
- Produces: `MarketType { Curve, Dex }` — serde/sqlx wire 값 `"CURVE"` / `"DEX"`. graduate 전환 규칙은 `* → Dex` 단일.

- [ ] **Step 1: 실패하는 테스트 작성**

`src/types/mod.rs` 파일 끝에 추가:

```rust
#[cfg(test)]
mod market_type_tests {
    use super::MarketType;

    // giwa: market_type wire 값은 CURVE/DEX 두 개뿐 (V2 prefix 제거).
    // observer giwa 브랜치가 DB market.market_type에 쓰는 값과 일치해야 한다.
    #[test]
    fn market_type_serializes_to_curve_and_dex_only() {
        assert_eq!(
            serde_json::to_string(&MarketType::Curve).unwrap(),
            "\"CURVE\""
        );
        assert_eq!(serde_json::to_string(&MarketType::Dex).unwrap(), "\"DEX\"");
    }

    #[test]
    fn market_type_rejects_v2_wire_values() {
        assert_eq!(
            serde_json::from_str::<MarketType>("\"CURVE\"").unwrap(),
            MarketType::Curve
        );
        assert_eq!(
            serde_json::from_str::<MarketType>("\"DEX\"").unwrap(),
            MarketType::Dex
        );
        // V2 값은 giwa에서 더 이상 유효한 wire 값이 아니다
        assert!(serde_json::from_str::<MarketType>("\"V2_CURVE\"").is_err());
        assert!(serde_json::from_str::<MarketType>("\"V2_DEX\"").is_err());
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `cargo test --lib market_type -- --nocapture 2>&1 | tail -5`
Expected: FAIL — `market_type_rejects_v2_wire_values` (현재는 `"V2_CURVE"` 역직렬화가 성공하므로)

- [ ] **Step 3: enum 축소**

`src/types/mod.rs:51-65`:

```rust
// OLD
pub enum MarketType {
    #[default]
    Curve, // "CURVE"
    Dex,   // "DEX"
    #[serde(rename = "V2_CURVE")]
    #[sqlx(rename = "V2_CURVE")]
    V2Curve, // "V2_CURVE"
    #[serde(rename = "V2_DEX")]
    #[sqlx(rename = "V2_DEX")]
    V2Dex, // "V2_DEX"
}
// NEW
pub enum MarketType {
    #[default]
    Curve, // "CURVE"
    Dex,   // "DEX"
}
```

- [ ] **Step 4: v2 curve 스트림의 스탬프 교체 (5곳)**

`src/stream/v2/curve/stream.rs` — `MarketType::V2Curve`를 전부 `MarketType::Curve`로 교체 (438 MarketInfo, 587 Buy, 648 Sell, 1054/1178 테스트 픽스처). 427 주석도 갱신:

```rust
// OLD (line 427)
                // V2 Curve: MarketType::V2Curve, market_id = V2_BONDING_CURVE_ADDRESS
// NEW
                // Curve: MarketType::Curve, market_id = V2_BONDING_CURVE_ADDRESS
```

교체 후 확인: `grep -n "MarketType::V2Curve" src/stream/v2/curve/stream.rs` → 0건

- [ ] **Step 5: cache manager의 분기 정리 (5곳)**

`src/db/cache/mod.rs:743-750` — fee_info 재조회 조건 (giwa에선 모든 토큰이 v2 커브 출신이라 market_type 분기 불필요):

```rust
// OLD
            let fee_info = if matches!(
                local_data.market_type,
                MarketType::V2Curve | MarketType::V2Dex
            ) && local_data.fee_info.is_none()
            {
// NEW
            let fee_info = if local_data.fee_info.is_none() {
```

`src/db/cache/mod.rs:784-787` — 주석만 갱신:

```rust
// OLD
                // V2 정보 보존을 위해 저장된 market_type을 그대로 사용
                // (이전엔 is_graduated만 보고 Curve/Dex로 derive 했으나
                //  V2Curve/V2Dex 정보가 손실되는 버그였음)
// NEW
                // 저장된 market_type을 그대로 사용
```

`src/db/cache/mod.rs:1064-1069`:

```rust
// OLD
            // V1 Dex와 V2 Dex 둘 다 graduated로 인식
            // (이전엔 V2Dex가 matches!에서 빠져 graduated 안 한 것으로 잘못 박힘)
            is_graduated: matches!(
                market_info.market_type,
                crate::types::MarketType::Dex | crate::types::MarketType::V2Dex
            ),
// NEW
            is_graduated: matches!(market_info.market_type, crate::types::MarketType::Dex),
```

`src/db/cache/mod.rs:1753-1758` — CreateCurve 시 market_type (fee_info용 `is_v2` 분기는 유지):

```rust
// OLD
        // CreateCurve 시점엔 항상 graduated 전이므로 V1=Curve, V2=V2Curve
        let market_type = if is_v2 {
            crate::types::MarketType::V2Curve
        } else {
            crate::types::MarketType::Curve
        };
// NEW
        // CreateCurve 시점엔 항상 graduated 전이므로 Curve
        let market_type = crate::types::MarketType::Curve;
```

`src/db/cache/mod.rs:2128-2158` — graduate 시 fee 재조회 조건과 전환 규칙:

```rust
// OLD
        let refreshed_fee_info = if matches!(current_market_type, Some(MarketType::V2Curve)) {
            self.get_fee_info(&graduate.token).await
        } else {
            None
        };
// NEW
        let refreshed_fee_info = if matches!(current_market_type, Some(MarketType::Curve)) {
            self.get_fee_info(&graduate.token).await
        } else {
            None
        };
```

```rust
// OLD
        // LocalStore의 market 데이터 업데이트:
        // - is_graduated = true, market_id = pool
        // - market_type: Curve → Dex, V2Curve → V2Dex (graduated 후 socket 응답이 여전히
        //   Curve/V2Curve로 내려가던 버그 수정)
        // - fee_info: V2면 재조회 결과로 갱신 (None이었던 캐시를 채워줌)
        self.local_store.update_market(&graduate.token, |data| {
            data.is_graduated = true;
            data.market_id = graduate.pool.clone();
            data.market_type = match data.market_type {
                MarketType::Curve | MarketType::Dex => MarketType::Dex,
                MarketType::V2Curve | MarketType::V2Dex => MarketType::V2Dex,
            };
            if matches!(data.market_type, MarketType::V2Dex) {
                if let Some(fee_info) = refreshed_fee_info.clone() {
                    data.fee_info = Some(fee_info);
                }
            }
        });
// NEW
        // LocalStore의 market 데이터 업데이트:
        // - is_graduated = true, market_id = pool
        // - market_type: Curve → Dex (graduated 후 socket 응답이 여전히
        //   Curve로 내려가던 버그 수정)
        // - fee_info: 재조회 결과로 갱신 (None이었던 캐시를 채워줌)
        self.local_store.update_market(&graduate.token, |data| {
            data.is_graduated = true;
            data.market_id = graduate.pool.clone();
            data.market_type = MarketType::Dex;
            if let Some(fee_info) = refreshed_fee_info.clone() {
                data.fee_info = Some(fee_info);
            }
        });
```

기타 주석 2곳 (2130, 2143-2144 근처의 `V2Curve` 언급)도 `Curve`로 갱신.

- [ ] **Step 6: 전체 확인 및 테스트**

Run: `grep -rn "MarketType::V2\|V2Curve\|V2Dex" src --include="*.rs"`
Expected: 0건

Run: `cargo test --lib 2>&1 | tail -5`
Expected: 전부 pass (Step 1 테스트 포함)

- [ ] **Step 7: Commit**

```bash
git add -A src
git commit -m "feat: reduce MarketType wire format to CURVE/DEX for giwa"
```

---

### Task 5: monadLogs → 표준 eth_subscribe("logs")

giwa(Arbitrum 계열)에는 `monadLogs`가 없다. 두 스트림 모두 기존 `RpcClient::get_stream()`(표준 logs 구독)으로 전환하고 Monad 타입을 삭제한다.

**Files:**
- Modify: `src/stream/v2/curve/stream.rs` (import + 스트림 루프)
- Modify: `src/stream/v1/dex/stream.rs` (import + 스트림 루프)
- Modify: `src/client/mod.rs` (get_monad_stream 삭제)
- Modify: `src/types/mod.rs` (`pub mod monad;` 제거)
- Delete: `src/types/monad.rs`

**Interfaces:**
- Consumes: `RpcClient::get_stream(&Filter) -> Result<SubscriptionStream<Log>>` (기존 함수, `client/mod.rs:1092-1096`)

- [ ] **Step 1: v2 curve 스트림 전환**

`src/stream/v2/curve/stream.rs` import:

```rust
// OLD
use crate::{
    error_log,
    types::{monad::CommitState, MarketInfo, MarketType, TokenInfo},
};
// NEW
use crate::{
    error_log,
    types::{MarketInfo, MarketType, TokenInfo},
};
```

스트림 생성 (line 165-177 부근):

```rust
// OLD
        // 새로운 스트림 생성 시도 (monadLogs 사용)
        let mut stream = match client.get_monad_stream(&filter).await {
// NEW
        // 새로운 스트림 생성 시도 (표준 eth_subscribe("logs"))
        let mut stream = match client.get_stream(&filter).await {
```

```rust
// OLD
        info!("V2 Curve monad stream started successfully");
// NEW
        info!("V2 Curve log stream started successfully");
```

이벤트 수신 (line 194-205 부근) — commit_state 필터와 변환 제거:

```rust
// OLD
                Ok(Some(monad_log)) => {
                    // Proposed 상태만 처리 (가장 빠른 응답, 중복 방지)
                    if monad_log.commit_state != Some(CommitState::Proposed) {
                        continue;
                    }

                    // V2 curve 스트림에서 로그를 받았으므로 스트림이 살아있음을 기록
                    crate::metrics::METRICS.stream.record_curve_event();

                    let log = monad_log.into_log();
// NEW
                Ok(Some(log)) => {
                    // V2 curve 스트림에서 로그를 받았으므로 스트림이 살아있음을 기록
                    crate::metrics::METRICS.stream.record_curve_event();
```

- [ ] **Step 2: dex 스트림 전환**

`src/stream/v1/dex/stream.rs` — 동일 패턴:

import에서 `monad::CommitState` 제거:

```rust
// OLD
    types::{
        monad::CommitState,
        stream::{DexBurn, DexMint},
        MarketType,
    },
// NEW
    types::{
        stream::{DexBurn, DexMint},
        MarketType,
    },
```

```rust
// OLD
        // 새로운 스트림 생성 시도 (monadLogs 사용)
        let mut stream = match client.get_monad_stream(&filter).await {
// NEW
        // 새로운 스트림 생성 시도 (표준 eth_subscribe("logs"))
        let mut stream = match client.get_stream(&filter).await {
```

```rust
// OLD
        info!("DEX monad stream started successfully");
// NEW
        info!("DEX log stream started successfully");
```

```rust
// OLD
                Ok(Some(monad_log)) => {
                    // Proposed 상태만 처리 (가장 빠른 응답, 중복 방지)
                    if monad_log.commit_state != Some(CommitState::Proposed) {
                        continue;
                    }

                    // 스트림에서 로그를 받았으므로 DEX 스트림이 살아있음을 기록
                    crate::metrics::METRICS.stream.record_dex_event();

                    let log = monad_log.into_log();
// NEW
                Ok(Some(log)) => {
                    // 스트림에서 로그를 받았으므로 DEX 스트림이 살아있음을 기록
                    crate::metrics::METRICS.stream.record_dex_event();
```

- [ ] **Step 3: client와 타입 삭제**

`src/client/mod.rs`:
- `use crate::types::monad::MonadLog;` (line 17) 삭제
- `get_monad_stream` 함수 전체 삭제 (lines 1077-1089, doc comment 포함):

```rust
// DELETE
    /// Monad logs 구독 (제안 상태에서 ~1초 빠른 응답)
    /// 표준 eth_subscribe("logs") 대신 eth_subscribe("monadLogs") 사용
    /// MonadLog 타입으로 반환하여 commitState 확인 가능
    pub async fn get_monad_stream(&self, filter: &Filter) -> Result<SubscriptionStream<MonadLog>> {
        ...
    }
```

`src/types/mod.rs:4`의 `pub mod monad;` 삭제 후:

```bash
git rm src/types/monad.rs
```

- [ ] **Step 4: 빌드 및 테스트**

Run: `grep -rn "monad\|Monad\|CommitState" src --include="*.rs"`
Expected: 0건

Run: `cargo build 2>&1 | tail -5` → `Finished`
Run: `cargo test --lib 2>&1 | tail -5` → 전부 pass

- [ ] **Step 5: Commit**

```bash
git add -A src
git commit -m "feat: switch log subscription from monadLogs to standard eth_subscribe"
```

---

### Task 6: Pyth 질의 정렬 — 블록 modulo → timestamp 버킷팅

Arbitrum 계열은 블록 생성이 수요 기반이라 블록번호가 시간과 비례하지 않는다. timestamp를 직접 버킷팅한다. **observer giwa 브랜치와 상수 동일해야 함 (LAG 3s / BUCKET 10s).**

**Files:**
- Modify: `src/stream/price/mod.rs:106-135` (+ 파일 끝 테스트 추가)

**Interfaces:**
- Produces: `fn align_pyth_ts(ts: u64) -> u64` (private, `pyth_query_ts`에서 사용)

- [ ] **Step 1: 실패하는 테스트 작성**

`src/stream/price/mod.rs` 파일 끝에 추가:

```rust
#[cfg(test)]
mod pyth_ts_tests {
    use super::align_pyth_ts;

    // giwa(Arbitrum 계열)는 블록 생성이 수요 기반이라 블록번호 modulo 정렬이
    // 벽시계 시간과 비례하지 않는다. timestamp를 직접 버킷팅해야 observer와
    // 같은 Pyth 질의 timestamp를 공유한다 (LAG 3s, BUCKET 10s — observer와 동일).
    #[test]
    fn aligns_to_10s_bucket_after_3s_lag() {
        assert_eq!(align_pyth_ts(1_000), 990); // 997 → 990
        assert_eq!(align_pyth_ts(1_013), 1_010); // 1010 → 1010 (경계)
        assert_eq!(align_pyth_ts(1_012), 1_000); // 1009 → 1000
        assert_eq!(align_pyth_ts(2), 0); // underflow는 saturating
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `cargo test --lib pyth_ts 2>&1 | tail -5`
Expected: 컴파일 실패 — `align_pyth_ts` 미정의

- [ ] **Step 3: 구현**

`src/stream/price/mod.rs:106-135` — `BLOCK_LAG`, `BUCKET_BLOCK_INTERVAL` 상수(doc comment 포함)와 `pyth_query_ts`를 다음으로 교체:

```rust
/// Pyth 질의 timestamp lag (초).
///
/// Pyth `/v2/updates/price/{ts}`는 현재 초(또는 ~1초 전)를 질의하면 404를
/// 반환하는 경우가 있다 — publisher가 아직 해당 초를 커밋하지 못한 상태.
/// 3초 뒤로 물러나면 publish window가 안정된 구간에 들어간다.
const PYTH_TS_LAG_SECS: u64 = 3;

/// Timestamp 버킷 (초) — observer giwa 브랜치의 값과 반드시 동일해야 한다.
/// 두 서비스가 같은 논리 윈도우에 대해 같은 timestamp로 Pyth를 질의하도록
/// (ts - LAG)를 이 배수로 내림 정렬한다.
const PYTH_TS_BUCKET_SECS: u64 = 10;

/// (ts - LAG)를 BUCKET 배수로 내림 정렬.
///
/// 기존 블록번호 modulo 방식(Monad 0.4s 고정 블록 가정)을 대체 — giwa
/// (Arbitrum 계열)는 블록 생성이 수요 기반이라 블록번호가 시간과 비례하지 않음.
fn align_pyth_ts(ts: u64) -> u64 {
    let target = ts.saturating_sub(PYTH_TS_LAG_SECS);
    target - (target % PYTH_TS_BUCKET_SECS)
}

/// 체인 최신 블록의 timestamp를 [`align_pyth_ts`]로 정렬해 Pyth 질의에 사용.
async fn pyth_query_ts(client: &RpcClient) -> Result<u64> {
    let latest = client.get_latest_block_number().await?;
    let ts = client.get_block_timestamp(latest).await?;
    Ok(align_pyth_ts(ts))
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `cargo test --lib pyth_ts 2>&1 | tail -5`
Expected: PASS (4 assertions)

Run: `cargo test --lib 2>&1 | tail -5` → 전부 pass

- [ ] **Step 5: Commit**

```bash
git add -A src
git commit -m "feat: replace block-modulo Pyth alignment with 10s timestamp bucketing"
```

---

### Task 7: 최종 검증 및 마무리

- [ ] **Step 1: 잔재 스윕**

Run: `grep -rn "V2Curve\|V2Pair\|V2Dex\|monadLogs\|MonadLog\|CommitState\|V1_BONDING_CURVE" src --include="*.rs"`
Expected: 0건

(참고: `V2_BONDING_CURVE`(env명), `IV2BondingCurve`(ABI 바인딩), `TokenVersion::V2`, `v2_curve`(모듈 별칭)는 의도적으로 남는다)

- [ ] **Step 2: 전체 빌드/테스트**

Run: `cargo build --all-targets 2>&1 | tail -5` → `Finished` (stress-test bin 포함)
Run: `cargo test 2>&1 | tail -10` → 전부 pass, skipped 0

- [ ] **Step 3: branches/giwa.md 갱신 (로컬 문서, 커밋 안 됨)**

`branches/giwa.md`의 Changes 섹션 체크리스트를 완료 상태로 갱신하고 주요 커밋 해시 기록.

- [ ] **Step 4: 푸시 전 리뷰**

CLAUDE.md 규칙에 따라 PR 생성 전 `/codex review` 실행 → AUTO-FIX 즉시 반영, ASK 항목은 사용자 확인 → 리뷰 커밋 포함해서 push → PR 오픈 (base: `v2`인지 `main`인지 사용자에게 확인).

## 배포 전 체크리스트 (코드 밖, 이 플랜 범위 외)

- [ ] giwa RPC 3개 endpoint 확보 + WS `eth_subscribe("logs")` 지원 확인
- [ ] observer giwa 브랜치: market_type `CURVE`/`DEX` 기록 확인, Pyth 정렬 LAG 3s/BUCKET 10s 일치 확인
- [ ] env: `V2_BONDING_CURVE`, `V1_DEX_FACTORY`, `WMON`(giwa wrapped native), quote_token 테이블의 Pyth feed ID
