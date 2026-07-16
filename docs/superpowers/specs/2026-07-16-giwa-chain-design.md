# giwa chain websocket-server 설계

날짜: 2026-07-16
브랜치: `giwa` (base: `v2`)
접근: **A — 수술적 수정** (파일 구조 유지, 내용만 giwa에 맞게 변경)

## 배경

giwa chain(Arbitrum 기반 L2)에 websocket-server를 새로 배포한다.
giwa에 배포되는 컨트랙트 조합은 **v2 bonding curve + v1 dex(Uniswap V3)** 이다.
Monad 배포(v2 브랜치)와 달리 v1 bonding curve와 v2 pair(NadFunPair)는 존재하지 않는다.

## 결정 사항 (사용자 확인 완료)

| 항목 | 결정 |
|------|------|
| v1 curve / v2 pair 스트림 | 코드에서 제거 |
| 가격 오라클 | 기존 구조 유지 (Pyth + `quote_token` 테이블, env 설정만 변경) |
| Graduate 대상 | v1 dex (Uniswap V3 pool) |
| 토큰 검증 | 7777 suffix + whitelist 유지 |
| market_type wire format | v2 prefix 제거 — `CURVE` / `DEX` 2종만 사용 |
| giwa 블록 특성 | Arbitrum 기반: 고정 블록 타임 없음, 수요 기반 생성 (최소 간격 ~250ms) |

## 설계

### 1. 스트림 구성

- 유지: `stream/v2/curve` (bonding curve 담당), `stream/v1/dex` (Uniswap V3 담당).
  디렉토리 경로는 rename하지 않는다 (v2 브랜치 cherry-pick 여지 보존).
- 삭제: `stream/v1/curve`, `stream/v2/pair` 모듈 및 관련 테스트.
  - `fetch_token_metadata`는 `stream/v1/curve/stream.rs`에 있고 삭제 후 유일한
    호출자가 v2 curve이므로 `stream/v2/curve/stream.rs`로 이동한다.
- `main.rs`: v2 curve + v1 dex 2개 스트림만 spawn.
- `EventType`: `Curve`, `Dex` 2개로 축소 (`V2Curve`, `V2Pair` variant 제거).
  v2 curve 스트림 코드가 `EventType::Curve`를 사용한다.

### 2. market_type 정리

- `MarketType` enum: `{ Curve, Dex }` — wire/DB 값 `"CURVE"` / `"DEX"`.
  `V2Curve`(`V2_CURVE`), `V2Dex`(`V2_DEX`) variant 제거.
- v2 curve 스트림이 생성하는 모든 이벤트/MarketInfo는 `MarketType::Curve` 스탬프.
- Graduate 시 `update_cache_from_graduate`의 전환 규칙: `Curve → Dex` 단일 규칙만 남긴다.
- v1 dex 스트림은 기존대로 `MarketType::Dex` 스탬프 (변경 없음).
- fee_info 재조회 등 기존에 `V2Curve` 조건이던 분기는 `Curve` 조건으로 변경
  (giwa의 curve는 v2 컨트랙트이므로 fee_config가 존재한다).
- ⚠️ observer giwa 브랜치도 동일하게 `CURVE`/`DEX`를 쓰도록 맞춰야 한다 (DB `market.market_type` 값 정합).

### 3. Graduate 흐름 (v2 curve → v1 dex)

- v2 curve의 `Graduate { token, pair }` 파싱 시 기존 로직 유지:
  `insert_pool_pair(pool, token0, token1)` + `insert_white_list_pool(pool)`.
- v1 dex 스트림의 `check_pool_and_get_pair`가 같은 캐시 프리미티브를 읽으므로
  graduate된 pool의 Swap/Mint/Burn이 자동으로 whitelist를 통과한다 — 추가 배선 불필요.
- graduate 시 market_info 조회 재시도(5회 지수백오프) 로직 유지.

### 4. 체인 구독 방식 (Monad 전용 코드 제거)

- `eth_subscribe("monadLogs")` → 표준 `eth_subscribe("logs")` 전환:
  각 스트림에서 `client.get_monad_stream()` 호출을 기존 `client.get_stream()`으로 교체.
- `CommitState::Proposed` 필터링 제거 — 표준 logs는 로그당 1회 전달되므로 커밋 상태 분기 불필요.
- `types/monad.rs` (`MonadLog`, `CommitState`) 및 `get_monad_stream()` 삭제.
- ⚠️ 배포 전 확인: giwa RPC 제공자가 WebSocket `eth_subscribe("logs")`를 지원하는지.

### 5. 가격 (Pyth)

- Pyth provider / `quote_token` 테이블 / batch fetch 구조는 그대로 유지.
- `pyth_query_ts`만 변경: 블록 번호 modulo 정렬(`BLOCK_LAG=5`, `BUCKET_BLOCK_INTERVAL=25`,
  Monad 0.4s 가정)은 Arbitrum 계열의 수요 기반 블록 생성과 맞지 않는다.
  - 변경안: 최신 블록의 **timestamp**를 가져와 `(ts - LAG_SECS)`를 `BUCKET_SECS`로 내림.
  - 값: `LAG_SECS = 3`, `BUCKET_SECS = 10` (기존 ~10s 버킷 의미 유지).
- ⚠️ observer giwa 브랜치의 Pyth 질의 정렬도 동일 방식/동일 값으로 맞춰야 한다
  (두 서비스가 같은 논리 윈도우에 대해 같은 timestamp로 Pyth를 질의해야 함).

### 6. 설정 / env

- 제거: `V1_BONDING_CURVE` env 요구 (v1 curve 삭제에 따라).
- 유지: `V2_BONDING_CURVE`(giwa bonding curve 주소), `V1_DEX_FACTORY`, `WMON`(giwa wrapped native),
  `V2_GIFT_VAULT` / `V2_BURN_VAULT`(giwa에 없으면 미설정 = 빈 문자열 동작 유지).
- env 변수명은 rename하지 않는다 (배포 스크립트 churn 방지).
- native 토큰(ETH 등) 가격은 기존 구조대로 env/`quote_token` 데이터로 해결 — 코드 변경 없음.

### 7. 테스트 (TDD)

- 삭제 모듈의 테스트 제거, 나머지는 기존 테스트 유지.
- 신규/변경 로직에 대한 테스트 우선 작성:
  - `MarketType` 축소 후 serde/sqlx 라운드트립 (`CURVE`/`DEX`).
  - graduate 전환 규칙 `Curve → Dex`.
  - `pyth_query_ts` timestamp 버킷팅 (경계값: 정확히 10s 배수, lag 적용).
- 검증: `cargo build` + 변경 범위를 덮는 최소 `cargo test` 스코프.

## 범위 밖 (배포 시점에 별도 처리)

- giwa RPC endpoint / 컨트랙트 주소 / Pyth feed ID 등 실제 env 값.
- observer giwa 브랜치 작업 (별도 서비스).
- deploy/ 하위 ECS 설정의 giwa 리전 구성.

## 리스크

1. **observer 정합**: market_type 값과 Pyth 버킷 정렬은 observer와 어긋나면 안 됨 — 배포 전 크로스체크 필수.
2. **RPC 구독 지원**: giwa RPC의 WS logs 구독 미지원 시 폴링 fallback 설계가 추가로 필요.
3. **v1 dex 필터 범위**: dex 스트림은 주소 필터 없이 이벤트 시그니처로 체인 전체를 구독 —
   giwa 초기 트래픽에서는 문제없으나 체인 활성화 시 whitelist 필터 부하 모니터링 필요.
