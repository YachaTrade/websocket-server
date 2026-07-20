# giwa

## Purpose

giwa chain(Arbitrum 기반 L2) 배포용 websocket-server 브랜치.
v2 bonding curve + v1 dex(Uniswap V3) 조합으로 스트림을 재구성하고, Monad 전용 코드(monadLogs, CommitState, 블록 modulo Pyth 정렬)를 제거한다.
설계: `docs/superpowers/specs/2026-07-16-giwa-chain-design.md`
플랜: `docs/superpowers/plans/2026-07-16-giwa-chain.md`

## Changes

- [x] `722c5b5` chore: sqlx offline query cache 갱신 (샌드박스 빌드용)
- [x] `a32b7a4` v1 curve 스트림 제거 (`fetch_token_metadata`는 v2 curve로 이식, `V1_BONDING_CURVE` env 제거)
- [x] `9183d66` v2 pair 스트림 제거 (graduate 대상이 v1 dex이므로)
- [x] `1f5bb1b` EventType `{Curve, Dex}` 축소 + 스트림 메트릭 통합
- [x] `3638d48` MarketType wire format `CURVE`/`DEX` 축소 (serde 거부 테스트 포함)
- [x] `2c98b34` monadLogs → 표준 eth_subscribe("logs") 전환, `types/monad.rs` 삭제
- [x] `299b6a2` Pyth 정렬: 블록 modulo → timestamp 10s 버킷 + 3s lag (`align_pyth_ts` 테스트 포함)

검증: 잔재 스윕 grep 0건, `cargo build --all-targets` 성공, `cargo test` 46 passed / 0 failed.

## 리뷰 이력

- 2026-07-17 코드리뷰 서브에이전트 (Opus, fresh context, `v2...giwa` 전체 diff): **PASS** (P1 없음)
  - [P2] 표준 logs 구독 전환 후 reorg `removed=true` 로그 미필터 → **수정** (`0de9822`, 양쪽 스트림에 가드 추가)
  - [P3] `get_stream` doc comment "finalized 블록만" 부정확 → **수정** (`0de9822`)
  - [P3] cache 레이어 변경(fee_info 조건, graduate 전환) 무테스트 → **수용**: 리뷰어가 코드 리딩으로 정합 확인 (fee miss 30s TTL 캐시로 재조회 폭주 없음, graduate 전환 멱등). CacheManager 단위 테스트는 인프라 목킹 규모 대비 실익 낮아 보류.
  - [P3] `V2_CURVE`/`V2_DEX` wire 값 하드 거부 → **수용**: 의도된 변경. 배포 제약으로 기록 — 레거시 V2_* 행이 남은 DB/Redis에 이 바이너리를 연결하면 decode 에러. giwa는 fresh DB 전제.
  - Checked & clear: fee_info 조건 완화, graduate 멱등성, is_graduated 판정, align_pyth_ts 수학/경계, fetch_token_metadata 이식(byte-identical), 메트릭 배선, graduate→dex 핸드오프.
- 판정자(Advisor) 검증: 수정 후 `cargo build` + 전체 `cargo test` 46 passed / 0 failed 재확인.

## Outcome

(PR/머지 시 작성)

배포 전 체크리스트 (코드 밖):
- giwa RPC 3개 endpoint + WS `eth_subscribe("logs")` 지원 확인
- observer giwa 브랜치: market_type `CURVE`/`DEX`, Pyth LAG 3s / BUCKET 10s 일치 확인
- env: `.env.example` 의 필수 블록을 그대로 채울 것. 특히 `BONDING_CURVE`, `WETH`(giwa wrapped
  native), `MAIN_RPC_URL`/`SUB_RPC_URL_1`/`SUB_RPC_URL_2`, quote_token 테이블의 Pyth feed ID.
  컨테이너 배포 시 `IP=0.0.0.0` 필수 (기본값은 127.0.0.1).
  env 키의 v1/v2 접두사는 `refactor/drop-v1-v2-naming` 에서 제거됐다.
