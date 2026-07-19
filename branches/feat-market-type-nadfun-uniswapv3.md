# feat/market-type-nadfun-uniswapv3

## Purpose

observer가 DB `market_type` 저장 값을 `CURVE`→`NADFUN`, `DEX`→`UNISWAPV3`로 바꿈에 따라 wire/DB 계약을 일치시킨다.

## Changes

- `MarketType` enum에 명시적 serde/sqlx rename (`NADFUN`/`UNISWAPV3`) — variant 이름(Curve/Dex)은 유지
- wire 계약 테스트 갱신: 직렬화/역직렬화 = 신규 값, legacy `CURVE`/`DEX`/`V2_*` wire 값 거부
- cache SQL predicate 2곳 갱신 (`= 'UNISWAPV3'`, `IN ('UNISWAPV3','V2_DEX')`)
- 검증: cargo build + `--lib` 47 passed. 실행: Codex(gpt-5.6-sol medium), 검증·커밋: orchestrator

## Outcome

- (머지 시 작성)
