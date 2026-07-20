# refactor/drop-fee-info

## Purpose

`fee_info` 기능을 통째로 제거한다. giwa 제품에서 수수료 데이터를 더 이상 필요로 하지 않는다.
이는 의도된 **wire 계약 변경**이다 — 이 변경 후 클라이언트에게 나가는 모든 market payload 에서
`fee_info` 키가 사라진다. (기존엔 `Option<FeeInfo>` 라 `"fee_info": null` 또는 객체로 나갔다.)

부수 효과로, graduate 시 `market_type == Curve` 일 때만 fee_info 를 재조회하던 가드도 함께 사라진다.
그 가드는 원래 "v2 토큰일 때만"(fee 있는 종류) 판별이었는데 v1/v2 통합 후 의미가 뒤틀려 있었다.

## Changes

`FeeInfo` 는 수수료율 3개(`creator_fee_rate`, `curve_protocol_fee_rate`, `dex_protocol_fee_rate`,
전부 i16 basis points)를 담던 타입. 관련 배관 전부 제거:

- `types/mod.rs` — `struct FeeInfo` 삭제, `MarketInfo.fee_info` 필드 삭제
- `db/local_store.rs` — `TokenMarketData.fee_info` 필드 삭제
- `db/cache/mod.rs`
  - `fee_info_cache` DashMap 필드 + 생성자 초기화 삭제
  - `FEE_INFO_CACHE_TTL_HIT/MISS` 상수 삭제 (이 둘만 담던 impl 블록도 함께)
  - `get_fee_info()` 메서드 전체 삭제
  - market 조회 쿼리의 `LEFT JOIN fee_config` + `fc.* as fee_*` SELECT 컬럼 + MarketRow 의 fee 필드 3개 삭제
  - CreateCurve / get_market_info / graduate 경로의 fee_info 생성·set 지점 전부 삭제
  - unused 가 된 `use std::time::{Duration, Instant}` import 정리
- `stream/curve/stream.rs` — CreateCurve 시 `tokio::join!(get_quote_info, get_fee_info)` 를
  quote 단일 조회로 단순화, MarketInfo 빌드에서 `fee_info` 필드 제거

diffstat: 4 files, +3 / -173.

## 검증 (Advisor 직접)

순수 삭제라 리뷰 서브에이전트 대신 Advisor 가 직접 게이트:
- grep-zero: `fee_info|FeeInfo|fee_config|get_fee_info|FEE_INFO_CACHE|*_fee_rate` over `src/ bin/` → 0건
- market SQL: `fee_config` JOIN 제거 후 마지막 SELECT 컬럼(`quote_image_uri`)에 trailing comma 없음,
  dangling `fc.` 참조 없음 — 직접 확인
- `cargo build --all-targets` 경고 0 (struct literal 완전성은 컴파일러가 보장)
- `cargo test` 47 passed / 0 failed
- remote/PR 단계에서 정식 코드리뷰 서브에이전트 게이트 적용 예정

## 주의 — 배포 전 확인

wire 변경이다. giwa 프론트/모바일이 market payload 의 `fee_info` 키를 읽고 있으면 깨진다.
observer/frontend 와 이 제거를 맞춰야 한다 (market_type 축소 때와 동일한 종류의 조율).

## Outcome

(PR/머지 시 작성)
