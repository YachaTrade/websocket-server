# refactor/drop-v1-v2-naming

## Purpose

코드에 남아 있던 v1/v2 네이밍을 전부 제거한다. 제품 레벨의 v1/v2 구분은 이미 사라졌고
(`MarketType`/`EventType` 은 Curve/Dex 2종, 토큰 `version` 필드 제거됨), 남은 건 Monad 시절
포크에서 따라온 이름뿐이라 읽는 사람을 혼동시키는 상태였다. 함께 안 쓰는 env 키도 정리한다.

## Changes

**디렉토리** — `git mv` 로 히스토리 유지
- `src/stream/v2/curve/` → `src/stream/curve/`
- `src/stream/v1/dex/` → `src/stream/dex/`
- `src/stream/{v1,v2}/mod.rs` 삭제, `src/stream/mod.rs` 가 `curve`/`dex` 를 직접 선언
- `abi/v2/BondingCurve.json` → `abi/BondingCurve.json`, `abi/v1/IUniswapV3Pool.json` → `abi/IUniswapV3Pool.json`
- 참조가 없던 Monad 시절 ABI 27개 삭제 (필요 시 `git checkout 116216d -- abi/` 로 복구)

**env 키**
- `V2_BONDING_CURVE` → `BONDING_CURVE`, `V2_GIFT_VAULT` → `GIFT_VAULT`, `V2_BURN_VAULT` → `BURN_VAULT`
- `V1_DEX_FACTORY_ADDRESS` lazy_static 삭제 — config.rs 에만 선언돼 있고 어디서도 안 읽으면서
  `V1_DEX_FACTORY` 를 필수 env 로 강제하고 있었다. dex 스트림은 주소 필터 없이 이벤트 시그니처로만
  구독하고 whitelist 는 `check_white_list_pool` 로 거르므로 factory 주소가 필요 없다.
- 코드가 안 읽는 키 18개를 `.env` 에서 제거 (Monad 시절 컨트랙트 주소 `V1_*` 16개,
  `DB_URL`(실제로는 `DATABASE_URL` 을 읽음), `CHANNEL_MONITOR_INTERVAL`)
- `.env.example` 을 코드의 `env::var` 호출 기준으로 재작성하고 `.gitignore` 에 `!.env.example`
  추가 — 그 전까지 `.env.*` 패턴에 걸려 저장소에 추적되지 않고 있었다.

**식별자**
- `IV2BondingCurve` → `IBondingCurve`, `V2CurveEventHandler` → `CurveEventHandler`
- `stream_v2_curve_events` → `stream_curve_events`, `receive_v2_curve_event` → `receive_curve_event`
- `handle_v2_*_event` 6종 → `handle_*_event`
- 인라인 테스트 `v2_curve_pair_*` → `curve_pair_*`

**주석/로그** — 존재하지 않는 v1/v2 구분을 설명하던 주석과 `"V2 Curve"` 로그 문자열 정리.
`market_type_rejects_v2_wire_values` 등 레거시 wire 값 거부를 검증하는 테스트 3개는 V2 언급이
의도된 것이라 그대로 뒀다.

**부수적으로 드러난 것**
- `RPC_TIME_OUT` 이 필수인데 `.env` 에 없어서 로컬 실행이 기동 시 panic 하는 상태였다 → 추가
- `RUST_LOG` 은 동작하지 않는다. `main.rs` 가 `EnvFilter` 없이 `with_max_level(Level::INFO)` 로
  하드코딩. `Dockerfile`/`scripts/deploy.sh`/`debug_block_updater.sh` 가 여전히 설정하고 있지만
  무시된다. 이 브랜치에서는 사실만 문서화하고 고치지 않았다.

## 리뷰 이력

- 2026-07-21 코드리뷰 서브에이전트 (Opus, fresh context, staged diff 48파일): **PASS** (P1 없음)
  - [P2] `.env.example` 의 `IP` 기본값 주석이 거짓 (`server/mod.rs:24` 는 `127.0.0.1` fallback).
    컨테이너에서 loopback 바인딩 시 내부 healthcheck 는 통과하지만 ECS awsvpc ENI 주소로는 접속이
    안 돼 ALB 타겟이 죽는다 → **수정**
  - [P3] `local_store.rs` 재작성 주석이 거짓이 됨 (2종 체계에선 `is_graduated` 로 market_type 복원
    가능) → **수정**
  - [P3] `cache/mod.rs` graduate fee 재조회 가드 주석이 why 를 잃음 → **수정**
  - [P3] `.env.example` 에 `DEFAULT_IMAGE_1~5` 누락 (동적 키라 grep 감사에서 빠짐) → **수정**
  - [P3] `branches/giwa.md` 배포 체크리스트의 env 키가 stale → **수정**
  - [P3] 브랜치 개념 문서 없음 → **이 문서**
  - [P3] `RUST_LOG` 이 3곳에 설정돼 있으나 무시됨 → **수용**, 위에 기록만 남김
  - Checked & clear: `check_white_list_pool` SQL 축소 안전성, 주석 재작성이 근거를 보존했는지,
    dead code 삭제 정확성, 값(wire/DB/Redis key)이 식별자로 오인돼 rename 되지 않았는지, env 정합성
- 판정자(Advisor) 검증: 수정 후 `cargo build --all-targets` 경고 0, `cargo test` 47 passed / 0 failed

**리뷰어가 함께 지적한 범위 밖 이슈** (이 브랜치에서 고치지 않음): graduate 시 fee_info 재조회
가드는 indexer 가 먼저 `market.market_type` 을 DEX 로 넘긴 뒤 graduate 가 도착하면 정작 필요한
재조회를 건너뛴다. 재조회가 존재하는 이유인 레이스와 같은 상황이다. 기존 동작이라 그대로 뒀다.

## Outcome

(PR/머지 시 작성)
