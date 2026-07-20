pub mod curve;
pub mod dex;
pub mod price;

pub mod handler;

use std::time::Duration;

use anyhow::{anyhow, Result};

use crate::db::cache::CacheManager;

/// nad.fun 토큰은 vanity address 규칙으로 마지막 4자가 "7777"이어야 한다.
/// 컨트랙트 레벨에서 강제되지만 본 서버에서도 defensive check.
pub fn is_valid_nadfun_token_address(token: &str) -> bool {
    token.to_lowercase().ends_with("7777")
}

/// Curve(Buy/Sell) 진입 시 호출되는 공통 토큰 검증.
///
/// 1) 7777 suffix 확인 (즉시)
/// 2) whitelist 확인 — Create와 같은 tx에 동시 emit되는 race 대응으로 LocalStore에
///    `Some(true)` 가 박힐 때까지 짧게 polling (500ms × 6 = 최대 3초).
///    negative cache(`Some(false)`)는 race 중 PG miss로 박힐 수 있어 신뢰하지 않음.
/// 3) polling 실패 시 DB까지 fallback 가능한 일반 `check_white_list_token` 호출.
///
/// 통과: `Ok(())`.
/// 거부: `Err("Not a white list token: ...")` — stream loop가 이 문자열을 매칭해
///       노이즈 로그를 억제함.
pub async fn validate_curve_token(cache_manager: &CacheManager, token: &str) -> Result<()> {
    if !is_valid_nadfun_token_address(token) {
        return Err(anyhow!(
            "Not a white list token (suffix mismatch): {}",
            token
        ));
    }

    const MAX_POLLS: u32 = 6;
    const POLL_DELAY_MS: u64 = 500;
    for _ in 0..MAX_POLLS {
        if matches!(
            cache_manager.check_white_list_token_local(token),
            Some(true)
        ) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(POLL_DELAY_MS)).await;
    }

    // 마지막 시도 — DB까지 다 확인 (Create handler가 늦거나 PostgreSQL 인덱싱 race일 수 있음)
    match cache_manager.check_white_list_token(token).await {
        Ok(true) => Ok(()),
        _ => Err(anyhow!("Not a white list token: {}", token)),
    }
}
