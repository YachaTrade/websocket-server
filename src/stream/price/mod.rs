pub mod provider;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::client::RpcClient;
use provider::{PriceProvider, build_provider, normalize_feed_id};

lazy_static! {
    /// 전역 Native(ETH) 가격 싱글톤 인스턴스
    ///
    /// Thread-safe하게 읽기/쓰기 가능
    /// - 읽기: 여러 스레드에서 동시 접근 가능 (매우 빠름)
    /// - 쓰기: monitor 스레드에서만 주기적으로 업데이트
    ///
    /// 초기값은 첫 Pyth fetch 전(콜드스타트 ~10s)과 Pyth 장애 지속 시에만 노출되는
    /// placeholder다. Native가 ETH이므로 대략적인 ETH/USD 값으로 둔다 — 과거 MON
    /// 기준 $0.03을 그대로 두면 이 구간의 USD 가격이 ~10만배 어긋난다.
    pub static ref NATIVE_PRICE: Arc<RwLock<BigDecimal>> = Arc::new(RwLock::new(
        BigDecimal::from_str("3000").unwrap() // 초기값: ~$3000/ETH (첫 fetch 시 덮어씀)
    ));

    /// Quote token 가격 매핑 (quote_address → USD 가격)
    /// WETH이 아닌 quote token의 가격을 저장
    /// register_quote_token()으로 등록하면 자동으로 주기적 업데이트됨
    pub static ref QUOTE_PRICES: Arc<DashMap<String, BigDecimal>> = Arc::new(DashMap::new());

    /// 등록된 quote token의 Pyth feed ID 매핑 (quote_address → pyth_feed_id)
    pub static ref QUOTE_FEED_IDS: Arc<DashMap<String, String>> = Arc::new(DashMap::new());
}

/// Native(ETH) Pyth feed ID — Crypto.ETH/USD. Always fetched alongside any
/// registered quote feeds in each polling cycle. Must match observer's value.
const NATIVE_FEED_ID: &str =
    "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace";

/// Pyth 요청 주기 — 벽시계가 아니라 **블록 진행** 기준. 마지막 요청 이후
/// 체인이 이만큼 블록을 진행하면 다음 Pyth 요청을 낸다. giwa 는 ~1s/block
/// 이라 100블록 ≈ 100s. 블록에 맞추므로 체인이 한산해 블록이 느려지면
/// Pyth 콜도 자연히 줄어든다 (거래가 없으면 신선한 가격도 불필요).
///
/// observer 와 egress IP 를 공유해 합산 Pyth 부하가 30 req/10s 한도를
/// 넘기던 이력(1s → 10s → 30s → 블록 기반)의 연장선 — ws-server 부하를 더
/// 낮춤. 트레이드오프: USD 가격 freshness 가 ~100s 로 늘어남 (quote 토큰은
/// 느리게 움직여 일반 거래 화면엔 무시 가능). align_pyth_ts(10s 버킷)은
/// 그대로라 observer 와 같은 timestamp 를 질의하는 성질은 유지된다.
const BLOCKS_PER_POLL: u64 = 100;

/// 블록 높이 확인 주기. 이 간격으로 (저렴한) eth_blockNumber 를 폴링해
/// [`BLOCKS_PER_POLL`] 도달 여부만 확인하고, 도달했을 때만 Pyth 를 호출한다.
/// Pyth 호출 주기가 아니라 게이트 확인 주기다.
const BLOCK_CHECK_INTERVAL: Duration = Duration::from_secs(10);
/// 에러 시 다음 retry까지 대기 시간 (provider 내부 backoff에 더해 최후 안전망).
const ERROR_BACKOFF: Duration = Duration::from_secs(2);

/// Native(ETH) 가격 조회
///
/// 메모리에서 즉시 반환 (~1μs)
pub async fn get_native_price() -> BigDecimal {
    NATIVE_PRICE.read().await.clone()
}

/// Quote token 가격 조회
/// WETH이면 native_price 반환, 아니면 QUOTE_PRICES에서 조회
/// 등록되지 않은 quote token이면 native_price를 fallback으로 반환
pub async fn get_quote_price(quote_id: &str) -> BigDecimal {
    let weth = crate::config::WETH_ADDRESS.as_str();
    if quote_id.eq_ignore_ascii_case(weth) {
        return get_native_price().await;
    }

    if let Some(price) = QUOTE_PRICES.get(quote_id) {
        return price.clone();
    }

    let lower = quote_id.to_lowercase();
    if let Some(price) = QUOTE_PRICES.get(&lower) {
        return price.clone();
    }

    warn!(
        "Quote token {} 가격 미등록, native_price를 fallback으로 사용",
        quote_id
    );
    get_native_price().await
}

/// Quote token 등록
/// Pyth feed ID와 함께 등록하면 주기적으로 가격 업데이트됨
/// 초기 가격은 0으로 설정되며, 첫 번째 업데이트에서 실제 가격으로 갱신됨
pub fn register_quote_token(address: &str, pyth_feed_id: &str) {
    let addr_lower = address.to_lowercase();
    QUOTE_FEED_IDS.insert(addr_lower.clone(), pyth_feed_id.to_string());
    QUOTE_PRICES.insert(addr_lower.clone(), BigDecimal::from(0));
    info!(
        "📝 Quote token 등록: address={}, feed_id={}",
        addr_lower, pyth_feed_id
    );
}

async fn set_native_price(price: BigDecimal) {
    let mut native_price = NATIVE_PRICE.write().await;
    *native_price = price;
}

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

/// 마지막으로 Pyth 를 조회한 블록(`last_polled`) 대비 `latest` 가
/// [`BLOCKS_PER_POLL`] 이상 진행됐는지 판정. 첫 조회(None)면 즉시 true.
/// 재조직/후퇴로 latest < last_polled 여도 saturating_sub → 0 → false.
fn should_poll(latest: u64, last_polled: Option<u64>) -> bool {
    match last_polled {
        None => true,
        Some(prev) => latest.saturating_sub(prev) >= BLOCKS_PER_POLL,
    }
}

/// Native + Quote Price 업데이트 시작.
///
/// 체인이 [`BLOCKS_PER_POLL`] 만큼 진행할 때마다 native(ETH) feed와 등록된 모든
/// quote feed를 **단일 batch 요청**으로 한 번에 가져와 인메모리 캐시
/// (NATIVE_PRICE, QUOTE_PRICES)에 반영.
///
/// 가격 fetch는 [`PriceProvider`] trait를 거치며, 이 abstraction은 observer
/// 측과 동일한 형태(`provider/{mod,pyth,mock}.rs`)로 정렬되어 있어 두
/// 프로젝트의 메커니즘을 한 곳만 보면 이해 가능.
pub async fn start_update_price() -> Result<()> {
    let provider: Arc<dyn PriceProvider> =
        build_provider().context("Failed to build PriceProvider")?;
    let client = RpcClient::instance().context("RpcClient not initialized")?;

    tokio::spawn(async move {
        info!(
            "🚀 Price monitor started (Pyth batch fetch, every {} blocks)",
            BLOCKS_PER_POLL
        );

        // 마지막으로 Pyth 를 조회한 블록 높이. None 이면 아직 한 번도 안 함.
        let mut last_polled: Option<u64> = None;

        loop {
            // 최신 블록 높이만 저렴하게 확인 (Pyth 아님, giwa 노드 eth_blockNumber).
            let latest = match client.get_latest_block_number().await {
                Ok(b) => b,
                Err(e) => {
                    error!("❌ Failed to get latest block for Pyth gate: {}", e);
                    tokio::time::sleep(ERROR_BACKOFF).await;
                    continue;
                }
            };

            // 마지막 요청 이후 BLOCKS_PER_POLL 만큼 진행하지 않았으면 대기 후 재확인.
            if !should_poll(latest, last_polled) {
                tokio::time::sleep(BLOCK_CHECK_INTERVAL).await;
                continue;
            }

            // 모든 등록된 feed_id 수집 (native + quote tokens).
            let mut feed_ids: Vec<String> = vec![NATIVE_FEED_ID.to_string()];
            for entry in QUOTE_FEED_IDS.iter() {
                feed_ids.push(entry.value().clone());
            }
            let feed_id_refs: Vec<&str> = feed_ids.iter().map(|s| s.as_str()).collect();

            // 체인의 최신 블록 timestamp를 그대로 Pyth에 사용.
            // 블록은 항상 과거에 commit됐으므로 Pyth `/price/{ts}` 가 404 안 남.
            // observer가 block_timestamp를 넘기는 것과 동일한 의미.
            let ts = match pyth_query_ts(client).await {
                Ok(ts) => ts,
                Err(e) => {
                    error!("❌ Failed to resolve chain timestamp for Pyth: {}", e);
                    tokio::time::sleep(ERROR_BACKOFF).await;
                    continue;
                }
            };

            match provider.fetch_batch(&feed_id_refs, ts).await {
                Ok(prices) => {
                    // Native price 업데이트
                    let native_key = normalize_feed_id(NATIVE_FEED_ID);
                    if let Some(price) = prices.get(&native_key) {
                        set_native_price(price.clone()).await;
                        info!("💰 Native price updated: ${}", price);
                    } else {
                        warn!("⚠️  Pyth response missing native feed");
                    }

                    // Quote token 가격 일괄 업데이트
                    for entry in QUOTE_FEED_IDS.iter() {
                        let address = entry.key();
                        let feed_id = entry.value();
                        let key = normalize_feed_id(feed_id);
                        if let Some(price) = prices.get(&key) {
                            QUOTE_PRICES.insert(address.clone(), price.clone());
                            info!(
                                "💰 Quote token price updated: {}=${}",
                                address, price
                            );
                        } else {
                            warn!(
                                "⚠️  Pyth response missing feed for {}: feed_id={}",
                                address, feed_id
                            );
                        }
                    }

                    // 이번 요청 성공 → 기준 블록 갱신. 다음 요청은 여기서
                    // BLOCKS_PER_POLL 만큼 더 진행한 뒤에 나간다.
                    last_polled = Some(latest);
                    tokio::time::sleep(BLOCK_CHECK_INTERVAL).await;
                }
                Err(e) => {
                    // last_polled 를 갱신하지 않아 다음 확인에서 곧바로 재시도.
                    error!("❌ Batch price fetch failed: {}", e);
                    tokio::time::sleep(ERROR_BACKOFF).await;
                }
            }
        }
    });

    info!("✅ Price monitor initialized");
    Ok(())
}

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

#[cfg(test)]
mod should_poll_tests {
    use super::should_poll;

    // 의도: 첫 조회는 즉시, 이후엔 정확히 BLOCKS_PER_POLL(100) 블록이 진행됐을
    // 때만 Pyth 요청. 이 100블록 경계가 곧 Pyth 콜 빈도(≈ 가격 freshness)이자
    // 429 안전 마진이라, 경계·후퇴 케이스를 못 박아 로직이 흔들리면 깨지게 한다.
    #[test]
    fn first_poll_fires_immediately() {
        assert!(should_poll(12_345, None));
    }

    #[test]
    fn fires_only_after_100_blocks() {
        assert!(!should_poll(1_099, Some(1_000))); // 99블록 진행 → 아직 아님
        assert!(should_poll(1_100, Some(1_000))); // 정확히 100블록 → 요청
        assert!(should_poll(1_250, Some(1_000))); // 100블록 초과 → 요청
    }

    #[test]
    fn backward_jump_does_not_fire() {
        // 재조직/후퇴로 latest < last_polled 여도 saturating_sub → 0 → false.
        assert!(!should_poll(500, Some(1_000)));
    }
}
