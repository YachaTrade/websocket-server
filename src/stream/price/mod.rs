pub mod provider;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::client::RpcClient;
use provider::{build_provider, normalize_feed_id, PriceProvider};

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
const NATIVE_FEED_ID: &str = "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace";

/// 루프가 깨어나는 주기 — **Pyth 호출 주기가 아니다.** 실제 호출 주기는
/// [`BUCKET_WINDOW_SECS`]가 결정하고, 같은 버킷에 떨어진 tick은
/// [`needs_fetch`]에서 걸러진다 (60초 버킷 기준 6틱 중 5틱).
///
/// 이 값은 새 버킷이 열린 뒤 얼마나 빨리 반영되는가의 상한이다. 실제 Pyth
/// 요청은 성공한 버킷당 한 번뿐이므로 호출 빈도는 약 1 req/min 이다.
const POLL_INTERVAL: Duration = Duration::from_secs(10);
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

struct CompletePriceBatch {
    native_price: BigDecimal,
    quote_prices: Vec<(String, BigDecimal)>,
}

/// Validate the entire requested response before exposing any values for
/// application. An incomplete HTTP-200 response produces no batch, preventing
/// partial cache updates and leaving the bucket eligible for retry.
fn prepare_complete_batch(
    prices: &HashMap<String, BigDecimal>,
    quote_feeds: &[(String, String)],
) -> Option<CompletePriceBatch> {
    let native_price = prices.get(&normalize_feed_id(NATIVE_FEED_ID))?.clone();
    let quote_prices = quote_feeds
        .iter()
        .map(|(address, feed_id)| {
            prices
                .get(&normalize_feed_id(feed_id))
                .cloned()
                .map(|price| (address.clone(), price))
        })
        .collect::<Option<Vec<_>>>()?;

    Some(CompletePriceBatch {
        native_price,
        quote_prices,
    })
}

async fn apply_complete_batch(batch: CompletePriceBatch) {
    set_native_price(batch.native_price.clone()).await;
    info!("💰 Native price updated: ${}", batch.native_price);

    for (address, price) in batch.quote_prices {
        QUOTE_PRICES.insert(address.clone(), price.clone());
        info!("💰 Quote token price updated: {}=${}", address, price);
    }
}

/// Number of blocks to step back from `latest` before resolving the timestamp
/// used for the Pyth query. This keeps the selected block behind chain head.
const BLOCK_LAG: u64 = 5;

/// Wall-clock seconds per Pyth fetch bucket. This must match observer's
/// `event::common::BUCKET_WINDOW_SECS` so both services query the same timestamp.
const BUCKET_WINDOW_SECS: u64 = 60;

/// Floor a block timestamp to the shared wall-clock bucket grid.
fn bucket_of_ts(block_timestamp: u64) -> u64 {
    block_timestamp - (block_timestamp % BUCKET_WINDOW_SECS)
}

/// Return whether this bucket differs from the last successfully fetched one.
/// Inequality deliberately refetches an older bucket after a head rewind.
fn needs_fetch(bucket_ts: u64, last_fetched: Option<u64>) -> bool {
    last_fetched != Some(bucket_ts)
}

/// Advance the deduplication marker only after a successful batch request.
fn last_fetched_after_attempt(
    last_fetched: Option<u64>,
    bucket_ts: u64,
    succeeded: bool,
) -> Option<u64> {
    if succeeded {
        Some(bucket_ts)
    } else {
        last_fetched
    }
}

/// Resolve the timestamp of `latest - 5`, then floor it to the shared 60-second
/// wall-clock grid used by observer.
async fn pyth_query_ts(client: &RpcClient) -> Result<u64> {
    let latest = client.get_latest_block_number().await?;
    let target = latest.saturating_sub(BLOCK_LAG);
    let ts = client.get_block_timestamp(target).await?;
    Ok(bucket_of_ts(ts))
}

/// Native + Quote Price 업데이트 시작.
///
/// [`POLL_INTERVAL`]마다 native(ETH) feed와 등록된 모든 quote feed를 단일
/// batch 요청으로 가져온다. 이미 성공한 60초 버킷은 건너뛴다.
///
/// 가격 fetch는 [`PriceProvider`] trait를 거치며, 이 abstraction은 observer
/// 측과 동일한 형태(`provider/{mod,pyth,mock}.rs`)로 정렬되어 있어 두
/// 프로젝트의 메커니즘을 한 곳만 보면 이해 가능.
pub async fn start_update_price() -> Result<()> {
    let provider: Arc<dyn PriceProvider> =
        build_provider().context("Failed to build PriceProvider")?;
    let client = RpcClient::instance().context("RpcClient not initialized")?;

    tokio::spawn(async move {
        info!("🚀 Price monitor started (Pyth batch fetch)");
        info!(
            "[PRICE] bucket window = {}s (must match observer BUCKET_WINDOW_SECS)",
            BUCKET_WINDOW_SECS
        );

        // 실패한 fetch는 갱신하지 않아 다음 tick에서 같은 버킷을 재시도한다.
        let mut last_fetched_bucket: Option<u64> = None;

        loop {
            // 모든 등록된 feed_id 수집 (native + quote tokens).
            let quote_feeds: Vec<(String, String)> = QUOTE_FEED_IDS
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();
            let mut feed_ids: Vec<String> = vec![NATIVE_FEED_ID.to_string()];
            feed_ids.extend(quote_feeds.iter().map(|(_, feed_id)| feed_id.clone()));
            let feed_id_refs: Vec<&str> = feed_ids.iter().map(|s| s.as_str()).collect();

            // latest-5 블록 timestamp를 observer와 공유하는 60초 격자로 내림한다.
            let ts = match pyth_query_ts(client).await {
                Ok(ts) => ts,
                Err(e) => {
                    error!("❌ Failed to resolve chain timestamp for Pyth: {}", e);
                    tokio::time::sleep(ERROR_BACKOFF).await;
                    continue;
                }
            };

            if !needs_fetch(ts, last_fetched_bucket) {
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }

            match provider.fetch_batch(&feed_id_refs, ts).await {
                Ok(prices) => match prepare_complete_batch(&prices, &quote_feeds) {
                    Some(batch) => {
                        last_fetched_bucket =
                            last_fetched_after_attempt(last_fetched_bucket, ts, true);
                        apply_complete_batch(batch).await;
                        tokio::time::sleep(POLL_INTERVAL).await;
                    }
                    None => {
                        last_fetched_bucket =
                            last_fetched_after_attempt(last_fetched_bucket, ts, false);
                        warn!(
                            "⚠️  Pyth batch response incomplete for bucket {}; retrying",
                            ts
                        );
                        tokio::time::sleep(ERROR_BACKOFF).await;
                    }
                },
                Err(e) => {
                    last_fetched_bucket =
                        last_fetched_after_attempt(last_fetched_bucket, ts, false);
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
mod tests {
    use super::*;

    #[test]
    fn bucket_window_is_60_seconds_matching_observer() {
        assert_eq!(BUCKET_WINDOW_SECS, 60);
    }

    #[test]
    fn bucket_timestamp_floors_to_60s_boundary_without_lag() {
        assert_eq!(bucket_of_ts(0), 0);
        assert_eq!(bucket_of_ts(59), 0);
        assert_eq!(bucket_of_ts(60), 60);
        assert_eq!(bucket_of_ts(119), 60);
        assert_eq!(bucket_of_ts(1_785_138_347), 1_785_138_300);
    }

    #[test]
    fn bucket_timestamp_never_exceeds_the_block_timestamp() {
        for ts in [0u64, 1, 59, 60, 61, 1_785_138_347] {
            assert!(bucket_of_ts(ts) <= ts);
        }
    }

    #[test]
    fn first_bucket_needs_fetch() {
        assert!(needs_fetch(12_345, None));
    }

    #[test]
    fn successful_bucket_is_skipped_until_window_changes() {
        assert!(!needs_fetch(1_785_138_300, Some(1_785_138_300)));
        assert!(needs_fetch(1_785_138_360, Some(1_785_138_300)));
    }

    #[test]
    fn older_bucket_after_head_rewind_is_refetched() {
        assert!(needs_fetch(1_785_138_240, Some(1_785_138_300)));
    }

    #[test]
    fn failed_fetch_leaves_bucket_eligible_for_retry() {
        let bucket = 1_785_138_300;
        let last_fetched_bucket = last_fetched_after_attempt(None, bucket, false);

        assert!(needs_fetch(bucket, last_fetched_bucket));

        let last_fetched_bucket = last_fetched_after_attempt(last_fetched_bucket, bucket, true);
        assert!(!needs_fetch(bucket, last_fetched_bucket));
    }

    #[test]
    fn polls_across_six_windows_issue_six_fetches_not_thirty_six() {
        let start = 1_785_393_600;
        let ticks = 36u64;
        let mut last_fetched = None;
        let mut fetches = 0u64;

        for i in 0..ticks {
            let bucket = bucket_of_ts(start + i * POLL_INTERVAL.as_secs());
            if needs_fetch(bucket, last_fetched) {
                fetches += 1;
                last_fetched = Some(bucket);
            }
        }

        let windows = ticks * POLL_INTERVAL.as_secs() / BUCKET_WINDOW_SECS;
        assert_eq!(fetches, windows);
        assert_eq!(fetches, 6);
        assert_eq!(ticks, 36);
    }

    #[test]
    fn a_tick_landing_mid_window_skips_until_the_next_window() {
        let mut last_fetched = None;
        let mut fetches = 0;

        for ts in [1_785_393_637, 1_785_393_647, 1_785_393_657, 1_785_393_667] {
            let bucket = bucket_of_ts(ts);
            if needs_fetch(bucket, last_fetched) {
                fetches += 1;
                last_fetched = Some(bucket);
            }
        }

        assert_eq!(fetches, 2);
    }

    #[test]
    fn incomplete_price_maps_do_not_produce_snapshot_or_advance_marker() {
        let bucket = 1_785_138_300;
        let previous_bucket = Some(1_785_138_240);
        let quote_feeds = vec![("0xquote".to_string(), "0xquote-feed".to_string())];

        let missing_native =
            HashMap::from([(normalize_feed_id("0xquote-feed"), BigDecimal::from(2))]);
        let missing_quote =
            HashMap::from([(normalize_feed_id(NATIVE_FEED_ID), BigDecimal::from(3_100))]);

        for prices in [missing_native, missing_quote] {
            let prepared = prepare_complete_batch(&prices, &quote_feeds);
            let marker = last_fetched_after_attempt(previous_bucket, bucket, prepared.is_some());

            assert!(prepared.is_none());
            assert_eq!(marker, previous_bucket);
            assert!(needs_fetch(bucket, marker));
        }
    }
}
