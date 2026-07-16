//! Pyth Hermes HTTP price provider.
//!
//! Owns the HTTP client, the Pyth rate limiter, and the retry/backoff loop.
//! All constants below are Pyth-specific and intentionally local to this
//! module. Mirrors observer's PythProvider so both projects share identical
//! mechanics.

use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use reqwest::Client;
use serde::Deserialize;
use tokio::{sync::Mutex, time::Instant};
use tracing::{error, info, warn};

use super::{PriceProvider, normalize_feed_id};

const PYTH_API_URL: &str = "https://hermes.pyth.network/v2/updates/price";
const REQUEST_TIMEOUT_SECS: u64 = 30;
/// Pyth Hermes: 30 requests per 10 seconds.
const MAX_REQUESTS_PER_10_SECONDS: usize = 30;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;

/// Pyth Hermes API response.
#[derive(Debug, Deserialize)]
struct PriceFeedResponse {
    parsed: Vec<ParsedPrice>,
}

#[derive(Debug, Deserialize)]
struct ParsedPrice {
    /// Pyth feed ID — comes back without `0x` prefix.
    id: String,
    price: PriceData,
}

#[derive(Debug, Deserialize)]
struct PriceData {
    price: String,
    expo: i32,
}

/// Sliding-window rate limiter for the Pyth Hermes API.
struct RateLimiter {
    request_times: Mutex<Vec<Instant>>,
    max_requests: usize,
    window: Duration,
}

impl RateLimiter {
    fn new(max_requests: usize, window: Duration) -> Self {
        Self {
            request_times: Mutex::new(Vec::new()),
            max_requests,
            window,
        }
    }

    async fn wait_if_needed(&self) {
        let wait = {
            let mut times = self.request_times.lock().await;
            let now = Instant::now();
            times.retain(|&t| now.duration_since(t) < self.window);

            let wait = if times.len() >= self.max_requests {
                times.first().map(|&oldest| {
                    self.window.saturating_sub(now.duration_since(oldest))
                        + Duration::from_millis(100)
                })
            } else {
                None
            };

            // Reserve our slot BEFORE sleeping so other callers see us in-flight.
            times.push(now);
            wait
        }; // MutexGuard dropped here

        if let Some(w) = wait {
            if w > Duration::ZERO {
                tokio::time::sleep(w).await;
            }
        }
    }
}

pub struct PythProvider {
    http: Client,
    rate_limiter: RateLimiter,
}

impl PythProvider {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("Failed to create HTTP client for PythProvider")?;
        Ok(Self {
            http,
            rate_limiter: RateLimiter::new(MAX_REQUESTS_PER_10_SECONDS, RATE_LIMIT_WINDOW),
        })
    }
}

#[async_trait]
impl PriceProvider for PythProvider {
    async fn fetch_batch(
        &self,
        feed_ids: &[&str],
        timestamp: u64,
    ) -> Result<HashMap<String, BigDecimal>> {
        if feed_ids.is_empty() {
            return Ok(HashMap::new());
        }

        // Single rate-limit slot covers the whole batch.
        self.rate_limiter.wait_if_needed().await;

        let mut retry_count: u32 = 0;
        let mut backoff = Duration::from_secs(1);

        loop {
            // Pyth `/v2/updates/price/{ts}` accepts repeated `ids[]=` params
            // and returns every requested feed in one parsed[] array.
            // ts=now() returns the latest published price (effectively
            // equivalent to `/price/latest` but with unified URL shape).
            let mut url = format!(
                "{}/{}?encoding=hex&parsed=true&ignore_invalid_price_ids=true",
                PYTH_API_URL, timestamp
            );
            for feed_id in feed_ids {
                url.push_str("&ids%5B%5D=");
                url.push_str(feed_id);
            }

            let response = self
                .http
                .get(&url)
                .header("Accept", "application/json")
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let feed: PriceFeedResponse = resp
                            .json()
                            .await
                            .context("Failed to parse Pyth batch response")?;

                        let mut out: HashMap<String, BigDecimal> =
                            HashMap::with_capacity(feed.parsed.len());
                        for parsed in feed.parsed {
                            let price_bigint = match parsed.price.price.parse::<i128>() {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(
                                        "Failed to parse price string for feed {} ts={}: {}",
                                        parsed.id, timestamp, e
                                    );
                                    continue;
                                }
                            };
                            let expo = parsed.price.expo;
                            // BigDecimal::new(int, scale) = int * 10^(-scale).
                            let price = BigDecimal::new(
                                bigdecimal::num_bigint::BigInt::from(price_bigint),
                                -(expo as i64),
                            );
                            out.insert(normalize_feed_id(&parsed.id), price);
                        }

                        info!(
                            "Fetched Pyth batch: feeds={} ts={} returned={}",
                            feed_ids.len(),
                            timestamp,
                            out.len()
                        );
                        return Ok(out);
                    } else if resp.status() == 429 {
                        error!("Pyth batch rate limit hit (429), backoff={:?}", backoff);
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            return Err(anyhow::anyhow!("Max retries exceeded for rate limit"));
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = backoff * 2 + Duration::from_millis(1000);
                        if backoff > Duration::from_secs(60) {
                            backoff = Duration::from_secs(60);
                        }
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth batch API returned status: {}",
                            resp.status()
                        ));
                    }
                }
                Err(e) => {
                    if retry_count < MAX_RETRIES {
                        warn!("Pyth batch request failed, retrying: {}", e);
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "Pyth batch request failed after retries: {}",
                            e
                        ));
                    }
                }
            }
        }
    }
}
