//! Price oracle provider abstraction.
//!
//! Mirrors the structure used by the observer (`observer/src/event/common/
//! price/provider/`) so both projects share identical mechanics for Pyth
//! fetches, rate limiting, and 429 handling. Polling cadence and call sites
//! still differ by use-case (real-time push here vs. historical block-aligned
//! attribution in the observer), but the provider abstraction is identical.

pub mod mock;
pub mod pyth;

use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use std::collections::HashMap;
use std::sync::Arc;

/// Normalize a Pyth feed ID for map-key comparison.
///
/// Pyth Hermes responses strip the `0x` prefix from `parsed[].id`, so we
/// normalize both sides (lowercase + strip prefix) before keying maps so
/// callers can pass feed IDs in either form.
pub fn normalize_feed_id(feed_id: &str) -> String {
    feed_id.trim_start_matches("0x").to_lowercase()
}

/// A price oracle capable of returning a spot price for a feed at (or near)
/// the supplied unix timestamp (seconds).
#[async_trait]
pub trait PriceProvider: Send + Sync {
    /// Batch-fetch multiple feeds at one timestamp.
    ///
    /// Returns a map keyed by [`normalize_feed_id`] (lowercase, no `0x`
    /// prefix). Missing entries indicate the oracle had no data for that
    /// feed at the given timestamp.
    ///
    /// Implementations should consume only one oracle slot for the whole
    /// batch — that's the entire point of this method (rate-limit pressure
    /// becomes O(timestamps) instead of O(timestamps × feeds)).
    async fn fetch_batch(
        &self,
        feed_ids: &[&str],
        timestamp: u64,
    ) -> Result<HashMap<String, BigDecimal>>;
}

/// Build the price provider. Always Pyth Hermes-backed [`pyth::PythProvider`].
/// ([`mock::MockProvider`] is retained for unit tests only.)
pub fn build_provider() -> Result<Arc<dyn PriceProvider>> {
    tracing::info!("[PRICE] Using PythProvider");
    Ok(Arc::new(pyth::PythProvider::new()?))
}
