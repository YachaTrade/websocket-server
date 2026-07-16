//! In-memory price provider used for testnet runtime and unit tests.

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;

use super::{PriceProvider, normalize_feed_id};

/// Always returns a single fixed price regardless of feed/timestamp.
#[derive(Debug, Clone)]
pub struct MockProvider {
    price: BigDecimal,
}

impl MockProvider {
    pub fn fixed(price: BigDecimal) -> Self {
        Self { price }
    }

    pub fn fixed_str(price: &str) -> Self {
        Self {
            price: BigDecimal::from_str(price)
                .expect("MockProvider::fixed_str received invalid decimal"),
        }
    }
}

#[async_trait]
impl PriceProvider for MockProvider {
    async fn fetch_batch(
        &self,
        feed_ids: &[&str],
        _timestamp: u64,
    ) -> Result<HashMap<String, BigDecimal>> {
        Ok(feed_ids
            .iter()
            .map(|id| (normalize_feed_id(id), self.price.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn batch_returns_fixed_price_for_every_feed() {
        let provider = MockProvider::fixed_str("0.03");
        let prices = provider
            .fetch_batch(&["0xdeadbeef", "0xCAFEBABE"], 1_700_000_000)
            .await
            .expect("fetch_batch must not error");

        assert_eq!(prices.len(), 2);
        assert_eq!(
            prices.get("deadbeef"),
            Some(&BigDecimal::from_str("0.03").unwrap())
        );
        assert_eq!(
            prices.get("cafebabe"),
            Some(&BigDecimal::from_str("0.03").unwrap())
        );
    }

    #[tokio::test]
    async fn batch_with_empty_feeds_returns_empty_map() {
        let provider = MockProvider::fixed_str("0.03");
        let prices = provider.fetch_batch(&[], 0).await.unwrap();
        assert!(prices.is_empty());
    }
}
