use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::eips::BlockNumberOrTag;

use alloy::primitives::TxHash;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::Block;
use alloy::rpc::types::Filter;

use alloy::rpc::types::Log;
use alloy::rpc::types::Transaction;

use crate::{config::RPC_TIME_OUT, measure_rpc};
use anyhow::Context;
use anyhow::Result;
use reqwest::Url;
use tokio::sync::Mutex;
use tokio::sync::OnceCell as TokioOnceCell;
use tracing::{error, info, warn};

// Global RPC client instance
static RPC_CLIENT: TokioOnceCell<RpcClient> = TokioOnceCell::const_new();

#[derive(Clone, Debug)]
pub struct ProviderConfig {
    pub url: String,
    pub name: String,
    pub score: f32,
    pub success_count: u32,
    pub fail_count: u32,
    pub last_used: Option<Instant>,
    pub priority: usize,       // 0=Main(최우선), 1=Sub1, 2=Sub2, ...
    pub is_reconnecting: bool, // 재연결 중 플래그 (HealthCheck에서 skip)
}

impl ProviderConfig {
    pub fn new(url: String, name: String, priority: usize) -> Self {
        Self {
            url,
            name,
            score: 100.0, // 시작 점수
            success_count: 0,
            fail_count: 0,
            last_used: None,
            priority,
            is_reconnecting: false,
        }
    }

    // 100점 만점 스코어링 시스템 (observer v2.2와 동일)
    fn calculate_current_score(&self) -> f32 {
        let total_attempts = self.success_count + self.fail_count;

        // 기본 성능 점수 (0-70점)
        let performance_score = if total_attempts == 0 {
            self.score * 0.7 // 초기에는 70점 만점
        } else {
            let success_rate = self.success_count as f32 / total_attempts as f32;
            self.score * success_rate * 0.7 // 성공률 반영하여 최대 70점
        };

        // 우선순위 보너스 (인덱스 낮을수록 높은 점수)
        let priority_bonus = match self.priority {
            0 => 30.0, // Main: +30점 (총 100점 가능)
            1 => 20.0, // Sub1: +20점 (총 90점 가능)
            2 => 10.0, // Sub2: +10점 (총 80점 가능)
            _ => 0.0,  // 기타: +0점 (총 70점 가능)
        };

        // 실패 페널티 (엄격하게)
        let failure_penalty = if self.fail_count > 0 {
            match self.fail_count {
                1..=2 => 15.0,  // 1-2회 실패: -15점
                3..=5 => 30.0,  // 3-5회 실패: -30점
                6..=10 => 50.0, // 6-10회 실패: -50점
                _ => 70.0,      // 11회+ 실패: -70점
            }
        } else {
            0.0
        };

        // 최종 점수 (0-100점)
        (performance_score + priority_bonus - failure_penalty).clamp(0.0, 100.0)
    }

    // 공개 메서드로 점수 조회
    pub fn calculate_score(&self) -> f32 {
        self.calculate_current_score()
    }

    // 성공 기록 (overflow 방지) - v2.2 자동 회복 시스템
    fn record_success(&mut self) {
        // Overflow 방지: 카운트가 너무 크면 비율을 유지하며 리셋
        if self.success_count > u32::MAX - 1000 || self.fail_count > u32::MAX - 1000 {
            self.reset_counts_with_ratio();
        }
        self.success_count = self.success_count.saturating_add(1);

        // 성공할 때마다 실패 카운트를 줄여서 회복 가능하게 함 (NEW v2.2)
        if self.fail_count > 0 {
            // 성공할 때마다 실패 카운트 2개 감소 (빠른 회복을 위해 1:2 비율로 개선)
            self.fail_count = self.fail_count.saturating_sub(2);
        }

        self.score = (self.score + 2.0).min(100.0); // 성공 시 더 빠른 회복
        self.last_used = Some(Instant::now());
    }

    // 실패 기록 (overflow 방지)
    fn record_failure(&mut self) {
        // Overflow 방지: 카운트가 너무 크면 비율을 유지하며 리셋
        if self.success_count > u32::MAX - 1000 || self.fail_count > u32::MAX - 1000 {
            self.reset_counts_with_ratio();
        }
        self.fail_count = self.fail_count.saturating_add(1);
        // 실패 시 더 엄격한 점수 감소
        let penalty = match self.fail_count {
            1..=2 => 10.0, // 초기 실패: -10점
            3..=5 => 20.0, // 반복 실패: -20점
            _ => 30.0,     // 연속 실패: -30점
        };
        self.score = (self.score - penalty).max(5.0); // 최소 5점만 유지
        self.last_used = Some(Instant::now());
    }

    // 비율을 유지하면서 카운트 리셋 (overflow 방지)
    fn reset_counts_with_ratio(&mut self) {
        let total = self.success_count.saturating_add(self.fail_count);
        if total > 0 {
            // 1/10 스케일로 축소하되 최소값 보장
            let scale_factor = 10;
            self.success_count = (self.success_count / scale_factor).max(1);
            self.fail_count = (self.fail_count / scale_factor).max(1);
            warn!("[HealthCheck] ⚠️ Provider {} counts reset to prevent overflow: success={}, fail={}", 
                  self.name, self.success_count, self.fail_count);
        }
    }
}

pub struct RpcClient {
    // Vector of RPC providers (Arc<Mutex>로 변경하여 안전한 교체 가능)
    providers: Arc<Mutex<Vec<Option<DynProvider>>>>,
    // Provider configurations with weights and health scores
    provider_configs: Arc<Mutex<Vec<ProviderConfig>>>,
    // Current index to use
    current_index: Arc<Mutex<usize>>,
    // Maximum number of retry attempts (미래 확장용으로 유지)
    #[allow(dead_code)]
    max_retries: usize,
}

impl RpcClient {
    // Initialize the global RPC client
    pub async fn init(urls: Vec<String>, max_retries: Option<usize>) -> Result<&'static Self> {
        // 이미 초기화되었는지 확인
        if let Some(client) = RPC_CLIENT.get() {
            return Ok(client);
        }

        // 클라이언트 생성
        let client = Self::create_client(urls, max_retries).await?;

        // OnceCell에 클라이언트 설정
        match RPC_CLIENT.set(client) {
            Ok(_) => Ok(RPC_CLIENT.get().unwrap()),
            Err(_) => {
                // 다른 스레드에서 이미 초기화한 경우
                info!("RPC client was already initialized by another thread");
                Ok(RPC_CLIENT.get().unwrap())
            }
        }
    }

    // Helper function to create a new RPC client
    async fn create_client(urls: Vec<String>, max_retries: Option<usize>) -> Result<Self> {
        if urls.is_empty() {
            return Err(anyhow::anyhow!("At least one RPC URL must be provided"));
        }

        let mut providers = Vec::new();
        let mut success_count = 0;
        let mut connection_errors = Vec::new();

        for (index, url) in urls.iter().enumerate() {
            let name = match index {
                0 => "Main".to_string(),
                1 => "Sub1".to_string(),
                2 => "Sub2".to_string(),
                _ => format!("Provider{}", index),
            };

            // provider 생성 시도 (3초 timeout 포함)
            match Self::create_provider(url).await {
                Ok(provider) => {
                    info!("[CLIENT] ✓ Successfully connected to {}: {}", name, url);
                    providers.push(Some(provider));
                    success_count += 1;
                }
                Err(e) => {
                    error!("[CLIENT] ✗ Failed to connect to {}: {} - {}", name, url, e);
                    providers.push(None);
                    connection_errors.push((url.clone(), e.to_string()));
                }
            }
        }

        // 최소 1개 이상의 provider가 성공해야 함
        if success_count == 0 {
            return Err(anyhow::anyhow!(
                "Failed to initialize any provider. Errors: {:?}",
                connection_errors
            ));
        }

        if success_count < urls.len() {
            warn!(
                "[CLIENT] ⚠️ Only {}/{} providers connected successfully",
                success_count,
                urls.len()
            );
        }

        // Provider configs 생성 (Main 우선순위 시스템)
        let provider_configs: Vec<ProviderConfig> = urls
            .iter()
            .enumerate()
            .map(|(i, url)| {
                let name = match i {
                    0 => "Main".to_string(),
                    1 => "Sub1".to_string(),
                    2 => "Sub2".to_string(),
                    _ => format!("Provider{}", i),
                };
                ProviderConfig::new(url.clone(), name, i) // index 추가로 우선순위 설정
            })
            .collect();

        let client = Self {
            providers: Arc::new(Mutex::new(providers)),
            provider_configs: Arc::new(Mutex::new(provider_configs)),
            current_index: Arc::new(Mutex::new(0)),
            max_retries: max_retries.unwrap_or(3),
        };

        Ok(client)
    }

    // 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<&'static Self> {
        RPC_CLIENT.get().ok_or_else(|| {
            anyhow::anyhow!("RPC Client not initialized. Call RpcClient::init() first.")
        })
    }

    // 새 인스턴스 생성 (글로벌이 아님) - 테스트나 특수 케이스용
    pub async fn new(urls: Vec<String>) -> Self {
        if urls.is_empty() {
            panic!("At least one RPC URL must be provided");
        }

        let providers = urls
            .iter()
            .map(|url| {
                let url = Url::parse(url).expect("URL must be valid");
                let provider = ProviderBuilder::new().connect_http(url);
                Some(DynProvider::new(provider))
            })
            .collect();

        let provider_configs: Vec<ProviderConfig> = urls
            .iter()
            .enumerate()
            .map(|(i, url)| {
                let name = match i {
                    0 => "Main".to_string(),
                    1 => "Sub1".to_string(),
                    2 => "Sub2".to_string(),
                    _ => format!("Provider{}", i),
                };
                ProviderConfig::new(url.clone(), name, i) // index 추가로 우선순위 설정
            })
            .collect();

        Self {
            providers: Arc::new(Mutex::new(providers)),
            provider_configs: Arc::new(Mutex::new(provider_configs)),
            current_index: Arc::new(Mutex::new(0)),
            max_retries: 3,
        }
    }

    // Get the current provider based on the index
    pub async fn get_current_provider(&self) -> Result<DynProvider> {
        let index = *self.current_index.lock().await;
        let providers = self.providers.lock().await;

        providers[index]
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Current provider[{}] is None", index))
    }

    // Get provider for contract interactions (최신 alloy 패턴 지원)
    pub async fn get_provider(&self) -> Result<DynProvider> {
        self.get_current_provider().await
    }

    // 점수 기반으로 최적 provider 선택 (observer v2.2 스타일)
    async fn select_best_provider(&self) -> usize {
        let configs = self.provider_configs.lock().await;
        let providers = self.providers.lock().await;

        // Primary provider(0번)가 활성 상태면 우선 선택
        if providers.first().and_then(|p| p.as_ref()).is_some() {
            return 0;
        }

        // Primary가 None이면 활성 provider 중 최고 점수 선택
        let mut best_index = None;
        let mut best_score = 0.0;

        for (index, config) in configs.iter().enumerate() {
            // None인 provider는 제외
            if providers.get(index).and_then(|p| p.as_ref()).is_none() {
                continue;
            }

            let score = config.calculate_current_score();
            if score > best_score {
                best_score = score;
                best_index = Some(index);
            }
        }

        // 활성 provider가 없으면 0 반환 (fallback)
        best_index.unwrap_or(0)
    }

    // 최적 provider 선택 및 인덱스 업데이트
    pub async fn update_best_provider(&self) {
        let best_index = self.select_best_provider().await;

        // 선택된 provider가 실제로 활성 상태인지 검증
        let is_active = {
            let providers = self.providers.lock().await;
            providers.get(best_index).and_then(|p| p.as_ref()).is_some()
        };

        if !is_active {
            warn!(
                "[HealthCheck] ⚠️ Selected provider[{}] is None, all providers may be down",
                best_index
            );
        }

        let mut index = self.current_index.lock().await;
        *index = best_index;

        let configs = self.provider_configs.lock().await;
        if let Some(config) = configs.get(best_index) {
            if is_active {
                info!(
                    "[HealthCheck] 🎯 Selected best provider: [{}] {} (Score: {:.2})",
                    best_index,
                    config.name,
                    config.calculate_score()
                );
            } else {
                warn!(
                    "[HealthCheck] ⚠️ Fallback to provider[{}] {} but it's currently None",
                    best_index, config.name
                );
            }
        }
    }

    // 성공 기록 (observer v2.2 스타일)
    async fn record_provider_success(&self, provider_index: usize) {
        let (_name, _score) = {
            let mut configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get_mut(provider_index) {
                config.record_success();
                (config.name.clone(), config.calculate_score())
            } else {
                return;
            }
        };
    }

    // 실패 기록 (observer v2.2 스타일)
    async fn record_provider_failure(&self, provider_index: usize) {
        let (_name, _score) = {
            let mut configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get_mut(provider_index) {
                config.record_failure();
                (config.name.clone(), config.calculate_score())
            } else {
                return;
            }
        };
    }

    // 스마트 라이브 체인 검증 (observer v2.2 스타일)
    async fn smart_chain_validation_test(&self) -> Vec<(usize, bool, u64)> {
        let provider_count = {
            let providers = self.providers.lock().await;
            providers.len()
        };
        if provider_count == 0 {
            return Vec::new();
        }

        info!(
            "[HealthCheck] 🎥 Starting smart live chain validation for {} providers",
            provider_count
        );

        let mut validation_results = Vec::new();

        let providers_to_check = {
            let providers = self.providers.lock().await;
            providers.clone()
        };

        for (i, provider_opt) in providers_to_check.iter().enumerate() {
            let (provider_name, is_reconnecting) = {
                let configs = self.provider_configs.lock().await;
                configs
                    .get(i)
                    .map(|c| (c.name.clone(), c.is_reconnecting))
                    .unwrap_or_else(|| (format!("Provider{}", i), false))
            };

            // 재연결 중인 provider는 skip (score 변경 없음)
            if is_reconnecting {
                info!(
                    "[HealthCheck] ⏳ Provider[{}] {} is reconnecting, skipping health check",
                    i, provider_name
                );
                continue;
            }

            // provider가 None인 경우 재연결 시도
            let provider = match provider_opt {
                Some(p) => p,
                None => {
                    warn!(
                        "[HealthCheck] 🔄 Provider[{}] {} is None, attempting reconnection",
                        i, provider_name
                    );
                    // None이면 무조건 재연결 시도
                    self.try_replace_failed_provider(i).await;
                    validation_results.push((i, false, 0));
                    continue;
                }
            };

            // 첫 번째 블록 번호 수집 (5초 타임아웃)
            let first_result =
                tokio::time::timeout(Duration::from_secs(5), provider.get_block_number()).await;

            if let Ok(Ok(block1)) = first_result {
                // 2초 대기 (observer v2.2 스타일)
                tokio::time::sleep(Duration::from_secs(2)).await;

                // 두 번째 블록 번호 수집 (5초 타임아웃)
                let second_result =
                    tokio::time::timeout(Duration::from_secs(5), provider.get_block_number()).await;

                if let Ok(Ok(block2)) = second_result {
                    match block2 > block1 {
                        true => {
                            // 블록 증가 = 라이브 체인
                            let block_progress = block2 - block1;
                            validation_results.push((i, true, block_progress));
                            info!(
                                "[HealthCheck] ✅ Provider[{}] {} - block: {} [LIVE] (+{})",
                                i, provider_name, block2, block_progress
                            );
                        }
                        false => {
                            // 블록 같음 = 정지된 체인
                            validation_results.push((i, false, 0));
                            warn!(
                                "[HealthCheck] ⚠️ Provider[{}] {} - block: {} [STALE]",
                                i, provider_name, block1
                            );
                        }
                    }
                } else {
                    // 두 번째 쿼리 실패
                    validation_results.push((i, false, 0));
                    warn!(
                        "[HealthCheck] ❌ Provider[{}] {} - second query failed",
                        i, provider_name
                    );
                }
            } else {
                // 첫 번째 쿼리 실패
                validation_results.push((i, false, 0));
                warn!(
                    "[HealthCheck] ❌ Provider[{}] {} - first query failed or timeout",
                    i, provider_name
                );
            }
        }

        validation_results
    }

    // 점수 업데이트 및 최적 provider 선택 (observer v2.2 스타일)
    async fn update_provider_scores_and_select_best(
        &self,
        validation_results: Vec<(usize, bool, u64)>,
    ) {
        for (provider_index, is_live, _block_progress) in validation_results {
            let old_score = {
                let configs = self.provider_configs.lock().await;
                configs
                    .get(provider_index)
                    .map(|c| c.calculate_current_score())
                    .unwrap_or(0.0)
            };

            let provider_name = {
                let configs = self.provider_configs.lock().await;
                configs
                    .get(provider_index)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| format!("Provider{}", provider_index))
            };

            if is_live {
                // 라이브 체인: 성공 처리
                self.record_provider_success(provider_index).await;

                // 개별 프로바이더 상태를 healthy로 설정
                crate::metrics::METRICS
                    .provider
                    .set_provider_health(&provider_name, true);

                let new_score = {
                    let configs = self.provider_configs.lock().await;
                    configs
                        .get(provider_index)
                        .map(|c| c.calculate_current_score())
                        .unwrap_or(0.0)
                };

                if (new_score - old_score).abs() > 0.01 {
                    info!(
                        "[HealthCheck] ✅ Provider[{}] {} score updated: {:.2} → {:.2}",
                        provider_index, provider_name, old_score, new_score
                    );
                }
            } else {
                // 스탈 또는 실패 체인: 실패 처리
                self.record_provider_failure(provider_index).await;

                // 개별 프로바이더 상태를 unhealthy로 설정
                crate::metrics::METRICS
                    .provider
                    .set_provider_health(&provider_name, false);

                let (new_score, fail_count) = {
                    let configs = self.provider_configs.lock().await;
                    if let Some(config) = configs.get(provider_index) {
                        (config.calculate_current_score(), config.fail_count)
                    } else {
                        (0.0, 0)
                    }
                };

                warn!(
                    "[HealthCheck] ❌ Provider[{}] {} score decreased: {:.2} → {:.2} (fails: {})",
                    provider_index, provider_name, old_score, new_score, fail_count
                );

                // 실패한 provider 교체 시도 (observer v2.2 스타일)
                self.try_replace_failed_provider(provider_index).await;
            }
        }

        // 모든 헬스체크 완료 후 최신 점수로 최적 provider 선택 (v2.2 개선사항)
        let (best_index, best_name, best_score) = {
            let configs = self.provider_configs.lock().await;
            let mut best_idx = 0;
            let mut best_score = 0.0;
            let mut best_name = String::new();

            for (index, config) in configs.iter().enumerate() {
                let current_score = config.calculate_current_score();
                if current_score > best_score {
                    best_score = current_score;
                    best_idx = index;
                    best_name = config.name.clone();
                }
            }
            (best_idx, best_name, best_score)
        };

        // current_index 업데이트
        {
            let mut index = self.current_index.lock().await;
            *index = best_index;
        }

        info!(
            "[HealthCheck] 🎯 Selected best provider: [{}] {} (Score: {:.2})",
            best_index, best_name, best_score
        );
    }

    // 메인 헬스체크 메서드 (observer v2.2 스타일)
    pub async fn health_check_all_providers(&self) {
        info!("[HealthCheck] 🎥 Starting health check for all providers");

        // 1. 스마트 라이브 체인 검증
        let validation_results = self.smart_chain_validation_test().await;

        // 2. 점수 업데이트 및 최적 provider 선택
        self.update_provider_scores_and_select_best(validation_results)
            .await;

        // Provider 점수는 Metrics에서 출력됨
    }

    pub async fn health_check_loop(&self) -> Result<()> {
        let interval_ms = std::env::var("HEALTH_CHECK_INTERVAL")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()
            .unwrap_or(60);

        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            ticker.tick().await;
            self.health_check_all_providers().await;
        }
    }

    pub async fn start_health_check_loop() -> Result<()> {
        match Self::instance() {
            Ok(client) => match client.health_check_loop().await {
                Ok(()) => Ok(()),
                Err(err) => {
                    warn!("[HealthCheck] ❌ Provider health loop stopped: {err}");
                    Err(err)
                }
            },
            Err(err) => {
                warn!("[HealthCheck] ❌ Failed to obtain RpcClient instance: {err}");
                Err(err)
            }
        }
    }

    // 실패한 provider 교체 시도 (observer v2.2 스타일)
    async fn try_replace_failed_provider(&self, provider_index: usize) {
        // 점수가 너무 낮은 경우에만 교체 시도 (과도한 교체 방지)
        let should_replace = {
            let configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get(provider_index) {
                let score = config.calculate_current_score();

                // 점수가 30 이하이거나 연속 실패가 3회 이상인 경우 교체
                score <= 30.0 || config.fail_count >= 3
            } else {
                false
            }
        };

        if !should_replace {
            return;
        }

        // 기존 URL 가져오기
        let original_url = {
            let configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get(provider_index) {
                config.url.clone()
            } else {
                return;
            }
        };

        let provider_name = {
            let configs = self.provider_configs.lock().await;
            configs
                .get(provider_index)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("Provider{}", provider_index))
        };

        warn!(
            "[HealthCheck] 🔄 Attempting to replace failed provider[{}] {} - URL: {}",
            provider_index, provider_name, original_url
        );

        // 새로운 provider 생성 시도
        match Self::create_provider(&original_url).await {
            Ok(new_provider) => {
                // providers Vec 안전하게 업데이트
                {
                    let mut providers = self.providers.lock().await;
                    if provider_index < providers.len() {
                        // 기존 provider를 명시적으로 drop하여 WebSocket 연결과 subscription 정리
                        let old_provider = providers[provider_index].take();
                        if old_provider.is_some() {
                            info!(
                                "[HealthCheck] 🗑️ Dropping old provider[{}] {} to clean up subscriptions",
                                provider_index, provider_name
                            );
                        }
                        drop(old_provider); // 명시적 drop으로 WebSocket 연결 정리
                    } else {
                        error!(
                            "[HealthCheck] ❌ Invalid provider index for replacement: {}",
                            provider_index
                        );
                        return;
                    }
                }

                // WebSocket close handshake가 완료될 시간 제공 (100ms 대기)
                tokio::time::sleep(Duration::from_millis(100)).await;

                // 새 provider 설정
                {
                    let mut providers = self.providers.lock().await;
                    if provider_index < providers.len() {
                        providers[provider_index] = Some(new_provider);
                    }
                }

                // 설정 리셋 (새로운 연결이므로 점수 초기화)
                {
                    let mut configs = self.provider_configs.lock().await;
                    if let Some(config) = configs.get_mut(provider_index) {
                        config.score = 100.0;
                        config.success_count = 0;
                        config.fail_count = 0;
                        config.last_used = Some(std::time::Instant::now());
                    }
                }

                info!(
                    "[HealthCheck] ✅ Successfully replaced provider[{}] {} - Score reset to 100.0",
                    provider_index, provider_name
                );
            }
            Err(e) => {
                // 재연결 실패 시 기존 provider를 None으로 설정
                {
                    let mut providers = self.providers.lock().await;
                    if provider_index < providers.len() {
                        let old_provider = providers[provider_index].take();
                        drop(old_provider); // 실패한 경우에도 명시적 drop
                        providers[provider_index] = None;
                    }
                }

                // WebSocket close handshake 시간 제공
                tokio::time::sleep(Duration::from_millis(100)).await;

                warn!(
                    "[HealthCheck] ❌ Failed to replace provider[{}] {} - Error: {}, set to None",
                    provider_index, provider_name, e
                );
            }
        }
    }

    // Helper function to create a single provider instance (observer 스타일)
    async fn create_provider(url: &str) -> Result<DynProvider> {
        // WebSocket URL로 변환
        let ws_url = if url.starts_with("http://") {
            url.replace("http://", "ws://")
        } else if url.starts_with("https://") {
            url.replace("https://", "wss://")
        } else if url.starts_with("ws://") || url.starts_with("wss://") {
            url.to_string()
        } else {
            // 기본적으로 wss://로 가정
            format!("wss://{}", url)
        };

        // WebSocket 연결 (3초 timeout 적용)
        let ws = WsConnect::new(ws_url.clone());
        let ws_provider = tokio::time::timeout(
            Duration::from_secs(3),
            ProviderBuilder::new().connect_ws(ws),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Provider connection timeout after 3 seconds"))?;

        match ws_provider {
            Ok(ws_provider) => {
                info!(
                    "[HealthCheck] 🔧 Successfully created fresh provider: {}",
                    ws_url
                );

                // 새로운 provider에 대해 즉시 ping-pong 테스트
                let provider = DynProvider::new(ws_provider);
                let ping_test_result =
                    tokio::time::timeout(Duration::from_secs(3), provider.get_block_number()).await;

                match ping_test_result {
                    Ok(Ok(_)) => {
                        info!(
                            "[HealthCheck] ✅ Fresh provider ping test successful: {}",
                            ws_url
                        );
                        Ok(provider)
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "[HealthCheck] ❌ Fresh provider ping test failed: {} - {}",
                            ws_url, e
                        );
                        Err(anyhow::anyhow!("Provider ping test failed: {}", e))
                    }
                    Err(_) => {
                        warn!(
                            "[HealthCheck] ⏰ Fresh provider ping test timeout (3s): {}",
                            ws_url
                        );
                        Err(anyhow::anyhow!("Provider ping test timeout"))
                    }
                }
            }
            Err(ws_err) => {
                error!(
                    "[HealthCheck] ❌ Failed to create fresh provider {}: {}",
                    ws_url, ws_err
                );
                Err(anyhow::anyhow!("Failed to create provider: {}", ws_err))
            }
        }
    }

    // 현재 provider index 가져오기 (stream에서 변경 감지용)
    pub async fn get_current_provider_index(&self) -> usize {
        let index = self.current_index.lock().await;
        *index
    }

    // Timeout으로 인한 provider 재연결 (subscription cleanup 보장)
    pub async fn reconnect_current_provider(&self) -> Result<()> {
        let provider_index = *self.current_index.lock().await;

        // 기존 URL 가져오기
        let original_url = {
            let configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get(provider_index) {
                config.url.clone()
            } else {
                return Err(anyhow::anyhow!(
                    "Invalid provider index: {}",
                    provider_index
                ));
            }
        };

        let provider_name = {
            let configs = self.provider_configs.lock().await;
            configs
                .get(provider_index)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("Provider{}", provider_index))
        };

        // 재연결 시작: is_reconnecting 플래그 설정 (HealthCheck에서 skip하도록)
        {
            let mut configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get_mut(provider_index) {
                config.is_reconnecting = true;
            }
        }

        warn!(
            "[Reconnect] 🔄 Reconnecting provider[{}] {} due to timeout - URL: {}",
            provider_index, provider_name, original_url
        );

        // 기존 provider를 명시적으로 drop하여 WebSocket 연결과 모든 subscription 정리
        {
            let mut providers = self.providers.lock().await;
            if provider_index < providers.len() {
                let old_provider = providers[provider_index].take();
                if old_provider.is_some() {
                    info!(
                        "[Reconnect] 🗑️ Dropping old provider[{}] {} to clean up all subscriptions",
                        provider_index, provider_name
                    );
                }
                drop(old_provider); // WebSocket 연결 및 모든 subscription 정리
            }
        }

        // WebSocket close handshake가 완료될 시간 제공 (200ms 대기)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 새로운 provider 생성 시도
        let result = match Self::create_provider(&original_url).await {
            Ok(new_provider) => {
                // 새 provider 설정
                {
                    let mut providers = self.providers.lock().await;
                    if provider_index < providers.len() {
                        providers[provider_index] = Some(new_provider);
                    }
                }

                info!(
                    "[Reconnect] ✅ Successfully reconnected provider[{}] {}",
                    provider_index, provider_name
                );
                Ok(())
            }
            Err(e) => {
                // 재연결 실패 시 provider를 None으로 유지
                warn!(
                    "[Reconnect] ❌ Failed to reconnect provider[{}] {} - Error: {}",
                    provider_index, provider_name, e
                );

                // 다른 provider로 전환 시도
                self.update_best_provider().await;

                Err(e)
            }
        };

        // 재연결 완료: is_reconnecting 플래그 해제
        {
            let mut configs = self.provider_configs.lock().await;
            if let Some(config) = configs.get_mut(provider_index) {
                config.is_reconnecting = false;
            }
        }

        result
    }

    // 점수 기반 요청 실행 (observer v2.2 스타일)
    async fn execute_with_fallback<F, T>(&self, operation: F) -> Result<T>
    where
        F: Fn(&DynProvider) -> Pin<Box<dyn Future<Output = Result<T>> + Send + '_>>,
    {
        // 최고 점수 provider부터 시작
        let best_index = self.select_best_provider().await;

        // None이 아닌 활성 provider만 필터링
        let active_providers: Vec<(usize, DynProvider)> = {
            let providers = self.providers.lock().await;
            providers
                .iter()
                .enumerate()
                .filter_map(|(idx, provider_opt)| provider_opt.as_ref().map(|p| (idx, p.clone())))
                .collect()
        };

        // 활성 provider가 없으면 즉시 에러 반환
        if active_providers.is_empty() {
            warn!("[HealthCheck] ❌ No active providers available");
            return Err(anyhow::anyhow!("No active providers available"));
        }

        let providers_count = active_providers.len();
        let mut last_error = None;

        // best_index를 기준으로 활성 provider 목록에서 시작 위치 찾기
        let start_position = active_providers
            .iter()
            .position(|(idx, _)| *idx == best_index)
            .unwrap_or(0);

        for attempt in 0..providers_count {
            let position = (start_position + attempt) % providers_count;
            let (provider_index, provider) = &active_providers[position];

            let provider_name = {
                let configs = self.provider_configs.lock().await;
                configs
                    .get(*provider_index)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| format!("Provider{}", provider_index))
            };

            match tokio::time::timeout(Duration::from_millis(*RPC_TIME_OUT), operation(provider))
                .await
            {
                Ok(Ok(result)) => {
                    // 성공 기록
                    self.record_provider_success(*provider_index).await;

                    // current_index 업데이트
                    {
                        let mut index = self.current_index.lock().await;
                        *index = *provider_index;
                    }

                    if attempt > 0 {
                        info!(
                            "[HealthCheck] ✅ Provider[{}] {} succeeded on fallback attempt {}",
                            provider_index,
                            provider_name,
                            attempt + 1
                        );
                    }
                    return Ok(result);
                }
                Ok(Err(e)) => {
                    self.record_provider_failure(*provider_index).await;
                    warn!(
                        "[HealthCheck] ❌ Provider[{}] {} RPC error in fallback: {}",
                        provider_index, provider_name, e
                    );

                    last_error = Some(e);
                    if attempt < providers_count - 1 {
                        let next_position = (position + 1) % providers_count;
                        let (next_index, _) = &active_providers[next_position];
                        let next_name = {
                            let configs = self.provider_configs.lock().await;
                            configs
                                .get(*next_index)
                                .map(|c| c.name.clone())
                                .unwrap_or_else(|| format!("Provider{}", next_index))
                        };
                        warn!(
                            "[HealthCheck] 🔄 Trying next provider: [{}] {}",
                            next_index, next_name
                        );
                    }
                }
                Err(_) => {
                    self.record_provider_failure(*provider_index).await;
                    warn!(
                        "[HealthCheck] ⏰ Provider[{}] {} timeout ({}ms) in fallback",
                        provider_index, provider_name, *RPC_TIME_OUT
                    );

                    last_error = Some(anyhow::anyhow!("Request timeout"));
                    if attempt < providers_count - 1 {
                        let next_position = (position + 1) % providers_count;
                        let (next_index, _) = &active_providers[next_position];
                        let next_name = {
                            let configs = self.provider_configs.lock().await;
                            configs
                                .get(*next_index)
                                .map(|c| c.name.clone())
                                .unwrap_or_else(|| format!("Provider{}", next_index))
                        };
                        warn!(
                            "[HealthCheck] 🔄 Trying next provider: [{}] {}",
                            next_index, next_name
                        );
                    }
                }
            }

            // 마지막 시도가 아닌 경우에만 짧은 대기 (500ms → 100ms로 최적화)
            if attempt < providers_count - 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("All providers failed")))
    }

    /// 표준 logs 구독 — chain head를 따라가며, reorg 시 같은 로그가
    /// removed=true 플래그와 함께 재전달될 수 있다 (호출자가 필터링).
    pub async fn get_stream(&self, filter: &Filter) -> Result<SubscriptionStream<Log>> {
        let provider = self.get_current_provider().await?;
        let stream = measure_rpc!("subscribe_logs", provider.subscribe_logs(filter))?.into_stream();
        Ok(stream)
    }

    // 실시간으로 최신 블록 번호 가져오기 (네트워크 요청)
    pub async fn get_latest_block_number(&self) -> Result<u64> {
        measure_rpc!(
            "get_latest_block_number",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_block_number()
                        .await
                        .context("Failed to get latest block number")
                })
            })
        )
    }

    async fn get_block_by_number(&self, block_number: u64) -> Result<Option<Block>> {
        measure_rpc!(
            "get_block_by_number",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_block_by_number(BlockNumberOrTag::Number(block_number))
                        .await
                        .context(format!("Failed to get block by number {}", block_number))
                })
            })
        )
    }

    pub async fn get_block_timestamp(&self, block_number: u64) -> Result<u64> {
        let block = self.get_block_by_number(block_number).await?;

        match block {
            Some(b) => Ok(b.header.timestamp),
            None => Ok(chrono::Utc::now().timestamp() as u64),
        }
    }

    pub async fn get_transaction_by_hash(&self, hash: TxHash) -> Result<Option<Transaction>> {
        measure_rpc!(
            "get_transaction_by_hash",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_transaction_by_hash(hash)
                        .await
                        .context("Failed to get transaction by hash")
                })
            })
        )
    }

    pub async fn get_code(&self, address: alloy::primitives::Address) -> Result<alloy::primitives::Bytes> {
        measure_rpc!(
            "get_code",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_code_at(address)
                        .await
                        .context("Failed to get code")
                })
            })
        )
    }

    pub async fn get_transaction_receipt(&self, hash: TxHash) -> Result<Option<alloy::rpc::types::TransactionReceipt>> {
        measure_rpc!(
            "get_transaction_receipt",
            self.execute_with_fallback(|provider| {
                Box::pin(async move {
                    provider
                        .get_transaction_receipt(hash)
                        .await
                        .context("Failed to get transaction receipt")
                })
            })
        )
    }
}
