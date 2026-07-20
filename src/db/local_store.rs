//! 로컬 메모리 저장소
//! Redis 대신 DashMap을 사용하여 metrics/market 데이터를 저장합니다.
//! 인스턴스별로 독립적인 데이터이므로 로컬 메모리가 더 효율적입니다.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bigdecimal::BigDecimal;
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::types::chart::{Chart, ChartInterval, ChartUpdateOrder, ChartUpdateParams};
use crate::types::metrics::{PriceSnapshot, SwapSnapshot};
use crate::types::{AccountInfo, TokenInfo};

/// 전역 LocalStore 인스턴스
static LOCAL_STORE: OnceCell<Arc<LocalStore>> = OnceCell::new();

/// 토큰별 Metrics 데이터 (price, swap 히스토리)
#[derive(Debug, Default)]
pub struct TokenMetricsData {
    /// Price 히스토리 (timestamp -> PriceSnapshot)
    /// BTreeMap으로 timestamp 정렬 유지
    pub prices: RwLock<BTreeMap<i64, PriceSnapshot>>,

    /// Swap 히스토리 (timestamp -> SwapSnapshot)
    /// 같은 timestamp에 여러 swap이 있을 수 있으므로 Vec 사용
    pub swaps: RwLock<Vec<SwapSnapshot>>,

    /// DB에서 25시간 히스토리를 로드했는지 여부
    /// true면 DB 로드 완료, false면 아직 로드 안됨
    pub db_loaded: std::sync::atomic::AtomicBool,
}

/// 토큰별 Market 데이터 (현재 가격, reserve, volume 등)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenMarketData {
    pub market_id: String,
    pub quote_info: crate::types::QuoteInfo,
    pub price: BigDecimal,
    pub reserve_native: BigDecimal,
    pub reserve_token: BigDecimal,
    pub total_supply: BigDecimal,
    pub fdv: BigDecimal,
    pub volume_24h: BigDecimal,
    /// ATH 가격 (USD) - 기존 호환성 유지
    pub ath_price: BigDecimal,
    /// ATH 가격 (Native/MON)
    pub ath_price_native: BigDecimal,
    pub holder_count: i64,
    pub is_graduated: bool,
    /// 마켓 타입 (Curve/Dex)
    /// 현재는 is_graduated와 1:1 대응이라 값 자체는 중복이지만, socket 응답이
    /// 이 필드를 그대로 내려주므로 조회 시점에 derive하지 않고 저장해 둔다.
    /// (derive하던 시절 graduate 후에도 Dex 토큰이 Curve로 내려가는 버그가 있었다)
    #[serde(default)]
    pub market_type: crate::types::MarketType,
    /// total_supply, holder_count 마지막 갱신 시각 (unix timestamp)
    #[serde(skip)]
    pub last_stats_update: i64,
}

/// 토큰별 Chart 데이터 (interval별 현재 캔들)
/// key: interval (예: "1", "5", "1H", "D" 등)
#[derive(Debug, Default)]
pub struct TokenChartData {
    /// interval별 현재 캔들 (interval -> Chart)
    pub candles: RwLock<HashMap<String, Chart>>,
    /// interval별 현재 캔들의 순서 및 거래 가격 범위 상태
    pub orders: RwLock<HashMap<String, ChartCandleState>>,
}

#[derive(Debug, Clone)]
pub struct ChartCandleState {
    pub timestamp: i64,
    pub first: Option<ChartUpdateOrder>,
    pub last: Option<ChartUpdateOrder>,
    pub open_order: Option<ChartUpdateOrder>,
    pub price_high: BigDecimal,
    pub price_low: BigDecimal,
    pub usd_price_high: BigDecimal,
    pub usd_price_low: BigDecimal,
}

impl ChartCandleState {
    fn new(
        timestamp: i64,
        order: Option<ChartUpdateOrder>,
        open_order: Option<ChartUpdateOrder>,
        price: &BigDecimal,
        usd_price: &BigDecimal,
    ) -> Self {
        Self {
            timestamp,
            first: order,
            last: order,
            open_order,
            price_high: price.clone(),
            price_low: price.clone(),
            usd_price_high: usd_price.clone(),
            usd_price_low: usd_price.clone(),
        }
    }

    fn from_chart(chart: &Chart) -> Self {
        Self {
            timestamp: chart.t,
            first: None,
            last: None,
            open_order: None,
            price_high: chart.h.clone(),
            price_low: chart.l.clone(),
            usd_price_high: chart.usd_h.clone(),
            usd_price_low: chart.usd_l.clone(),
        }
    }

    fn record_price(&mut self, price: &BigDecimal, usd_price: &BigDecimal) {
        if price > &self.price_high {
            self.price_high = price.clone();
        }
        if price < &self.price_low {
            self.price_low = price.clone();
        }
        if usd_price > &self.usd_price_high {
            self.usd_price_high = usd_price.clone();
        }
        if usd_price < &self.usd_price_low {
            self.usd_price_low = usd_price.clone();
        }
    }
}

/// 로컬 메모리 저장소
/// DashMap을 사용하여 동시성 안전하게 데이터 관리
pub struct LocalStore {
    /// 토큰별 Metrics 데이터
    pub metrics: DashMap<String, TokenMetricsData>,

    /// 토큰별 Market 데이터
    pub market: DashMap<String, TokenMarketData>,

    /// 토큰별 Chart 데이터
    pub chart: DashMap<String, TokenChartData>,

    //-------------------------------------------------------------------------
    // Redis 대체 캐시 데이터 (기존 Redis에서 이동)
    //-------------------------------------------------------------------------
    /// 토큰 화이트리스트 (token_id -> is_white)
    pub white_list_token: DashMap<String, bool>,

    /// POOL 화이트리스트 (pool_id -> is_white)
    pub white_list_pool: DashMap<String, bool>,

    /// 토큰 개발자 정보 (token_id -> account)
    pub token_dev: DashMap<String, String>,

    /// 토큰-POOL 매핑 (token_id -> pool_id)
    pub token_pool: DashMap<String, String>,

    /// POOL 페어 정보 (pool_id -> (token0, token1))
    pub pool_pair: DashMap<String, (String, String)>,

    /// 토큰 정보 (token_id -> TokenInfo)
    pub token_info: DashMap<String, TokenInfo>,

    /// 계정 정보 (account_id -> AccountInfo)
    pub account_info: DashMap<String, AccountInfo>,

    /// 토큰 총 공급량 (token_id -> total_supply)
    pub token_total_supply: DashMap<String, String>,

    /// 최신 네이티브 가격
    pub latest_price: RwLock<Option<String>>,

    /// 블록 타임스탬프 (block_number -> timestamp)
    pub block_timestamp: DashMap<u64, u64>,
}

impl LocalStore {
    fn recalculate_chart_extrema(chart: &mut Chart, state: &ChartCandleState) {
        chart.h = if chart.o > state.price_high {
            chart.o.clone()
        } else {
            state.price_high.clone()
        };
        chart.l = if chart.o < state.price_low {
            chart.o.clone()
        } else {
            state.price_low.clone()
        };
        chart.usd_h = if chart.usd_o > state.usd_price_high {
            chart.usd_o.clone()
        } else {
            state.usd_price_high.clone()
        };
        chart.usd_l = if chart.usd_o < state.usd_price_low {
            chart.usd_o.clone()
        } else {
            state.usd_price_low.clone()
        };
    }

    fn is_immediate_previous_candle(
        interval: &str,
        previous_timestamp: i64,
        current_timestamp: i64,
    ) -> bool {
        ChartInterval::try_from(interval)
            .map(|interval| {
                interval.previous_candle_start(current_timestamp) == previous_timestamp
            })
            .unwrap_or(false)
    }

    /// LocalStore 초기화
    pub fn init() {
        if LOCAL_STORE.get().is_some() {
            info!("LocalStore already initialized");
            return;
        }

        let store = LocalStore {
            metrics: DashMap::new(),
            market: DashMap::new(),
            chart: DashMap::new(),
            // Redis 대체 캐시 데이터
            white_list_token: DashMap::new(),
            white_list_pool: DashMap::new(),
            token_dev: DashMap::new(),
            token_pool: DashMap::new(),
            pool_pair: DashMap::new(),
            token_info: DashMap::new(),
            account_info: DashMap::new(),
            token_total_supply: DashMap::new(),
            latest_price: RwLock::new(None),
            block_timestamp: DashMap::new(),
        };

        if LOCAL_STORE.set(Arc::new(store)).is_err() {
            info!("LocalStore was initialized by another task");
        } else {
            info!("LocalStore global instance initialized successfully");
        }
    }

    /// 글로벌 인스턴스 가져오기
    pub fn instance() -> Arc<LocalStore> {
        LOCAL_STORE
            .get()
            .map(Arc::clone)
            .expect("LocalStore not initialized. Call LocalStore::init() first")
    }

    //-------------------------------------------------------------------------
    // Metrics 관련 메서드들
    //-------------------------------------------------------------------------

    /// Metrics 데이터가 존재하는지 확인
    pub fn exists_metrics(&self, token_id: &str) -> bool {
        if let Some(data) = self.metrics.get(token_id) {
            let prices = data.prices.read();
            let swaps = data.swaps.read();
            !prices.is_empty() || !swaps.is_empty()
        } else {
            false
        }
    }

    /// DB에서 metrics 히스토리를 로드했는지 확인
    pub fn is_metrics_db_loaded(&self, token_id: &str) -> bool {
        if let Some(data) = self.metrics.get(token_id) {
            data.db_loaded.load(std::sync::atomic::Ordering::Relaxed)
        } else {
            false
        }
    }

    /// DB에서 metrics 히스토리 로드 완료 표시
    pub fn set_metrics_db_loaded(&self, token_id: &str, loaded: bool) {
        let entry = self.metrics.entry(token_id.to_string()).or_default();
        entry
            .db_loaded
            .store(loaded, std::sync::atomic::Ordering::Relaxed);
    }

    /// Price 데이터 추가
    pub fn add_price(&self, token_id: &str, snapshot: PriceSnapshot) {
        let entry = self.metrics.entry(token_id.to_string()).or_default();
        let mut prices = entry.prices.write();
        match prices.get(&snapshot.timestamp) {
            Some(existing)
                if (
                    existing.block_number,
                    existing.tx_index,
                    existing.log_index,
                ) > (snapshot.block_number, snapshot.tx_index, snapshot.log_index) => {}
            _ => {
                prices.insert(snapshot.timestamp, snapshot);
            }
        }
    }

    /// Swap 데이터 추가
    pub fn add_swap(&self, token_id: &str, snapshot: SwapSnapshot) {
        let entry = self.metrics.entry(token_id.to_string()).or_default();
        let mut swaps = entry.swaps.write();
        swaps.push(snapshot);
    }

    /// 특정 시간 이전의 price 데이터 삭제
    pub fn remove_old_prices(&self, token_id: &str, cutoff: i64) {
        if let Some(data) = self.metrics.get(token_id) {
            let mut prices = data.prices.write();
            // cutoff 이전 데이터 삭제 (cutoff 포함하지 않음)
            prices.retain(|&ts, _| ts >= cutoff);
        }
    }

    /// 특정 시간 이전의 swap 데이터 삭제
    pub fn remove_old_swaps(&self, token_id: &str, cutoff: i64) {
        if let Some(data) = self.metrics.get(token_id) {
            let mut swaps = data.swaps.write();
            swaps.retain(|s| s.timestamp >= cutoff);
        }
    }

    /// 특정 시간 이후의 price 데이터 조회
    pub fn get_prices_since(&self, token_id: &str, since: i64) -> Vec<PriceSnapshot> {
        if let Some(data) = self.metrics.get(token_id) {
            let prices = data.prices.read();
            prices.range(since..).map(|(_, v)| v.clone()).collect()
        } else {
            Vec::new()
        }
    }

    /// 특정 시간 이후의 swap 데이터 조회
    pub fn get_swaps_since(&self, token_id: &str, since: i64) -> Vec<SwapSnapshot> {
        if let Some(data) = self.metrics.get(token_id) {
            let swaps = data.swaps.read();
            swaps
                .iter()
                .filter(|s| s.timestamp >= since)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// 모든 price 데이터 조회 (정렬된 상태)
    pub fn get_all_prices(&self, token_id: &str) -> Vec<PriceSnapshot> {
        if let Some(data) = self.metrics.get(token_id) {
            let prices = data.prices.read();
            prices.values().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// 모든 swap 데이터 조회
    pub fn get_all_swaps(&self, token_id: &str) -> Vec<SwapSnapshot> {
        if let Some(data) = self.metrics.get(token_id) {
            let swaps = data.swaps.read();
            swaps.clone()
        } else {
            Vec::new()
        }
    }

    //-------------------------------------------------------------------------
    // Market 관련 메서드들
    //-------------------------------------------------------------------------

    /// Market 데이터 존재 여부 확인
    pub fn exists_market(&self, token_id: &str) -> bool {
        self.market.contains_key(token_id)
    }

    /// Market 데이터 조회
    pub fn get_market(&self, token_id: &str) -> Option<TokenMarketData> {
        self.market.get(token_id).map(|v| v.clone())
    }

    /// Market 데이터 설정 (전체 업데이트)
    pub fn set_market(&self, token_id: &str, data: TokenMarketData) {
        self.market.insert(token_id.to_string(), data);
    }

    /// Market 데이터 부분 업데이트
    pub fn update_market<F>(&self, token_id: &str, updater: F)
    where
        F: FnOnce(&mut TokenMarketData),
    {
        let mut entry = self.market.entry(token_id.to_string()).or_default();
        updater(entry.value_mut());
    }

    /// Volume 증가
    pub fn increment_volume(&self, token_id: &str, amount: &BigDecimal) {
        self.update_market(token_id, |data| {
            data.volume_24h = &data.volume_24h + amount;
        });
    }

    //-------------------------------------------------------------------------
    // Chart 관련 메서드들
    //-------------------------------------------------------------------------

    /// Chart 데이터 조회
    pub fn get_chart(&self, token_id: &str, interval: &str) -> Option<Chart> {
        if let Some(data) = self.chart.get(token_id) {
            let candles = data.candles.read();
            candles.get(interval).cloned()
        } else {
            None
        }
    }

    /// Chart 데이터 설정
    pub fn set_chart(&self, token_id: &str, interval: &str, chart: Chart) {
        let entry = self.chart.entry(token_id.to_string()).or_default();
        let mut candles = entry.candles.write();
        candles.insert(interval.to_string(), chart);
        entry.orders.write().remove(interval);
    }

    /// Chart atomic 업데이트 (OHLCV + USD OHLCV + total_supply 계산)
    /// 같은 타임스탬프면 업데이트, 미래 타임스탬프면 새 캔들 생성
    /// 직전 캔들 이벤트가 늦게 도착하면 현재 캔들의 open만 보정
    /// 반환: 업데이트된 Chart
    ///
    /// # Arguments
    /// * `params` - 차트 업데이트 파라미터
    pub fn update_chart_atomic(&self, params: &ChartUpdateParams) -> Option<Chart> {
        use crate::types::chart::CHART_STATUS_OK;
        use bigdecimal::RoundingMode;

        let token_id = params.token_id;
        let interval = params.interval;
        let timestamp = params.timestamp;
        let native_price = params.native_price;
        let total_supply = params.total_supply;
        let order = params.order;
        let interval_key = interval.to_string();

        let entry = self.chart.entry(token_id.to_string()).or_default();
        let mut candles = entry.candles.write();
        let mut orders = entry.orders.write();

        // price를 소수점 10자리로 라운딩 (RoundUp)
        let price = params.price.with_scale_round(10, RoundingMode::Up);
        // USD 가격 계산
        let usd_price = (&price * native_price).with_scale_round(10, RoundingMode::Up);

        let volume_val = params.volume.cloned().unwrap_or_default();
        let usd_volume_val = &volume_val * native_price;
        let is_create_curve = params.volume.is_none() || volume_val == BigDecimal::from(0);

        if let Some(existing) = candles.get_mut(interval) {
            debug!(
                "[CHART_LOCAL] token={}, interval={}, input: price={}, usd_price={}, volume={}, is_create_curve={}, existing: o={}, h={}, l={}, c={}, t={}",
                token_id, interval, price, usd_price, volume_val, is_create_curve,
                existing.o, existing.h, existing.l, existing.c, existing.t
            );

            if existing.t == timestamp {
                let state = orders
                    .entry(interval_key.clone())
                    .or_insert_with(|| ChartCandleState::from_chart(existing));
                if state.timestamp != timestamp {
                    *state = ChartCandleState::new(timestamp, order, None, &price, &usd_price);
                }

                // 같은 캔들 업데이트
                if is_create_curve {
                    // CreateCurve (volume=0): Open과 Low만 업데이트
                    let should_update_open = match order {
                        Some(incoming) => {
                            if state.first.map_or(true, |first| incoming <= first) {
                                state.first = Some(incoming);
                                true
                            } else {
                                false
                            }
                        }
                        None => true,
                    };
                    if should_update_open {
                        existing.o = price.clone();
                        existing.usd_o = usd_price.clone();
                    }
                    state.record_price(&price, &usd_price);
                    Self::recalculate_chart_extrema(existing, state);
                    existing.total_supply = total_supply.clone();
                    debug!(
                        "[CHART_LOCAL] SAME_CANDLE CreateCurve: token={}, interval={}, updated: o={}, l={}, usd_o={}, usd_l={}",
                        token_id, interval, existing.o, existing.l, existing.usd_o, existing.usd_l
                    );
                } else {
                    // 일반 거래: OHLCV 모두 업데이트
                    state.record_price(&price, &usd_price);
                    let should_update_close = match order {
                        Some(incoming) => {
                            if state.first.map_or(true, |first| incoming < first) {
                                state.first = Some(incoming);
                            }
                            if state.last.map_or(true, |last| incoming >= last) {
                                state.last = Some(incoming);
                                true
                            } else {
                                false
                            }
                        }
                        None => true,
                    };
                    if should_update_close {
                        existing.c = price.clone();
                        existing.usd_c = usd_price.clone();
                    }
                    Self::recalculate_chart_extrema(existing, state);
                    existing.v = &existing.v + &volume_val;
                    existing.usd_v = &existing.usd_v + &usd_volume_val;
                    existing.total_supply = total_supply.clone();
                    debug!(
                        "[CHART_LOCAL] SAME_CANDLE Trade: token={}, interval={}, updated: h={}, l={}, c={}, v={}, usd_h={}, usd_l={}, usd_c={}",
                        token_id, interval, existing.h, existing.l, existing.c, existing.v, existing.usd_h, existing.usd_l, existing.usd_c
                    );
                }
                Some(existing.clone())
            } else if timestamp > existing.t {
                // 새 캔들: 이전 close를 open으로 (USD도 동일하게)
                let prev_close_order = orders.get(interval).and_then(|state| {
                    if state.timestamp == existing.t {
                        state.last
                    } else {
                        None
                    }
                });
                let prev_close = existing.c.clone();
                let prev_usd_close = existing.usd_c.clone();

                // MON/TOKEN 가격: high = max(prev_close, price), low = min(prev_close, price)
                let new_high = if prev_close > price {
                    prev_close.clone()
                } else {
                    price.clone()
                };
                let new_low = if prev_close < price {
                    prev_close.clone()
                } else {
                    price.clone()
                };

                // USD 가격: 독립적으로 비교
                let new_usd_high = if prev_usd_close > usd_price {
                    prev_usd_close.clone()
                } else {
                    usd_price.clone()
                };
                let new_usd_low = if prev_usd_close < usd_price {
                    prev_usd_close.clone()
                } else {
                    usd_price.clone()
                };

                debug!(
                    "[CHART_LOCAL] NEW_CANDLE: token={}, interval={}, prev_close={}, price={}, new_high={}, new_low={}, new_t={}, usd_open={}, usd_high={}, usd_low={}",
                    token_id, interval, prev_close, price, new_high, new_low, timestamp, prev_usd_close, new_usd_high, new_usd_low
                );

                let new_chart = Chart {
                    s: CHART_STATUS_OK.to_string(),
                    o: prev_close,
                    h: new_high,
                    l: new_low,
                    c: price.clone(),
                    v: volume_val,
                    t: timestamp,
                    usd_o: prev_usd_close,
                    usd_h: new_usd_high,
                    usd_l: new_usd_low,
                    usd_c: usd_price.clone(),
                    usd_v: usd_volume_val,
                    total_supply: total_supply.clone(),
                };
                candles.insert(interval_key.clone(), new_chart.clone());
                orders.insert(
                    interval_key,
                    ChartCandleState::new(
                        timestamp,
                        order,
                        prev_close_order,
                        &price,
                        &usd_price,
                    ),
                );
                Some(new_chart)
            } else {
                if Self::is_immediate_previous_candle(interval, timestamp, existing.t) {
                    if let Some(incoming) = order {
                        let state = orders
                            .entry(interval_key.clone())
                            .or_insert_with(|| ChartCandleState::from_chart(existing));
                        if state.timestamp != existing.t {
                            *state = ChartCandleState::from_chart(existing);
                        }
                        if state
                            .open_order
                            .map_or(true, |open_order| incoming >= open_order)
                        {
                            state.open_order = Some(incoming);
                            existing.o = price.clone();
                            existing.usd_o = usd_price.clone();
                            Self::recalculate_chart_extrema(existing, state);
                            debug!(
                                "[CHART_LOCAL] LATE_PREVIOUS_CANDLE adjusted current open: token={}, interval={}, previous_t={}, current_t={}, open={}, usd_open={}",
                                token_id, interval, timestamp, existing.t, existing.o, existing.usd_o
                            );
                            return Some(existing.clone());
                        }
                    }
                }
                // 라이브 스트림은 로그별 비동기 처리라 늦게 도착한 이전 캔들 이벤트가
                // 현재 캔들을 덮을 수 있다. 이 저장소는 interval별 최신 캔들 1개만
                // 들고 있으므로 과거 캔들은 여기서 갱신하지 않고 최신 상태를 유지한다.
                debug!(
                    "[CHART_LOCAL] OUT_OF_ORDER_OLD_CANDLE ignored: token={}, interval={}, input_t={}, current_t={}",
                    token_id, interval, timestamp, existing.t
                );
                Some(existing.clone())
            }
        } else {
            // 캔들이 없음 - None 반환 (PostgreSQL에서 로드 필요)
            debug!(
                "[CHART_LOCAL] NO_CANDLE: token={}, interval={}, need to load from PostgreSQL",
                token_id, interval
            );
            None
        }
    }

    /// Chart 존재 여부 확인
    pub fn exists_chart(&self, token_id: &str, interval: &str) -> bool {
        if let Some(data) = self.chart.get(token_id) {
            let candles = data.candles.read();
            candles.contains_key(interval)
        } else {
            false
        }
    }

    //-------------------------------------------------------------------------
    // 정리 메서드들
    //-------------------------------------------------------------------------

    /// 특정 토큰의 모든 데이터 삭제
    pub fn remove_token(&self, token_id: &str) {
        self.metrics.remove(token_id);
        self.market.remove(token_id);
        self.chart.remove(token_id);
    }

    /// 모든 데이터 삭제 (테스트용)
    pub fn clear_all(&self) {
        self.metrics.clear();
        self.market.clear();
        self.chart.clear();
    }

    /// Metrics 데이터 개수 반환 (디버깅용)
    pub fn metrics_count(&self) -> usize {
        self.metrics.len()
    }

    /// Market 데이터 개수 반환 (디버깅용)
    pub fn market_count(&self) -> usize {
        self.market.len()
    }

    /// Chart 데이터 개수 반환 (디버깅용)
    pub fn chart_count(&self) -> usize {
        self.chart.len()
    }

    //-------------------------------------------------------------------------
    // 화이트리스트 토큰 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// 화이트리스트에 토큰 추가
    pub fn insert_white_list_token(&self, token: &str, is_white: bool) {
        self.white_list_token.insert(token.to_string(), is_white);
        debug!("White list token inserted: {} = {}", token, is_white);
    }

    /// 토큰이 화이트리스트에 있는지 확인
    pub fn check_white_list_token(&self, token: &str) -> Option<bool> {
        self.white_list_token.get(token).map(|v| *v)
    }

    //-------------------------------------------------------------------------
    // 화이트리스트 POOL 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// 화이트리스트에 POOL 추가
    pub fn insert_white_list_pool(&self, pool: &str, is_white: bool) {
        self.white_list_pool.insert(pool.to_string(), is_white);
        debug!("White list pool inserted: {} = {}", pool, is_white);
    }

    /// POOL이 화이트리스트에 있는지 확인
    pub fn check_white_list_pool(&self, pool: &str) -> Option<bool> {
        self.white_list_pool.get(pool).map(|v| *v)
    }

    //-------------------------------------------------------------------------
    // 토큰-개발자 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// 토큰 개발자 정보 저장
    pub fn insert_token_dev(&self, token: &str, account: &str) {
        self.token_dev
            .insert(token.to_string(), account.to_string());
        debug!(
            "Token dev mapping stored: token={}, account={}",
            token, account
        );
    }

    /// 계정이 토큰의 개발자인지 확인
    pub fn check_token_dev(&self, token: &str, account: &str) -> bool {
        self.token_dev
            .get(token)
            .is_some_and(|dev| dev.value() == account)
    }

    /// 토큰 개발자 정보 조회
    pub fn get_token_dev(&self, token: &str) -> Option<String> {
        self.token_dev.get(token).map(|v| v.clone())
    }

    //-------------------------------------------------------------------------
    // 토큰-POOL 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// 토큰-POOL 관계 저장
    pub fn insert_token_pool(&self, token: &str, pool: &str) {
        self.token_pool.insert(token.to_string(), pool.to_string());
        debug!("Token pool mapping stored: token={}, pool={}", token, pool);
    }

    /// 토큰에 대한 POOL 정보 조회
    pub fn get_token_pool(&self, token: &str) -> Option<String> {
        self.token_pool.get(token).map(|v| v.clone())
    }

    //-------------------------------------------------------------------------
    // POOL 페어 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// POOL 페어 정보 저장 (token0, token1)
    pub fn insert_pool_pair(&self, pool: &str, token0: &str, token1: &str) {
        self.pool_pair
            .insert(pool.to_string(), (token0.to_string(), token1.to_string()));
        debug!(
            "Pool pair stored: pool={}, token0={}, token1={}",
            pool, token0, token1
        );
    }

    /// POOL 페어 정보 조회
    pub fn get_pool_pair(&self, pool: &str) -> Option<(String, String)> {
        self.pool_pair.get(pool).map(|v| v.clone())
    }

    //-------------------------------------------------------------------------
    // TokenInfo 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// TokenInfo 저장
    pub fn set_token_info(&self, token_id: &str, info: TokenInfo) {
        self.token_info.insert(token_id.to_string(), info);
        debug!("Token info stored: token_id={}", token_id);
    }

    /// TokenInfo 조회
    pub fn get_token_info(&self, token_id: &str) -> Option<TokenInfo> {
        self.token_info.get(token_id).map(|v| v.clone())
    }

    //-------------------------------------------------------------------------
    // AccountInfo 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// AccountInfo 저장
    pub fn set_account_info(&self, account_id: &str, info: AccountInfo) {
        self.account_info.insert(account_id.to_string(), info);
        debug!("Account info stored: account_id={}", account_id);
    }

    /// AccountInfo 조회
    pub fn get_account_info(&self, account_id: &str) -> Option<AccountInfo> {
        self.account_info.get(account_id).map(|v| v.clone())
    }

    //-------------------------------------------------------------------------
    // 토큰 총 공급량 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// 토큰 총 공급량 저장
    pub fn set_token_total_supply(&self, token_id: &str, total_supply: &str) {
        self.token_total_supply
            .insert(token_id.to_string(), total_supply.to_string());
        debug!(
            "Token total supply stored: token_id={}, total_supply={}",
            token_id, total_supply
        );
    }

    /// 토큰 총 공급량 조회
    pub fn get_token_total_supply(&self, token_id: &str) -> Option<String> {
        self.token_total_supply.get(token_id).map(|v| v.clone())
    }

    //-------------------------------------------------------------------------
    // 최신 가격 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// 최신 네이티브 가격 저장
    pub fn set_latest_price(&self, price: &str) {
        let mut latest = self.latest_price.write();
        *latest = Some(price.to_string());
        debug!("Latest price stored: {}", price);
    }

    /// 최신 네이티브 가격 조회
    pub fn get_latest_price(&self) -> Option<String> {
        let latest = self.latest_price.read();
        latest.clone()
    }

    //-------------------------------------------------------------------------
    // 블록 타임스탬프 관련 메서드들 (Redis 대체)
    //-------------------------------------------------------------------------

    /// 블록 타임스탬프 저장
    pub fn set_block_timestamp(&self, block_number: u64, timestamp: u64) {
        self.block_timestamp.insert(block_number, timestamp);
        debug!(
            "Block timestamp stored: block={}, timestamp={}",
            block_number, timestamp
        );
    }

    /// 블록 타임스탬프 조회
    pub fn get_block_timestamp(&self, block_number: u64) -> Option<u64> {
        self.block_timestamp.get(&block_number).map(|v| *v)
    }

    //-------------------------------------------------------------------------
    // 캐시 통계 메서드
    //-------------------------------------------------------------------------

    /// 캐시 통계 조회
    pub fn get_cache_stats(&self) -> CacheStats {
        CacheStats {
            white_list_tokens: self.white_list_token.len(),
            white_list_pools: self.white_list_pool.len(),
            token_devs: self.token_dev.len(),
            token_pools: self.token_pool.len(),
            pool_pairs: self.pool_pair.len(),
            token_infos: self.token_info.len(),
            account_infos: self.account_info.len(),
            token_total_supplies: self.token_total_supply.len(),
            block_timestamps: self.block_timestamp.len(),
        }
    }
}

/// 캐시 통계 구조체
#[derive(Debug)]
pub struct CacheStats {
    pub white_list_tokens: usize,
    pub white_list_pools: usize,
    pub token_devs: usize,
    pub token_pools: usize,
    pub pool_pairs: usize,
    pub token_infos: usize,
    pub account_infos: usize,
    pub token_total_supplies: usize,
    pub block_timestamps: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::chart::{ChartUpdateParams, CHART_STATUS_OK};
    use dashmap::DashMap;
    use parking_lot::RwLock;
    use std::str::FromStr;

    fn bd(value: &str) -> BigDecimal {
        BigDecimal::from_str(value).unwrap()
    }

    fn test_store() -> LocalStore {
        LocalStore {
            metrics: DashMap::new(),
            market: DashMap::new(),
            chart: DashMap::new(),
            white_list_token: DashMap::new(),
            white_list_pool: DashMap::new(),
            token_dev: DashMap::new(),
            token_pool: DashMap::new(),
            pool_pair: DashMap::new(),
            token_info: DashMap::new(),
            account_info: DashMap::new(),
            token_total_supply: DashMap::new(),
            latest_price: RwLock::new(None),
            block_timestamp: DashMap::new(),
        }
    }

    fn chart(timestamp: i64, close: &str) -> Chart {
        let close = bd(close);
        Chart {
            s: CHART_STATUS_OK.to_string(),
            o: close.clone(),
            h: close.clone(),
            l: close.clone(),
            c: close.clone(),
            v: bd("10"),
            t: timestamp,
            usd_o: close.clone(),
            usd_h: close.clone(),
            usd_l: close.clone(),
            usd_c: close,
            usd_v: bd("10"),
            total_supply: bd("1000"),
        }
    }

    fn apply_update(
        store: &LocalStore,
        token_id: &str,
        interval: &str,
        timestamp: i64,
        price: &str,
        volume: Option<&str>,
        order: Option<ChartUpdateOrder>,
    ) -> Chart {
        let price = bd(price);
        let volume = volume.map(bd);
        let native_price = bd("1");
        let total_supply = bd("1000");
        let params = ChartUpdateParams {
            token_id,
            interval,
            timestamp,
            price: &price,
            volume: volume.as_ref(),
            native_price: &native_price,
            total_supply: &total_supply,
            order,
        };

        store
            .update_chart_atomic(&params)
            .expect("chart should update")
    }

    fn order(transaction_index: u64, log_index: u64) -> ChartUpdateOrder {
        ChartUpdateOrder {
            block_number: 10,
            transaction_index,
            log_index,
        }
    }

    fn price_snapshot(
        timestamp: i64,
        block_number: i64,
        tx_index: i32,
        log_index: i64,
        price: &str,
    ) -> PriceSnapshot {
        PriceSnapshot {
            timestamp,
            block_number,
            tx_index,
            log_index,
            price: bd(price),
        }
    }

    #[test]
    fn newer_candle_uses_previous_close_as_open() {
        let store = test_store();
        let token_id = "0xTOKEN";
        let interval = "1";
        store.set_chart(token_id, interval, chart(60, "1.2"));

        let updated = apply_update(&store, token_id, interval, 120, "1.5", Some("3"), None);

        assert_eq!(updated.t, 120);
        assert_eq!(updated.o, bd("1.2"));
        assert_eq!(updated.c, bd("1.5"));
        assert_eq!(updated.v, bd("3"));
    }

    #[test]
    fn older_candle_does_not_replace_newer_live_candle() {
        let store = test_store();
        let token_id = "0xTOKEN";
        let interval = "1";
        store.set_chart(token_id, interval, chart(120, "2"));

        let updated = apply_update(&store, token_id, interval, 60, "1.5", Some("3"), None);
        let stored = store.get_chart(token_id, interval).unwrap();

        assert_eq!(updated.t, 120);
        assert_eq!(stored.t, 120);
        assert_eq!(stored.c, bd("2"));
        assert_eq!(stored.v, bd("10"));
    }

    #[test]
    fn same_candle_out_of_order_trade_keeps_chain_open_and_close() {
        let store = test_store();
        let token_id = "0xTOKEN";
        let interval = "1";
        store.set_chart(token_id, interval, chart(0, "1"));

        let newer_order = ChartUpdateOrder {
            block_number: 10,
            transaction_index: 2,
            log_index: 20,
        };
        let older_order = ChartUpdateOrder {
            block_number: 10,
            transaction_index: 1,
            log_index: 10,
        };

        apply_update(
            &store,
            token_id,
            interval,
            60,
            "2",
            Some("3"),
            Some(newer_order),
        );
        let updated = apply_update(
            &store,
            token_id,
            interval,
            60,
            "1.5",
            Some("4"),
            Some(older_order),
        );

        assert_eq!(updated.t, 60);
        assert_eq!(updated.o, bd("1"));
        assert_eq!(updated.c, bd("2"));
        assert_eq!(updated.h, bd("2"));
        assert_eq!(updated.l, bd("1"));
        assert_eq!(updated.v, bd("7"));
    }

    #[test]
    fn late_previous_candle_close_updates_next_candle_open() {
        let store = test_store();
        let token_id = "0xTOKEN";
        let interval = "1";
        store.set_chart(token_id, interval, chart(0, "3"));

        apply_update(
            &store,
            token_id,
            interval,
            60,
            "3",
            Some("1"),
            Some(order(1, 10)),
        );
        let current = apply_update(
            &store,
            token_id,
            interval,
            120,
            "2",
            Some("3"),
            Some(order(2, 10)),
        );

        assert_eq!(current.o, bd("3"));
        assert_eq!(current.h, bd("3"));
        assert_eq!(current.l, bd("2"));

        let updated = apply_update(
            &store,
            token_id,
            interval,
            60,
            "1.5",
            Some("4"),
            Some(order(1, 20)),
        );

        assert_eq!(updated.t, 120);
        assert_eq!(updated.o, bd("1.5"));
        assert_eq!(updated.c, bd("2"));
        assert_eq!(updated.h, bd("2"));
        assert_eq!(updated.l, bd("1.5"));
        assert_eq!(updated.v, bd("3"));

        let unchanged = apply_update(
            &store,
            token_id,
            interval,
            60,
            "0.5",
            Some("5"),
            Some(order(1, 15)),
        );

        assert_eq!(unchanged.o, bd("1.5"));
        assert_eq!(unchanged.h, bd("2"));
        assert_eq!(unchanged.l, bd("1.5"));
        assert_eq!(unchanged.v, bd("3"));
    }

    #[test]
    fn same_timestamp_price_keeps_latest_chain_order() {
        let store = test_store();
        let token_id = "0xTOKEN";

        store.add_price(token_id, price_snapshot(60, 10, 2, 20, "2"));
        store.add_price(token_id, price_snapshot(60, 10, 1, 10, "1"));
        store.add_price(token_id, price_snapshot(60, 11, 0, 1, "3"));

        let prices = store.get_prices_since(token_id, 0);

        assert_eq!(prices.len(), 1);
        assert_eq!(prices[0].price, bd("3"));
        assert_eq!(prices[0].block_number, 11);
        assert_eq!(prices[0].tx_index, 0);
        assert_eq!(prices[0].log_index, 1);
    }
}
