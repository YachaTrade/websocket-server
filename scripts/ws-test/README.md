# WebSocket 접속 스크립트

`wscat` 기반 채널별 1-shot subscribe 스크립트. 각 스크립트는 연결 → 해당 채널 구독 → stdout으로 stream.

## 준비

```bash
npm install -g wscat
chmod +x scripts/ws-test/*.sh
```

## 채널별 (인자 1개)

```bash
./scripts/ws-test/market.sh   <token_id>
./scripts/ws-test/swap.sh     <token_id>
./scripts/ws-test/metrics.sh  <token_id>
./scripts/ws-test/order.sh    <token_id>
```

## Chart (인자 더 있음)

```bash
./scripts/ws-test/chart.sh <token_id> [resolution] [price_type]
# resolution: 1, 5, 15, 30, 60/1H, 4H, D, W, M (기본 1)
# price_type: price, price_usd, market_cap, market_cap_usd (기본 price)

./scripts/ws-test/chart.sh 0xabc... 1H price_usd
```

## 인터랙티브 (직접 메시지 입력)

```bash
./scripts/ws-test/connect.sh
> {"jsonrpc":"2.0","method":"market_subscribe","params":{"token_id":"0x..."}}
< { ... 응답 ... }
```

unsubscribe / 임의 메시지 보낼 때 사용.

## 환경변수

| 변수 | 기본 | 설명 |
|---|---|---|
| `WS_URL` | `wss://dev-wss.nadapp.net/wss` | 다른 환경: `WS_URL=ws://localhost:8001/wss ./swap.sh ...` |
| `WS_TOKEN_ID` | (없음) | `<token_id>` 인자 생략 시 default |
| `WS_WAIT` | `86400` (1일) | wscat `-x` 후 connection 유지 시간 (초). 짧게 끊고 싶으면 override |

## graduate actor 검증 시나리오

두 터미널에서 각각:

```bash
# 터미널 A
./scripts/ws-test/market.sh 0xTOKEN_ID

# 터미널 B
./scripts/ws-test/swap.sh 0xTOKEN_ID
```

다른 단말에서 `buy 260000 → graduate → sell 50%` 트리거 후:

| 채널 | 확인 |
|---|---|
| market | `result.market_info.market_type` 이 `V2Curve` → `V2Dex` 전환 |
| swap | `result.account_id` 가 `0x000...dead` / `0x000...0` 이 **아니라** 실제 EOA 또는 vault 주소 |
