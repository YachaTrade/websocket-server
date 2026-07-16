#!/usr/bin/env bash
# chart_subscribe — 캔들 차트 push.
# 사용: ./chart.sh <token_id> [resolution] [price_type]
#   resolution: 1, 5, 15, 30, 60/1H, 4H, D, W, M (기본 1)
#   price_type: price, price_usd, market_cap, market_cap_usd (기본 price)
set -euo pipefail
WS_URL="${WS_URL:-wss://dev-wss.nadapp.net/wss}"
TOKEN="${1:-${WS_TOKEN_ID:-}}"
RES="${2:-1}"
PT="${3:-price}"
[[ -z "$TOKEN" ]] && { echo "Usage: $0 <token_id> [resolution] [price_type]" >&2; exit 1; }
MSG="{\"jsonrpc\":\"2.0\",\"method\":\"chart_subscribe\",\"params\":{\"token_id\":\"$TOKEN\",\"resolution\":\"$RES\",\"price_type\":\"$PT\"}}"
echo "[chart] $WS_URL  token=$TOKEN resolution=$RES price_type=$PT" >&2
exec wscat -c "$WS_URL" -x "$MSG" --wait "${WS_WAIT:-86400}"
