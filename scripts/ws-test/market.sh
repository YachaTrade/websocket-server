#!/usr/bin/env bash
# market_subscribe — 토큰 시세/리저브/메타 push.
# 사용: ./market.sh <token_id>
set -euo pipefail
WS_URL="${WS_URL:-wss://dev-wss.nadapp.net/wss}"
TOKEN="${1:-${WS_TOKEN_ID:-}}"
[[ -z "$TOKEN" ]] && { echo "Usage: $0 <token_id>" >&2; exit 1; }
MSG="{\"jsonrpc\":\"2.0\",\"method\":\"market_subscribe\",\"params\":{\"token_id\":\"$TOKEN\"}}"
echo "[market] $WS_URL  token=$TOKEN" >&2
exec wscat -c "$WS_URL" -x "$MSG" --wait "${WS_WAIT:-86400}"
