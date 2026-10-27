# ============================================================
# QQQ_PROJECT_02
# TradeStation SIM - Read-Only API Foundation
# No orders, no Excel changes, no LIVE trading
# ============================================================

import os
import json
from datetime import datetime, timezone

PROJECT_NAME = "QQQ_PROJECT_02"
SYMBOL = "QQQ"
API_BASE = "https://sim-api.tradestation.com/v3"

SIM_ONLY = True
READ_ONLY = True
ORDER_EXECUTION_ENABLED = False
LIVE_TRADING_ENABLED = False


def health_check():
    return {
        "project": PROJECT_NAME,
        "symbol": SYMBOL,
        "api_base": API_BASE,
        "environment": "SIM",
        "read_only": READ_ONLY,
        "order_execution_enabled": ORDER_EXECUTION_ENABLED,
        "live_trading_enabled": LIVE_TRADING_ENABLED,
        "api_connection_tested": False,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "FOUNDATION_READY"
    }


if __name__ == "__main__":
    print(json.dumps(health_check(), indent=2))
