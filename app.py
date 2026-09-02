import os
import time
import secrets
import logging
import csv
import json
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, redirect, request, send_file

app = Flask(__name__)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("zerolag")

TS_CLIENT_ID = os.getenv("TS_CLIENT_ID", "").strip()
TS_CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET", "").strip()
TS_REDIRECT_URI = os.getenv("TS_REDIRECT_URI", "").strip()
TS_API_BASE_URL = os.getenv(
    "TS_API_BASE_URL",
    "https://sim-api.tradestation.com/v3"
).rstrip("/")
TS_LIVE_API_BASE_URL = "https://api.tradestation.com/v3"
TS_SIM_ACCOUNT_ID = os.getenv("TS_SIM_ACCOUNT_ID", "").strip()
TS_LIVE_ACCOUNT_ID = os.getenv("TS_LIVE_ACCOUNT_ID", "").strip()

# Master switch used by the existing service. Keep YES only when execution is intended.
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "NO").strip().upper()

# SOXL Regular can be routed independently. Default remains SIM.
SOXL_REGULAR_EXECUTION_MODE = os.getenv(
    "SOXL_REGULAR_EXECUTION_MODE",
    "SIM"
).strip().upper()

# SOXL Overnight can be routed independently. Default remains SIM.
SOXL_OVERNIGHT_EXECUTION_MODE = os.getenv(
    "SOXL_OVERNIGHT_EXECUTION_MODE",
    "SIM"
).strip().upper()

# Second, independent gate required before any LIVE order can be sent.
LIVE_TRADING_ENABLED = os.getenv(
    "LIVE_TRADING_ENABLED",
    "NO"
).strip().upper()

# Independent master gate for ODTS QQQ option execution in TradeStation SIM.
# Default NO so deployment alone can never submit an option order.
ODTS_SIM_TRADING_ENABLED = os.getenv(
    "ODTS_SIM_TRADING_ENABLED",
    "NO"
).strip().upper()

# Independent master gate for ODTS QQQ SIM exits. Default NO.
ODTS_SIM_EXIT_ENABLED = os.getenv(
    "ODTS_SIM_EXIT_ENABLED",
    "NO"
).strip().upper()

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

TS_AUTHORIZE_URL = "https://signin.tradestation.com/authorize"
TS_TOKEN_URL = "https://signin.tradestation.com/oauth/token"
TS_AUDIENCE = "https://api.tradestation.com"
TS_SCOPES = "openid profile offline_access MarketData ReadAccount Trade OptionSpreads"

# ==============================================================
# SIM SAFETY SETTINGS
# ==============================================================

ALLOWED_SYMBOL = "SOXL"

ALLOWED_STRATEGIES = {
    "SOXL_REGULAR",
    "SOXL_OVERNIGHT",
}

MAX_TEST_QTY = 1
LIVE_MAX_QTY = 1
DUPLICATE_WINDOW_SECONDS = 20


# ==============================================================
# EXECUTION JOURNAL SETTINGS
# ==============================================================

JOURNAL_DIR = os.getenv(
    "JOURNAL_DIR",
    "/tmp/zerolag_journal"
).strip()

TV_TIMEFRAME = os.getenv("TV_TIMEFRAME", "5m").strip()

ET = ZoneInfo("America/New_York")

TV_CSV_PATH = os.path.join(
    JOURNAL_DIR,
    "TV_Signals.csv"
)

TS_CSV_PATH = os.path.join(
    JOURNAL_DIR,
    "TS_Executions.csv"
)

TV_HEADERS = [
    "Date",
    "TV Time",
    "Symbol",
    "Action",
    "TV Price",
    "Qty",
    "Strategy",
    "Time Frame",
    "Alert Status",
]

TS_HEADERS = [
    "Symbol",
    "Qty",
    "AvgPrice",
    "OpenPL",
    "Pos",
    "Action",
    "FillQty",
    "FillPrice",
    "OrdState",
    "TradePL",
    "DayPL",
    "Last",
    "Bid",
    "Ask",
    "Net%",
    "VTot",
    "Dollar_Vol",
    "RS_Volume_Ratio",
    "VWAP",
    "Int",
]

journal_lock = threading.Lock()

# Serializes LIVE position-check + order submission so two webhooks
# cannot pass the position gate at the same time.
live_order_lock = threading.Lock()

# Completely separate lock/state for ODTS QQQ option approvals/orders.
odts_order_lock = threading.Lock()
odts_proposals = {}
odts_last_order = {
    "proposal_id": None,
    "symbol": None,
    "order_id": None,
    "limit_price": None,
    "time": 0,
}

ODTS_PROPOSAL_TTL_SECONDS = 120
ODTS_DUPLICATE_WINDOW_SECONDS = 60
ODTS_MAX_ASK_INCREASE_PCT = 5.0

oauth_state = None

token_store = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0,
}

last_webhook = {
    "received": False,
    "payload": None,
    "received_at": None,
}

last_signal = {
    "key": None,
    "time": 0,
}


# ==============================================================
# JOURNAL FUNCTIONS
# ==============================================================

def ensure_journal_files():
    os.makedirs(
        JOURNAL_DIR,
        exist_ok=True
    )

    for path, headers in [
        (TV_CSV_PATH, TV_HEADERS),
        (TS_CSV_PATH, TS_HEADERS),
    ]:
        if (
            not os.path.exists(path)
            or os.path.getsize(path) == 0
        ):
            with open(
                path,
                "w",
                newline="",
                encoding="utf-8"
            ) as f:
                csv.writer(f).writerow(headers)


def append_csv(path, headers, row):
    try:
        ensure_journal_files()

        with journal_lock:
            with open(
                path,
                "a",
                newline="",
                encoding="utf-8"
            ) as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=headers
                )

                writer.writerow(
                    {
                        h: row.get(h, "")
                        for h in headers
                    }
                )

        return True

    except Exception as exc:
        log.exception(
            "JOURNAL WRITE FAILED | path=%s | error=%s",
            path,
            exc
        )

        return False


def now_et():
    return datetime.now(ET)


def normalize_tv_time(payload):
    raw = (
        payload.get("tv_time")
        or payload.get("time")
        or payload.get("timestamp")
    )

    if raw:
        return str(raw)

    return now_et().strftime("%H:%M:%S")


def journal_tv_signal(
    payload,
    action,
    symbol,
    strategy_name
):
    dt = now_et()

    row = {
        "Date": dt.strftime("%Y-%m-%d"),
        "TV Time": normalize_tv_time(payload),
        "Symbol": symbol,
        "Action": action,
        "TV Price": payload.get("price", ""),
        "Qty": payload.get(
            "qty",
            payload.get("size", "")
        ),
        "Strategy": strategy_name,
        "Time Frame": payload.get(
            "timeframe",
            payload.get(
                "interval",
                TV_TIMEFRAME
            )
        ),
        "Alert Status": "RECEIVED",
    }

    append_csv(
        TV_CSV_PATH,
        TV_HEADERS,
        row
    )


# ==============================================================
# TRADESTATION ORDER/POSITION HELPERS
# ==============================================================

def extract_first_position(body):
    if not isinstance(body, dict):
        return None

    positions = body.get(
        "Positions",
        []
    )

    if not isinstance(
        positions,
        list
    ):
        return None

    for position in positions:
        if (
            str(
                position.get(
                    "Symbol",
                    ""
                )
            )
            .upper()
            .strip()
            == ALLOWED_SYMBOL
        ):
            return position

    return None


def extract_order_id(order_response):
    if not isinstance(
        order_response,
        dict
    ):
        return None

    orders = order_response.get(
        "Orders",
        []
    )

    if (
        isinstance(orders, list)
        and orders
        and isinstance(
            orders[0],
            dict
        )
    ):
        value = (
            orders[0].get("OrderID")
            or orders[0].get("OrderId")
        )

        if value is not None:
            return str(value)

    return None


def fetch_order_details(
    access_token,
    order_id
):
    if not order_id:
        return None

    url = (
        f"{TS_API_BASE_URL}"
        f"/brokerage/accounts/"
        f"{TS_SIM_ACCOUNT_ID}"
        f"/orders"
    )

    try:
        response = requests.get(
            url,
            headers=ts_headers(
                access_token
            ),
            timeout=20
        )

    except requests.RequestException as exc:
        log.warning(
            "ORDER JOURNAL QUERY FAILED | %s",
            exc
        )

        return None

    if not response.ok:
        log.warning(
            "ORDER JOURNAL QUERY FAILED | status=%s body=%s",
            response.status_code,
            response.text[:500]
        )

        return None

    try:
        body = response.json()

    except ValueError:
        return None

    orders = (
        body.get("Orders", [])
        if isinstance(body, dict)
        else []
    )

    if not isinstance(
        orders,
        list
    ):
        return None

    for order in orders:
        candidate = str(
            order.get("OrderID")
            or order.get("OrderId")
            or ""
        )

        if candidate == str(order_id):
            return order

    return None


def journal_ts_execution_background(
    access_token,
    action,
    order_response
):
    try:
        time.sleep(1.25)

        order_id = extract_order_id(
            order_response
        )

        order_detail = fetch_order_details(
            access_token,
            order_id
        )

        (
            pos_ok,
            position_qty,
            position_body
        ) = get_soxl_position(
            access_token
        )

        position = (
            extract_first_position(
                position_body
            )
            if pos_ok
            else None
        )

        qty = (
            position_qty
            if pos_ok
            else ""
        )

        avg_price = ""
        open_pl = ""
        pos_text = ""

        if position:
            avg_price = (
                position.get(
                    "AveragePrice"
                )
                or position.get(
                    "AvgPrice"
                )
                or ""
            )

            open_pl = (
                position.get(
                    "UnrealizedProfitLoss"
                )
                or position.get(
                    "OpenPL"
                )
                or ""
            )

        if pos_ok:
            if position_qty > 0:
                pos_text = "LONG"

            elif position_qty < 0:
                pos_text = "SHORT"

            else:
                pos_text = "FLAT"

        fill_qty = MAX_TEST_QTY
        fill_price = ""
        order_state = "SENT"

        if order_detail:
            fill_qty = (
                order_detail.get(
                    "FilledQuantity"
                )
                or order_detail.get(
                    "Quantity"
                )
                or MAX_TEST_QTY
            )

            fill_price = (
                order_detail.get(
                    "FilledPrice"
                )
                or order_detail.get(
                    "AverageFilledPrice"
                )
                or ""
            )

            order_state = (
                order_detail.get(
                    "Status"
                )
                or order_detail.get(
                    "State"
                )
                or order_detail.get(
                    "OrderStatus"
                )
                or "SENT"
            )

        if (
            not fill_price
            and action == "BUY"
            and avg_price
        ):
            fill_price = avg_price

        row = {
            "Symbol": ALLOWED_SYMBOL,
            "Qty": qty,
            "AvgPrice": avg_price,
            "OpenPL": open_pl,
            "Pos": pos_text,
            "Action": action,
            "FillQty": fill_qty,
            "FillPrice": fill_price,
            "OrdState": order_state,
            "TradePL": "",
            "DayPL": "",
            "Last": "",
            "Bid": "",
            "Ask": "",
            "Net%": "",
            "VTot": "",
            "Dollar_Vol": "",
            "RS_Volume_Ratio": "",
            "VWAP": "",
            "Int": TV_TIMEFRAME,
        }

        append_csv(
            TS_CSV_PATH,
            TS_HEADERS,
            row
        )

        log.info(
            "JOURNAL TS SAVED | action=%s order_id=%s fill_price=%s state=%s",
            action,
            order_id,
            fill_price,
            order_state
        )

    except Exception as exc:
        log.exception(
            "TS JOURNAL BACKGROUND ERROR | %s",
            exc
        )


ensure_journal_files()


# ==============================================================
# SIM SAFETY / CONFIGURATION
# ==============================================================

def sim_environment_ok():
    return (
        TS_API_BASE_URL
        .lower()
        .startswith(
            "https://sim-api.tradestation.com/v3"
        )
    )


def order_capability_ready():
    return bool(
        sim_environment_ok()
        and TS_CLIENT_ID
        and TS_CLIENT_SECRET
        and TS_REDIRECT_URI
        and TS_SIM_ACCOUNT_ID
        and WEBHOOK_TOKEN
    )


def missing_config():
    missing = []

    for name, value in [
        ("TS_CLIENT_ID", TS_CLIENT_ID),
        (
            "TS_CLIENT_SECRET",
            TS_CLIENT_SECRET
        ),
        (
            "TS_REDIRECT_URI",
            TS_REDIRECT_URI
        ),
        (
            "TS_SIM_ACCOUNT_ID",
            TS_SIM_ACCOUNT_ID
        ),
        (
            "WEBHOOK_TOKEN",
            WEBHOOK_TOKEN
        ),
    ]:
        if not value:
            missing.append(name)

    return missing


def live_regular_mode_selected():
    return SOXL_REGULAR_EXECUTION_MODE == "LIVE"


def live_overnight_mode_selected():
    return SOXL_OVERNIGHT_EXECUTION_MODE == "LIVE"


def live_strategy_mode_selected(strategy_name):
    if strategy_name == "SOXL_REGULAR":
        return live_regular_mode_selected()

    if strategy_name == "SOXL_OVERNIGHT":
        return live_overnight_mode_selected()

    return False


def live_order_capability_ready():
    return bool(
        TS_CLIENT_ID
        and TS_CLIENT_SECRET
        and TS_REDIRECT_URI
        and TS_LIVE_ACCOUNT_ID
        and WEBHOOK_TOKEN
        and LIVE_MAX_QTY == 1
    )


def live_market_session_now():
    """
    LIVE orders in this version are intentionally restricted to the
    regular U.S. equity session. The Overnight strategy may HOLD a
    position overnight, but its entry/exit orders must arrive between
    09:30 and 16:00 ET.
    """
    dt = now_et()

    # Monday=0 ... Sunday=6
    if dt.weekday() > 4:
        return False

    minutes = dt.hour * 60 + dt.minute
    return (9 * 60 + 30) <= minutes < (16 * 60)


def live_regular_session_now():
    # Backward-compatible alias used by older status/test code.
    return live_market_session_now()


def odts_sim_session_now():
    """ODTS V1 entries are restricted to the regular QQQ session."""
    return live_market_session_now()


def odts_sim_environment_ok():
    base = TS_API_BASE_URL.lower()
    return "sim-api.tradestation.com" in base


# ==============================================================
# OAUTH
# ==============================================================

def save_token_response(data):
    token_store["access_token"] = (
        data.get("access_token")
    )

    if data.get("refresh_token"):
        token_store["refresh_token"] = (
            data.get("refresh_token")
        )

    expires_in = int(
        data.get(
            "expires_in",
            1200
        )
    )

    token_store["expires_at"] = (
        time.time()
        + max(
            expires_in - 60,
            60
        )
    )


def refresh_access_token():
    refresh_token = (
        token_store.get(
            "refresh_token"
        )
    )

    if not refresh_token:
        return (
            False,
            "No refresh token is available. "
            "Please visit /login again."
        )

    payload = {
        "grant_type": "refresh_token",
        "client_id": TS_CLIENT_ID,
        "client_secret": TS_CLIENT_SECRET,
        "refresh_token": refresh_token,
    }

    try:
        response = requests.post(
            TS_TOKEN_URL,
            data=payload,
            headers={
                "Content-Type":
                "application/x-www-form-urlencoded"
            },
            timeout=20
        )

    except requests.RequestException as exc:
        return (
            False,
            f"Refresh request failed: {exc}"
        )

    if not response.ok:
        return (
            False,
            f"Refresh failed "
            f"({response.status_code}): "
            f"{response.text[:500]}"
        )

    data = response.json()

    if not data.get("access_token"):
        return (
            False,
            "TradeStation refresh response "
            "did not include an access token."
        )

    save_token_response(data)

    return (
        True,
        "Access token refreshed."
    )


def get_valid_access_token():
    access_token = (
        token_store.get(
            "access_token"
        )
    )

    if (
        access_token
        and time.time()
        < token_store.get(
            "expires_at",
            0
        )
    ):
        return (
            access_token,
            None
        )

    if token_store.get(
        "refresh_token"
    ):
        ok, message = (
            refresh_access_token()
        )

        if ok:
            return (
                token_store.get(
                    "access_token"
                ),
                None
            )

        return (
            None,
            message
        )

    return (
        None,
        "Not authenticated. "
        "Please visit /login first."
    )


def ts_headers(access_token):
    return {
        "Authorization":
            f"Bearer {access_token}",
        "Accept":
            "application/json",
        "Content-Type":
            "application/json",
    }


# ==============================================================
# ODTS QQQ UNDERLYING QUOTE TEST
# READ ONLY / NO ORDER SUBMISSION
# ==============================================================

@app.get("/odts-qqq-test")
def odts_qqq_test():
    access_token, error = get_valid_access_token()

    if not access_token:
        return jsonify({
            "ok": False,
            "error": error,
            "next_step": "Open /login"
        }), 401

    symbol = "QQQ"

    encoded_symbol = requests.utils.quote(
        symbol,
        safe=""
    )

    url = (
        f"{TS_API_BASE_URL}"
        f"/marketdata/stream/quotes/"
        f"{encoded_symbol}"
    )

    try:
        response = requests.get(
            url,
            headers=ts_headers(access_token),
            stream=True,
            timeout=20
        )

        if not response.ok:
            return jsonify({
                "ok": False,
                "read_only": True,
                "order_sent": False,
                "symbol": symbol,
                "status_code": response.status_code,
                "response": response.text[:1000]
            }), response.status_code

        for line in response.iter_lines():
            if not line:
                continue

            text = line.decode("utf-8").strip()

            try:
                quote = json.loads(text)
            except Exception:
                quote = {"raw": text}

            response.close()

            return jsonify({
                "ok": True,
                "read_only": True,
                "order_sent": False,
                "symbol": symbol,
                "bid": quote.get("Bid", ""),
                "ask": quote.get("Ask", ""),
                "last": quote.get("Last", ""),
                "volume": quote.get("Volume", ""),
                "previous_close": quote.get("PreviousClose", ""),
                "net_change": quote.get("NetChange", ""),
                "net_change_pct": quote.get("NetChangePct", "")
            })

        response.close()

        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": "No QQQ quote data was returned."
        }), 502

    except requests.RequestException as exc:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": f"QQQ quote request failed: {exc}"
        }), 502



# ==============================================================
# ODTS QQQ 3-MIN INDICATORS TEST
# READ ONLY / NO ORDER SUBMISSION
# ==============================================================

def _odts_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _odts_ema(values, length):
    if not values:
        return None

    alpha = 2.0 / (length + 1.0)
    ema_value = float(values[0])

    for value in values[1:]:
        ema_value = (
            alpha * float(value)
            + (1.0 - alpha) * ema_value
        )

    return ema_value


def _odts_wilder(values, length):
    if len(values) < length:
        return []

    first = sum(values[:length]) / length
    result = [None] * (length - 1) + [first]
    previous = first

    for value in values[length:]:
        previous = (
            (previous * (length - 1))
            + value
        ) / length
        result.append(previous)

    return result


def _odts_adx(highs, lows, closes, length=14):
    if len(closes) < (length * 2 + 1):
        return None

    tr_values = []
    plus_dm_values = []
    minus_dm_values = []

    for i in range(1, len(closes)):
        high = highs[i]
        low = lows[i]
        prev_high = highs[i - 1]
        prev_low = lows[i - 1]
        prev_close = closes[i - 1]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

        up_move = high - prev_high
        down_move = prev_low - low

        plus_dm = (
            up_move
            if up_move > down_move and up_move > 0
            else 0.0
        )

        minus_dm = (
            down_move
            if down_move > up_move and down_move > 0
            else 0.0
        )

        tr_values.append(tr)
        plus_dm_values.append(plus_dm)
        minus_dm_values.append(minus_dm)

    atr = _odts_wilder(tr_values, length)
    plus_dm_smoothed = _odts_wilder(plus_dm_values, length)
    minus_dm_smoothed = _odts_wilder(minus_dm_values, length)

    dx_values = []

    for i in range(len(tr_values)):
        if (
            i >= len(atr)
            or atr[i] is None
            or atr[i] == 0
            or plus_dm_smoothed[i] is None
            or minus_dm_smoothed[i] is None
        ):
            continue

        plus_di = (
            100.0
            * plus_dm_smoothed[i]
            / atr[i]
        )

        minus_di = (
            100.0
            * minus_dm_smoothed[i]
            / atr[i]
        )

        denominator = plus_di + minus_di

        if denominator == 0:
            dx = 0.0
        else:
            dx = (
                100.0
                * abs(plus_di - minus_di)
                / denominator
            )

        dx_values.append(dx)

    if len(dx_values) < length:
        return None

    adx_series = _odts_wilder(dx_values, length)

    valid = [
        value
        for value in adx_series
        if value is not None
    ]

    if not valid:
        return None

    return valid[-1]


@app.get("/odts-qqq-indicators-test")
def odts_qqq_indicators_test():
    access_token, error = get_valid_access_token()

    if not access_token:
        return jsonify({
            "ok": False,
            "error": error,
            "next_step": "Open /login"
        }), 401

    symbol = "QQQ"

    url = (
        f"{TS_API_BASE_URL}"
        f"/marketdata/barcharts/{symbol}"
    )

    params = {
        "interval": "3",
        "unit": "Minute",
        "barsback": "260",
        "sessiontemplate": "Default"
    }

    try:
        response = requests.get(
            url,
            headers=ts_headers(access_token),
            params=params,
            timeout=20
        )

    except requests.RequestException as exc:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": (
                f"QQQ bar request failed: {exc}"
            )
        }), 502

    if not response.ok:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "status_code": response.status_code,
            "response": response.text[:1000]
        }), response.status_code

    try:
        body = response.json()
    except ValueError:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": (
                "TradeStation bar response "
                "was not valid JSON."
            )
        }), 502

    bars = (
        body.get("Bars", [])
        if isinstance(body, dict)
        else []
    )

    if not isinstance(bars, list) or not bars:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": "No QQQ bars were returned."
        }), 502

    bars = sorted(
        bars,
        key=lambda item: int(
            item.get("Epoch", 0) or 0
        )
    )

    closed_bars = []

    for bar in bars:
        status = str(
            bar.get("BarStatus", "")
        ).strip().lower()

        if status and status != "closed":
            continue

        closed_bars.append(bar)

    if len(closed_bars) < 50:
        closed_bars = bars

    highs = [
        _odts_float(bar.get("High"))
        for bar in closed_bars
    ]

    lows = [
        _odts_float(bar.get("Low"))
        for bar in closed_bars
    ]

    closes = [
        _odts_float(bar.get("Close"))
        for bar in closed_bars
    ]

    volumes = [
        _odts_float(bar.get("TotalVolume"))
        for bar in closed_bars
    ]

    if len(closes) < 25:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": (
                "Not enough completed 3-minute "
                "bars were returned."
            )
        }), 502

    ema21 = _odts_ema(closes, 21)

    zlema_length = 5
    lag = int((zlema_length - 1) / 2)

    adjusted = []

    for i, close_value in enumerate(closes):
        if i < lag:
            adjusted.append(close_value)
        else:
            adjusted.append(
                close_value
                + (
                    close_value
                    - closes[i - lag]
                )
            )

    zlema_series = []
    alpha = 2.0 / (zlema_length + 1.0)
    running = adjusted[0]
    zlema_series.append(running)

    for value in adjusted[1:]:
        running = (
            alpha * value
            + (1.0 - alpha) * running
        )
        zlema_series.append(running)

    zlema_state = 0

    if len(zlema_series) >= 2:
        if zlema_series[-1] > zlema_series[-2]:
            zlema_state = 1
        elif zlema_series[-1] < zlema_series[-2]:
            zlema_state = -1

    confirm_bars = 0

    if zlema_state != 0:
        for i in range(
            len(zlema_series) - 1,
            0,
            -1
        ):
            current_state = 0

            if zlema_series[i] > zlema_series[i - 1]:
                current_state = 1
            elif zlema_series[i] < zlema_series[i - 1]:
                current_state = -1

            if current_state != zlema_state:
                break

            confirm_bars += 1

            if confirm_bars >= 2:
                break

    adx14 = _odts_adx(
        highs,
        lows,
        closes,
        14
    )

    latest_bar = closed_bars[-1]
    latest_timestamp = latest_bar.get(
        "TimeStamp",
        ""
    )

    latest_session_date = None

    try:
        latest_dt = datetime.fromisoformat(
            latest_timestamp.replace(
                "Z",
                "+00:00"
            )
        ).astimezone(ET)

        latest_session_date = latest_dt.date()
    except Exception:
        latest_session_date = None

    session_pv = 0.0
    session_volume = 0.0

    for bar, high, low, close_value, volume in zip(
        closed_bars,
        highs,
        lows,
        closes,
        volumes
    ):
        include_bar = True

        if latest_session_date is not None:
            try:
                bar_dt = datetime.fromisoformat(
                    str(
                        bar.get(
                            "TimeStamp",
                            ""
                        )
                    ).replace(
                        "Z",
                        "+00:00"
                    )
                ).astimezone(ET)

                include_bar = (
                    bar_dt.date()
                    == latest_session_date
                )
            except Exception:
                include_bar = False

        if not include_bar:
            continue

        typical_price = (
            high
            + low
            + close_value
        ) / 3.0

        session_pv += (
            typical_price
            * volume
        )

        session_volume += volume

    vwap = (
        session_pv / session_volume
        if session_volume > 0
        else None
    )

    return jsonify({
        "ok": True,
        "read_only": True,
        "order_sent": False,
        "symbol": symbol,
        "timeframe": "3 Minute",
        "bar_timestamp": latest_timestamp,
        "bars_used": len(closed_bars),
        "qqq_close": round(closes[-1], 6),
        "ema21": (
            round(ema21, 6)
            if ema21 is not None
            else ""
        ),
        "vwap": (
            round(vwap, 6)
            if vwap is not None
            else ""
        ),
        "zlema_state": zlema_state,
        "zlema_confirm": confirm_bars,
        "adx14": (
            round(adx14, 6)
            if adx14 is not None
            else ""
        )
    })


# ==============================================================
# ODTS QQQ OPTION CONTRACT SELECTOR V1
# READ ONLY / NO ORDER SUBMISSION
# ==============================================================

@app.get("/odts-option-test")
def odts_option_test():
    access_token, error = get_valid_access_token()

    if not access_token:
        return jsonify({
            "ok": False,
            "error": error,
            "next_step": "Open /login"
        }), 401

    # ----------------------------------------------------------
    # ODTS V1 FROZEN SELECTION INPUTS
    # ----------------------------------------------------------
    underlying = "QQQ"
    allowed_dte = [1, 2]
    target_delta_min = 0.50
    target_delta_max = 0.65
    target_delta_mid = (
        target_delta_min + target_delta_max
    ) / 2.0

    # Spread rule discussed for V1:
    # <= 10% preferred, > 15% rejected.
    preferred_spread_pct = 10.0
    max_spread_pct = 15.0

    # ODTS SIM V1 frozen risk/target controls.
    # These are DISPLAY-ONLY in this revision. No option order can
    # be submitted from this route.
    quantity_contracts = 1
    max_loss_pct = 35.0
    profit_target_pct = 50.0

    # Number of strikes above and below the underlying used by
    # TradeStation's option-chain stream. 12 gives ample room
    # around ATM for the 0.50-0.65 Delta target.
    strike_proximity = 12

    # Direction is intentionally READ ONLY and may be supplied as:
    #   /odts-option-test?direction=BULLISH
    #   /odts-option-test?direction=BEARISH
    #   /odts-option-test?direction=BOTH
    # Until the signal engine is connected, BOTH is the default.
    direction = str(
        request.args.get("direction", "BOTH")
    ).strip().upper()

    if direction in {"CALL", "LONG_CALL"}:
        direction = "BULLISH"
    elif direction in {"PUT", "LONG_PUT"}:
        direction = "BEARISH"

    if direction not in {"BULLISH", "BEARISH", "BOTH"}:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "underlying": underlying,
            "error": (
                "direction must be BULLISH, BEARISH, or BOTH"
            )
        }), 400

    def safe_float(value, default=None):
        try:
            if value in (None, ""):
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def safe_int(value, default=0):
        try:
            if value in (None, ""):
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def parse_expiration(value):
        raw = str(value or "").strip()

        if not raw:
            return None

        # TradeStation expirations normally arrive as
        # 2026-09-03T00:00:00Z. Keep a few safe fallbacks.
        candidates = [
            raw,
            raw.replace("Z", "+00:00"),
            raw[:10],
        ]

        for candidate in candidates:
            try:
                return datetime.fromisoformat(
                    candidate
                ).date()
            except Exception:
                pass

        for fmt in (
            "%m-%d-%Y",
            "%Y-%m-%d",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(
                    raw[:10],
                    fmt
                ).date()
            except Exception:
                pass

        return None

    def candidate_from_chain(item, dte, expiration_date):
        if not isinstance(item, dict):
            return None

        # Ignore stream-status and error messages.
        if item.get("StreamStatus") or item.get("Error"):
            return None

        legs = item.get("Legs", [])
        leg = (
            legs[0]
            if isinstance(legs, list)
            and legs
            and isinstance(legs[0], dict)
            else {}
        )

        option_type = str(
            item.get("Side")
            or leg.get("OptionType")
            or ""
        ).strip().upper()

        if option_type not in {"CALL", "PUT"}:
            return None

        delta_raw = safe_float(item.get("Delta"))

        if delta_raw is None:
            return None

        abs_delta = abs(delta_raw)

        if not (
            target_delta_min
            <= abs_delta
            <= target_delta_max
        ):
            return None

        bid = safe_float(item.get("Bid"))
        ask = safe_float(item.get("Ask"))
        last = safe_float(item.get("Last"))
        mid = safe_float(item.get("Mid"))

        if (
            bid is None
            or ask is None
            or bid <= 0
            or ask <= 0
            or ask < bid
        ):
            return None

        if mid is None or mid <= 0:
            mid = (bid + ask) / 2.0

        spread = ask - bid
        spread_pct = (
            (spread / mid) * 100.0
            if mid > 0
            else None
        )

        if (
            spread_pct is None
            or spread_pct > max_spread_pct
        ):
            return None

        symbol = str(
            leg.get("Symbol")
            or item.get("Symbol")
            or ""
        ).strip()

        strike = safe_float(
            leg.get("StrikePrice")
        )

        if strike is None:
            strikes = item.get("Strikes", [])
            if isinstance(strikes, list) and strikes:
                strike = safe_float(strikes[0])

        open_interest = safe_int(
            item.get("DailyOpenInterest"),
            0
        )
        volume = safe_int(
            item.get("Volume"),
            0
        )

        # No hard OI/volume cutoff yet. We use both as ranking
        # preferences so V1 does not invent an unapproved rule.
        # Lower score is better.
        delta_distance = abs(
            abs_delta - target_delta_mid
        )

        spread_penalty = spread_pct / 100.0
        oi_bonus = min(open_interest, 5000) / 500000.0
        volume_bonus = min(volume, 5000) / 1000000.0

        score = (
            delta_distance
            + spread_penalty
            - oi_bonus
            - volume_bonus
        )

        spread_status = (
            "PREFERRED"
            if spread_pct <= preferred_spread_pct
            else "ACCEPTABLE"
        )

        gross_cost = ask * 100.0 * quantity_contracts
        planned_exit_price = ask * (1.0 - max_loss_pct / 100.0)
        planned_target_price = ask * (1.0 + profit_target_pct / 100.0)
        planned_risk_dollars = gross_cost * (max_loss_pct / 100.0)
        planned_profit_dollars = gross_cost * (profit_target_pct / 100.0)
        reward_risk = (
            planned_profit_dollars / planned_risk_dollars
            if planned_risk_dollars > 0
            else None
        )

        return {
            "symbol": symbol,
            "option_type": option_type.title(),
            "expiration": expiration_date.isoformat(),
            "dte": dte,
            "strike": (
                round(strike, 4)
                if strike is not None
                else ""
            ),
            "delta": round(delta_raw, 6),
            "abs_delta": round(abs_delta, 6),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "mid": round(mid, 4),
            "last": (
                round(last, 4)
                if last is not None
                else ""
            ),
            "spread": round(spread, 4),
            "spread_pct": round(spread_pct, 4),
            "spread_status": spread_status,
            "volume": volume,
            "open_interest": open_interest,
            "gamma": item.get("Gamma", ""),
            "theta": item.get("Theta", ""),
            "vega": item.get("Vega", ""),
            "implied_volatility": item.get(
                "ImpliedVolatility",
                ""
            ),
            "probability_itm": item.get(
                "ProbabilityITM",
                ""
            ),
            "probability_otm": item.get(
                "ProbabilityOTM",
                ""
            ),
            "quantity_contracts": quantity_contracts,
            "gross_premium_cost": round(gross_cost, 2),
            "absolute_worst_case_loss": round(gross_cost, 2),
            "max_loss_pct": max_loss_pct,
            "planned_exit_option_price": round(planned_exit_price, 4),
            "planned_risk_dollars": round(planned_risk_dollars, 2),
            "profit_target_pct": profit_target_pct,
            "planned_target_option_price": round(planned_target_price, 4),
            "planned_profit_dollars": round(planned_profit_dollars, 2),
            "reward_risk": (
                round(reward_risk, 4)
                if reward_risk is not None
                else ""
            ),
            "score": round(score, 8),
        }

    # ----------------------------------------------------------
    # 1) GET AVAILABLE QQQ EXPIRATIONS
    # ----------------------------------------------------------
    expiration_url = (
        f"{TS_API_BASE_URL}"
        f"/marketdata/options/expirations/{underlying}"
    )

    try:
        expiration_response = requests.get(
            expiration_url,
            headers=ts_headers(access_token),
            timeout=20
        )
    except requests.RequestException as exc:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "underlying": underlying,
            "error": (
                f"QQQ expiration request failed: {exc}"
            )
        }), 502

    if not expiration_response.ok:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "underlying": underlying,
            "status_code": expiration_response.status_code,
            "response": expiration_response.text[:1000]
        }), expiration_response.status_code

    try:
        expiration_body = expiration_response.json()
    except ValueError:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "underlying": underlying,
            "error": (
                "TradeStation expiration response was not valid JSON."
            )
        }), 502

    expirations = (
        expiration_body.get("Expirations", [])
        if isinstance(expiration_body, dict)
        else []
    )

    today_et = now_et().date()
    eligible_expirations = []

    for expiration_item in expirations:
        if not isinstance(expiration_item, dict):
            continue

        expiration_date = parse_expiration(
            expiration_item.get("Date")
        )

        if expiration_date is None:
            continue

        dte = (
            expiration_date - today_et
        ).days

        expiration_type = str(
            expiration_item.get("Type", "")
        ).strip()

        # Match the OptionStation setup: Weeklys only, 1-2 DTE.
        if (
            dte in allowed_dte
            and expiration_type.lower() == "weekly"
        ):
            eligible_expirations.append({
                "date": expiration_date,
                "dte": dte,
                "type": expiration_type,
            })

    eligible_expirations.sort(
        key=lambda item: (
            item["dte"],
            item["date"]
        )
    )

    if not eligible_expirations:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "underlying": underlying,
            "allowed_dte": allowed_dte,
            "expiration_type": "Weekly",
            "error": (
                "No QQQ Weekly expiration was found at exactly "
                "1 or 2 calendar DTE."
            )
        }), 404

    # ----------------------------------------------------------
    # 2) STREAM INITIAL CHAIN SNAPSHOT FOR EACH 1-2 DTE EXPIRY
    # ----------------------------------------------------------
    all_candidates = []
    stream_notes = []

    chain_url = (
        f"{TS_API_BASE_URL}"
        f"/marketdata/stream/options/chains/{underlying}"
    )

    for expiry in eligible_expirations:
        expiration_date = expiry["date"]
        expiration_param = expiration_date.strftime(
            "%m-%d-%Y"
        )

        chain_params = {
            "expiration": expiration_param,
            "strikeProximity": strike_proximity,
            "spreadType": "Single",
            "enableGreeks": "true",
            "optionType": "All",
        }

        chain_response = None
        message_count = 0
        candidate_count = 0

        try:
            chain_response = requests.get(
                chain_url,
                headers=ts_headers(access_token),
                params=chain_params,
                stream=True,
                timeout=(5, 4)
            )

            if not chain_response.ok:
                stream_notes.append({
                    "expiration": expiration_date.isoformat(),
                    "dte": expiry["dte"],
                    "ok": False,
                    "status_code": chain_response.status_code,
                    "response": chain_response.text[:500]
                })
                chain_response.close()
                continue

            # For Single + All with strikeProximity=12, an initial
            # snapshot is normally about 50 contracts (25 strikes x
            # Call/Put). We stop after 60 messages so this HTTP route
            # never becomes a permanent streaming connection.
            for line in chain_response.iter_lines():
                if not line:
                    continue

                text = line.decode(
                    "utf-8",
                    errors="replace"
                ).strip()

                if not text:
                    continue

                try:
                    item = json.loads(text)
                except ValueError:
                    continue

                if item.get("StreamStatus") == "EndSnapshot":
                    break

                if item.get("StreamStatus") == "GoAway":
                    break

                message_count += 1

                candidate = candidate_from_chain(
                    item,
                    expiry["dte"],
                    expiration_date
                )

                if candidate is not None:
                    all_candidates.append(candidate)
                    candidate_count += 1

                if message_count >= 60:
                    break

        except requests.RequestException as exc:
            # If the stream times out after its initial burst, keep
            # any candidates already collected and report the note.
            stream_notes.append({
                "expiration": expiration_date.isoformat(),
                "dte": expiry["dte"],
                "ok": candidate_count > 0,
                "message_count": message_count,
                "candidate_count": candidate_count,
                "note": str(exc)[:300]
            })
        finally:
            if chain_response is not None:
                try:
                    chain_response.close()
                except Exception:
                    pass

        if not any(
            note.get("expiration")
            == expiration_date.isoformat()
            for note in stream_notes
        ):
            stream_notes.append({
                "expiration": expiration_date.isoformat(),
                "dte": expiry["dte"],
                "ok": True,
                "message_count": message_count,
                "candidate_count": candidate_count,
            })

    calls = [
        item
        for item in all_candidates
        if item["option_type"] == "Call"
    ]

    puts = [
        item
        for item in all_candidates
        if item["option_type"] == "Put"
    ]

    calls.sort(key=lambda item: item["score"])
    puts.sort(key=lambda item: item["score"])

    best_call = calls[0] if calls else None
    best_put = puts[0] if puts else None

    selected = None

    if direction == "BULLISH":
        selected = best_call
    elif direction == "BEARISH":
        selected = best_put

    if direction == "BULLISH" and best_call is None:
        selection_status = "NO QUALIFIED CALL"
    elif direction == "BEARISH" and best_put is None:
        selection_status = "NO QUALIFIED PUT"
    elif direction == "BOTH":
        selection_status = (
            "REFERENCE ONLY - BOTH SIDES RETURNED"
        )
    else:
        selection_status = "QUALIFIED CONTRACT FOUND"

    return jsonify({
        "ok": True,
        "read_only": True,
        "order_sent": False,
        "approval_enabled": False,
        "underlying": underlying,
        "direction": direction,
        "selection_status": selection_status,
        "rules": {
            "expiration_type": "Weekly",
            "allowed_dte": allowed_dte,
            "target_abs_delta": [
                target_delta_min,
                target_delta_max
            ],
            "preferred_spread_pct_max": (
                preferred_spread_pct
            ),
            "reject_spread_pct_above": max_spread_pct,
            "liquidity_rule": (
                "Open interest and volume are ranking preferences; "
                "no hard cutoff is enabled in V1."
            ),
            "quantity_contracts": quantity_contracts,
            "max_loss_pct": max_loss_pct,
            "profit_target_pct": profit_target_pct,
            "exit_rule": (
                "EXIT WHEN -35% MAX LOSS OR +50% PROFIT TARGET "
                "IS REACHED, WHICHEVER OCCURS FIRST"
            ),
        },
        "eligible_expirations": [
            {
                "date": item["date"].isoformat(),
                "dte": item["dte"],
                "type": item["type"],
            }
            for item in eligible_expirations
        ],
        "qualified_candidate_count": len(all_candidates),
        "best_call": best_call,
        "best_put": best_put,
        "selected_contract": selected,
        "stream_notes": stream_notes,
        "risk_gate": {
            "quantity_contracts": quantity_contracts,
            "entry_reference": "LIVE ASK",
            "max_loss_pct": max_loss_pct,
            "profit_target_pct": profit_target_pct,
            "reward_risk": round(profit_target_pct / max_loss_pct, 4),
            "exit_rule": (
                "WHICHEVER OCCURS FIRST: -35% PREMIUM LOSS OR "
                "+50% PREMIUM GAIN"
            ),
            "approval": "DISABLED",
            "order_capability": "DISABLED - READ ONLY"
        },
        "next_step": (
            "Verify selector output against OptionStation Pro. "
            "No option order can be sent from this route."
        )
    })


# ==============================================================
# ODTS QQQ SIM APPROVE / PASS + 1-CONTRACT EXECUTION V1
# COMPLETELY SEPARATE FROM SOXL ORDER FUNCTIONS
# ==============================================================

def _odts_selector_snapshot(direction):
    """
    Reuse the already-verified ODTS option selector and return its
    selected contract as a plain dict. No order is placed here.
    """
    with app.test_request_context(
        f"/odts-option-test?direction={direction}",
        method="GET"
    ):
        response = odts_option_test()

    status_code = 200
    if isinstance(response, tuple):
        flask_response = response[0]
        if len(response) > 1:
            status_code = int(response[1])
    else:
        flask_response = response
        status_code = int(getattr(flask_response, "status_code", 200))

    try:
        payload = flask_response.get_json()
    except Exception:
        payload = None

    if status_code >= 400 or not isinstance(payload, dict):
        return False, {
            "error": "ODTS selector did not return a usable response.",
            "status_code": status_code,
        }

    if not payload.get("ok"):
        return False, payload

    selected = payload.get("selected_contract")
    if not isinstance(selected, dict) or not selected.get("symbol"):
        return False, {
            "error": "No qualified ODTS option contract is available.",
            "selector": payload,
        }

    return True, selected


def _odts_new_proposal(direction, selected):
    proposal_id = secrets.token_urlsafe(18)
    now_ts = time.time()

    proposal = {
        "proposal_id": proposal_id,
        "created_at": now_ts,
        "expires_at": now_ts + ODTS_PROPOSAL_TTL_SECONDS,
        "direction": direction,
        "symbol": str(selected.get("symbol", "")).strip(),
        "option_type": str(selected.get("option_type", "")).strip(),
        "expiration": selected.get("expiration"),
        "dte": selected.get("dte"),
        "strike": selected.get("strike"),
        "delta": selected.get("delta"),
        "abs_delta": selected.get("abs_delta"),
        "bid": selected.get("bid"),
        "ask": selected.get("ask"),
        "spread_pct": selected.get("spread_pct"),
        "quantity_contracts": 1,
        "gross_premium_cost": selected.get("gross_premium_cost"),
        "max_loss_pct": 35.0,
        "planned_exit_option_price": selected.get("planned_exit_option_price"),
        "planned_risk_dollars": selected.get("planned_risk_dollars"),
        "profit_target_pct": 50.0,
        "planned_target_option_price": selected.get("planned_target_option_price"),
        "planned_profit_dollars": selected.get("planned_profit_dollars"),
        "reward_risk": selected.get("reward_risk"),
        "used": False,
    }

    odts_proposals[proposal_id] = proposal

    # Keep memory bounded.
    cutoff = now_ts - 900
    stale_ids = [
        key for key, value in odts_proposals.items()
        if value.get("created_at", 0) < cutoff
    ]
    for key in stale_ids:
        odts_proposals.pop(key, None)

    return proposal


def submit_odts_sim_option_limit_order(
    access_token,
    option_symbol,
    limit_price,
    quantity=1,
):
    """
    ODTS-only long option entry. This function cannot route to LIVE,
    cannot trade SOXL, and cannot submit more than one contract.
    """
    if not odts_sim_environment_ok():
        return False, {
            "error": "Blocked: ODTS option orders require the TradeStation SIM API base URL."
        }

    if ODTS_SIM_TRADING_ENABLED != "YES":
        return False, {
            "error": "Blocked: ODTS_SIM_TRADING_ENABLED is not YES."
        }

    if not TS_SIM_ACCOUNT_ID:
        return False, {
            "error": "Blocked: TS_SIM_ACCOUNT_ID is missing."
        }

    if int(quantity) != 1:
        return False, {
            "error": "Blocked: ODTS V1 permits exactly 1 option contract."
        }

    option_symbol = str(option_symbol or "").strip()
    if not option_symbol.upper().startswith("QQQ"):
        return False, {
            "error": "Blocked: ODTS V1 permits QQQ option symbols only."
        }

    try:
        limit_price = round(float(limit_price), 2)
    except (TypeError, ValueError):
        return False, {"error": "Blocked: invalid limit price."}

    if limit_price <= 0:
        return False, {"error": "Blocked: limit price must be positive."}

    if not odts_sim_session_now():
        return False, {
            "error": "Blocked: ODTS SIM option entry is outside 09:30-16:00 ET."
        }

    url = f"{TS_API_BASE_URL}/orderexecution/orders"
    order = {
        "AccountID": TS_SIM_ACCOUNT_ID,
        "Symbol": option_symbol,
        "Quantity": "1",
        "OrderType": "Limit",
        "LimitPrice": str(limit_price),
        "TradeAction": "BUYTOOPEN",
        "TimeInForce": {"Duration": "DAY"},
    }

    try:
        response = requests.post(
            url,
            headers=ts_headers(access_token),
            json=order,
            timeout=20,
        )
    except requests.RequestException as exc:
        return False, {
            "error": f"ODTS SIM option order request failed: {exc}",
            "submitted_order": order,
        }

    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}

    if not response.ok:
        return False, {
            "status_code": response.status_code,
            "response": body,
            "submitted_order": order,
        }

    return True, {
        "response": body,
        "submitted_order": order,
    }


@app.get("/odts-approval-test")
def odts_approval_test():
    direction = str(request.args.get("direction", "")).strip().upper()

    if direction not in {"BULLISH", "BEARISH"}:
        return jsonify({
            "ok": False,
            "order_sent": False,
            "approval_enabled": False,
            "error": "direction must be BULLISH or BEARISH",
        }), 400

    access_token, error = get_valid_access_token()
    if not access_token:
        return jsonify({
            "ok": False,
            "order_sent": False,
            "approval_enabled": False,
            "error": error,
            "next_step": "Open /login",
        }), 401

    selector_ok, selected = _odts_selector_snapshot(direction)
    if not selector_ok:
        return jsonify({
            "ok": False,
            "order_sent": False,
            "approval_enabled": False,
            "direction": direction,
            "error": selected.get("error", "No qualified contract."),
            "selector_detail": selected,
        }), 409

    proposal = _odts_new_proposal(direction, selected)
    proposal_id = proposal["proposal_id"]

    approval_enabled = bool(
        proposal.get("symbol")
        and float(proposal.get("ask") or 0) > 0
        and float(proposal.get("spread_pct") or 999) <= 15.0
        and 0.50 <= float(proposal.get("abs_delta") or 0) <= 0.65
    )

    return jsonify({
        "ok": True,
        "environment": "SIM",
        "direction": direction,
        "approval_enabled": approval_enabled,
        "order_sent": False,
        "order_gate": {
            "ODTS_SIM_TRADING_ENABLED": ODTS_SIM_TRADING_ENABLED,
            "sim_api": odts_sim_environment_ok(),
            "regular_session_now": odts_sim_session_now(),
            "quantity_contracts": 1,
            "trade_action": "BUYTOOPEN",
            "order_type": "Limit",
            "limit_reference": "Current Ask revalidated at APPROVE time",
        },
        "proposal": proposal,
        "approve_action": (
            f"/odts-approval-decision?proposal_id={proposal_id}&decision=APPROVE"
        ),
        "pass_action": (
            f"/odts-approval-decision?proposal_id={proposal_id}&decision=PASS"
        ),
        "safety": (
            "QQQ option execution is isolated from SOXL. APPROVE can submit only "
            "when the independent ODTS SIM gate is YES, market is open, the "
            "proposal is fresh, and the contract passes immediate revalidation."
        ),
    })


@app.route("/odts-approval-decision", methods=["GET", "POST"])
def odts_approval_decision():
    decision = str(request.values.get("decision", "")).strip().upper()
    proposal_id = str(request.values.get("proposal_id", "")).strip()

    if decision not in {"APPROVE", "PASS"}:
        return jsonify({
            "ok": False,
            "order_sent": False,
            "error": "decision must be APPROVE or PASS",
        }), 400

    proposal = odts_proposals.get(proposal_id)
    if not proposal:
        return jsonify({
            "ok": False,
            "order_sent": False,
            "error": "Proposal not found or expired. Generate a new approval proposal.",
        }), 404

    if decision == "PASS":
        proposal["used"] = True
        return jsonify({
            "ok": True,
            "environment": "SIM",
            "direction": proposal.get("direction"),
            "decision": "PASS",
            "proposal_id": proposal_id,
            "approval_recorded": False,
            "order_sent": False,
            "result": "PASSED - NO ORDER",
            "safety": "No TradeStation order function was called.",
        })

    # From this point forward, all checks and submission are serialized.
    with odts_order_lock:
        now_ts = time.time()

        if proposal.get("used"):
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: this proposal has already been used.",
            }), 409

        if now_ts > float(proposal.get("expires_at", 0)):
            proposal["used"] = True
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: approval proposal expired. Generate a fresh proposal.",
            }), 409

        if ODTS_SIM_TRADING_ENABLED != "YES":
            return jsonify({
                "ok": False,
                "environment": "SIM",
                "decision": "APPROVE",
                "approval_recorded": True,
                "order_sent": False,
                "proposal_id": proposal_id,
                "result": "APPROVED BUT ORDER BLOCKED",
                "error": "ODTS_SIM_TRADING_ENABLED is not YES.",
            }), 403

        if not odts_sim_environment_ok():
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: TradeStation API base is not SIM.",
            }), 403

        if not odts_sim_session_now():
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: outside 09:30-16:00 ET regular session.",
            }), 403

        direction = proposal.get("direction")
        selector_ok, current = _odts_selector_snapshot(direction)
        if not selector_ok:
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: current selector revalidation failed.",
                "detail": current,
            }), 409

        if str(current.get("symbol", "")).strip() != proposal.get("symbol"):
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: selected contract changed since approval proposal.",
                "approved_symbol": proposal.get("symbol"),
                "current_symbol": current.get("symbol"),
            }), 409

        try:
            approved_ask = float(proposal.get("ask"))
            current_ask = float(current.get("ask"))
            current_spread_pct = float(current.get("spread_pct"))
            current_abs_delta = float(current.get("abs_delta"))
        except (TypeError, ValueError):
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: invalid live option quote during revalidation.",
            }), 409

        if current_spread_pct > 15.0:
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: live spread now exceeds 15%.",
                "spread_pct": current_spread_pct,
            }), 409

        if not (0.50 <= current_abs_delta <= 0.65):
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: live Delta moved outside 0.50-0.65.",
                "abs_delta": current_abs_delta,
            }), 409

        max_allowed_ask = approved_ask * (1.0 + ODTS_MAX_ASK_INCREASE_PCT / 100.0)
        if current_ask > max_allowed_ask:
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: Ask increased more than 5% since proposal.",
                "approved_ask": approved_ask,
                "current_ask": current_ask,
                "max_allowed_ask": round(max_allowed_ask, 4),
            }), 409

        if (
            odts_last_order.get("symbol") == proposal.get("symbol")
            and now_ts - float(odts_last_order.get("time", 0))
            < ODTS_DUPLICATE_WINDOW_SECONDS
        ):
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": "Blocked: duplicate ODTS option order window is active.",
            }), 409

        access_token, error = get_valid_access_token()
        if not access_token:
            return jsonify({
                "ok": False,
                "order_sent": False,
                "error": error,
                "next_step": "Open /login",
            }), 401

        # Freeze this proposal immediately before the one allowed submit call.
        proposal["used"] = True
        ok, order_result = submit_odts_sim_option_limit_order(
            access_token=access_token,
            option_symbol=proposal.get("symbol"),
            limit_price=current_ask,
            quantity=1,
        )

        if not ok:
            return jsonify({
                "ok": False,
                "environment": "SIM",
                "decision": "APPROVE",
                "approval_recorded": True,
                "order_sent": False,
                "proposal_id": proposal_id,
                "error": "TradeStation SIM option order was not accepted.",
                "detail": order_result,
            }), 502

        # Preserve the submitted order identity for later fill capture.
        order_id = None
        try:
            orders = (order_result.get("response") or {}).get("Orders", [])
            if isinstance(orders, list) and orders:
                order_id = str(orders[0].get("OrderID") or "").strip() or None
        except Exception:
            order_id = None

        odts_last_order["proposal_id"] = proposal_id
        odts_last_order["symbol"] = proposal.get("symbol")
        odts_last_order["order_id"] = order_id
        odts_last_order["limit_price"] = round(current_ask, 2)
        odts_last_order["time"] = time.time()

        return jsonify({
            "ok": True,
            "environment": "SIM",
            "direction": direction,
            "decision": "APPROVE",
            "approval_recorded": True,
            "order_sent": True,
            "proposal_id": proposal_id,
            "selected_contract": proposal.get("symbol"),
            "quantity_contracts": 1,
            "trade_action": "BUYTOOPEN",
            "order_type": "Limit",
            "limit_price": round(current_ask, 2),
            "max_loss_pct": 35.0,
            "profit_target_pct": 50.0,
            "result": "ODTS QQQ 1-CONTRACT SIM ORDER SUBMITTED",
            "tradestation": order_result,
            "safety": "SOXL order functions were not used.",
        })


# ==============================================================
# ODTS QQQ FILL CAPTURE / POSITION MONITOR - READ ONLY V1
# ==============================================================

def get_odts_sim_option_position(access_token, option_symbol):
    """Read one exact QQQ option position from the TradeStation SIM account.

    This function is READ ONLY. It never submits, replaces, or cancels an order.
    TradeStation's Positions resource supplies AveragePrice, Quantity and Symbol;
    AveragePrice is used as the authoritative filled-position entry reference.
    """
    option_symbol = str(option_symbol or "").strip()
    if not option_symbol.upper().startswith("QQQ"):
        return False, None, {"error": "ODTS fill capture permits QQQ option symbols only."}

    if not odts_sim_environment_ok():
        return False, None, {"error": "ODTS fill capture requires the SIM API base URL."}

    if not TS_SIM_ACCOUNT_ID:
        return False, None, {"error": "TS_SIM_ACCOUNT_ID is missing."}

    url = f"{TS_API_BASE_URL}/brokerage/accounts/{TS_SIM_ACCOUNT_ID}/positions"
    try:
        response = requests.get(
            url,
            headers=ts_headers(access_token),
            params={"symbol": option_symbol},
            timeout=20,
        )
    except requests.RequestException as exc:
        return False, None, {"error": f"ODTS position request failed: {exc}"}

    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}

    if not response.ok:
        return False, None, {"status_code": response.status_code, "response": body}

    positions = body.get("Positions", []) if isinstance(body, dict) else []
    if not isinstance(positions, list):
        positions = []

    target = option_symbol.upper()
    for position in positions:
        symbol = str(position.get("Symbol") or "").strip().upper()
        if symbol != target:
            continue

        try:
            qty = float(position.get("Quantity") or 0)
        except (TypeError, ValueError):
            qty = 0.0
        try:
            avg_price = float(position.get("AveragePrice") or 0)
        except (TypeError, ValueError):
            avg_price = 0.0

        normalized = {
            "account_id": position.get("AccountID"),
            "position_id": position.get("PositionID"),
            "symbol": position.get("Symbol"),
            "asset_type": position.get("AssetType"),
            "long_short": position.get("LongShort"),
            "quantity": qty,
            "average_price": avg_price,
            "last": position.get("Last"),
            "bid": position.get("Bid"),
            "ask": position.get("Ask"),
            "market_value": position.get("MarketValue"),
            "unrealized_pl": position.get("UnrealizedProfitLoss"),
            "unrealized_pl_pct": position.get("UnrealizedProfitLossPercent"),
            "timestamp": position.get("Timestamp"),
        }
        return True, normalized, body

    return True, None, body


@app.get("/odts-fill-capture-test")
def odts_fill_capture_test():
    """READ-ONLY test of automatic fill/position capture.

    Before the first SIM trade this should normally return WAITING_FOR_POSITION.
    After a BUYTOOPEN fills, it captures TradeStation AveragePrice and computes
    the actual -35% and +50% option-price levels from the fill.
    """
    access_token, error = get_valid_access_token()
    if not access_token:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "error": error,
            "next_step": "Open /login",
        }), 401

    symbol = str(request.args.get("symbol") or odts_last_order.get("symbol") or "").strip()
    if not symbol:
        return jsonify({
            "ok": True,
            "read_only": True,
            "order_sent": False,
            "status": "NO_ODTS_ORDER_YET",
            "message": "No ODTS option symbol is stored yet. This is expected before the first SIM entry.",
            "last_order": odts_last_order,
        })

    ok, position, detail = get_odts_sim_option_position(access_token, symbol)
    if not ok:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": detail,
        }), 502

    if not position:
        return jsonify({
            "ok": True,
            "read_only": True,
            "order_sent": False,
            "status": "WAITING_FOR_POSITION",
            "symbol": symbol,
            "stored_order_id": odts_last_order.get("order_id"),
            "stored_limit_price": odts_last_order.get("limit_price"),
            "message": "No matching SIM position exists yet; no fill price has been captured.",
        })

    avg = float(position.get("average_price") or 0)
    if avg <= 0:
        return jsonify({
            "ok": True,
            "read_only": True,
            "order_sent": False,
            "status": "POSITION_FOUND_FILL_PRICE_PENDING",
            "position": position,
        })

    stop_price = round(avg * 0.65, 2)
    target_price = round(avg * 1.50, 2)
    actual_cost = round(avg * 100.0, 2)
    planned_risk = round(actual_cost * 0.35, 2)
    planned_profit = round(actual_cost * 0.50, 2)

    return jsonify({
        "ok": True,
        "read_only": True,
        "order_sent": False,
        "status": "FILL_CAPTURED_FROM_SIM_POSITION",
        "environment": "SIM",
        "symbol": symbol,
        "quantity_contracts": position.get("quantity"),
        "actual_fill_price": avg,
        "actual_contract_cost": actual_cost,
        "max_loss_pct": 35.0,
        "actual_stop_option_price": stop_price,
        "planned_risk_dollars": planned_risk,
        "profit_target_pct": 50.0,
        "actual_target_option_price": target_price,
        "planned_profit_dollars": planned_profit,
        "reward_risk": round(50.0 / 35.0, 4),
        "position": position,
        "stored_order_id": odts_last_order.get("order_id"),
        "safety": "READ ONLY. This endpoint cannot submit or close an order.",
    })



def submit_odts_sim_option_exit_order(access_token, option_symbol, quantity=1):
    """ODTS-only long-option exit framework. SIM only, exactly 1 contract.

    This function is hard-gated by ODTS_SIM_EXIT_ENABLED and is not called by
    the dry-run monitor. It uses SELLTOCLOSE with a market order only after the
    monitor has independently produced EXIT_STOP or EXIT_TARGET.
    """
    if not odts_sim_environment_ok():
        return False, {"error": "Blocked: ODTS option exits require the TradeStation SIM API base URL."}
    if ODTS_SIM_EXIT_ENABLED != "YES":
        return False, {"error": "Blocked: ODTS_SIM_EXIT_ENABLED is not YES."}
    if not TS_SIM_ACCOUNT_ID:
        return False, {"error": "Blocked: TS_SIM_ACCOUNT_ID is missing."}
    if int(quantity) != 1:
        return False, {"error": "Blocked: ODTS V1 permits exactly 1 option contract."}
    option_symbol = str(option_symbol or "").strip()
    if not option_symbol.upper().startswith("QQQ"):
        return False, {"error": "Blocked: ODTS V1 permits QQQ option symbols only."}
    if not odts_sim_session_now():
        return False, {"error": "Blocked: ODTS SIM option exit is outside 09:30-16:00 ET."}

    order = {
        "AccountID": TS_SIM_ACCOUNT_ID,
        "Symbol": option_symbol,
        "Quantity": "1",
        "OrderType": "Market",
        "TradeAction": "SELLTOCLOSE",
        "TimeInForce": {"Duration": "DAY"},
    }
    try:
        response = requests.post(
            f"{TS_API_BASE_URL}/orderexecution/orders",
            headers=ts_headers(access_token), json=order, timeout=20,
        )
    except requests.RequestException as exc:
        return False, {"error": f"ODTS SIM exit request failed: {exc}", "submitted_order": order}
    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}
    if not response.ok:
        return False, {"status_code": response.status_code, "response": body, "submitted_order": order}
    return True, {"response": body, "submitted_order": order}


@app.get("/odts-exit-framework-test")
def odts_exit_framework_test():
    """Safety/status test only. Never submits an exit order."""
    return jsonify({
        "ok": True,
        "environment": "SIM",
        "ODTS_SIM_EXIT_ENABLED": ODTS_SIM_EXIT_ENABLED,
        "quantity_contracts": 1,
        "trade_action": "SELLTOCLOSE",
        "order_type": "Market",
        "trigger_source": "EXIT_STOP or EXIT_TARGET from ODTS monitor",
        "exit_order_sent": False,
        "read_only_test": True,
        "safety": "Framework loaded. This test endpoint never calls the exit-order function.",
    })

# ==============================================================
# ODTS QQQ POSITION MONITOR / EXIT DECISION - READ ONLY V1
# ==============================================================

@app.get("/odts-position-monitor-test")
def odts_position_monitor_test():
    """READ-ONLY ODTS position monitor and exit-decision test.

    Uses the authoritative SIM position AveragePrice as the fill reference.
    For a long option, the current BID is used as the conservative executable
    exit reference. The endpoint returns HOLD, EXIT_STOP, or EXIT_TARGET only;
    it never calls SELLTOCLOSE or any other order function.
    """
    access_token, error = get_valid_access_token()
    if not access_token:
        return jsonify({
            "ok": False,
            "read_only": True,
            "exit_order_sent": False,
            "error": error,
            "next_step": "Open /login",
        }), 401

    symbol = str(request.args.get("symbol") or odts_last_order.get("symbol") or "").strip()
    if not symbol:
        return jsonify({
            "ok": True,
            "read_only": True,
            "exit_order_sent": False,
            "status": "NO_ODTS_ORDER_YET",
            "decision": "WAIT",
            "message": "No ODTS option symbol is stored yet. This is expected before the first SIM entry.",
        })

    ok, position, detail = get_odts_sim_option_position(access_token, symbol)
    if not ok:
        return jsonify({
            "ok": False,
            "read_only": True,
            "exit_order_sent": False,
            "symbol": symbol,
            "error": detail,
        }), 502

    if not position:
        return jsonify({
            "ok": True,
            "read_only": True,
            "exit_order_sent": False,
            "status": "WAITING_FOR_POSITION",
            "decision": "WAIT",
            "symbol": symbol,
            "message": "No matching SIM position exists yet; monitoring will begin after the BUYTOOPEN fill appears.",
        })

    try:
        avg = float(position.get("average_price") or 0)
    except (TypeError, ValueError):
        avg = 0.0
    try:
        qty = float(position.get("quantity") or 0)
    except (TypeError, ValueError):
        qty = 0.0
    try:
        bid = float(position.get("bid") or 0)
    except (TypeError, ValueError):
        bid = 0.0
    try:
        last = float(position.get("last") or 0)
    except (TypeError, ValueError):
        last = 0.0

    if avg <= 0 or qty <= 0:
        return jsonify({
            "ok": True,
            "read_only": True,
            "exit_order_sent": False,
            "status": "POSITION_DATA_NOT_READY",
            "decision": "WAIT",
            "position": position,
        })

    stop_price = round(avg * 0.65, 2)
    target_price = round(avg * 1.50, 2)

    # A long option is sold to exit, so BID is the conservative executable
    # reference. LAST is reported for information only and is not the trigger.
    current_exit_reference = bid if bid > 0 else None

    if current_exit_reference is None:
        decision = "WAIT"
        reason = "No valid BID is available; exit decision is intentionally withheld."
        status = "PRICE_NOT_READY"
        pnl_pct = None
        pnl_dollars = None
    else:
        pnl_pct = round(((current_exit_reference / avg) - 1.0) * 100.0, 2)
        pnl_dollars = round((current_exit_reference - avg) * 100.0 * qty, 2)
        if current_exit_reference <= stop_price:
            decision = "EXIT_STOP"
            reason = "Current BID is at or below the -35% stop level."
        elif current_exit_reference >= target_price:
            decision = "EXIT_TARGET"
            reason = "Current BID is at or above the +50% target level."
        else:
            decision = "HOLD"
            reason = "Current BID remains between the stop and target levels."
        status = "MONITORING_DRY_RUN"

    return jsonify({
        "ok": True,
        "read_only": True,
        "environment": "SIM",
        "exit_order_sent": False,
        "status": status,
        "decision": decision,
        "reason": reason,
        "symbol": symbol,
        "quantity_contracts": qty,
        "actual_fill_price": avg,
        "current_bid": bid if bid > 0 else None,
        "current_last": last if last > 0 else None,
        "exit_reference": "BID",
        "max_loss_pct": 35.0,
        "stop_option_price": stop_price,
        "profit_target_pct": 50.0,
        "target_option_price": target_price,
        "unrealized_pct_from_bid": pnl_pct,
        "unrealized_dollars_from_bid": pnl_dollars,
        "planned_exit_action": "SELLTOCLOSE",
        "safety": "DRY RUN ONLY. This endpoint cannot submit, replace, cancel, or close any order.",
        "position": position,
    })


# ==============================================================
# POSITION
# ==============================================================

def get_soxl_position(
    access_token
):
    url = (
        f"{TS_API_BASE_URL}"
        f"/brokerage/accounts/"
        f"{TS_SIM_ACCOUNT_ID}"
        f"/positions"
    )

    try:
        response = requests.get(
            url,
            headers=ts_headers(
                access_token
            ),
            timeout=20
        )

    except requests.RequestException as exc:
        return (
            False,
            None,
            {
                "error":
                f"Position request failed: {exc}"
            }
        )

    try:
        body = response.json()

    except ValueError:
        body = {
            "raw_response":
            response.text[:1500]
        }

    if not response.ok:
        return (
            False,
            None,
            {
                "status_code":
                    response.status_code,
                "response":
                    body
            }
        )

    positions = (
        body.get("Positions", [])
        if isinstance(body, dict)
        else []
    )

    if not isinstance(
        positions,
        list
    ):
        positions = []

    for position in positions:
        symbol = (
            str(
                position.get(
                    "Symbol",
                    ""
                )
            )
            .upper()
            .strip()
        )

        if symbol == ALLOWED_SYMBOL:
            raw_qty = (
                position.get(
                    "Quantity"
                )
                or position.get(
                    "LongQuantity"
                )
                or 0
            )

            try:
                quantity = float(
                    raw_qty
                )

            except (
                TypeError,
                ValueError
            ):
                quantity = 0.0

            return (
                True,
                quantity,
                body
            )

    return (
        True,
        0.0,
        body
    )


# ==============================================================
# ORDER SUBMISSION
# ==============================================================

def submit_sim_market_order(
    access_token,
    action
):
    if not sim_environment_ok():
        return (
            False,
            {
                "error":
                "Blocked: API base URL "
                "is not TradeStation SIM."
            }
        )

    if not TS_SIM_ACCOUNT_ID:
        return (
            False,
            {
                "error":
                "Blocked: TS_SIM_ACCOUNT_ID "
                "is missing."
            }
        )

    url = (
        f"{TS_API_BASE_URL}"
        f"/orderexecution/orders"
    )

    order = {
        "AccountID":
            TS_SIM_ACCOUNT_ID,
        "Symbol":
            ALLOWED_SYMBOL,
        "Quantity":
            str(MAX_TEST_QTY),
        "OrderType":
            "Market",
        "TradeAction":
            action,
        "TimeInForce": {
            "Duration": "DAY"
        },
    }

    try:
        response = requests.post(
            url,
            headers=ts_headers(
                access_token
            ),
            json=order,
            timeout=20
        )

    except requests.RequestException as exc:
        return (
            False,
            {
                "error":
                f"Order request failed: {exc}"
            }
        )

    try:
        body = response.json()

    except ValueError:
        body = {
            "raw_response":
            response.text[:1500]
        }

    if not response.ok:
        return (
            False,
            {
                "status_code":
                    response.status_code,
                "response":
                    body,
                "submitted_order":
                    order,
            }
        )

    return (
        True,
        body
    )


# ==============================================================
# LIVE POSITION / CONFIRM / ORDER HELPERS
# ==============================================================

def get_soxl_live_position(access_token):
    if not TS_LIVE_ACCOUNT_ID:
        return (
            False,
            None,
            {"error": "TS_LIVE_ACCOUNT_ID is missing."}
        )

    url = (
        f"{TS_LIVE_API_BASE_URL}"
        f"/brokerage/accounts/"
        f"{TS_LIVE_ACCOUNT_ID}"
        f"/positions"
    )

    try:
        response = requests.get(
            url,
            headers=ts_headers(access_token),
            timeout=20
        )
    except requests.RequestException as exc:
        return (
            False,
            None,
            {"error": f"LIVE position request failed: {exc}"}
        )

    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}

    if not response.ok:
        return (
            False,
            None,
            {
                "status_code": response.status_code,
                "response": body
            }
        )

    positions = (
        body.get("Positions", [])
        if isinstance(body, dict)
        else []
    )

    if not isinstance(positions, list):
        positions = []

    for position in positions:
        symbol = str(position.get("Symbol", "")).upper().strip()
        if symbol == ALLOWED_SYMBOL:
            raw_qty = (
                position.get("Quantity")
                or position.get("LongQuantity")
                or 0
            )
            try:
                quantity = float(raw_qty)
            except (TypeError, ValueError):
                quantity = 0.0

            return (True, quantity, body)

    return (True, 0.0, body)


def build_live_market_order(action):
    return {
        "AccountID": TS_LIVE_ACCOUNT_ID,
        "Symbol": ALLOWED_SYMBOL,
        "Quantity": str(LIVE_MAX_QTY),
        "OrderType": "Market",
        "TradeAction": action,
        "TimeInForce": {
            "Duration": "DAY"
        },
        "Route": "Intelligent"
    }


def confirm_live_market_order(access_token, action="BUY"):
    if action not in {"BUY", "SELL"}:
        return (False, {"error": "action must be BUY or SELL"})

    if not TS_LIVE_ACCOUNT_ID:
        return (False, {"error": "TS_LIVE_ACCOUNT_ID is missing."})

    url = f"{TS_LIVE_API_BASE_URL}/orderexecution/orderconfirm"
    order = build_live_market_order(action)

    try:
        response = requests.post(
            url,
            headers=ts_headers(access_token),
            json=order,
            timeout=20
        )
    except requests.RequestException as exc:
        return (
            False,
            {"error": f"LIVE Confirm Order request failed: {exc}"}
        )

    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}

    if not response.ok:
        return (
            False,
            {
                "status_code": response.status_code,
                "response": body,
                "confirmed_order": order
            }
        )

    return (True, body)


def submit_live_market_order(
    access_token,
    action,
    strategy_name
):
    if action not in {"BUY", "SELL"}:
        return (False, {"error": "action must be BUY or SELL"})

    if strategy_name not in ALLOWED_STRATEGIES:
        return (
            False,
            {"error": "Blocked: strategy is not authorized for LIVE."}
        )

    if LIVE_TRADING_ENABLED != "YES":
        return (
            False,
            {"error": "Blocked: LIVE_TRADING_ENABLED is not YES."}
        )

    if not live_strategy_mode_selected(strategy_name):
        return (
            False,
            {
                "error": (
                    f"Blocked: {strategy_name} execution mode "
                    "is not LIVE."
                )
            }
        )

    if not live_order_capability_ready():
        return (
            False,
            {"error": "Blocked: LIVE order capability is not ready."}
        )

    if not live_market_session_now():
        return (
            False,
            {
                "error": (
                    "Blocked: LIVE Market order is outside "
                    "09:30-16:00 ET."
                )
            }
        )

    url = f"{TS_LIVE_API_BASE_URL}/orderexecution/orders"
    order = build_live_market_order(action)

    try:
        response = requests.post(
            url,
            headers=ts_headers(access_token),
            json=order,
            timeout=20
        )
    except requests.RequestException as exc:
        return (
            False,
            {"error": f"LIVE order request failed: {exc}"}
        )

    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}

    if not response.ok:
        return (
            False,
            {
                "status_code": response.status_code,
                "response": body,
                "submitted_order": order
            }
        )

    return (True, body)


# ==============================================================
# DUPLICATE SIGNAL PROTECTION
# ==============================================================

def duplicate_signal(
    action,
    symbol,
    strategy_name
):
    now = time.time()

    key = (
        f"{strategy_name}|"
        f"{action}|"
        f"{symbol}"
    )

    if (
        last_signal["key"] == key
        and now
        - last_signal["time"]
        < DUPLICATE_WINDOW_SECONDS
    ):
        return True

    last_signal["key"] = key
    last_signal["time"] = now

    return False


# ==============================================================
# HOME / HEALTH
# ==============================================================

@app.get("/")
def home():
    return jsonify({
        "service":
            "ZeroLag AutoTrader",

        "status":
            "running",

        "mode":
            "TRADESTATION CONTROLLED SIM/LIVE EXECUTION",

        "environment":
            (
                "SIM"
                if sim_environment_ok()
                else "BLOCKED"
            ),

        "trading_enabled":
            TRADING_ENABLED,

        "orders_available":
            order_capability_ready(),

        "soxl_regular_execution_mode":
            SOXL_REGULAR_EXECUTION_MODE,

        "soxl_overnight_execution_mode":
            SOXL_OVERNIGHT_EXECUTION_MODE,

        "live_trading_enabled":
            LIVE_TRADING_ENABLED,

        "live_order_capability_ready":
            live_order_capability_ready(),

        "allowed_symbol":
            ALLOWED_SYMBOL,

        "allowed_strategies":
            sorted(
                ALLOWED_STRATEGIES
            ),

        "max_test_quantity":
            MAX_TEST_QTY,

        "journal_available":
            True,

        "tv_csv":
            "/journal/tv.csv",

        "ts_csv":
            "/journal/ts.csv",
    })


@app.get("/health")
def health():
    return jsonify({
        "ok":
            True,

        "service":
            "ZeroLag AutoTrader",

        "api_base":
            TS_API_BASE_URL,

        "sim_environment_ok":
            sim_environment_ok(),

        "trading_enabled":
            TRADING_ENABLED,

        "orders_available":
            order_capability_ready(),
    })


# ==============================================================
# JOURNAL ROUTES
# ==============================================================

@app.get("/journal/status")
def journal_status():
    ensure_journal_files()

    def count_rows(path):
        try:
            with open(
                path,
                "r",
                encoding="utf-8"
            ) as f:
                return max(
                    sum(1 for _ in f) - 1,
                    0
                )

        except Exception:
            return 0

    return jsonify({
        "ok":
            True,

        "journal_dir":
            JOURNAL_DIR,

        "tv_rows":
            count_rows(
                TV_CSV_PATH
            ),

        "ts_rows":
            count_rows(
                TS_CSV_PATH
            ),

        "tv_csv":
            "/journal/tv.csv",

        "ts_csv":
            "/journal/ts.csv",

        "storage_note":
            "Default /tmp storage is temporary "
            "until a Render persistent disk "
            "is mounted.",
    })


@app.get("/journal/tv.csv")
def download_tv_csv():
    ensure_journal_files()

    return send_file(
        TV_CSV_PATH,
        mimetype="text/csv",
        as_attachment=False,
        download_name="TV_Signals.csv"
    )


@app.get("/journal/ts.csv")
def download_ts_csv():
    ensure_journal_files()

    return send_file(
        TS_CSV_PATH,
        mimetype="text/csv",
        as_attachment=False,
        download_name="TS_Executions.csv"
    )


# ==============================================================
# AUTH STATUS
# ==============================================================

@app.get("/auth-status")
def auth_status():
    missing = missing_config()

    return jsonify({
        "configured":
            len(missing) == 0,

        "missing_environment_variables":
            missing,

        "authenticated":
            bool(
                token_store.get(
                    "access_token"
                )
            ),

        "refresh_token_present":
            bool(
                token_store.get(
                    "refresh_token"
                )
            ),

        "trading_enabled":
            TRADING_ENABLED,

        "orders_available":
            order_capability_ready(),

        "sim_environment_ok":
            sim_environment_ok(),

        "api_base":
            TS_API_BASE_URL,

        "live_api_base":
            TS_LIVE_API_BASE_URL,

        "soxl_regular_execution_mode":
            SOXL_REGULAR_EXECUTION_MODE,

        "soxl_overnight_execution_mode":
            SOXL_OVERNIGHT_EXECUTION_MODE,

        "live_trading_enabled":
            LIVE_TRADING_ENABLED,

        "live_account_configured":
            bool(TS_LIVE_ACCOUNT_ID),

        "live_order_capability_ready":
            live_order_capability_ready(),

        "redirect_uri":
            TS_REDIRECT_URI,
    })


# ==============================================================
# LOGIN
# ==============================================================

@app.get("/login")
def login():
    global oauth_state

    missing = [
        x
        for x in [
            "TS_CLIENT_ID",
            "TS_CLIENT_SECRET",
            "TS_REDIRECT_URI"
        ]
        if not os.getenv(
            x,
            ""
        ).strip()
    ]

    if missing:
        return jsonify({
            "ok":
                False,

            "error":
                "Missing Render "
                "environment variables",

            "missing":
                missing
        }), 500

    oauth_state = (
        secrets.token_urlsafe(32)
    )

    params = {
        "response_type":
            "code",

        "client_id":
            TS_CLIENT_ID,

        "audience":
            TS_AUDIENCE,

        "redirect_uri":
            TS_REDIRECT_URI,

        "scope":
            TS_SCOPES,

        "state":
            oauth_state,

        "prompt":
            "login",
    }

    return redirect(
        f"{TS_AUTHORIZE_URL}?"
        f"{urlencode(params)}"
    )


# ==============================================================
# AUTH CALLBACK
# ==============================================================

@app.get("/auth/callback")
def auth_callback():
    global oauth_state

    error = request.args.get(
        "error"
    )

    if error:
        return jsonify({
            "ok":
                False,

            "error":
                error,

            "error_description":
                request.args.get(
                    "error_description",
                    ""
                ),
        }), 400

    code = request.args.get(
        "code"
    )

    returned_state = (
        request.args.get(
            "state"
        )
    )

    if not code:
        return jsonify({
            "ok":
                False,

            "error":
                "No authorization code "
                "was returned by TradeStation."
        }), 400

    if (
        not oauth_state
        or returned_state
        != oauth_state
    ):
        return jsonify({
            "ok":
                False,

            "error":
                "OAuth state check failed. "
                "Please restart at /login."
        }), 400

    payload = {
        "grant_type":
            "authorization_code",

        "client_id":
            TS_CLIENT_ID,

        "client_secret":
            TS_CLIENT_SECRET,

        "code":
            code,

        "redirect_uri":
            TS_REDIRECT_URI,
    }

    try:
        response = requests.post(
            TS_TOKEN_URL,
            data=payload,
            headers={
                "Content-Type":
                "application/x-www-form-urlencoded"
            },
            timeout=20
        )

    except requests.RequestException as exc:
        return jsonify({
            "ok":
                False,

            "error":
                f"Token request failed: {exc}"
        }), 502

    oauth_state = None

    if not response.ok:
        return jsonify({
            "ok":
                False,

            "error":
                "TradeStation token "
                "exchange failed",

            "status_code":
                response.status_code,

            "details":
                response.text[:1000],
        }), response.status_code

    data = response.json()

    if not data.get(
        "access_token"
    ):
        return jsonify({
            "ok":
                False,

            "error":
                "TradeStation response "
                "did not contain an access token."
        }), 502

    save_token_response(
        data
    )

    return """
    <html>
    <body style="font-family:Arial,sans-serif;margin:40px;">
    <h2>TradeStation authentication successful.</h2>
    <p>Render received an access token successfully.</p>
    <p><strong>Trading remains controlled by TRADING_ENABLED.</strong></p>
    <p><a href="/account-test">Test TradeStation SIM account connection</a></p>
    <p><a href="/live-account-test">Test TradeStation LIVE accounts - READ ONLY</a></p>
    <p><a href="/live-position-test">Test SOXL LIVE position - READ ONLY</a></p>
    <p><a href="/live-confirm-buy-test">Confirm 1-share SOXL LIVE BUY - NO ORDER</a></p>
    <p><a href="/position-test">Test SOXL SIM position</a></p>
    <p><a href="/auth-status">View authentication status</a></p>
    </body>
    </html>
    """


# ==============================================================
# ACCOUNT TEST
# ==============================================================

@app.get("/account-test")
def account_test():
    access_token, error = (
        get_valid_access_token()
    )

    if not access_token:
        return jsonify({
            "ok":
                False,

            "error":
                error,

            "next_step":
                "Open /login"
        }), 401

    url = (
        f"{TS_API_BASE_URL}"
        f"/brokerage/accounts"
    )

    try:
        response = requests.get(
            url,
            headers={
                "Authorization":
                    f"Bearer {access_token}",

                "Accept":
                    "application/json"
            },
            timeout=20
        )

    except requests.RequestException as exc:
        return jsonify({
            "ok":
                False,

            "error":
                f"Account request failed: {exc}"
        }), 502

    try:
        body = response.json()

    except ValueError:
        body = {
            "raw_response":
                response.text[:1500]
        }

    if not response.ok:
        return jsonify({
            "ok":
                False,

            "status_code":
                response.status_code,

            "endpoint":
                "/brokerage/accounts",

            "response":
                body,
        }), response.status_code

    return jsonify({
        "ok":
            True,

        "environment":
            (
                "SIM"
                if sim_environment_ok()
                else "BLOCKED"
            ),

        "trading_enabled":
            TRADING_ENABLED,

        "orders_available":
            order_capability_ready(),

        "tradestation_response":
            body,
    })


# ==============================================================
# LIVE ACCOUNT TEST - READ ONLY / NO ORDER SUBMISSION
# ==============================================================

@app.get("/live-account-test")
def live_account_test():
    access_token, error = get_valid_access_token()

    if not access_token:
        return jsonify({
            "ok": False,
            "error": error,
            "next_step": "Open /login"
        }), 401

    url = (
        f"{TS_LIVE_API_BASE_URL}"
        f"/brokerage/accounts"
    )

    try:
        response = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json"
            },
            timeout=20
        )

    except requests.RequestException as exc:
        return jsonify({
            "ok": False,
            "environment": "LIVE",
            "live_order_submission_enabled": False,
            "error": f"LIVE account request failed: {exc}"
        }), 502

    try:
        body = response.json()

    except ValueError:
        body = {
            "raw_response": response.text[:1500]
        }

    if not response.ok:
        return jsonify({
            "ok": False,
            "environment": "LIVE",
            "live_order_submission_enabled": False,
            "status_code": response.status_code,
            "endpoint": "/brokerage/accounts",
            "response": body
        }), response.status_code

    return jsonify({
        "ok": True,
        "environment": "LIVE",
        "live_api_base": TS_LIVE_API_BASE_URL,
        "live_order_submission_enabled": False,
        "message": (
            "LIVE accounts retrieved successfully. "
            "This route cannot place an order."
        ),
        "tradestation_response": body
    })


# ==============================================================
# LIVE POSITION TEST - READ ONLY
# ==============================================================

@app.get("/live-position-test")
def live_position_test():
    access_token, error = get_valid_access_token()

    if not access_token:
        return jsonify({
            "ok": False,
            "error": error,
            "next_step": "Open /login"
        }), 401

    if not TS_LIVE_ACCOUNT_ID:
        return jsonify({
            "ok": False,
            "environment": "LIVE",
            "error": "TS_LIVE_ACCOUNT_ID is not configured in Render.",
            "live_order_submission_enabled": False
        }), 503

    ok, quantity, body = get_soxl_live_position(access_token)

    if not ok:
        return jsonify({
            "ok": False,
            "environment": "LIVE",
            "error": "LIVE SOXL position query failed.",
            "details": body
        }), 502

    return jsonify({
        "ok": True,
        "environment": "LIVE",
        "symbol": ALLOWED_SYMBOL,
        "quantity": quantity,
        "is_long": quantity > 0,
        "live_order_submission_enabled": (
            LIVE_TRADING_ENABLED == "YES"
            and (
                live_regular_mode_selected()
                or live_overnight_mode_selected()
            )
        ),
        "soxl_regular_execution_mode":
            SOXL_REGULAR_EXECUTION_MODE,
        "soxl_overnight_execution_mode":
            SOXL_OVERNIGHT_EXECUTION_MODE
    })


# ==============================================================
# LIVE CONFIRM ORDER TEST - NO ORDER IS PLACED
# ==============================================================

@app.get("/live-confirm-buy-test")
def live_confirm_buy_test():
    access_token, error = get_valid_access_token()

    if not access_token:
        return jsonify({
            "ok": False,
            "error": error,
            "next_step": "Open /login"
        }), 401

    if not TS_LIVE_ACCOUNT_ID:
        return jsonify({
            "ok": False,
            "environment": "LIVE",
            "error": "TS_LIVE_ACCOUNT_ID is not configured in Render.",
            "order_sent": False
        }), 503

    ok, body = confirm_live_market_order(access_token, "BUY")

    if not ok:
        return jsonify({
            "ok": False,
            "environment": "LIVE",
            "order_sent": False,
            "error": "TradeStation Confirm Order failed.",
            "details": body
        }), 502

    return jsonify({
        "ok": True,
        "environment": "LIVE",
        "symbol": ALLOWED_SYMBOL,
        "quantity": LIVE_MAX_QTY,
        "action": "BUY",
        "order_sent": False,
        "message": (
            "TradeStation Confirm Order succeeded. "
            "No LIVE order was placed."
        ),
        "tradestation_confirmation": body
    })


# ==============================================================
# POSITION TEST
# ==============================================================

@app.get("/position-test")
def position_test():
    access_token, error = (
        get_valid_access_token()
    )

    if not access_token:
        return jsonify({
            "ok":
                False,

            "error":
                error,

            "next_step":
                "Open /login"
        }), 401

    (
        ok,
        quantity,
        body
    ) = get_soxl_position(
        access_token
    )

    if not ok:
        return jsonify({
            "ok":
                False,

            "error":
                "TradeStation position "
                "query failed",

            "details":
                body,
        }), 502

    return jsonify({
        "ok":
            True,

        "symbol":
            ALLOWED_SYMBOL,

        "quantity":
            quantity,

        "is_long":
            quantity > 0,

        "trading_enabled":
            TRADING_ENABLED,

        "orders_available":
            order_capability_ready(),
    })


# ==============================================================
# WEBHOOK
# ==============================================================

@app.post("/webhook/<token>")
def webhook(token):

    # ----------------------------------------------------------
    # TOKEN CHECK
    # ----------------------------------------------------------

    if (
        not WEBHOOK_TOKEN
        or token != WEBHOOK_TOKEN
    ):
        log.warning(
            "Rejected webhook: invalid token"
        )

        return jsonify({
            "ok":
                False,

            "error":
                "Unauthorized webhook token"
        }), 401


    # ----------------------------------------------------------
    # JSON CHECK
    # ----------------------------------------------------------

    if not request.is_json:
        return jsonify({
            "ok":
                False,

            "error":
                "JSON body required"
        }), 400


    # ----------------------------------------------------------
    # READ SIGNAL
    # ----------------------------------------------------------

    payload = (
        request.get_json(
            silent=True
        )
        or {}
    )

    action = (
        str(
            payload.get(
                "action",
                ""
            )
        )
        .upper()
        .strip()
    )

    symbol = (
        str(
            payload.get(
                "symbol",
                ""
            )
        )
        .upper()
        .strip()
    )

    strategy_name = (
        str(
            payload.get(
                "strategy",
                ""
            )
        )
        .strip()
    )


    # ----------------------------------------------------------
    # ACTION CHECK
    # ----------------------------------------------------------

    if action not in {
        "BUY",
        "SELL"
    }:
        return jsonify({
            "ok":
                False,

            "error":
                "action must be BUY or SELL"
        }), 400


    # ----------------------------------------------------------
    # SYMBOL SAFETY
    # ----------------------------------------------------------

    if symbol != ALLOWED_SYMBOL:
        return jsonify({
            "ok":
                False,

            "error":
                f"Only {ALLOWED_SYMBOL} "
                "is allowed by this execution service.",
        }), 400


    # ----------------------------------------------------------
    # STRATEGY SAFETY
    # ONLY THE TWO CURRENT STRATEGIES MAY TRADE
    # ----------------------------------------------------------

    if (
        strategy_name
        not in ALLOWED_STRATEGIES
    ):
        log.warning(
            "STRATEGY BLOCKED | "
            "strategy=%s action=%s "
            "symbol=%s | allowed=%s",
            strategy_name,
            action,
            symbol,
            sorted(
                ALLOWED_STRATEGIES
            )
        )

        return jsonify({
            "ok":
                False,

            "received":
                True,

            "order_sent":
                False,

            "error":
                "Strategy is not authorized "
                "for this execution service.",

            "strategy":
                strategy_name,

            "allowed_strategies":
                sorted(
                    ALLOWED_STRATEGIES
                ),
        }), 403


    # ----------------------------------------------------------
    # RECORD WEBHOOK
    # ----------------------------------------------------------

    last_webhook["received"] = True

    last_webhook["payload"] = (
        payload
    )

    last_webhook["received_at"] = (
        time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime()
        )
    )

    journal_tv_signal(
        payload,
        action,
        symbol,
        strategy_name
    )

    log.info(
        "WEBHOOK | strategy=%s "
        "action=%s symbol=%s "
        "trading_enabled=%s",
        strategy_name,
        action,
        symbol,
        TRADING_ENABLED
    )


    # ----------------------------------------------------------
    # MASTER TRADING SWITCH
    # ----------------------------------------------------------

    if TRADING_ENABLED != "YES":
        return jsonify({
            "ok":
                True,

            "received":
                True,

            "dry_run":
                True,

            "trading_enabled":
                TRADING_ENABLED,

            "orders_available":
                order_capability_ready(),

            "message":
                "Webhook received. "
                "Trading is disabled; "
                "no TradeStation order was sent.",
        }), 200


    # ----------------------------------------------------------
    # SOXL LIVE ROUTE - REGULAR + OVERNIGHT INDEPENDENT
    #
    # Each TradingView strategy owns its own BUY/SELL sequence.
    # Regular and Overnight may each hold one share simultaneously.
    # Account-level maximum = 2 SOXL shares.
    # No BUY inheritance and no cross-strategy handoff.
    # ----------------------------------------------------------

    if live_strategy_mode_selected(strategy_name):
        if LIVE_TRADING_ENABLED != "YES":
            return jsonify({
                "ok": True, "received": True, "order_sent": False,
                "environment": "LIVE", "strategy": strategy_name,
                "message": "LIVE_TRADING_ENABLED is not YES; no order sent."
            }), 200

        if not live_order_capability_ready():
            return jsonify({
                "ok": False, "received": True, "order_sent": False,
                "environment": "LIVE", "strategy": strategy_name,
                "error": "LIVE order capability is not ready."
            }), 503

        if not live_market_session_now():
            return jsonify({
                "ok": True, "received": True, "order_sent": False,
                "environment": "LIVE", "strategy": strategy_name,
                "message": "LIVE order blocked outside 09:30-16:00 ET regular session."
            }), 200

        if duplicate_signal(action, symbol, strategy_name):
            return jsonify({
                "ok": True, "received": True, "duplicate_blocked": True,
                "order_sent": False, "environment": "LIVE",
                "strategy": strategy_name,
                "message": "Duplicate LIVE signal blocked."
            }), 200

        access_token, error = get_valid_access_token()
        if not access_token:
            return jsonify({
                "ok": False, "received": True, "order_sent": False,
                "environment": "LIVE", "strategy": strategy_name,
                "error": "TradeStation authentication is required.",
                "details": error, "next_step": "Open /login"
            }), 401

        # Serialize account check + order submission so simultaneous
        # Regular/Overnight signals cannot race each other.
        with live_order_lock:
            pos_ok, position_qty, pos_body = get_soxl_live_position(access_token)

            if not pos_ok:
                return jsonify({
                    "ok": False, "received": True, "order_sent": False,
                    "environment": "LIVE", "strategy": strategy_name,
                    "error": "LIVE position verification failed. Order blocked.",
                    "details": pos_body
                }), 502

            # Only long-only combined holdings of 0, 1, or 2 are allowed.
            if position_qty not in {0.0, 1.0, 2.0}:
                return jsonify({
                    "ok": False, "received": True, "order_sent": False,
                    "environment": "LIVE", "strategy": strategy_name,
                    "error": "LIVE safety block: SOXL account position must be 0, 1, or 2 shares.",
                    "position_quantity": position_qty
                }), 409

            # A BUY from either strategy is independent. It is allowed
            # whenever the combined account has fewer than 2 shares.
            if action == "BUY" and position_qty >= 2:
                return jsonify({
                    "ok": True, "received": True, "order_sent": False,
                    "environment": "LIVE", "strategy": strategy_name,
                    "message": "LIVE BUY blocked: combined SOXL maximum of 2 shares already reached.",
                    "position_quantity": position_qty
                }), 200

            # A SELL from either strategy closes exactly one share.
            # TradingView strategy logic remains responsible for pairing
            # that SELL with the same strategy's preceding BUY.
            if action == "SELL" and position_qty <= 0:
                return jsonify({
                    "ok": True, "received": True, "order_sent": False,
                    "environment": "LIVE", "strategy": strategy_name,
                    "message": "LIVE SELL blocked: account is already flat.",
                    "position_quantity": position_qty
                }), 200

            order_ok, order_response = submit_live_market_order(
                access_token, action, strategy_name
            )

            if not order_ok:
                log.error(
                    "LIVE ORDER FAILED | strategy=%s action=%s symbol=%s response=%s",
                    strategy_name, action, symbol, order_response
                )
                return jsonify({
                    "ok": False, "received": True, "order_sent": False,
                    "environment": "LIVE", "strategy": strategy_name,
                    "error": "TradeStation LIVE order submission failed.",
                    "details": order_response
                }), 502

            log.warning(
                "LIVE ORDER SENT | strategy=%s action=%s symbol=%s qty=%s "
                "account_position_before=%s response=%s",
                strategy_name, action, symbol, LIVE_MAX_QTY,
                position_qty, order_response
            )

            return jsonify({
                "ok": True, "received": True, "dry_run": False,
                "order_sent": True, "environment": "LIVE",
                "strategy": strategy_name, "symbol": symbol,
                "action": action, "quantity": LIVE_MAX_QTY,
                "account_position_before": position_qty,
                "tradestation_response": order_response
            }), 200


    # ----------------------------------------------------------
    # SIM-ONLY PROTECTION
    # ----------------------------------------------------------

    if not sim_environment_ok():
        return jsonify({
            "ok":
                False,

            "error":
                "Order blocked because "
                "TS_API_BASE_URL is not SIM.",
        }), 403


    # ----------------------------------------------------------
    # CONFIGURATION CHECK
    # ----------------------------------------------------------

    if not order_capability_ready():
        return jsonify({
            "ok":
                False,

            "error":
                "Order capability is not ready.",

            "missing":
                missing_config(),
        }), 503


    # ----------------------------------------------------------
    # DUPLICATE SIGNAL CHECK
    # ----------------------------------------------------------

    if duplicate_signal(
        action,
        symbol,
        strategy_name
    ):
        return jsonify({
            "ok":
                True,

            "received":
                True,

            "duplicate_blocked":
                True,

            "message":
                "Duplicate signal blocked. "
                "No order was sent.",
        }), 200


    # ----------------------------------------------------------
    # AUTHENTICATION
    # ----------------------------------------------------------

    access_token, error = (
        get_valid_access_token()
    )

    if not access_token:
        return jsonify({
            "ok":
                False,

            "error":
                "TradeStation authentication "
                "is required.",

            "details":
                error,

            "next_step":
                "Open /login",
        }), 401


    # ----------------------------------------------------------
    # CHECK ACTUAL TS SOXL POSITION
    # ----------------------------------------------------------

    (
        pos_ok,
        position_qty,
        pos_body
    ) = get_soxl_position(
        access_token
    )

    if not pos_ok:
        return jsonify({
            "ok":
                False,

            "error":
                "Position verification failed. "
                "Order blocked.",

            "details":
                pos_body,
        }), 502


    # ----------------------------------------------------------
    # REGULAR -> OVERNIGHT HANDOFF
    #
    # If a BUY arrives while TS already owns SOXL:
    # KEEP THE EXISTING POSITION.
    # DO NOT BUY A SECOND SHARE.
    # ----------------------------------------------------------

    if (
        action == "BUY"
        and position_qty > 0
    ):
        log.info(
            "HANDOFF / BUY BLOCKED | "
            "strategy=%s symbol=%s "
            "existing_position=%s | "
            "Existing SOXL long retained; "
            "no duplicate BUY sent.",
            strategy_name,
            symbol,
            position_qty
        )

        return jsonify({
            "ok":
                True,

            "received":
                True,

            "order_sent":
                False,

            "handoff":
                True,

            "strategy":
                strategy_name,

            "message":
                "Existing SOXL long retained. "
                "No duplicate BUY sent.",

            "position_quantity":
                position_qty,
        }), 200


    # ----------------------------------------------------------
    # SELL SAFETY
    #
    # NEVER CREATE AN ACCIDENTAL SHORT.
    # ----------------------------------------------------------

    if (
        action == "SELL"
        and position_qty <= 0
    ):
        log.info(
            "SELL BLOCKED | "
            "strategy=%s symbol=%s "
            "existing_position=%s | "
            "Account already flat; "
            "no SELL sent.",
            strategy_name,
            symbol,
            position_qty
        )

        return jsonify({
            "ok":
                True,

            "received":
                True,

            "order_sent":
                False,

            "strategy":
                strategy_name,

            "message":
                "SELL blocked: "
                "account already flat.",

            "position_quantity":
                position_qty,
        }), 200


    # ----------------------------------------------------------
    # SEND SIM ORDER
    # ----------------------------------------------------------

    order_ok, order_response = (
        submit_sim_market_order(
            access_token,
            action
        )
    )

    if not order_ok:
        log.error(
            "SIM ORDER FAILED | "
            "action=%s symbol=%s "
            "response=%s",
            action,
            symbol,
            order_response
        )

        return jsonify({
            "ok":
                False,

            "received":
                True,

            "order_sent":
                False,

            "error":
                "TradeStation SIM "
                "order submission failed.",

            "details":
                order_response,
        }), 502


    # ----------------------------------------------------------
    # ORDER SUCCESS
    # ----------------------------------------------------------

    log.info(
        "SIM ORDER SENT | "
        "action=%s symbol=%s "
        "qty=%s response=%s",
        action,
        symbol,
        MAX_TEST_QTY,
        order_response
    )


    # ----------------------------------------------------------
    # BACKGROUND JOURNAL
    # ----------------------------------------------------------

    threading.Thread(
        target=journal_ts_execution_background,
        args=(
            access_token,
            action,
            order_response
        ),
        daemon=True
    ).start()


    # ----------------------------------------------------------
    # WEBHOOK RESPONSE
    # ----------------------------------------------------------

    return jsonify({
        "ok":
            True,

        "received":
            True,

        "dry_run":
            False,

        "order_sent":
            True,

        "environment":
            "SIM",

        "strategy":
            strategy_name,

        "symbol":
            symbol,

        "action":
            action,

        "quantity":
            MAX_TEST_QTY,

        "tradestation_response":
            order_response,

        "journal":
            "queued",
    }), 200


# ==============================================================
# WEBHOOK STATUS
# ==============================================================

@app.get("/webhook-status")
def webhook_status():
    return jsonify({
        "ok":
            True,

        "last_webhook_received":
            last_webhook["received"],

        "received_at":
            last_webhook["received_at"],

        "payload":
            last_webhook["payload"],

        "trading_enabled":
            TRADING_ENABLED,

        "orders_available":
            order_capability_ready(),

        "soxl_regular_execution_mode":
            SOXL_REGULAR_EXECUTION_MODE,

        "live_trading_enabled":
            LIVE_TRADING_ENABLED,

        "live_order_capability_ready":
            live_order_capability_ready(),
    })


# ==============================================================
# START SERVER
# ==============================================================

if __name__ == "__main__":
    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
