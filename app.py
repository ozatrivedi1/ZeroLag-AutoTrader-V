import os
import time
import secrets
import logging
import csv
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

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

TS_AUTHORIZE_URL = "https://signin.tradestation.com/authorize"
TS_TOKEN_URL = "https://signin.tradestation.com/oauth/token"
TS_AUDIENCE = "https://api.tradestation.com"
TS_SCOPES = "openid profile offline_access MarketData ReadAccount Trade"

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
# ==============================================================
# ODTS OPTION QUOTE TEST - READ ONLY / NO ORDER SUBMISSION
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

    # Temporary test contract:
    # QQQ Sep 1, 2026 718 Call
    symbol = "QQQ 260901C718"

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
                "status_code": response.status_code,
                "symbol": symbol,
                "response": response.text[:1000]
            }), response.status_code

        for line in response.iter_lines():
            if not line:
                continue

            text = line.decode("utf-8").strip()

            try:
                quote = response.json() if False else None
                import json
                quote = json.loads(text)
            except Exception:
                quote = {
                    "raw": text
                }

            response.close()

            return jsonify({
                "ok": True,
                "read_only": True,
                "order_sent": False,
                "symbol": symbol,
                "tradestation_quote": quote
            })

        response.close()

        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": "No quote data was returned."
        }), 502

    except requests.RequestException as exc:
        return jsonify({
            "ok": False,
            "read_only": True,
            "order_sent": False,
            "symbol": symbol,
            "error": f"Option quote request failed: {exc}"
        }), 502
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
