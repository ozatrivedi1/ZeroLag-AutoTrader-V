import os
import time
import secrets
import logging
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, redirect, request

app = Flask(__name__)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("zerolag")

TS_CLIENT_ID = os.getenv("TS_CLIENT_ID", "").strip()
TS_CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET", "").strip()
TS_REDIRECT_URI = os.getenv("TS_REDIRECT_URI", "").strip()
TS_API_BASE_URL = os.getenv("TS_API_BASE_URL", "https://sim-api.tradestation.com/v3").rstrip("/")
TS_SIM_ACCOUNT_ID = os.getenv("TS_SIM_ACCOUNT_ID", "").strip()
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "NO").strip().upper()
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

TS_AUTHORIZE_URL = "https://signin.tradestation.com/authorize"
TS_TOKEN_URL = "https://signin.tradestation.com/oauth/token"
TS_AUDIENCE = "https://api.tradestation.com"
TS_SCOPES = "openid profile offline_access MarketData ReadAccount Trade"

ALLOWED_SYMBOL = "SOXL"
MAX_TEST_QTY = 1
DUPLICATE_WINDOW_SECONDS = 20

oauth_state = None
token_store = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0,
}
last_webhook = {"received": False, "payload": None, "received_at": None}
last_signal = {"key": None, "time": 0}

def sim_environment_ok():
    return TS_API_BASE_URL.lower().startswith("https://sim-api.tradestation.com/v3")

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
        ("TS_CLIENT_SECRET", TS_CLIENT_SECRET),
        ("TS_REDIRECT_URI", TS_REDIRECT_URI),
        ("TS_SIM_ACCOUNT_ID", TS_SIM_ACCOUNT_ID),
        ("WEBHOOK_TOKEN", WEBHOOK_TOKEN),
    ]:
        if not value:
            missing.append(name)
    return missing

def save_token_response(data):
    token_store["access_token"] = data.get("access_token")
    if data.get("refresh_token"):
        token_store["refresh_token"] = data.get("refresh_token")
    expires_in = int(data.get("expires_in", 1200))
    token_store["expires_at"] = time.time() + max(expires_in - 60, 60)

def refresh_access_token():
    refresh_token = token_store.get("refresh_token")
    if not refresh_token:
        return False, "No refresh token is available. Please visit /login again."

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
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=20,
        )
    except requests.RequestException as exc:
        return False, f"Refresh request failed: {exc}"

    if not response.ok:
        return False, f"Refresh failed ({response.status_code}): {response.text[:500]}"

    data = response.json()
    if not data.get("access_token"):
        return False, "TradeStation refresh response did not include an access token."

    save_token_response(data)
    return True, "Access token refreshed."

def get_valid_access_token():
    access_token = token_store.get("access_token")
    if access_token and time.time() < token_store.get("expires_at", 0):
        return access_token, None

    if token_store.get("refresh_token"):
        ok, message = refresh_access_token()
        if ok:
            return token_store.get("access_token"), None
        return None, message

    return None, "Not authenticated. Please visit /login first."

def ts_headers(access_token):
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def get_soxl_position(access_token):
    url = f"{TS_API_BASE_URL}/brokerage/accounts/{TS_SIM_ACCOUNT_ID}/positions"
    try:
        response = requests.get(url, headers=ts_headers(access_token), timeout=20)
    except requests.RequestException as exc:
        return False, None, {"error": f"Position request failed: {exc}"}

    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}

    if not response.ok:
        return False, None, {"status_code": response.status_code, "response": body}

    positions = body.get("Positions", []) if isinstance(body, dict) else []
    if not isinstance(positions, list):
        positions = []

    for position in positions:
        symbol = str(position.get("Symbol", "")).upper().strip()
        if symbol == ALLOWED_SYMBOL:
            raw_qty = position.get("Quantity") or position.get("LongQuantity") or 0
            try:
                quantity = float(raw_qty)
            except (TypeError, ValueError):
                quantity = 0.0
            return True, quantity, body

    return True, 0.0, body

def submit_sim_market_order(access_token, action):
    if not sim_environment_ok():
        return False, {"error": "Blocked: API base URL is not TradeStation SIM."}
    if not TS_SIM_ACCOUNT_ID:
        return False, {"error": "Blocked: TS_SIM_ACCOUNT_ID is missing."}

    url = f"{TS_API_BASE_URL}/orderexecution/orders"
    order = {
        "AccountID": TS_SIM_ACCOUNT_ID,
        "Symbol": ALLOWED_SYMBOL,
        "Quantity": str(MAX_TEST_QTY),
        "OrderType": "Market",
        "TradeAction": action,
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
        return False, {"error": f"Order request failed: {exc}"}

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

    return True, body

def duplicate_signal(action, symbol, strategy_name):
    now = time.time()
    key = f"{strategy_name}|{action}|{symbol}"
    if (
        last_signal["key"] == key
        and now - last_signal["time"] < DUPLICATE_WINDOW_SECONDS
    ):
        return True
    last_signal["key"] = key
    last_signal["time"] = now
    return False

@app.get("/")
def home():
    return jsonify({
        "service": "ZeroLag AutoTrader",
        "status": "running",
        "mode": "TRADESTATION SIM ORDER TEST",
        "environment": "SIM" if sim_environment_ok() else "BLOCKED",
        "trading_enabled": TRADING_ENABLED,
        "orders_available": order_capability_ready(),
        "allowed_symbol": ALLOWED_SYMBOL,
        "max_test_quantity": MAX_TEST_QTY,
    })

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "ZeroLag AutoTrader",
        "api_base": TS_API_BASE_URL,
        "sim_environment_ok": sim_environment_ok(),
        "trading_enabled": TRADING_ENABLED,
        "orders_available": order_capability_ready(),
    })

@app.get("/auth-status")
def auth_status():
    missing = missing_config()
    return jsonify({
        "configured": len(missing) == 0,
        "missing_environment_variables": missing,
        "authenticated": bool(token_store.get("access_token")),
        "refresh_token_present": bool(token_store.get("refresh_token")),
        "trading_enabled": TRADING_ENABLED,
        "orders_available": order_capability_ready(),
        "sim_environment_ok": sim_environment_ok(),
        "api_base": TS_API_BASE_URL,
        "redirect_uri": TS_REDIRECT_URI,
    })

@app.get("/login")
def login():
    global oauth_state
    missing = [x for x in ["TS_CLIENT_ID", "TS_CLIENT_SECRET", "TS_REDIRECT_URI"]
               if not os.getenv(x, "").strip()]
    if missing:
        return jsonify({"ok": False, "error": "Missing Render environment variables", "missing": missing}), 500

    oauth_state = secrets.token_urlsafe(32)
    params = {
        "response_type": "code",
        "client_id": TS_CLIENT_ID,
        "audience": TS_AUDIENCE,
        "redirect_uri": TS_REDIRECT_URI,
        "scope": TS_SCOPES,
        "state": oauth_state,
        "prompt": "login",
    }
    return redirect(f"{TS_AUTHORIZE_URL}?{urlencode(params)}")

@app.get("/auth/callback")
def auth_callback():
    global oauth_state

    error = request.args.get("error")
    if error:
        return jsonify({
            "ok": False,
            "error": error,
            "error_description": request.args.get("error_description", ""),
        }), 400

    code = request.args.get("code")
    returned_state = request.args.get("state")

    if not code:
        return jsonify({"ok": False, "error": "No authorization code was returned by TradeStation."}), 400

    if not oauth_state or returned_state != oauth_state:
        return jsonify({"ok": False, "error": "OAuth state check failed. Please restart at /login."}), 400

    payload = {
        "grant_type": "authorization_code",
        "client_id": TS_CLIENT_ID,
        "client_secret": TS_CLIENT_SECRET,
        "code": code,
        "redirect_uri": TS_REDIRECT_URI,
    }

    try:
        response = requests.post(
            TS_TOKEN_URL,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=20,
        )
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Token request failed: {exc}"}), 502

    oauth_state = None

    if not response.ok:
        return jsonify({
            "ok": False,
            "error": "TradeStation token exchange failed",
            "status_code": response.status_code,
            "details": response.text[:1000],
        }), response.status_code

    data = response.json()
    if not data.get("access_token"):
        return jsonify({"ok": False, "error": "TradeStation response did not contain an access token."}), 502

    save_token_response(data)

    return """
    <html><body style="font-family:Arial,sans-serif;margin:40px;">
    <h2>TradeStation authentication successful.</h2>
    <p>Render received an access token successfully.</p>
    <p><strong>Trading remains controlled by TRADING_ENABLED.</strong></p>
    <p><a href="/account-test">Test TradeStation SIM account connection</a></p>
    <p><a href="/position-test">Test SOXL position</a></p>
    <p><a href="/auth-status">View authentication status</a></p>
    </body></html>
    """

@app.get("/account-test")
def account_test():
    access_token, error = get_valid_access_token()
    if not access_token:
        return jsonify({"ok": False, "error": error, "next_step": "Open /login"}), 401

    url = f"{TS_API_BASE_URL}/brokerage/accounts"
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=20,
        )
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Account request failed: {exc}"}), 502

    try:
        body = response.json()
    except ValueError:
        body = {"raw_response": response.text[:1500]}

    if not response.ok:
        return jsonify({
            "ok": False,
            "status_code": response.status_code,
            "endpoint": "/brokerage/accounts",
            "response": body,
        }), response.status_code

    return jsonify({
        "ok": True,
        "environment": "SIM" if sim_environment_ok() else "BLOCKED",
        "trading_enabled": TRADING_ENABLED,
        "orders_available": order_capability_ready(),
        "tradestation_response": body,
    })

@app.get("/position-test")
def position_test():
    access_token, error = get_valid_access_token()
    if not access_token:
        return jsonify({"ok": False, "error": error, "next_step": "Open /login"}), 401

    ok, quantity, body = get_soxl_position(access_token)
    if not ok:
        return jsonify({
            "ok": False,
            "error": "TradeStation position query failed",
            "details": body,
        }), 502

    return jsonify({
        "ok": True,
        "symbol": ALLOWED_SYMBOL,
        "quantity": quantity,
        "is_long": quantity > 0,
        "trading_enabled": TRADING_ENABLED,
        "orders_available": order_capability_ready(),
    })

@app.post("/webhook/<token>")
def webhook(token):
    if not WEBHOOK_TOKEN or token != WEBHOOK_TOKEN:
        log.warning("Rejected webhook: invalid token")
        return jsonify({"ok": False, "error": "Unauthorized webhook token"}), 401

    if not request.is_json:
        return jsonify({"ok": False, "error": "JSON body required"}), 400

    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "")).upper().strip()
    symbol = str(payload.get("symbol", "")).upper().strip()
    strategy_name = str(payload.get("strategy", "")).strip()

    if action not in {"BUY", "SELL"}:
        return jsonify({"ok": False, "error": "action must be BUY or SELL"}), 400

    if symbol != ALLOWED_SYMBOL:
        return jsonify({
            "ok": False,
            "error": f"Only {ALLOWED_SYMBOL} is allowed during SIM test.",
        }), 400

    last_webhook["received"] = True
    last_webhook["payload"] = payload
    last_webhook["received_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    log.info(
        "WEBHOOK | strategy=%s action=%s symbol=%s trading_enabled=%s",
        strategy_name, action, symbol, TRADING_ENABLED
    )

    if TRADING_ENABLED != "YES":
        return jsonify({
            "ok": True,
            "received": True,
            "dry_run": True,
            "trading_enabled": TRADING_ENABLED,
            "orders_available": order_capability_ready(),
            "message": "Webhook received. Trading is disabled; no TradeStation order was sent.",
        }), 200

    if not sim_environment_ok():
        return jsonify({
            "ok": False,
            "error": "Order blocked because TS_API_BASE_URL is not SIM.",
        }), 403

    if not order_capability_ready():
        return jsonify({
            "ok": False,
            "error": "Order capability is not ready.",
            "missing": missing_config(),
        }), 503

    if duplicate_signal(action, symbol, strategy_name):
        return jsonify({
            "ok": True,
            "received": True,
            "duplicate_blocked": True,
            "message": "Duplicate signal blocked. No order was sent.",
        }), 200

    access_token, error = get_valid_access_token()
    if not access_token:
        return jsonify({
            "ok": False,
            "error": "TradeStation authentication is required.",
            "details": error,
            "next_step": "Open /login",
        }), 401

    pos_ok, position_qty, pos_body = get_soxl_position(access_token)
    if not pos_ok:
        return jsonify({
            "ok": False,
            "error": "Position verification failed. Order blocked.",
            "details": pos_body,
        }), 502

    if action == "BUY" and position_qty > 0:
        return jsonify({
            "ok": True,
            "received": True,
            "order_sent": False,
            "message": "BUY blocked: SOXL position is already long.",
            "position_quantity": position_qty,
        }), 200

    if action == "SELL" and position_qty <= 0:
        return jsonify({
            "ok": True,
            "received": True,
            "order_sent": False,
            "message": "SELL blocked: no SOXL long position exists.",
            "position_quantity": position_qty,
        }), 200

    order_ok, order_response = submit_sim_market_order(access_token, action)

    if not order_ok:
        log.error(
            "SIM ORDER FAILED | action=%s symbol=%s response=%s",
            action, symbol, order_response
        )
        return jsonify({
            "ok": False,
            "received": True,
            "order_sent": False,
            "error": "TradeStation SIM order submission failed.",
            "details": order_response,
        }), 502

    log.info(
        "SIM ORDER SENT | action=%s symbol=%s qty=%s response=%s",
        action, symbol, MAX_TEST_QTY, order_response
    )

    return jsonify({
        "ok": True,
        "received": True,
        "dry_run": False,
        "order_sent": True,
        "environment": "SIM",
        "symbol": symbol,
        "action": action,
        "quantity": MAX_TEST_QTY,
        "tradestation_response": order_response,
    }), 200

@app.get("/webhook-status")
def webhook_status():
    return jsonify({
        "ok": True,
        "last_webhook_received": last_webhook["received"],
        "received_at": last_webhook["received_at"],
        "payload": last_webhook["payload"],
        "trading_enabled": TRADING_ENABLED,
        "orders_available": order_capability_ready(),
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
