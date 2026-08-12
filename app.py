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
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "NO").strip().upper()
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

TS_AUTHORIZE_URL = "https://signin.tradestation.com/authorize"
TS_TOKEN_URL = "https://signin.tradestation.com/oauth/token"
TS_AUDIENCE = "https://api.tradestation.com"
TS_SCOPES = "openid profile offline_access MarketData ReadAccount Trade"

oauth_state = None

token_store = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0,
    "scope": None,
    "token_type": None,
}

last_webhook = {
    "received": False,
    "payload": None,
    "received_at": None,
}

def missing_config():
    missing = []
    if not TS_CLIENT_ID:
        missing.append("TS_CLIENT_ID")
    if not TS_CLIENT_SECRET:
        missing.append("TS_CLIENT_SECRET")
    if not TS_REDIRECT_URI:
        missing.append("TS_REDIRECT_URI")
    if not WEBHOOK_TOKEN:
        missing.append("WEBHOOK_TOKEN")
    return missing

def save_token_response(data):
    token_store["access_token"] = data.get("access_token")
    if data.get("refresh_token"):
        token_store["refresh_token"] = data.get("refresh_token")
    expires_in = int(data.get("expires_in", 1200))
    token_store["expires_at"] = time.time() + max(expires_in - 60, 60)
    token_store["scope"] = data.get("scope")
    token_store["token_type"] = data.get("token_type", "Bearer")

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

@app.get("/")
def home():
    return jsonify({
        "service": "ZeroLag AutoTrader",
        "status": "running",
        "mode": "SIM AUTH + WEBHOOK DRY RUN",
        "trading_enabled": TRADING_ENABLED,
        "orders_available": False,
        "webhook_test_available": True,
    })

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "ZeroLag AutoTrader",
        "trading_enabled": TRADING_ENABLED,
        "orders_available": False,
        "webhook_test_available": True,
        "api_base": TS_API_BASE_URL,
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
        "orders_available": False,
        "api_base": TS_API_BASE_URL,
        "redirect_uri": TS_REDIRECT_URI,
    })

@app.get("/login")
def login():
    global oauth_state

    missing = missing_config()
    if missing:
        return jsonify({
            "ok": False,
            "error": "Missing Render environment variables",
            "missing": missing,
        }), 500

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
        return jsonify({
            "ok": False,
            "error": "No authorization code was returned by TradeStation.",
        }), 400

    if not oauth_state or returned_state != oauth_state:
        return jsonify({
            "ok": False,
            "error": "OAuth state check failed. Please restart at /login.",
        }), 400

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
        return jsonify({
            "ok": False,
            "error": f"Token request failed: {exc}",
        }), 502

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
        return jsonify({
            "ok": False,
            "error": "TradeStation response did not contain an access token.",
        }), 502

    save_token_response(data)

    return """
    <html>
      <head><title>TradeStation Connected</title></head>
      <body style="font-family:Arial,sans-serif;margin:40px;">
        <h2>TradeStation authentication successful.</h2>
        <p>Render received an access token successfully.</p>
        <p><strong>Trading remains disabled.</strong></p>
        <p><a href="/account-test">Test TradeStation SIM account connection</a></p>
        <p><a href="/auth-status">View authentication status</a></p>
      </body>
    </html>
    """

@app.get("/account-test")
def account_test():
    access_token, error = get_valid_access_token()

    if not access_token:
        return jsonify({
            "ok": False,
            "error": error,
            "next_step": "Open /login",
        }), 401

    url = f"{TS_API_BASE_URL}/brokerage/accounts"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
    except requests.RequestException as exc:
        return jsonify({
            "ok": False,
            "error": f"Account request failed: {exc}",
        }), 502

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
        "environment": "SIM" if "sim-api" in TS_API_BASE_URL else "UNKNOWN",
        "trading_enabled": TRADING_ENABLED,
        "orders_available": False,
        "tradestation_response": body,
    })

@app.post("/webhook/<token>")
def webhook_test(token):
    if not WEBHOOK_TOKEN or token != WEBHOOK_TOKEN:
        log.warning("Rejected webhook: invalid token")
        return jsonify({
            "ok": False,
            "error": "Unauthorized webhook token",
        }), 401

    if not request.is_json:
        return jsonify({
            "ok": False,
            "error": "JSON body required",
        }), 400

    payload = request.get_json(silent=True) or {}

    action = str(payload.get("action", "")).upper().strip()
    symbol = str(payload.get("symbol", "")).upper().strip()
    strategy_name = str(payload.get("strategy", "")).strip()
    size = payload.get("size")

    if action not in {"BUY", "SELL"}:
        return jsonify({
            "ok": False,
            "error": "action must be BUY or SELL",
        }), 400

    if not symbol:
        return jsonify({
            "ok": False,
            "error": "symbol is required",
        }), 400

    last_webhook["received"] = True
    last_webhook["payload"] = payload
    last_webhook["received_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    log.info(
        "WEBHOOK DRY RUN | strategy=%s action=%s symbol=%s size=%s",
        strategy_name,
        action,
        symbol,
        size,
    )

    return jsonify({
        "ok": True,
        "received": True,
        "dry_run": True,
        "trading_enabled": TRADING_ENABLED,
        "orders_available": False,
        "message": "TradingView webhook received successfully. No TradeStation order was sent.",
        "parsed": {
            "strategy": strategy_name,
            "action": action,
            "symbol": symbol,
            "size": size,
        },
    }), 200

@app.get("/webhook-status")
def webhook_status():
    return jsonify({
        "ok": True,
        "last_webhook_received": last_webhook["received"],
        "received_at": last_webhook["received_at"],
        "payload": last_webhook["payload"],
        "trading_enabled": TRADING_ENABLED,
        "orders_available": False,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
