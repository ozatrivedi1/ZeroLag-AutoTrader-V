import os
import time
import secrets
import logging
import csv
import json
import threading
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
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

# Independent gate for the ODTS background position monitor.
# Default NO: deployment alone never starts automatic exit monitoring.
ODTS_CONTINUOUS_MONITOR_ENABLED = os.getenv(
    "ODTS_CONTINUOUS_MONITOR_ENABLED",
    "NO"
).strip().upper()

try:
    ODTS_MONITOR_INTERVAL_SECONDS = max(
        3.0,
        float(os.getenv("ODTS_MONITOR_INTERVAL_SECONDS", "5"))
    )
except (TypeError, ValueError):
    ODTS_MONITOR_INTERVAL_SECONDS = 5.0

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

# ==============================================================
# ODTS QQQ EMAIL APPROVAL ALERT SETTINGS
# ==============================================================
# Notification-only feature. It cannot place, modify, cancel, or close orders.
ODTS_EMAIL_SENDER = os.getenv("ODTS_EMAIL_SENDER", "").strip()
ODTS_EMAIL_RECIPIENT = os.getenv("ODTS_EMAIL_RECIPIENT", "").strip()
ODTS_EMAIL_APP_PASSWORD = "".join(os.getenv("ODTS_EMAIL_APP_PASSWORD", "").split())
ODTS_EMAIL_ALERT_ENABLED = os.getenv("ODTS_EMAIL_ALERT_ENABLED", "YES").strip().upper()

try:
    ODTS_EMAIL_ALERT_INTERVAL_SECONDS = max(
        30.0,
        float(os.getenv("ODTS_EMAIL_ALERT_INTERVAL_SECONDS", "30"))
    )
except (TypeError, ValueError):
    ODTS_EMAIL_ALERT_INTERVAL_SECONDS = 30.0

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
MAX_LIVE_SHARE_QTY = 10
DUPLICATE_WINDOW_SECONDS = 20


# ==============================================================
# EXECUTION JOURNAL SETTINGS
# ==============================================================

JOURNAL_DIR = os.getenv(
    "JOURNAL_DIR",
    "/tmp/zerolag_journal"
