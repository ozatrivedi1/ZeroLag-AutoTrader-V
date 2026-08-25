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
