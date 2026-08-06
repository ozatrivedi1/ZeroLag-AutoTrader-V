from flask import Flask, request, jsonify
import os

app = Flask(__name__)

@app.get("/")
def home():
    return {"service":"ZeroLag AutoTrader V1","status":"running"}

@app.get("/health")
def health():
    return {"ok":True}

@app.post("/webhook/<token>")
def webhook(token):
    if token != os.getenv("WEBHOOK_TOKEN",""):
        return jsonify({"ok":False,"error":"Unauthorized"}),401
    return jsonify({"ok":True,"message":"Webhook received (dry run)."})

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=10000)
