"""
token_webhook.py - Receives the daily access_token Upstox pushes here
after you approve the "semi-automated" token request on your phone (see
trigger_token_request.py in this same folder). Writes the token to a
shared file that feed-listener/paper-trader-daemon/scanner-app read via
systemd's EnvironmentFile= directive, then restarts those three
services so they immediately pick up the fresh token -- no manual
copy-pasting into terminals, no browser login, just one daily approval
tap on your phone.

PAYLOAD CONTRACT (verified against Upstox's current Access Token
Request API docs): Upstox POSTs this exact JSON body here once you
approve:
    {
      "client_id": "...", "user_id": "...", "access_token": "...",
      "token_type": "Bearer", "expires_at": "...", "issued_at": "...",
      "message_type": "access_token"
    }

SECURITY NOTE -- READ THIS:
    Upstox's docs don't describe any signature/secret verification on
    this webhook -- it relies on the URL itself being hard to guess
    (see WEBHOOK_PATH below) plus HTTPS in transit. This is a real,
    known limitation: anyone who discovers this exact URL could POST a
    fake payload and have it treated as a real token. Keep
    WEBHOOK_PATH long/random and never share the full webhook URL
    publicly. This module does check that client_id matches your own
    app's, which blocks the most trivial "wrong app entirely" mistake,
    but is not real authentication.

SETUP:
    pip install flask
    Set both of these env vars before running (neither is hardcoded here
    on purpose -- WEBHOOK_PATH in particular IS this endpoint's only
    security, per the note above, so it must never be committed/shared):
        $env:UPSTOX_WEBHOOK_PATH = "/upstox-webhook-<your-own-long-random-string>"
        $env:UPSTOX_API_KEY = "your_app's_api_key"
    python token_webhook.py
    (Runs on plain HTTP on port 5001 -- nginx handles the real HTTPS
    termination in front of this, see the nginx setup instructions
    provided alongside this file.)
"""
import json
import os
import subprocess
import time

from flask import Flask, request, jsonify

# Long/random and NEVER hardcoded/committed -- this IS the "security" for
# this endpoint, per the module docstring's security note above. Must be
# set via env var before running; there's deliberately no fallback value.
WEBHOOK_PATH = os.environ.get("UPSTOX_WEBHOOK_PATH")
if not WEBHOOK_PATH:
    raise RuntimeError(
        "UPSTOX_WEBHOOK_PATH not set -- pick your own long/random path "
        "(e.g. '/upstox-webhook-' + 32 random hex chars) and set it as an "
        "env var. Never hardcode or commit this value -- it's the only "
        "thing keeping this endpoint from being guessable, per the "
        "security note above."
    )

EXPECTED_CLIENT_ID = os.environ.get("UPSTOX_API_KEY")  # your app's API key
if not EXPECTED_CLIENT_ID:
    raise RuntimeError("UPSTOX_API_KEY not set.")

# EnvironmentFile= format: KEY=VALUE lines, no quotes, no spaces around =.
# systemd re-reads this file every time a unit using it is (re)started --
# it does NOT hot-reload into an already-running process, which is
# exactly why this script restarts the services below after writing.
TOKEN_ENV_FILE = "/home/ubuntu/dryarapureddy-tick-ML/upstox_token.env"

SERVICES_TO_RESTART = ["feed-listener", "paper-trader-daemon", "scanner-app"]

app = Flask(__name__)


@app.route(WEBHOOK_PATH, methods=["POST"])
def receive_token():
    try:
        payload = request.get_json(force=True)
    except Exception as e:
        return jsonify({"error": f"invalid JSON: {e}"}), 400

    if payload.get("message_type") != "access_token":
        return jsonify({"error": "unexpected message_type"}), 400

    if payload.get("client_id") != EXPECTED_CLIENT_ID:
        print(f"WARNING: received a token for client_id={payload.get('client_id')!r}, "
              f"which doesn't match EXPECTED_CLIENT_ID -- ignoring.")
        return jsonify({"error": "client_id mismatch"}), 403

    access_token = payload.get("access_token")
    if not access_token:
        return jsonify({"error": "no access_token in payload"}), 400

    # Write to the shared env file, temp-file-then-rename so a service
    # restarting mid-write never reads a half-written file.
    tmp_path = TOKEN_ENV_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(f"UPSTOX_ACCESS_TOKEN={access_token}\n")
    os.replace(tmp_path, TOKEN_ENV_FILE)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] New token written to {TOKEN_ENV_FILE}")

    # Restart the services so they actually pick up the new token --
    # requires a narrowly-scoped passwordless sudo rule for exactly
    # this command (see the setup instructions), not blanket sudo.
    restart_results = {}
    for svc in SERVICES_TO_RESTART:
        try:
            subprocess.run(["sudo", "systemctl", "restart", svc], check=True, timeout=30)
            restart_results[svc] = "restarted"
            print(f"  Restarted {svc}")
        except Exception as e:
            restart_results[svc] = f"FAILED: {e}"
            print(f"  FAILED to restart {svc}: {e}")

    return jsonify({"status": "ok", "restarted": restart_results}), 200


@app.route("/upstox-webhook-health", methods=["GET"])
def health():
    """Plain, unauthenticated health check -- confirms the webhook
    server itself is up, without exposing anything about token state."""
    return jsonify({"status": "listening"}), 200


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001)
