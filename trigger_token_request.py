"""
trigger_token_request.py - Kicks off Upstox's "semi-automated" access
token flow: calls the Access Token Request API, which sends YOU a push
notification (in-app + WhatsApp) to approve. Once you tap approve, the
token is delivered automatically to token_webhook.py (running on your
EC2 instance) -- no browser, no OTP typing, no copy-pasting tokens.

Run this ONCE per day (the resulting token still expires at 3:30 AM the
next day regardless of when you request it, per Upstox's own docs --
this doesn't remove the daily cycle, it just replaces "manual browser
login" with "one tap of approval on your phone").

SETUP:
    Set API_KEY and API_SECRET below (or pass as env vars -- see
    bottom of this file). The Notifier Webhook Endpoint must already
    be configured on your "Claude" app to point at your EC2 instance's
    HTTPS webhook URL (see nginx setup instructions provided alongside
    this file) -- this script does NOT configure that, it only
    triggers a request assuming it's already set up.

USAGE:
    python trigger_token_request.py
"""
import os
import sys

import requests

API_KEY = os.environ.get("UPSTOX_API_KEY")  # no hardcoded fallback -- set this env var
if not API_KEY:
    raise RuntimeError("UPSTOX_API_KEY not set.")
API_SECRET = os.environ.get("UPSTOX_API_SECRET")  # set this each time you run, don't hardcode

TRIGGER_URL_TEMPLATE = "https://api.upstox.com/v2/login/auth/token/request/{client_id}"


def main():
    api_secret = API_SECRET
    if not api_secret:
        print("Paste your API secret, then press Enter:")
        api_secret = input("> ").strip()

    url = TRIGGER_URL_TEMPLATE.format(client_id=API_KEY)
    resp = requests.post(
        url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"client_secret": api_secret},
        timeout=20,
    )

    print(f"HTTP status: {resp.status_code}")
    try:
        body = resp.json()
    except Exception:
        print(f"Response text: {resp.text}")
        sys.exit(1)

    print(f"Response: {body}")

    if resp.status_code >= 400:
        print("\n⚠ Request failed. Common causes: client_id/client_secret wrong, or "
              "the Notifier Webhook Endpoint isn't configured on this app yet.")
        sys.exit(1)

    notifier_url = body.get("data", {}).get("notifier_url")
    expiry = body.get("data", {}).get("authorization_expiry")
    print(f"\n✓ Request sent. Check your Upstox app / WhatsApp for an approval "
          f"notification and tap approve.")
    print(f"  Token will be sent to: {notifier_url}")
    print(f"  This request expires at: {expiry} (ms epoch -- 3:30 AM tomorrow per Upstox's rule)")
    print(f"\nOnce you approve, token_webhook.py on your EC2 instance will receive the "
          f"token and restart your trading services automatically.")


if __name__ == "__main__":
    main()
