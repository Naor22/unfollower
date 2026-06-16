"""Export the logged-in Instagram session (cookies) to data/session.json.

Run this on the machine where you're already logged in via the CDP Chrome
(port 9222). Copy the resulting data/session.json to the Raspberry Pi — the bot
there launches its own headless Chromium using these cookies, so it doesn't need
to log in. Re-run this whenever the Pi's session expires.

  python export_session.py
"""

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).parent
OUT = ROOT / "data" / "session.json"
CDP = "http://localhost:9222"

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def main():
    with sync_playwright() as p:
        b = p.chromium.connect_over_cdp(CDP)
        if not b.contexts:
            sys.exit("no browser context found over CDP — is the Chrome on :9222 running?")
        ctx = b.contexts[0]
        OUT.parent.mkdir(exist_ok=True)
        ctx.storage_state(path=str(OUT))
        # quick sanity: count instagram cookies
        import json
        data = json.loads(OUT.read_text(encoding="utf-8"))
        cookies = data.get("cookies", [])
        ig = [c for c in cookies if "instagram" in c.get("domain", "")]
        has_session = any(c.get("name") == "sessionid" for c in ig)
        print(f"[+] wrote {OUT}")
        print(f"[+] {len(cookies)} cookies ({len(ig)} instagram), sessionid present: {has_session}")
        if not has_session:
            print("[!] WARNING: no 'sessionid' cookie found — you may not be logged in.")


if __name__ == "__main__":
    main()
