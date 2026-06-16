"""One-time Instagram login on the Pi into a PERSISTENT profile (no GUI needed).

Run this once on the Pi over SSH. It launches headless Chromium using the
profile dir from config.yaml (browser.user_data_dir), logs in with the
credentials in .env, and prompts you in the terminal for a 2FA code if needed.
After it succeeds, the bot reuses that profile headlessly — staying logged in
across runs and reboots, with no session.json copying.

  python pi_login.py

Requires in config.yaml:  browser.user_data_dir: "/home/pi/unfollower/data/ig-profile"
and (usually) browser.executable_path: "/usr/bin/chromium".
If Instagram shows an image captcha (not a code), this can't be solved in a
terminal — use the copy-session method instead (export_session.py on your PC).
"""

import sys
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

import bot

ROOT = Path(__file__).parent

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def main():
    cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    bc = cfg["browser"]
    udd = bc.get("user_data_dir") or ""
    if not udd:
        sys.exit("Set browser.user_data_dir in config.yaml first "
                 "(e.g. /home/pi/unfollower/data/ig-profile).")
    Path(udd).mkdir(parents=True, exist_ok=True)

    load_dotenv(ROOT / ".env", override=True)
    import os
    username = os.getenv("IG_USERNAME")
    password = os.getenv("IG_PASSWORD")
    if not username or not password:
        sys.exit("IG_USERNAME / IG_PASSWORD must be set in .env")

    b = bot.Bot(bot.StateManager())  # reuse its login selectors/helpers

    pkwargs = {
        "user_data_dir": udd,
        "headless": True,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-default-browser-check", "--no-first-run",
            "--no-sandbox", "--disable-gpu",
        ],
        "viewport": {"width": bc["viewport_width"], "height": bc["viewport_height"]},
        "locale": bc["locale"],
        "user_agent": bc["user_agent"],
    }
    if bc.get("executable_path"):
        pkwargs["executable_path"] = bc["executable_path"]
    elif bc.get("channel"):
        pkwargs["channel"] = bc["channel"]

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(**pkwargs)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()

        if b._is_logged_in(page):
            print("[+] Already logged in on this profile — nothing to do.")
            ctx.close()
            return

        print("[*] Opening login page...")
        page.goto("https://www.instagram.com/accounts/login/", wait_until="domcontentloaded")
        b._dismiss_cookie_banner(page)

        user_field, _ = b._wait_for_any(page, b.USERNAME_SELECTORS, 30)
        pw_field, _ = b._first_visible(page, b.PASSWORD_SELECTORS)
        if user_field is None or pw_field is None:
            ctx.close()
            sys.exit("Could not find the login form. Instagram may be showing a "
                     "captcha — use the copy-session method instead.")

        print("[*] Entering credentials...")
        user_field.click(); user_field.fill(""); user_field.type(username, delay=60)
        pw_field.click(); pw_field.fill(""); pw_field.type(password, delay=60)
        submit, _ = b._first_visible(page, b.SUBMIT_SELECTORS)
        if submit is not None:
            submit.click()
        else:
            pw_field.press("Enter")

        # Poll for: success / 2FA prompt / error. Prompt for the 2FA code if asked.
        twofa_sel = ('input[name="verificationCode"], input[autocomplete="one-time-code"], '
                     'input[aria-label*="code" i]')
        error_sel = 'p[id*="slfErrorAlert"], div[role="alert"]'
        twofa_done = False
        end = time.monotonic() + 180
        while time.monotonic() < end:
            url = page.url
            if "/accounts/login" not in url and "/challenge" not in url and "/two_factor" not in url:
                if "instagram.com" in url:
                    print("[✓] Logged in. Profile saved at:", udd)
                    # settle, then dismiss "save info?" prompts
                    time.sleep(2)
                    for label in ("Not now", "Not Now"):
                        try:
                            page.get_by_role("button", name=label).first.click(timeout=2000)
                        except Exception:
                            pass
                    ctx.close()
                    return

            if not twofa_done:
                try:
                    if page.locator(twofa_sel).first.is_visible(timeout=1000):
                        code = input("Enter the 2FA code Instagram sent you: ").strip()
                        page.locator(twofa_sel).first.fill(code)
                        sub, _ = b._first_visible(page, b.SUBMIT_SELECTORS)
                        if sub is not None:
                            sub.click()
                        else:
                            page.locator(twofa_sel).first.press("Enter")
                        twofa_done = True
                        end = time.monotonic() + 120
                        continue
                except Exception:
                    pass

            try:
                if page.locator(error_sel).first.is_visible(timeout=500):
                    msg = page.locator(error_sel).first.inner_text()[:200]
                    print("[!] Instagram says:", msg)
            except Exception:
                pass
            time.sleep(1)

        print("[!] Timed out. If a captcha/checkpoint appeared, approve it from the "
              "Instagram phone app and run this again, or use the copy-session method.")
        ctx.close()


if __name__ == "__main__":
    main()
