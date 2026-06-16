"""One-time login for the BURNER scraper account into its own persistent profile.

Mirrors pi_login.py, but targets the scraper's profile dir
(`scraper.user_data_dir` in config.yaml) and the burner credentials in .env
(`SCRAPER_IG_USERNAME` / `SCRAPER_IG_PASSWORD`). Run once on the Pi over SSH; it
prompts for a 2FA code in the terminal if needed. After it succeeds the scraper
service reuses that profile headlessly.

  python scraper_login.py

Requires in config.yaml:
  scraper.user_data_dir: "/home/naor223/unfollower/data/scraper-profile"
  scraper.cdp_endpoint: ""          # empty -> persistent profile model (like the main bot)
and in .env:
  SCRAPER_IG_USERNAME=...           # the throwaway scraper account
  SCRAPER_IG_PASSWORD=...
"""

import os
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
    scr = cfg.get("scraper", {}) or {}
    udd = scr.get("user_data_dir") or ""
    if not udd:
        sys.exit("Set scraper.user_data_dir in config.yaml first "
                 "(e.g. /home/naor223/unfollower/data/scraper-profile), and "
                 "scraper.cdp_endpoint: \"\".")
    Path(udd).mkdir(parents=True, exist_ok=True)

    load_dotenv(ROOT / ".env", override=True)
    username = os.getenv("SCRAPER_IG_USERNAME")
    password = os.getenv("SCRAPER_IG_PASSWORD")
    if not username or not password:
        sys.exit("SCRAPER_IG_USERNAME / SCRAPER_IG_PASSWORD must be set in .env "
                 "(the throwaway scraper account — NOT your main account).")

    b = bot.Bot(bot.StateManager(persist_events=False))  # reuse login selectors

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
    exe = scr.get("executable_path") or bc.get("executable_path")
    if exe:
        pkwargs["executable_path"] = exe
    elif bc.get("channel"):
        pkwargs["channel"] = bc["channel"]

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(**pkwargs)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()

        if b._is_logged_in(page):
            print("[+] Burner already logged in on this profile — nothing to do.")
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
                     "captcha — approve the login from the burner's phone app, or "
                     "try again.")

        print("[*] Entering credentials...")
        user_field.click(); user_field.fill(""); user_field.type(username, delay=60)
        pw_field.click(); pw_field.fill(""); pw_field.type(password, delay=60)
        submit, _ = b._first_visible(page, b.SUBMIT_SELECTORS)
        if submit is not None:
            submit.click()
        else:
            pw_field.press("Enter")

        twofa_sel = ('input[name="verificationCode"], input[autocomplete="one-time-code"], '
                     'input[aria-label*="code" i]')
        error_sel = 'p[id*="slfErrorAlert"], div[role="alert"]'
        twofa_done = False
        end = time.monotonic() + 180
        while time.monotonic() < end:
            url = page.url
            if "/accounts/login" not in url and "/challenge" not in url and "/two_factor" not in url:
                if "instagram.com" in url:
                    print("[✓] Burner logged in. Profile saved at:", udd)
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
                        code = input("Enter the 2FA code Instagram sent the burner: ").strip()
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
              "burner's Instagram app and run this again.")
        ctx.close()


if __name__ == "__main__":
    main()
