"""One-time login for the BURNER scraper account(s) into their own persistent profile(s).

Single burner (default): logs `SCRAPER_IG_USERNAME` / `SCRAPER_IG_PASSWORD` from .env
into `scraper.user_data_dir`. Run once on the Pi over SSH; it prompts for a 2FA code in
the terminal if needed. After it succeeds the scraper service reuses that profile headlessly.

  python scraper_login.py

MULTI-BURNER failover: list several profiles under `scraper.accounts` in config.yaml,
each with its own profile dir + credential env names; this logs in every one that isn't
already logged in, so the scraper can rotate to a healthy burner when one gets
rate-limited / checkpointed:

  scraper:
    accounts:
      - { user_data_dir: "/home/naor223/unfollower/data/scraper-profile",
          label: burner1, username_env: SCRAPER_IG_USERNAME,   password_env: SCRAPER_IG_PASSWORD }
      - { user_data_dir: "/home/naor223/unfollower/data/scraper-profile-2",
          label: burner2, username_env: SCRAPER_IG_USERNAME_2, password_env: SCRAPER_IG_PASSWORD_2 }

and the matching SCRAPER_IG_USERNAME* / SCRAPER_IG_PASSWORD* in .env (one pair per burner —
each MUST be a different throwaway account, never your main one).
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


def _accounts(cfg):
    """Resolve the burner profiles to log in: the scraper.accounts list if present,
    else the single scraper.user_data_dir. Returns [(user_data_dir, username, password,
    label)]. Credentials come from the account itself (configured in the dashboard),
    falling back to the username_env/password_env names in .env."""
    scr = cfg.get("scraper", {}) or {}
    rows = []
    accts = scr.get("accounts") or []
    if accts:
        for a in accts:
            if not isinstance(a, dict) or not (a.get("username") or a.get("label") or a.get("user_data_dir")):
                continue
            udd = bot.burner_profile_dir(a)
            username = a.get("username") or os.getenv(a.get("username_env", "SCRAPER_IG_USERNAME"))
            password = a.get("password") or os.getenv(a.get("password_env", "SCRAPER_IG_PASSWORD"))
            rows.append((udd, username, password, a.get("label") or a.get("username") or udd))
    else:
        udd = scr.get("user_data_dir") or ""
        if not udd:
            sys.exit("Set scraper.user_data_dir in config.yaml first "
                     "(e.g. /home/naor223/unfollower/data/scraper-profile), and "
                     "scraper.cdp_endpoint: \"\". Or list several under scraper.accounts.")
        rows.append((udd, os.getenv("SCRAPER_IG_USERNAME"),
                     os.getenv("SCRAPER_IG_PASSWORD"), "burner"))
    return rows


def login_one(p, b, bc, scr, udd, username, password, label):
    """Log one burner into its persistent profile. Returns True on success / already-in."""
    print(f"\n=== {label} → {udd} ===")
    if not username or not password:
        print("[!] Missing credentials in .env for this burner - skipping.")
        return False
    Path(udd).mkdir(parents=True, exist_ok=True)

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

    # A persistent Chromium profile can only be opened by ONE process. The scraper is
    # confirmed stopped (checked in main), so any leftover Singleton* lock is stale (from
    # a killed scraper) and safe to clear - otherwise Chromium aborts with
    # "Failed to create a ProcessSingleton for your profile directory".
    for lock in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (Path(udd) / lock).unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass

    ctx = p.chromium.launch_persistent_context(**pkwargs)
    try:
        page = ctx.pages[0] if ctx.pages else ctx.new_page()

        if b._is_logged_in(page):
            print(f"[+] {label} already logged in — nothing to do.")
            return True

        print("[*] Opening login page...")
        page.goto("https://www.instagram.com/accounts/login/", wait_until="domcontentloaded")
        b._dismiss_cookie_banner(page)

        user_field, _ = b._wait_for_any(page, b.USERNAME_SELECTORS, 30)
        pw_field, _ = b._first_visible(page, b.PASSWORD_SELECTORS)
        if user_field is None or pw_field is None:
            print("[!] Could not find the login form. Instagram may be showing a captcha — "
                  "approve the login from the burner's phone app, or try again.")
            return False

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
                    print(f"[✓] {label} logged in. Profile saved at:", udd)
                    time.sleep(2)
                    for lbl in ("Not now", "Not Now"):
                        try:
                            page.get_by_role("button", name=lbl).first.click(timeout=2000)
                        except Exception:
                            pass
                    return True

            if not twofa_done:
                try:
                    if page.locator(twofa_sel).first.is_visible(timeout=1000):
                        code = input(f"Enter the 2FA code Instagram sent {label}: ").strip()
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

        print(f"[!] {label} timed out. If a captcha/checkpoint appeared, approve it from the "
              "burner's Instagram app and run this again.")
        return False
    finally:
        ctx.close()


def main():
    cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    bc = cfg["browser"]
    scr = cfg.get("scraper", {}) or {}
    load_dotenv(ROOT / ".env", override=True)

    # The running scraper holds the burner profile(s) open - two processes can't open the
    # same Chromium profile, so login would fail with a ProcessSingleton error. Refuse
    # rather than collide.
    if bot.scraper_running():
        sys.exit("The scraper service is running and holds the burner profile(s) open, so "
                 "login can't open them too.\nStop it first: dashboard → Scraper → Stop "
                 "scraper, or `sudo systemctl stop unfollower` (then start it again after "
                 "this finishes), and re-run `python scraper_login.py`.")

    rows = _accounts(cfg)
    print(f"[*] {len(rows)} burner profile(s) to check.")
    ok = 0
    with sync_playwright() as p:
        b = bot.Bot(bot.StateManager(persist_events=False))  # reuse login selectors
        for udd, username, password, label in rows:
            try:
                if login_one(p, b, bc, scr, udd, username, password, label):
                    ok += 1
            except Exception as e:
                print(f"[!] {label} failed: {e}")
    print(f"\n[*] Done: {ok}/{len(rows)} burner(s) logged in / ready.")


if __name__ == "__main__":
    main()
