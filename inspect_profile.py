"""Inspect a real Instagram profile page to verify the unfollow button flow.

Assumes you are ALREADY logged in. Two ways to provide that session:

  A) CDP (recommended) — drive your own already-logged-in Chrome:
       1) Close all Chrome windows.
       2) Start Chrome with remote debugging, e.g.:
            & "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222
       3) In that Chrome, make sure you're logged into instagram.com.
       4) Run:  python inspect_profile.py <username>

  B) Saved session — use data/session.json (set --launch):
       python inspect_profile.py <username> --launch

By default this is READ-ONLY: it navigates to the profile, clicks "Following"
to open the options dialog, and dumps every clickable element it finds — but it
does NOT actually unfollow. Pass --unfollow to also click through and unfollow.
"""

import argparse
import json
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

# IG profiles contain emoji (story highlights, names). Force UTF-8 stdout so
# printing the DOM dump doesn't crash on Windows' default cp1252 console.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).parent
SESSION_PATH = ROOT / "data" / "session.json"
CDP_ENDPOINT = "http://localhost:9222"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def dump_clickables(scope, where: str):
    """Print every button / role=button / link element under `scope`."""
    js = """
    (root) => {
      const els = root.querySelectorAll(
        'button, [role="button"], a[role="link"], div[tabindex]'
      );
      const out = [];
      for (const el of els) {
        const r = el.getBoundingClientRect();
        if (r.width === 0 || r.height === 0) continue;   // skip invisible
        out.push({
          tag: el.tagName.toLowerCase(),
          role: el.getAttribute('role'),
          type: el.getAttribute('type'),
          text: (el.innerText || '').trim().slice(0, 50),
          ariaLabel: el.getAttribute('aria-label'),
          tabindex: el.getAttribute('tabindex'),
          outerHTML: el.outerHTML.slice(0, 160),
        });
      }
      return out;
    }
    """
    items = scope.evaluate(js)
    print(f"\n=== CLICKABLE ELEMENTS in {where} ({len(items)}) ===")
    print(json.dumps(items, indent=2, ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("username", help="Instagram handle to open, e.g. someuser")
    ap.add_argument("--launch", action="store_true",
                    help="Launch a fresh Chromium with data/session.json instead of CDP")
    ap.add_argument("--unfollow", action="store_true",
                    help="Actually click through and unfollow (default is read-only)")
    args = ap.parse_args()

    with sync_playwright() as p:
        close_browser = False
        if args.launch:
            print(f"[*] Launching Chromium with session {SESSION_PATH}")
            browser = p.chromium.launch(headless=False,
                                        args=["--disable-blink-features=AutomationControlled"])
            ctx_args = {"user_agent": UA, "locale": "en-US",
                        "viewport": {"width": 1280, "height": 900}}
            if SESSION_PATH.exists():
                ctx_args["storage_state"] = str(SESSION_PATH)
            else:
                print(f"[!] {SESSION_PATH} not found — you may not be logged in.")
            context = browser.new_context(**ctx_args)
            page = context.new_page()
            close_browser = True
        else:
            print(f"[*] Connecting to your Chrome at {CDP_ENDPOINT}")
            browser = p.chromium.connect_over_cdp(CDP_ENDPOINT)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.pages[0] if context.pages else context.new_page()

        url = f"https://www.instagram.com/{args.username}/"
        print(f"[*] Opening {url}")
        page.goto(url, wait_until="domcontentloaded")
        time.sleep(4)

        if page.locator("header").count() == 0:
            print("[!] No <header> found — page may not have loaded or you're not logged in.")
        else:
            print("[+] header present")

        # 1) Dump the profile header action buttons.
        header = page.locator("header").first
        dump_clickables(header, "profile <header>")

        # 2) Find the Following / Requested button.
        following_btn = None
        for name in ("Following", "Requested"):
            loc = page.get_by_role("button", name=name).first  # substring; chevron breaks exact
            try:
                if loc.is_visible(timeout=1500):
                    following_btn = loc
                    print(f"\n[+] Found '{name}' button via role=button substring match")
                    break
            except Exception:
                pass

        if following_btn is None:
            # broader fallback for inspection visibility
            print("\n[!] No exact 'Following'/'Requested' role=button found. "
                  "Trying any element whose text is exactly 'Following'...")
            loc = page.locator(
                'header :text-is("Following"), header :text-is("Requested")'
            ).first
            try:
                if loc.is_visible(timeout=1500):
                    following_btn = loc
                    print("[+] Found via text-is fallback")
            except Exception:
                pass

        if following_btn is None:
            if page.get_by_role("button", name="Follow", exact=True).count() > 0:
                print("[i] This account shows a 'Follow' button — you're NOT following them.")
            else:
                print("[!] Could not locate a Following/Requested button at all.")
            _finish(context, browser, close_browser)
            return

        # 3) Click it to open the options dialog.
        print("[*] Clicking the Following button to open the options dialog...")
        following_btn.click()
        time.sleep(2)

        dialog = page.locator('div[role="dialog"]').last
        if dialog.count() == 0:
            print("[!] No dialog opened after clicking Following.")
        else:
            dump_clickables(dialog, "options dialog (step 1)")

        if not args.unfollow:
            print("\n[i] Read-only mode. Not clicking Unfollow. "
                  "Re-run with --unfollow to perform the unfollow.")
            print("[i] Leaving the dialog open for 8s so you can eyeball it.")
            time.sleep(8)
            _finish(context, browser, close_browser)
            return

        # 4) Click Unfollow inside the dialog.
        print("[*] Clicking 'Unfollow' in the dialog...")
        clicked = _click_unfollow_in(dialog, page)
        if not clicked:
            print("[!] Could not find an 'Unfollow' item in the dialog.")
            _finish(context, browser, close_browser)
            return
        time.sleep(2)

        # 5) Some IG variants show a SECOND confirmation dialog.
        confirm = page.locator('div[role="dialog"]').last
        if confirm.count() > 0:
            dump_clickables(confirm, "possible confirm dialog (step 2)")
            _click_unfollow_in(confirm, page)
            time.sleep(2)

        # 6) Verify it worked — the header button should now read "Follow".
        time.sleep(2)
        if page.get_by_role("button", name="Follow", exact=True).count() > 0:
            print("\n[✓] SUCCESS — header now shows 'Follow'. Unfollow confirmed.")
        else:
            print("\n[?] Could not confirm via 'Follow' button. Check the window.")

        _finish(context, browser, close_browser)


def _click_unfollow_in(scope, page) -> bool:
    """Try several strategies to click the Unfollow control within `scope`."""
    # role=button matching the Unfollow label
    loc = scope.get_by_role("button", name="Unfollow").first
    try:
        if loc.is_visible(timeout=1500):
            loc.click(timeout=4000)
            return True
    except Exception:
        pass
    # any element whose text is exactly "Unfollow"
    loc = scope.locator(':text-is("Unfollow")').first
    try:
        if loc.is_visible(timeout=1500):
            loc.click(timeout=4000)
            return True
    except Exception:
        pass
    return False


def _finish(context, browser, close_browser):
    if close_browser:
        browser.close()
    else:
        print("[*] Leaving your Chrome open (CDP connection only detaches).")


if __name__ == "__main__":
    main()
