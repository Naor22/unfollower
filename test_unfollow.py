"""Ad-hoc single-target unfollow tester.

Connects to the same CDP Chrome the bot uses and runs the real Bot._unfollow
logic on ONE username, printing the result/reason — handy for testing a specific
profile (e.g. one that was failing) without running a whole batch.

The systemd service drives the same browser, so stop it first or they'll fight
over the tab:

    sudo systemctl stop unfollower
    python test_unfollow.py talaat.amash
    sudo systemctl start unfollower

Note: this performs a REAL unfollow. It does NOT write to the logs, so the main
bot may revisit the account later (and should now succeed via the post fallback).
"""

import sys

from playwright.sync_api import sync_playwright

import bot


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: python test_unfollow.py <username>")
        return 2
    target = sys.argv[1].lstrip("@").lower()

    cfg = bot.load_config()
    cdp_endpoint = cfg["browser"].get("cdp_endpoint") or ""
    if not cdp_endpoint:
        print("This tester only supports CDP mode (set browser.cdp_endpoint in config.yaml).")
        return 2

    worker = bot.Bot(bot.StateManager())

    with sync_playwright() as p:
        print(f"connecting to Chrome at {cdp_endpoint} ...")
        browser = p.chromium.connect_over_cdp(cdp_endpoint)
        context = browser.contexts[0] if browser.contexts else browser.new_context()

        page = None
        for pg in context.pages:
            if "instagram.com" in (pg.url or ""):
                page = pg
                break
        if page is None:
            page = context.new_page()

        if not worker._is_logged_in(page):
            print("not logged in on this Chrome profile — log into Instagram first.")
            return 1

        print(f"running unfollow on @{target} ...")
        result = worker._unfollow(page, target)
        print(f"\nRESULT: {result}")
        return 0 if result == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
