"""Ad-hoc single-target follow tester.

Connects to the same CDP Chrome the bot uses and runs the real Bot._follow logic
on ONE username, printing the result/reason — handy for testing the follow action
and the profile filters (private / no-posts / too-big) without running a batch.

The systemd service drives the same browser, so stop it first or they'll fight
over the tab:

    sudo systemctl stop unfollower
    python test_follow.py natgeo
    sudo systemctl start unfollower

Note: this performs a REAL follow (unless the account is filtered out). It does
NOT write to the logs, so the main bot may revisit the account later.

Pass --no-filters to bypass the config filters and force the follow attempt.
"""

import sys

from playwright.sync_api import sync_playwright

import bot


def main() -> int:
    args = [a for a in sys.argv[1:] if a != "--no-filters"]
    use_filters = "--no-filters" not in sys.argv
    if not args:
        print("usage: python test_follow.py <username> [--no-filters]")
        return 2
    target = args[0].lstrip("@").lower()

    cfg = bot.load_config()
    cdp_endpoint = cfg["browser"].get("cdp_endpoint") or ""
    if not cdp_endpoint:
        print("This tester only supports CDP mode (set browser.cdp_endpoint in config.yaml).")
        return 2

    filters = (cfg.get("follow", {}) or {}).get("filters", {}) if use_filters else {}

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

        # Show the counts the filters see, for debugging.
        page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded")
        worker._jitter(2.0, 3.5)
        print(f"counts: {worker._read_profile_counts(page)}  private={worker._is_private(page)}"
              f"  follows_you={worker._follows_you(page)}")

        print(f"running follow on @{target} (filters={'on' if use_filters else 'off'}) ...")
        result = worker._follow(page, target, filters)
        print(f"\nRESULT: {result}")
        return 0 if result == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
