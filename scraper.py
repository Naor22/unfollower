#!/usr/bin/env python3
"""Standalone scraper + filter service (separate process; 2nd Chrome / burner account).

Runs independently of the core bot: it scrapes the configured sources and
**browser-navigates** each candidate to filter it (account-agnostic checks only),
then atomically publishes a cleaned `data/follow_candidates.json` for the core bot
to consume - plus `data/scraper_status.json` for the dashboard's Scraper card.

Browser navigation only - never the Instagram API (official or private). It points
at its own Chrome via `scraper.cdp_endpoint` (default http://localhost:9223), which
is logged into a throwaway "scraper" account so the main account bears zero scraping
risk. See the scraper-service architecture notes.

Run on the Pi as the `unfollower-scraper` systemd service, or locally:
    python scraper.py
"""
import signal

import bot


def main() -> None:
    # persist_events=False: this process must NOT write the shared activity.json
    # (the server's StateManager owns it) - two writers would corrupt the feed.
    # log_stdout=True: mirror progress to stdout for the foreground run + journalctl.
    sm = bot.StateManager(persist_events=False, log_stdout=True,
                          event_log_path=bot.SCRAPER_ACTIVITY)
    b = bot.Bot(sm)
    print("[*] scraper service starting…", flush=True)

    def _shutdown(signum, frame):
        b._stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    b.run_scraper()


if __name__ == "__main__":
    main()
