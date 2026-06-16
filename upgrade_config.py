"""Add the follow/churn config keys to an existing config.yaml in place.

Run this once after upgrading an older deployment (e.g. the Pi's unfollow-only
config) to the follow/churn version. It injects `mode`, the `follow:` block, and
the new follow log paths with sensible defaults ONLY where they're missing — it
never changes values you've already set (daily caps, browser, server, etc.).

    python upgrade_config.py

Note: like every config save, this rewrites config.yaml via yaml.safe_dump, so
comments are dropped (the values are preserved).
"""

import bot

DEFAULT_FOLLOW = {
    "sources": {"follower_profiles": [], "liker_posts": []},
    "daily_cap": 80,
    "min_delay_seconds": 60,
    "max_delay_seconds": 200,
    "candidate_pool_min": 300,
    "scrape_per_source_cap": 600,
    "external_scraper": False,   # true = the separate scraper service owns the pool
    "filters": {
        "skip_already_follows_me": True,
        "skip_private": True,
        "skip_no_posts": True,
        "min_followers": 0,
        "max_followers": 5000,
        "max_following": 0,
    },
    "churn": {
        "unfollow_after_days": 4,
        "keep_followers_back": True,
        "daily_unfollow_cap": 80,
        "also_unfollow_following": False,  # also trim the existing following list each cycle
        "list_unfollow_cap": 40,           # max list-trim unfollows per churn cycle
        "interleave_unfollows": 2,         # interleave ratio (unfollows : follows per round)
        "interleave_follows": 1,
    },
}

DEFAULT_LOGS = {
    "followed_log": "data/followed.log",
    "follow_skipped_log": "data/follow_skipped.log",
    "follow_failed_log": "data/follow_failed.log",
    "churn_unfollowed_log": "data/churn_unfollowed.log",
    "follow_kept_log": "data/follow_kept.log",
    "filter_checked_log": "data/filter_checked.log",
    "filter_rejected_log": "data/filter_rejected.log",
}

DEFAULT_BEHAVIOR = {
    "keep_running": False,      # follow/churn: sleep & retry on empty pool instead of stopping
    "idle_recheck_min": 15,
    "idle_recheck_max": 30,
}

DEFAULT_PACING = {
    # per-calendar-day action safety (caps become true daily totals)
    "daily_volume_jitter": 0.3,
    "daily_action_cap": 0,          # combined follows+unfollows/day (0 = off)
    "soft_block_max_per_day": 2,
    # active-hours (local tz) — only act during human hours
    "active_hours_enabled": True,
    "active_hours_start": 8,
    "active_hours_end": 24,
}

DEFAULT_SCRAPER = {
    "enabled": False,
    "coordinate_with_bot": True,   # only scrape during the bot's dead time
    "pool_high_mult": 5,           # stop once ready pool >= this × follow.daily_cap
    # Persistent-profile model (like the main bot): cdp_endpoint "" + user_data_dir,
    # logged in once via scraper_login.py. Set cdp_endpoint to a :9223 URL to use CDP.
    "cdp_endpoint": "",
    "user_data_dir": "data/scraper-profile",
    "idle_seconds": 600,
    "min_delay": 4,
    "max_delay": 10,
    "long_break_every": 40,
    "long_break_min": 60,
    "long_break_max": 180,
    "browser": {   # distinct fingerprint from the main account (anti-association)
        "user_agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
        "viewport_width": 1440,
        "viewport_height": 900,
        "locale": "en-US",
        "timezone_id": "",
        "proxy": "",   # http://user:pass@host:port — route the burner via a different IP
    },
}


def _deep_fill(dst: dict, defaults: dict) -> int:
    """Recursively add missing keys from `defaults` into `dst`. Returns count added."""
    added = 0
    for k, v in defaults.items():
        if k not in dst:
            dst[k] = v
            added += 1
        elif isinstance(v, dict) and isinstance(dst.get(k), dict):
            added += _deep_fill(dst[k], v)
    return added


def main() -> int:
    cfg = bot.load_config()
    added = 0

    if "mode" not in cfg:
        cfg["mode"] = "unfollow"   # safe default; switch to follow/churn in the dashboard
        added += 1

    cfg.setdefault("follow", {})
    added += _deep_fill(cfg["follow"], DEFAULT_FOLLOW)

    cfg.setdefault("logging", {})
    added += _deep_fill(cfg["logging"], DEFAULT_LOGS)

    cfg.setdefault("behavior", {})
    added += _deep_fill(cfg["behavior"], DEFAULT_BEHAVIOR)

    cfg.setdefault("pacing", {})
    added += _deep_fill(cfg["pacing"], DEFAULT_PACING)

    cfg.setdefault("scraper", {})
    added += _deep_fill(cfg["scraper"], DEFAULT_SCRAPER)

    if added:
        bot.save_config(cfg)
        print(f"Added {added} missing config key(s). mode = {cfg['mode']}")
    else:
        print("Config already up to date — nothing to add.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
