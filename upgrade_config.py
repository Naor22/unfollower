"""Migrate config.yaml to the clean mode-oriented schema, in place.

Run this once after deploying the schema-rebuild (e.g. on the Pi, which keeps its
own config.yaml). It loads the existing config, rewrites it to the new schema via
`bot._migrate_config` (preserving every tuned value), fills any missing new keys
with sensible defaults, and saves. Idempotent - re-running adds nothing.

    python upgrade_config.py

Note: like every config save, this rewrites config.yaml via yaml.safe_dump, so
comments are dropped (values are preserved). Back up first if you keep comments:
    cp config.yaml config.yaml.bak
"""

import bot

# New-schema defaults — only MISSING keys are added (_deep_fill never overwrites).
DEFAULTS = {
    "mode": "follow",
    "limits": {
        "follows_per_day": 80,
        "unfollows_per_day": 80,
        "likes_per_day": 100,
        "combined_per_day": 0,      # follows+unfollows ceiling (0 = off)
        "daily_jitter": 0.3,
    },
    "pacing": {
        "action_delay_min": 60,
        "action_delay_max": 200,
        "long_break_every": 15,
        "long_break_min": 300,
        "long_break_max": 900,
        "distraction_chance": 0.08,
        "distraction_min": 60,
        "distraction_max": 240,
    },
    "safety": {
        "active_hours_enabled": False,   # off = run 24/7, stop only on daily caps
        "active_hours_start": 8,
        "active_hours_end": 24,
        "soft_block_max_per_day": 2,
        "rate_limit_max_hits": 3,
        "rate_limit_cooldown_min": 900,
        "rate_limit_cooldown_max": 1800,
        "follow_fail_rest_threshold": 5,
        "follow_fail_rest_min": 1200,
        "follow_fail_rest_max": 2400,
        "follow_rest_max_per_day": 3,
    },
    "targeting": {
        "sources": {"profiles": [], "post_likers": [], "post_commenters": [], "hashtags": []},
        "candidate_pool_min": 300,
        "scrape_per_source_cap": 600,
        "scrape_per_seed_cap": 150,
        "max_same_seed_streak": 2,
        "filters": {
            "skip_already_follows_me": True,
            "skip_private": False,
            "skip_no_posts": True,
            "min_followers": 0,
            "max_followers": 5000,
            "max_following": 0,
        },
        "discovery": {
            "enabled": False,
            "keywords": [],
            "negative_keywords": [],
            "min_followers": 5000,
            "max_followers": 500000,
        },
    },
    "engagement": {
        "reach_enabled": True,
        "reach_source": "hashtags",
        "reach_view_story": True,
        "reach_hashtags": [],
        "reach_cadence_min": 1,
        "reach_cadence_max": 4,
        "reach_cadence_fallback": 5,
        "reach_external_harvest": True,
        "reach_max_same_tag_streak": 2,
        "reach_scrape_per_tag": 60,
        "reach_like_min_delay": 30,
        "reach_like_max_delay": 90,
        "reach_mode": "likes",
        "reach_like_posts": 1,
        "on_follow_view_story": True,
        "on_follow_like_posts": 1,
        "story_min_delay": 8,
        "story_max_delay": 25,
        "story_recheck_hours": 20,
        "story_reach_background": False,
    },
    "marketing": {
        "unfollow_after_days": 2,
        "keep_followers_back": False,
        "also_trim_following": False,
        "list_trim_cap": 40,
        "ratio_unfollows": 5,
        "ratio_follows": 4,
    },
    "scraper": {
        "enabled": False,
        "external": False,           # core bot consumes the scraper's pool
        "keep_running": False,
        "coordinate_with_bot": True,
        "idle_recheck_min": 15,
        "idle_recheck_max": 30,
        "follow_pool_mult": 5,
        "reach_pool_mult": 5,
        "cdp_endpoint": "",
        "user_data_dir": "data/scraper-profile",
        "idle_seconds": 600,
        "filter_delay_min": 1,
        "filter_delay_max": 3,
        "long_break_every": 40,
        "long_break_min": 60,
        "long_break_max": 180,
        "browser": {
            "user_agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
            "viewport_width": 1440,
            "viewport_height": 900,
            "locale": "en-US",
            "timezone_id": "",
            "proxy": "",
        },
    },
    "behavior": {
        "use_following_cache": True,
        "skip_verified": False,
        "warmup_browse_seconds": 20,
        "daily_loop": False,
        "daily_loop_hours": 24,
        "unfollow_retries": 2,
        "unfollow_retry_backoff_seconds": [3, 8],
        "use_following_list_fallback": True,
        "account_resync_every": 40,
        "unfollow_give_up_after": 3,   # stop retrying a poison account after N failed unfollows
    },
    "logging": {
        "unfollowed_log": "data/unfollowed.log",
        "failed_log": "data/failed.log",
        "skipped_log": "data/skipped.log",
        "followed_log": "data/followed.log",
        "follow_skipped_log": "data/follow_skipped.log",
        "follow_failed_log": "data/follow_failed.log",
        "churn_unfollowed_log": "data/churn_unfollowed.log",
        "follow_kept_log": "data/follow_kept.log",
        "follow_outcomes_log": "data/follow_outcomes.log",
        "reach_liked_log": "data/reach_liked.log",
        "filter_checked_log": "data/filter_checked.log",
        "filter_rejected_log": "data/filter_rejected.log",
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
    # load_config already migrates legacy → new schema in memory.
    cfg = bot.load_config()
    added = _deep_fill(cfg, DEFAULTS)
    # Always re-save: even with nothing added, this rewrites the FILE to the clean
    # schema (the in-memory cfg is already migrated).
    bot.save_config(cfg)
    print(f"Config migrated to the clean schema; {added} default key(s) added. "
          f"mode = {cfg.get('mode')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
