# Growth Bot Plan — Follow + Scraping + Churn loop

> Status: **planned, not yet built.** Decisions locked with the user 2026-06-08.
> Resume from "Build order". The existing unfollow tool is finished and working.

## Goal / context

Today the bot only **unfollows** accounts from `data/following.json` (cleanup tool).
We're extending it for **marketing growth**: continuously **follow** strangers
scraped from competitor/niche accounts and posts, then **unfollow the ones who
don't follow back** after a few days (classic follow/unfollow "churn"), running
unattended on the Pi.

**Decisions made:**
- Strategy: full follow → wait → unfollow-non-followers-back **churn loop**.
- Target sources: **both** a profile's *followers* and a post's *likers*.
- Follow method: **visit each profile and filter** (skip private / already-following /
  no-posts / accounts too large to follow back) before following.

> ⚠️ **Risk:** mass-following is the most action-block/ban-prone IG automation,
> far riskier than unfollowing your own list. Real account (`naor223`). Use
> conservative caps (ceilings, not targets), a **combined** follow+unfollow daily
> budget, reuse `_rate_limited` backoff, and start low.

## Architecture

Add a `mode` selector and generalize the single-purpose run loop:
- `mode: unfollow` — existing behavior, untouched.
- `mode: follow` — follow from the candidate pool only (building block / testing).
- `mode: churn` — full loop: top up candidates → follow → unfollow non-followers-back.

Reused building blocks already in `bot.py`:
- Modal scraping: `_open_following_modal`, `_collect_into`, `_COLLECT_JS`, `_SCROLL_JS`, `_scrape_following` → generalize.
- Unfollow side (churn step): `_unfollow`, `_unfollow_via_post`, `_find_following_button`, `_click_unfollow_control`, `_rate_limited`.
- Plumbing: `_jitter`, `_interruptible_sleep`, `append_log`, `StateManager`, config/whitelist helpers.

## Data model (new files under `data/`)

- `follow_candidates.json` — queued usernames to follow (deduped pool).
- `followed.log` — `ts \t username \t source` per successful follow (drives churn timer).
- `follow_skipped.log` — `ts \t username \t reason` (private / no_posts / filtered / already_following).
- `follow_failed.log` — `ts \t username \t reason` (transient; retried).
- `churn_unfollowed.log` — `ts \t username` (unfollowed for not following back).
- `follow_kept.log` — `ts \t username` (followed us back — keep, stop re-checking).

**Follow "done" set** (never re-queue): `followed` ∪ permanent `follow_skipped`
(private/no_posts/filtered) ∪ `churn_unfollowed` ∪ current `following.json` ∪
`whitelist` ∪ own username. Transient failures/rate-limits stay retryable.

## Config additions (`config.yaml`)

```yaml
mode: "churn"                 # unfollow | follow | churn

follow:
  sources:
    follower_profiles: []     # ["competitor1"] -> scrape their followers
    liker_posts: []           # ["https://www.instagram.com/p/XXXX/"] -> scrape likers
  daily_cap: 80               # max NEW follows per day (start conservative)
  min_delay_seconds: 60
  max_delay_seconds: 200
  candidate_pool_min: 300     # auto-scrape more when pool drops below this
  scrape_per_source_cap: 600
  filters:
    skip_private: true
    skip_no_posts: true
    min_followers: 0
    max_followers: 5000       # skip big accounts unlikely to follow back
    max_following: 0          # 0 = ignore
  churn:
    unfollow_after_days: 4
    keep_followers_back: true
    daily_unfollow_cap: 80
```

Combined-action safety: churn budgets follows+unfollows against a shared ceiling.

## bot.py changes

**New action `_follow(page, target) -> str`** — mirror `_unfollow`. Visit profile:
- no header → `unavailable`
- already Following/Requested (`_find_following_button`) → `already_following`
- read counts via new `_read_profile_counts`; apply `follow.filters` → `skipped_private` / `skipped_no_posts` / `skipped_filtered`
- click `Follow`/`Follow Back` (role=button, exact), verify it flips by polling (reuse verify pattern + `_still_following`)
- `_rate_limited` → `rate_limited`; else `ok`

**New helpers:**
- `_read_profile_counts(page) -> dict` — posts/followers/following ints (parse `1,234`, `1.2k`, `1m`).
- `_follows_you(page) -> bool` — "Follows you" chip (reciprocity check). Default method; own-followers-scrape is a more-accurate optional fallback.
- `_scrape_list(page, profile, which)` — generalize `_open_following_modal`/`_scrape_following` to open *followers*/*following* of any profile + collect.
- `_scrape_likers(page, post_url)` — open post, click likes count, collect; return `[]` when IG hides likers.
- `_scrape_candidates(page, cfg)` — run all sources, dedup vs follow done-set, append to `follow_candidates.json` up to `candidate_pool_min`.

**Run loop:**
- `_run` reads `mode` and dispatches: `unfollow`→`_process_day` (existing); `follow`→`_process_follow_day`; `churn`→`_process_churn_cycle`.
- `_process_follow_day` — like `_process_day` but pulls from `follow_candidates.json`, calls `_follow`, follow-side logs, `follow.daily_cap` + follow pacing, tops up pool when low, reuses `_rate_limited` cooldown/abort.
- `_process_churn_cycle` — (1) scrape if pool low; (2) follow up to cap; (3) read `followed.log`, take entries older than `unfollow_after_days` not in kept/churn sets, visit profile: if `_follows_you` & `keep_followers_back` → `follow_kept`, else `_unfollow` → `churn_unfollowed` (honor `daily_unfollow_cap`); (4) sleep `daily_loop_hours`, repeat.
- Extend `BotState`: `followed_count`, `follow_failed_count`, `candidate_pool`, `churn_unfollowed_count`.

## server.py + static/index.html

- `GET /api/follow-lists` (mirror `get_lists`); `GET|PUT /api/sources`; `POST /api/scrape`; expose `mode` via existing config endpoints.
- UI (Alpine): mode selector + `follow` config in Config tab; new **Sources** tab (follower-profiles + liker-post URLs + "Scrape now"); new list tabs **Followed / Candidates / Follow failed**; follow/churn counters in header.

## Build order (stage + verify each)

1. **Follow action + follow mode** — `_follow`, `_read_profile_counts`, follow logs, `_process_follow_day`, `mode` dispatch. Verify with `test_follow.py` (clone of `test_unfollow.py`).
2. **Scraping** — `_scrape_list` + `_scrape_likers` + `_scrape_candidates` + sources config. Verify a small profile's followers + one post's likers populate `follow_candidates.json` (deduped).
3. **Churn loop** — `_follows_you`, `_process_churn_cycle`, churn logs/caps. Dry-run with `unfollow_after_days: 0` on throwaway follows.
4. **Server + UI** — endpoints, mode selector, Sources tab, new list tabs.

## Verification

- `python test_follow.py <user>` → correct `ok`/skip codes; private & big-account filters fire.
- Scrape dedups vs `followed.log` + `following.json` + whitelist.
- Churn dry-run: followers-back → `follow_kept.log`; non-followers → `churn_unfollowed.log`.
- `_rate_limited` cooldown/abort works on follow path too.
- Keep caps low for first live runs; watch `follow_failed.log` + rate-limit hits.
- Deploy: `scp bot.py config.yaml server.py static/index.html` to Pi + `sudo systemctl restart unfollower`.
