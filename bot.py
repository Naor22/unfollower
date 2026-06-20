"""
Bot core: stateful Instagram unfollow worker that runs in a background thread.

Exposes:
  - StateManager: thread-safe state + event broadcaster (with asyncio bridge).
  - Bot: start/stop/pause controller wrapping the Playwright flow.
  - Module-level helpers for reading/writing config, whitelist, and logs
    (consumed by both server.py and main.py).
"""

import collections
import copy
import json
import os
import random
import re
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PWTimeout, sync_playwright

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.yaml"
WHITELIST_PATH = ROOT / "whitelist.txt"
DATA_DIR = ROOT / "data"
SESSION_PATH = DATA_DIR / "session.json"
FOLLOWING_CACHE = DATA_DIR / "following.json"
FOLLOW_CANDIDATES = DATA_DIR / "follow_candidates.json"   # ELIGIBLE result list (bot consumes)
SCRAPER_TODO = DATA_DIR / "scraper_todo.json"            # scraper's raw backlog to vet (input)
REACH_TODO = DATA_DIR / "reach_todo.json"                # reach-prospect raw backlog to vet (prospects mode)
REACH_POOL = DATA_DIR / "reach_pool.json"                # harvested post links for reach liking (bot consumes)
DISCOVERED_SOURCES = DATA_DIR / "discovered_sources.json"
ACCOUNT_STATS = DATA_DIR / "account_stats.json"
SCRAPER_STATUS = DATA_DIR / "scraper_status.json"   # separate scraper service heartbeat/counts
SCRAPER_ACTIVITY = DATA_DIR / "scraper_activity.json"  # scraper's recent log lines (live feed)
SCRAPER_PID = DATA_DIR / "scraper.pid"              # so the server can track/stop the scraper
BURNER_COOLDOWNS = DATA_DIR / "burner_cooldowns.json"  # per-burner cooldown timestamps (multi-burner failover)
DAILY_COUNTS = DATA_DIR / "daily_counts.json"       # real per-CALENDAR-DAY action ledger (ban safety)
BOT_RUNTIME = DATA_DIR / "bot_runtime.json"         # bot "acting" signal so the scraper yields the Pi
ACCOUNT_HISTORY = DATA_DIR / "account_history.log"  # ts/followers/following time-series (growth graph)
RUNTIME_EVENTS = DATA_DIR / "runtime_events.log"    # lifecycle: starts/stops/errors/restarts (analytics)
ACTIVITY_LOG = DATA_DIR / "activity.json"   # shared live-activity feed (all devices)
ACTIVITY_MAX = 1000                          # ring buffer cap (auto-cleanup)

# Follow-side skip reasons that are PERMANENT (never re-queue the account).
# Transient ones (unavailable / transient failures) stay retryable.
PERMANENT_FOLLOW_SKIPS = {"private", "no_posts", "filtered", "already_following", "follows_you"}


# ---------- state ----------

@dataclass
class BotState:
    status: str = "idle"               # idle | starting | logging_in | warmup | scraping | running | paused | stopped | error
    phase_detail: str = ""
    current_target: Optional[str] = None
    unfollowed_count: int = 0
    failed_count: int = 0
    total_targets: int = 0
    progress_index: int = 0
    last_message: str = ""
    error: Optional[str] = None
    started_at: Optional[float] = None
    next_action_at: Optional[float] = None
    daily_cap: int = 0
    # follow / churn side
    followed_count: int = 0
    follow_failed_count: int = 0
    candidate_pool: int = 0
    churn_unfollowed_count: int = 0
    story_viewed_count: int = 0
    # reach marketing stats (this run)
    reach_scraped: int = 0
    reach_liked: int = 0
    reach_pool: int = 0
    # live account stats (fetched from our own IG profile, adjusted per action,
    # periodically re-synced). None until the first fetch.
    account_followers: Optional[int] = None
    account_following: Optional[int] = None
    # today's per-calendar-day action totals + their rolled caps (ban-safety ledger).
    # Declared here so asdict() serializes them to the dashboard top bar.
    day_follows: int = 0
    day_unfollows: int = 0
    day_likes: int = 0
    day_follows_cap: int = 0
    day_unfollows_cap: int = 0
    day_likes_cap: int = 0


class StateManager:
    """Thread-safe state container + asyncio event broadcaster.

    The bot thread calls update()/emit() synchronously; WebSocket subscribers
    receive messages via asyncio queues attached to the FastAPI event loop.
    """

    def __init__(self, persist_events: bool = True, log_stdout: bool = False,
                 event_log_path: "Optional[Path]" = None) -> None:
        self._lock = threading.Lock()
        self._state = BotState()
        self._subscribers: list = []
        self._loop = None
        # The separate scraper service uses its own StateManager but must NOT
        # touch the shared activity.json (the server's StateManager owns it) - two
        # processes writing the same file would corrupt the feed. persist_events=
        # False keeps emit/update working (in-memory) without disk I/O.
        self._persist = persist_events
        # The scraper has no WS subscribers, so its emit() would be invisible.
        # log_stdout=True mirrors emitted 'log' events to stdout, so the foreground
        # run and `journalctl -u unfollower-scraper` actually show progress.
        self._log_stdout = log_stdout
        # Optional SEPARATE ring file for this process's own log lines (the scraper uses
        # it so the dashboard can show a live scraper feed without the scraper writing the
        # shared activity.json). Written on every 'log' emit, capped small.
        self._event_log_path = event_log_path
        self._log_pool = ""   # scraper: which pipeline ("follow"/"reach") is emitting logs now
        # Liveness marker for the watchdog. Bumped on every state update/emit AND
        # on every interruptible-sleep tick, so legitimate long sleeps (cooldowns,
        # daily loop) keep it fresh while a genuine hang (e.g. a frozen Playwright
        # call) lets it go stale.
        self.last_heartbeat = time.time()
        # Shared live-activity feed (persisted to disk so every device sees the same
        # log). Ring buffer caps it; saved every few events.
        self._events = collections.deque(maxlen=ACTIVITY_MAX)
        self._events_since_save = 0
        if self._persist:
            self._load_events()

    def attach_loop(self, loop) -> None:
        self._loop = loop

    def touch(self) -> None:
        self.last_heartbeat = time.time()

    def subscribe(self, q) -> None:
        self._subscribers.append(q)

    def unsubscribe(self, q) -> None:
        if q in self._subscribers:
            self._subscribers.remove(q)

    def update(self, **fields) -> None:
        self.last_heartbeat = time.time()
        with self._lock:
            for k, v in fields.items():
                setattr(self._state, k, v)
            snap = asdict(self._state)
        self._broadcast({"type": "state", "data": snap})

    def emit(self, event_type: str, payload: dict) -> None:
        self.last_heartbeat = time.time()
        payload = dict(payload)
        payload.setdefault("_time", time.strftime("%H:%M:%S"))
        # Tag scraper log lines with the pipeline currently working (set by the bot), so
        # pipeline-agnostic lines ("scrape sweep", "+N from @x") get the right reach/follow
        # badge instead of being text-classified as "follow".
        if event_type == "log" and getattr(self, "_log_pool", ""):
            payload.setdefault("pool", self._log_pool)
        msg = {"type": event_type, "data": payload}
        # Record to the shared feed (everything except high-frequency 'state').
        self._events.append(msg)
        self._events_since_save += 1
        if self._persist and self._events_since_save >= 25:
            self._events_since_save = 0
            self._save_events()
        if self._log_stdout and event_type == "log":
            lvl = (payload.get("level") or "info").upper()
            print(f"[{payload.get('_time')}] {lvl}: {payload.get('msg', '')}", flush=True)
        # Mirror this process's log lines to its own ring file (scraper → live feed),
        # promptly so the dashboard tracks progress in near-real-time.
        if self._event_log_path is not None and event_type == "log":
            self._save_event_ring()
        self._broadcast(msg)

    def _save_event_ring(self) -> None:
        """Persist the last ~80 log lines to the dedicated ring file (atomic). Used by
        the scraper process so the server can surface a live scraper log feed."""
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            logs = [e for e in self._events if e.get("type") == "log"][-80:]
            tmp = self._event_log_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(logs), encoding="utf-8")
            os.replace(tmp, self._event_log_path)
        except Exception:
            pass

    # ---- shared activity feed (disk-backed, capped) ----

    def _load_events(self) -> None:
        try:
            if ACTIVITY_LOG.exists():
                data = json.loads(ACTIVITY_LOG.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    self._events.extend(data[-ACTIVITY_MAX:])
        except Exception:
            pass

    def _save_events(self) -> None:
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            ACTIVITY_LOG.write_text(json.dumps(list(self._events)), encoding="utf-8")
        except Exception:
            pass

    def recent_events(self) -> list:
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()
        self._events_since_save = 0
        try:
            ACTIVITY_LOG.unlink()
        except Exception:
            pass

    def snapshot(self) -> dict:
        with self._lock:
            return asdict(self._state)

    def _broadcast(self, message: dict) -> None:
        if not self._loop:
            return
        for q in list(self._subscribers):
            try:
                self._loop.call_soon_threadsafe(q.put_nowait, message)
            except Exception:
                pass


# ---------- file helpers ----------

def burner_profile_dir(account: dict) -> str:
    """Stable Chromium profile dir for a burner account. Uses an explicit user_data_dir
    if set, else derives one from the label/username so a dashboard-configured account
    (no profile dir) maps to the same dir at login time (scraper_login.py) and run time
    (the scraper). Shared by both so they never disagree."""
    if account.get("user_data_dir"):
        return account["user_data_dir"]
    key = re.sub(r"[^a-z0-9]+", "-", (account.get("label") or account.get("username") or "").lower()).strip("-")
    return f"data/scraper-profile-{key}" if key else "data/scraper-profile"


USERNAME_HREF_RE = re.compile(r"^/([A-Za-z0-9._]+)/?$")
RESERVED = {
    "explore", "reels", "direct", "accounts", "p", "stories", "tv", "about",
    "developer", "legal", "press", "api", "web", "graphql", "challenge",
}


_COUNT_RE = re.compile(r"([\d.,]+)\s*([kmb]?)", re.I)


def parse_count(text: str) -> Optional[int]:
    """Parse an IG count string into an int: '1,234' -> 1234, '1.2k' -> 1200,
    '3m' -> 3000000. Returns None when no number is present."""
    if not text:
        return None
    m = _COUNT_RE.search(text.strip())
    if not m:
        return None
    num_s, suffix = m.group(1), m.group(2).lower()
    # A plain grouped integer like '1,234' uses commas as thousands separators;
    # an abbreviated value like '1.2k' uses a dot as a decimal point.
    if suffix:
        num_s = num_s.replace(",", "")
        try:
            value = float(num_s)
        except ValueError:
            return None
        value *= {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[suffix]
    else:
        try:
            value = float(num_s.replace(",", ""))
        except ValueError:
            return None
    return int(value)


def parse_log_ts(s: str) -> Optional[float]:
    """Parse a 'YYYY-MM-DD HH:MM:SS' log timestamp to a local-time epoch, or
    None. Logs are written with time.strftime (local), so we read them back the
    same way - used by the churn timer to age out follows."""
    try:
        return time.mktime(time.strptime(s.strip(), "%Y-%m-%d %H:%M:%S"))
    except Exception:
        return None


# --------------------------------------------------------------------------
# Config schema migration (legacy → clean mode-oriented schema)
#
# The dashboard, config.yaml and the bot all speak ONE clean schema (blocks:
# mode/limits/pacing/safety/targeting/engagement/marketing/scraper/browser/
# behavior). `_migrate_config` rewrites an older config (or a partially-new one)
# into that schema and is applied on every load, so a Pi still holding the legacy
# config keeps running until `upgrade_config.py` rewrites the file in place.
# It is idempotent: re-running on a new-schema config is a no-op.
# --------------------------------------------------------------------------

def _cfg_get(d: dict, path: str):
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return (False, None)
        cur = cur[k]
    return (True, cur)


def _cfg_set(d: dict, path: str, val) -> None:
    cur = d
    parts = path.split(".")
    for k in parts[:-1]:
        nxt = cur.get(k)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[k] = nxt
        cur = nxt
    cur[parts[-1]] = val


def _cfg_del(d: dict, path: str) -> None:
    cur = d
    parts = path.split(".")
    for k in parts[:-1]:
        if not isinstance(cur, dict) or k not in cur:
            return
        cur = cur[k]
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


# (old_path, new_path) — plain renames/moves. Whole sub-dicts are moved for
# filters/discovery; source leaves are renamed individually below.
_CONFIG_MOVES = [
    # limits (daily ceilings, consolidated)
    ("follow.daily_cap", "limits.follows_per_day"),
    ("follow.engagement.story_reach_daily_cap", "limits.likes_per_day"),
    ("pacing.daily_action_cap", "limits.combined_per_day"),
    ("pacing.daily_volume_jitter", "limits.daily_jitter"),
    # pacing (cleaned)
    ("pacing.long_break_every_n", "pacing.long_break_every"),
    ("pacing.long_break_min_seconds", "pacing.long_break_min"),
    ("pacing.long_break_max_seconds", "pacing.long_break_max"),
    ("pacing.distraction_min_seconds", "pacing.distraction_min"),
    ("pacing.distraction_max_seconds", "pacing.distraction_max"),
    ("pacing.daily_loop_hours", "behavior.daily_loop_hours"),
    # safety (anti-block)
    ("pacing.active_hours_enabled", "safety.active_hours_enabled"),
    ("pacing.active_hours_start", "safety.active_hours_start"),
    ("pacing.active_hours_end", "safety.active_hours_end"),
    ("pacing.soft_block_max_per_day", "safety.soft_block_max_per_day"),
    ("pacing.rate_limit_max_hits", "safety.rate_limit_max_hits"),
    ("pacing.rate_limit_cooldown_min_seconds", "safety.rate_limit_cooldown_min"),
    ("pacing.rate_limit_cooldown_max_seconds", "safety.rate_limit_cooldown_max"),
    ("pacing.follow_fail_rest_threshold", "safety.follow_fail_rest_threshold"),
    ("pacing.follow_fail_rest_min_seconds", "safety.follow_fail_rest_min"),
    ("pacing.follow_fail_rest_max_seconds", "safety.follow_fail_rest_max"),
    ("pacing.follow_rest_max_per_day", "safety.follow_rest_max_per_day"),
    # targeting (sources + scrape knobs + filters + discovery)
    ("follow.sources.follower_profiles", "targeting.sources.profiles"),
    ("follow.sources.liker_posts", "targeting.sources.post_likers"),
    ("follow.sources.commenter_posts", "targeting.sources.post_commenters"),
    ("follow.sources.hashtags", "targeting.sources.hashtags"),
    ("follow.candidate_pool_min", "targeting.candidate_pool_min"),
    ("follow.scrape_per_source_cap", "targeting.scrape_per_source_cap"),
    ("follow.scrape_per_seed_cap", "targeting.scrape_per_seed_cap"),
    ("follow.max_same_seed_streak", "targeting.max_same_seed_streak"),
    ("follow.filters", "targeting.filters"),
    ("follow.discovery", "targeting.discovery"),
    # engagement (promoted from follow.engagement; reach keys de-jargoned)
    ("follow.engagement.story_reach_enabled", "engagement.reach_enabled"),
    ("follow.engagement.reach_source", "engagement.reach_source"),
    ("follow.engagement.reach_view_story", "engagement.reach_view_story"),
    ("follow.engagement.reach_hashtags", "engagement.reach_hashtags"),
    ("follow.engagement.story_reach_every_min", "engagement.reach_cadence_min"),
    ("follow.engagement.story_reach_every_max", "engagement.reach_cadence_max"),
    ("follow.engagement.story_reach_every_actions", "engagement.reach_cadence_fallback"),
    ("follow.engagement.reach_external_harvest", "engagement.reach_external_harvest"),
    ("follow.engagement.reach_max_same_tag_streak", "engagement.reach_max_same_tag_streak"),
    ("follow.engagement.reach_scrape_per_tag", "engagement.reach_scrape_per_tag"),
    ("follow.engagement.reach_like_min_delay", "engagement.reach_like_min_delay"),
    ("follow.engagement.reach_like_max_delay", "engagement.reach_like_max_delay"),
    ("follow.engagement.reach_mode", "engagement.reach_mode"),
    ("follow.engagement.reach_like_posts", "engagement.reach_like_posts"),
    ("follow.engagement.on_follow_view_story", "engagement.on_follow_view_story"),
    ("follow.engagement.on_follow_like_posts", "engagement.on_follow_like_posts"),
    ("follow.engagement.story_min_delay", "engagement.story_min_delay"),
    ("follow.engagement.story_max_delay", "engagement.story_max_delay"),
    ("follow.engagement.story_recheck_hours", "engagement.story_recheck_hours"),
    ("follow.engagement.story_reach_background", "engagement.story_reach_background"),
    # marketing (from follow.churn)
    ("follow.churn.unfollow_after_days", "marketing.unfollow_after_days"),
    ("follow.churn.keep_followers_back", "marketing.keep_followers_back"),
    ("follow.churn.also_unfollow_following", "marketing.also_trim_following"),
    ("follow.churn.list_unfollow_cap", "marketing.list_trim_cap"),
    ("follow.churn.interleave_unfollows", "marketing.ratio_unfollows"),
    ("follow.churn.interleave_follows", "marketing.ratio_follows"),
    # scraper (+ autopilot pulled in). (follow.external_scraper is retired - the burner
    # service is now the only scraper; the bot is always consume-only.)
    ("behavior.keep_running", "scraper.keep_running"),
    ("behavior.idle_recheck_min", "scraper.idle_recheck_min"),
    ("behavior.idle_recheck_max", "scraper.idle_recheck_max"),
    ("scraper.pool_high_mult", "scraper.follow_pool_mult"),
    ("scraper.reach_pool_high_mult", "scraper.reach_pool_mult"),
    ("scraper.min_delay", "scraper.filter_delay_min"),
    ("scraper.max_delay", "scraper.filter_delay_max"),
]

# (new_path, [old_paths in priority order]) — value comes from the first old path
# present. Used where two legacy keys collapse into one.
_CONFIG_MERGES = [
    ("limits.unfollows_per_day", ["follow.churn.daily_unfollow_cap", "pacing.daily_cap"]),
    ("pacing.action_delay_min", ["follow.min_delay_seconds", "pacing.min_delay_seconds"]),
    ("pacing.action_delay_max", ["follow.max_delay_seconds", "pacing.max_delay_seconds"]),
]

# Legacy keys fully consumed above — dropped so the rewritten file is clean.
_CONFIG_DROP = [
    "pacing.daily_cap", "pacing.min_delay_seconds", "pacing.max_delay_seconds",
    "follow",  # everything under follow has been moved out
]


def _migrate_config(raw: dict) -> dict:
    """Return `raw` rewritten into the clean schema. Idempotent."""
    if not isinstance(raw, dict):
        return raw
    c = copy.deepcopy(raw)

    if c.get("mode") == "churn":
        c["mode"] = "marketing"

    # Merges first (they read legacy keys that moves would otherwise delete).
    for new_path, old_paths in _CONFIG_MERGES:
        if _cfg_get(c, new_path)[0]:
            continue  # already new-schema
        for op in old_paths:
            ok, val = _cfg_get(c, op)
            if ok:
                _cfg_set(c, new_path, val)
                break

    # Plain moves: only set the new key when it isn't already present (idempotent),
    # then drop the old key so the rewritten file is clean.
    for old_path, new_path in _CONFIG_MOVES:
        ok, val = _cfg_get(c, old_path)
        if ok and not _cfg_get(c, new_path)[0]:
            _cfg_set(c, new_path, val)
        _cfg_del(c, old_path)

    for p in _CONFIG_DROP:
        _cfg_del(c, p)

    # Safety net: a stale dashboard once wrote targeting.sources with the /api/sources
    # LEGACY field names (follower_profiles/liker_posts/commenter_posts + a stray `ok`),
    # leaving no canonical `profiles` key → the bot/scraper saw zero profiles. Map any
    # such legacy keys back to canonical (without clobbering good data) and drop them, so
    # a corrupted config self-heals on the next load/save.
    src = (c.get("targeting") or {}).get("sources")
    if isinstance(src, dict):
        for old, new in (("follower_profiles", "profiles"), ("liker_posts", "post_likers"),
                         ("commenter_posts", "post_commenters")):
            if old in src:
                if not src.get(new):
                    src[new] = src[old]
                del src[old]
        src.pop("ok", None)

    return c


_config_cache = {"mtime": None, "cfg": None}


def load_config() -> dict:
    """Parse + migrate config.yaml, CACHED by file mtime. load_config is called from
    nearly every hot path (each read_*_log via _log_path, the run/scraper loops, the
    watchdog, reach ticks), and parsing YAML + running _migrate_config (a deepcopy +
    dozens of key walks) on every call is pure wasted CPU on the Pi. We re-parse only
    when the file actually changes, and return a deep copy so a caller mutating the
    result can't corrupt the cache."""
    try:
        mtime = os.path.getmtime(CONFIG_PATH)
    except OSError:
        mtime = None
    if _config_cache["cfg"] is None or _config_cache["mtime"] != mtime:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            _config_cache["cfg"] = _migrate_config(yaml.safe_load(f))
        _config_cache["mtime"] = mtime
    return copy.deepcopy(_config_cache["cfg"])


def save_config(data: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(_migrate_config(data), f, sort_keys=False, default_flow_style=False)
    _config_cache["mtime"] = None   # force a re-read on next load (mtime may be same-second)


def load_whitelist() -> set[str]:
    if not WHITELIST_PATH.exists():
        return set()
    out: set[str] = set()
    for line in WHITELIST_PATH.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.add(s.lstrip("@").lower())
    return out


def save_whitelist(text: str) -> None:
    WHITELIST_PATH.write_text(text, encoding="utf-8")


def read_unfollowed_log() -> list[dict]:
    log_path = ROOT / load_config()["logging"]["unfollowed_log"]
    if not log_path.exists():
        return []
    rows = []
    for line in _dedup_lines(log_path.read_text(encoding="utf-8")):
        parts = line.split("\t")
        if len(parts) >= 2:
            rows.append({
                "timestamp": parts[0],
                "username": parts[1],
                "note": parts[2] if len(parts) > 2 else "",
            })
    return rows


def read_failed_log() -> list[dict]:
    log_path = ROOT / load_config()["logging"]["failed_log"]
    if not log_path.exists():
        return []
    rows = []
    for line in _dedup_lines(log_path.read_text(encoding="utf-8")):
        parts = line.split("\t")
        if len(parts) >= 3:
            rows.append({"timestamp": parts[0], "username": parts[1], "reason": parts[2]})
    return rows


def read_skipped_log() -> list[dict]:
    """Accounts skipped (deleted/unavailable, or already not-following). Marked
    done so they aren't retried, but kept separate from real unfollows."""
    log_path = ROOT / load_config()["logging"].get("skipped_log", "data/skipped.log")
    if not log_path.exists():
        return []
    rows = []
    for line in _dedup_lines(log_path.read_text(encoding="utf-8")):
        parts = line.split("\t")
        if len(parts) >= 3:
            rows.append({"timestamp": parts[0], "username": parts[1], "reason": parts[2]})
    return rows


def read_following_cache() -> list[str]:
    if not FOLLOWING_CACHE.exists():
        return []
    return json.loads(FOLLOWING_CACHE.read_text(encoding="utf-8"))


def write_following_cache(usernames: list[str]) -> None:
    """Overwrite the following cache (kept oldest-first)."""
    FOLLOWING_CACHE.parent.mkdir(exist_ok=True)
    FOLLOWING_CACHE.write_text(json.dumps(usernames, indent=2), encoding="utf-8")


def scraper_pid() -> Optional[int]:
    """PID written by a running scraper service (run_scraper), or None."""
    try:
        return int(SCRAPER_PID.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def scraper_running() -> bool:
    """Is a scraper service alive? Checks the PID file's process liveness, so it
    works whether the scraper was launched by the server (subprocess), left
    orphaned after a server restart, or run by systemd."""
    pid = scraper_pid()
    if not pid:
        return False
    try:
        os.kill(pid, 0)   # signal 0 = liveness probe (no signal sent)
        return True
    except Exception:
        return False


# ---------- follow / churn file helpers ----------

def _log_path(key: str, default: str) -> Path:
    return ROOT / load_config()["logging"].get(key, default)


def _dedup_lines(text: str) -> list[str]:
    """Split into lines, dropping EXACT duplicate lines (same ts+user+reason)
    while preserving order. Cross-run retries differ by timestamp so they're
    kept; only true double-writes collapse."""
    seen: set[str] = set()
    out = []
    for line in text.splitlines():
        if not line or line in seen:
            continue
        seen.add(line)
        out.append(line)
    return out


def _read_reason_log(path: Path) -> list[dict]:
    """Read a 'ts \\t username \\t reason' style log into dict rows."""
    if not path.exists():
        return []
    rows = []
    for line in _dedup_lines(path.read_text(encoding="utf-8")):
        parts = line.split("\t")
        if len(parts) >= 3:
            rows.append({"timestamp": parts[0], "username": parts[1], "reason": parts[2]})
        elif len(parts) == 2:
            rows.append({"timestamp": parts[0], "username": parts[1], "reason": ""})
    return rows


def read_followed_log() -> list[dict]:
    """Successful follows: ts \\t username \\t source. 'source' drives the churn timer."""
    path = _log_path("followed_log", "data/followed.log")
    if not path.exists():
        return []
    rows = []
    for line in _dedup_lines(path.read_text(encoding="utf-8")):
        parts = line.split("\t")
        if len(parts) >= 2:
            rows.append({
                "timestamp": parts[0],
                "username": parts[1],
                "source": parts[2] if len(parts) > 2 else "",
            })
    return rows


def read_follow_skipped_log() -> list[dict]:
    return _read_reason_log(_log_path("follow_skipped_log", "data/follow_skipped.log"))


def read_follow_failed_log() -> list[dict]:
    return _read_reason_log(_log_path("follow_failed_log", "data/follow_failed.log"))


def read_churn_unfollowed_log() -> list[dict]:
    return _read_reason_log(_log_path("churn_unfollowed_log", "data/churn_unfollowed.log"))


def read_follow_kept_log() -> list[dict]:
    return _read_reason_log(_log_path("follow_kept_log", "data/follow_kept.log"))


def read_follow_candidates() -> list[dict]:
    """Queued accounts to follow. Stored as JSON; each entry is either a plain
    username string or a {"username", "source"} object. Normalized to dicts."""
    if not FOLLOW_CANDIDATES.exists():
        return []
    try:
        raw = json.loads(FOLLOW_CANDIDATES.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for item in raw:
        if isinstance(item, str):
            out.append({"username": item.lstrip("@").lower(), "source": ""})
        elif isinstance(item, dict) and item.get("username"):
            d = dict(item)   # preserve vetted metadata (followers/posts/private/vetted_at)
            d["username"] = str(item["username"]).lstrip("@").lower()
            d["source"] = item.get("source", "")
            out.append(d)
    return out


def _write_candidates_atomic(path: Path, entries: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def write_follow_candidates(entries: list[dict]) -> None:
    """Atomically replace the ELIGIBLE candidate list (the bot's input). Atomic so
    the core bot never reads a half-written file while the scraper republishes."""
    _write_candidates_atomic(FOLLOW_CANDIDATES, entries)


def read_reach_pool() -> list[dict]:
    """Harvested post links for reach-mode liking. Each entry is a plain URL string
    or {"url", "tag", "source", "added_at"}. Normalized to dicts. Filled by the
    scraper (sole writer); the bot only reads it + logs likes to reach_liked.log."""
    if not REACH_POOL.exists():
        return []
    try:
        raw = json.loads(REACH_POOL.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for item in raw:
        if isinstance(item, str):
            out.append({"url": item, "tag": "", "source": ""})
        elif isinstance(item, dict) and item.get("url"):
            d = dict(item)
            d["url"] = str(item["url"])
            d["tag"] = item.get("tag", "")
            d["source"] = item.get("source", item.get("tag", ""))
            out.append(d)
    return out


def write_reach_pool(entries: list[dict]) -> None:
    """Atomically replace the reach link pool. The SCRAPER is the only writer (the
    bot dedups via reach_liked.log instead of rewriting), so there's no write race."""
    _write_candidates_atomic(REACH_POOL, entries)


def read_scraper_activity() -> list[dict]:
    """The scraper service's recent log lines (live feed for the dashboard). Written
    by the scraper's StateManager ring; empty if it hasn't reported yet."""
    if not SCRAPER_ACTIVITY.exists():
        return []
    try:
        data = json.loads(SCRAPER_ACTIVITY.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def read_scraper_todo() -> list[dict]:
    """The scraper's private backlog of raw scraped accounts awaiting vetting."""
    if not SCRAPER_TODO.exists():
        return []
    try:
        raw = json.loads(SCRAPER_TODO.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for item in raw:
        if isinstance(item, str):
            out.append({"username": item.lstrip("@").lower(), "source": ""})
        elif isinstance(item, dict) and item.get("username"):
            out.append({"username": str(item["username"]).lstrip("@").lower(),
                        "source": item.get("source", "")})
    return out


def write_scraper_todo(entries: list[dict]) -> None:
    _write_candidates_atomic(SCRAPER_TODO, entries)


def read_reach_todo() -> list[dict]:
    """The reach harvester's private backlog of raw prospect accounts awaiting vetting
    (prospects mode) - same shape as the scraper todo."""
    if not REACH_TODO.exists():
        return []
    try:
        raw = json.loads(REACH_TODO.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for item in raw:
        if isinstance(item, str):
            out.append({"username": item.lstrip("@").lower(), "source": ""})
        elif isinstance(item, dict) and item.get("username"):
            out.append({"username": str(item["username"]).lstrip("@").lower(),
                        "source": item.get("source", "")})
    return out


def write_reach_todo(entries: list[dict]) -> None:
    _write_candidates_atomic(REACH_TODO, entries)


def read_filter_rejected_log() -> list[dict]:
    """Candidates the scraper service filtered OUT (failed a filter). Excluded
    from the follow done-set so the core bot never visits them."""
    return _read_reason_log(_log_path("filter_rejected_log", "data/filter_rejected.log"))


def read_filter_checked_log() -> list[dict]:
    """Candidates the scraper service already evaluated and KEPT - tracked only so
    the scraper doesn't re-check them (these stay in the pool to be followed)."""
    path = _log_path("filter_checked_log", "data/filter_checked.log")
    if not path.exists():
        return []
    rows = []
    for line in _dedup_lines(path.read_text(encoding="utf-8")):
        parts = line.split("\t")
        if len(parts) >= 2:
            rows.append({"timestamp": parts[0], "username": parts[1]})
    return rows


def read_discovered_sources() -> list[dict]:
    """Niche-influencer accounts the bot flagged during normal profile visits,
    awaiting one-click promotion into follow.sources. Each entry:
    {username, followers, matched, ts}."""
    if not DISCOVERED_SOURCES.exists():
        return []
    try:
        raw = json.loads(DISCOVERED_SOURCES.read_text(encoding="utf-8"))
    except Exception:
        return []
    return [r for r in raw if isinstance(r, dict) and r.get("username")]


def write_discovered_sources(entries: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DISCOVERED_SOURCES.write_text(json.dumps(entries, indent=2), encoding="utf-8")


def read_account_stats() -> dict:
    """Last-known own follower/following counts, persisted so the status bar shows
    immediately on dashboard open (even while idle / after a server restart)."""
    if not ACCOUNT_STATS.exists():
        return {}
    try:
        return json.loads(ACCOUNT_STATS.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_account_stats(followers, following) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ACCOUNT_STATS.write_text(json.dumps(
        {"followers": followers, "following": following, "ts": time.time()}),
        encoding="utf-8")


def read_follow_outcomes() -> list[dict]:
    """Per-account reciprocity measured at churn time: ts \\t username \\t source
    \\t followed_back(0|1). Drives the per-source conversion analytics."""
    path = _log_path("follow_outcomes_log", "data/follow_outcomes.log")
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 4:
            rows.append({"timestamp": parts[0], "username": parts[1],
                         "source": parts[2], "followed_back": parts[3] == "1"})
    return rows


def append_log(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def append_event(kind: str, detail: str = "") -> None:
    """Record a lifecycle event (bot/scraper start/stop, error, restart, checkpoint,
    soft_block) to runtime_events.log for the analytics page."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        append_log(RUNTIME_EVENTS, f"{ts}\t{kind}\t{detail}")
    except Exception:
        pass


def read_runtime_events() -> list[dict]:
    if not RUNTIME_EVENTS.exists():
        return []
    rows = []
    for line in RUNTIME_EVENTS.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            rows.append({"timestamp": parts[0], "kind": parts[1],
                         "detail": parts[2] if len(parts) > 2 else ""})
    return rows


def read_account_history() -> list[dict]:
    if not ACCOUNT_HISTORY.exists():
        return []
    rows = []
    for line in ACCOUNT_HISTORY.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 3:
            try:
                rows.append({"timestamp": parts[0],
                             "followers": int(parts[1]), "following": int(parts[2])})
            except ValueError:
                continue
    return rows


# ---------- Bot ----------

class Bot:
    def __init__(self, state: StateManager) -> None:
        self.state = state
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()  # set = paused
        self._lock = threading.Lock()
        self._me = ""  # logged-in handle; set in _run, used by the modal fallback
        self._run_skip = set()  # accounts that hard-failed THIS run - skip so one
                                # poison profile can't jam the queue every batch
        self._follow_fail_streak = 0  # follow failures since the last success, counted
                                      # ACROSS interleaved batches (drives the gated-rest)
        self._intent = ""       # current action intent ("follow"/"unfollow"/"like"),
                                # set at each flow's entry so every _step shows what
                                # the bot is TRYING to do, not just the final outcome
        self._scraper_service = False  # True only in the standalone scraper process, so
                                       # its long passes yield the Pi the moment the bot
                                       # starts acting (the core bot's self-scrape must not)
        self._warmed = False    # one-time external-scraper pool warm-up gate
        self._actions_since_resync = 0  # drives periodic account-count re-sync
        self._force_account_refresh = False  # set by a dashboard force-refresh while running
        self._likers_blocked_until = 0.0  # pause profile-liker scraping until this ts (IG gated)
        self._active_burner_dir = None  # which burner profile the scraper is currently using
        self._recent_follow_seeds = collections.deque(maxlen=20)  # last seeds followed, for source diversity
        self._recent_reach_tags = collections.deque(maxlen=20)  # last reach tags liked, for reach diversity
        self._reach_consumed = set()    # reach URLs picked this run (pre-log de-dupe guard)
        self._story_tick = 0            # interleaved story-reach: actions since last view
        self._story_today = 0           # stories viewed this batch (vs daily cap)
        self._story_queue = None        # lazily-built pool of accounts to story-view
        self._story_stop = threading.Event()   # signals the background story worker to end
        self._story_thread = None       # background story-reach worker (CDP mode)
        self._story_worker_active = False  # True while the concurrent worker owns story-reach
        self._reach_checked = 0         # reach items checked this run (for heartbeat)
        self._reach_acted = 0           # reach items actually liked/viewed
        self._reach_pool = []           # hashtag-reach: combined [(url, tag)] cache
        self._reach_tag = ""            # hashtag of the last-picked post (for labels)
        self._reach_last_scrape = 0.0   # monotonic ts of last hashtag-grid load
        self._reach_scrape_cooldown = 0.0  # min seconds between grid loads (grows if gated)
        self._story_next = 1            # actions until the next reach fires (randomized)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # --- lifecycle ---

    def start(self) -> bool:
        with self._lock:
            if self.is_running:
                return False
            self._stop_event.clear()
            self._pause_event.clear()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            return True

    def start_scrape(self) -> bool:
        """Run a one-shot source scrape in the background. Shares the run thread
        slot, so it's mutually exclusive with a normal run (they'd fight over the
        same browser tab)."""
        with self._lock:
            if self.is_running:
                return False
            self._stop_event.clear()
            self._pause_event.clear()
            self._thread = threading.Thread(target=self._scrape_once, daemon=True)
            self._thread.start()
            return True

    def stop(self) -> None:
        self._stop_event.set()
        self._pause_event.clear()

    def pause(self) -> None:
        if self.is_running:
            self._pause_event.set()
            self.state.update(status="paused")

    def resume(self) -> None:
        if self._pause_event.is_set():
            self._pause_event.clear()
            self.state.update(status="running")

    def join(self, timeout: Optional[float] = None) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)

    # --- timing helpers ---

    def _set_acting(self, acting: bool, running: bool = True) -> None:
        """Publish whether the main bot is actively working, so the separate scraper
        process can yield the Pi (only scrape during the bot's dead time). Refreshed
        periodically while alive (see _interruptible_sleep) for crash detection.
        `running` distinguishes a live-but-idle bot (between cycles / sleeping) from a
        stopped one - when the bot is stopped the scraper builds pools to HIGH-water."""
        self._acting = acting
        self._acting_written = time.time()
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = BOT_RUNTIME.with_suffix(".json.tmp")
            tmp.write_text(json.dumps({"acting": acting, "running": running, "ts": time.time()}),
                           encoding="utf-8")
            os.replace(tmp, BOT_RUNTIME)
        except Exception:
            pass

    def _bot_is_acting(self) -> bool:
        """Used by the scraper process: is the main bot actively working right now?
        (Fresh 'acting' flag - stale/absent means idle or crashed → free to scrape.)"""
        try:
            d = json.loads(BOT_RUNTIME.read_text(encoding="utf-8"))
        except Exception:
            return False
        return bool(d.get("acting")) and (time.time() - float(d.get("ts", 0))) < 90

    def _bot_is_running(self) -> bool:
        """Used by the scraper process: is the main bot alive (working OR idle between
        cycles), as opposed to stopped/crashed? A live bot refreshes bot_runtime.json
        every ~30s even while idle, so a stale/absent file or running=False means the
        bot is stopped → the scraper is free to build the pools all the way to
        high-water (nothing will consume them today)."""
        try:
            d = json.loads(BOT_RUNTIME.read_text(encoding="utf-8"))
        except Exception:
            return False
        return bool(d.get("running", True)) and (time.time() - float(d.get("ts", 0))) < 120

    def _interruptible_sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while True:
            self.state.touch()   # heartbeat: a sleeping bot is alive, a hung one isn't
            # Keep the bot_runtime signal fresh through long internal waits (jitter /
            # long-breaks / day-cap sleep). Refresh whether acting OR idle so the
            # scraper can tell a live-but-idle bot (stay paused / two-stage fill) from a
            # stopped one (build pools to high-water).
            if time.time() - getattr(self, "_acting_written", 0) > 30:
                self._set_acting(getattr(self, "_acting", False))
            if self._stop_event.is_set():
                return
            if self._pause_event.is_set():
                while self._pause_event.is_set() and not self._stop_event.is_set():
                    self.state.touch()
                    time.sleep(0.25)
                end = time.monotonic() + seconds
                continue
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.25, remaining))

    def _jitter(self, lo: float, hi: float, dist_chance: float = 0.0,
                dist_lo: float = 0.0, dist_hi: float = 0.0) -> None:
        d = random.uniform(lo, hi)
        if dist_chance and random.random() < dist_chance:
            d += random.uniform(dist_lo, dist_hi)
        self.state.update(next_action_at=time.time() + d)
        self._interruptible_sleep(d)
        self.state.update(next_action_at=None)

    def _human_type(self, page, selector: str, text: str) -> None:
        page.click(selector)
        time.sleep(random.uniform(0.2, 0.6))
        for ch in text:
            page.keyboard.type(ch, delay=random.randint(60, 220))
        time.sleep(random.uniform(0.3, 0.9))

    def _random_mouse(self, page) -> None:
        try:
            page.mouse.move(
                random.randint(100, 1100),
                random.randint(100, 700),
                steps=random.randint(8, 20),
            )
        except Exception:
            pass

    # --- IG flow ---

    # A visible login form means logged OUT; the feed nav means logged IN.
    _LOGIN_FORM_SEL = (
        'input[name="username"], input[name="email"], input[name="pass"], '
        'input[type="password"]'
    )
    _LOGGED_IN_SEL = (
        'svg[aria-label="Home"], a[href="/explore/"], a[href="/direct/inbox/"], '
        'svg[aria-label="New post"]'
    )

    def _is_logged_in(self, page) -> bool:
        """Determine login state deterministically from the home page contents.

        The old approach raced a /accounts/login/ -> home redirect on a 6s timer,
        which gave false negatives when the redirect was slow (then the bot wrongly
        fell into the login flow and crashed looking for a form that isn't there).
        Instead we load the home page and poll for either the login form (logged
        out) or the feed navigation (logged in)."""
        page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
        end = time.monotonic() + 20
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return False
            try:
                if page.locator(self._LOGGED_IN_SEL).count() > 0:
                    return True
                if page.locator(self._LOGIN_FORM_SEL).count() > 0:
                    return False
            except Exception:
                pass
            time.sleep(0.4)
        # Timed out with neither clear signal - fall back to "logged in only if
        # no login form is present" so we don't wrongly attempt a login.
        try:
            return page.locator(self._LOGIN_FORM_SEL).count() == 0
        except Exception:
            return False

    def _screenshot(self, page, label: str) -> str:
        DATA_DIR.mkdir(exist_ok=True)
        path = DATA_DIR / f"{label}_{int(time.time())}.png"
        try:
            page.screenshot(path=str(path), full_page=False)
            return path.name
        except Exception:
            return ""

    def _dismiss_cookie_banner(self, page) -> None:
        labels = (
            r"^Allow all cookies$",
            r"^Accept all$",
            r"^Accept All$",
            r"^Only allow essential cookies$",
            r"^Decline optional cookies$",
        )
        for pattern in labels:
            try:
                page.get_by_role("button", name=re.compile(pattern, re.I)).first.click(timeout=1500)
                time.sleep(random.uniform(0.5, 1.2))
                return
            except Exception:
                continue

    # Instagram's login inputs vary by deployment. Current production uses
    # name="email" + autocomplete="username webauthn" and name="pass". Older
    # IG (and the dedicated mobile UI) used name="username" / name="password".
    USERNAME_SELECTORS = (
        '#login_form input[type="text"]',
        'input[name="username"]',
        'input[name="email"]',
        'input[autocomplete~="username"]',
        'input[aria-label*="username" i]',
        'input[aria-label*="email" i]',
        'input[aria-label*="phone" i]',
        'form input[type="text"]',
    )
    PASSWORD_SELECTORS = (
        '#login_form input[type="password"]',
        'input[name="password"]',
        'input[name="pass"]',
        'input[type="password"]',
        'input[autocomplete="current-password"]',
        'input[aria-label*="password" i]',
    )
    SUBMIT_SELECTORS = (
        '#login_form input[type="submit"]',
        '#login_form button[type="submit"]',
        'button[type="submit"]',
        'input[type="submit"]',
        'div[role="button"]:has-text("Log in")',
        'button:has-text("Log in")',
    )

    def _first_visible(self, page, selectors: tuple[str, ...]):
        """Return the first locator from selectors that has a visible element."""
        for sel in selectors:
            loc = page.locator(sel).first
            try:
                if loc.is_visible(timeout=500):
                    return loc, sel
            except Exception:
                continue
        return None, None

    def _wait_for_any(self, page, selectors: tuple[str, ...], timeout_s: float):
        end = time.monotonic() + timeout_s
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return None, None
            loc, sel = self._first_visible(page, selectors)
            if loc is not None:
                return loc, sel
            time.sleep(0.3)
        return None, None

    def _login(self, page, username: str, password: str) -> None:
        self.state.update(status="logging_in", phase_detail="opening login page")
        if "/accounts/login" not in page.url:
            page.goto(
                "https://www.instagram.com/accounts/login/",
                wait_until="domcontentloaded",
            )

        self._dismiss_cookie_banner(page)

        self.state.update(phase_detail="waiting for login form")
        user_field, used_user_sel = self._wait_for_any(page, self.USERNAME_SELECTORS, 30)
        if user_field is None:
            shot = self._screenshot(page, "login_no_form")
            # Dump HTML alongside for debugging when selectors all miss.
            try:
                html_path = DATA_DIR / f"login_no_form_{int(time.time())}.html"
                html_path.write_text(page.content(), encoding="utf-8")
            except Exception:
                pass
            raise RuntimeError(
                f"login form not found on {page.url} (screenshot: {shot})"
            )

        pw_field, used_pw_sel = self._first_visible(page, self.PASSWORD_SELECTORS)
        if pw_field is None:
            # Click username first - IG sometimes lazy-mounts the password input on focus.
            try:
                user_field.click(timeout=2000)
                time.sleep(0.5)
            except Exception:
                pass
            pw_field, used_pw_sel = self._wait_for_any(page, self.PASSWORD_SELECTORS, 10)
        if pw_field is None:
            shot = self._screenshot(page, "login_no_password")
            raise RuntimeError(f"password field not found (screenshot: {shot})")

        # Type into the actual focused inputs with humanized cadence.
        self.state.update(phase_detail=f"typing username (sel: {used_user_sel})")
        user_field.click()
        time.sleep(random.uniform(0.3, 0.8))
        user_field.fill("")  # clear anything autofilled
        user_field.press_sequentially(username, delay=random.randint(70, 180))
        self._jitter(0.5, 1.4)

        self.state.update(phase_detail=f"typing password (sel: {used_pw_sel})")
        pw_field.click()
        time.sleep(random.uniform(0.3, 0.8))
        pw_field.fill("")
        pw_field.press_sequentially(password, delay=random.randint(70, 180))
        self._jitter(0.4, 1.2)

        # Sanity check the fields actually got our values.
        if not user_field.input_value():
            shot = self._screenshot(page, "login_username_empty")
            raise RuntimeError(f"username field empty after typing (screenshot: {shot})")
        if not pw_field.input_value():
            shot = self._screenshot(page, "login_password_empty")
            raise RuntimeError(f"password field empty after typing (screenshot: {shot})")

        self.state.update(phase_detail="submitting login")
        submit_loc, used_submit_sel = self._first_visible(page, self.SUBMIT_SELECTORS)
        clicked = False
        if submit_loc is not None:
            try:
                submit_loc.click(timeout=4000)
                clicked = True
                self.state.update(phase_detail=f"submitted via {used_submit_sel}")
            except Exception:
                clicked = False
        if not clicked:
            # Fall back: press Enter on the password field - the form submits natively.
            self.state.update(phase_detail="submitting login (Enter on password)")
            try:
                pw_field.press("Enter")
            except Exception:
                shot = self._screenshot(page, "login_no_submit")
                raise RuntimeError(f"could not submit login form (screenshot: {shot})")

        # After submit, IG may show: home (success), 2FA prompt, captcha/challenge,
        # or an inline error. We poll for up to 5 minutes so the user can solve a
        # captcha or enter a 2FA code by hand in the visible browser window.
        twofa_sel = 'input[name="verificationCode"], input[autocomplete="one-time-code"]'
        error_sel = 'p[id*="slfErrorAlert"], div[role="alert"]'
        login_wait_seconds = 60
        end = time.monotonic() + login_wait_seconds
        last_phase = ""

        def login_phase_from_url(url: str) -> str:
            if "/challenge" in url:
                return "captcha / challenge - solve it in the browser"
            if "/two_factor" in url or "two-factor" in url:
                return "2FA - enter the code in the browser"
            if "/accounts/login" in url:
                return "waiting for login response (solve captcha/2FA in browser - up to 1 min)"
            return "login almost done"

        while time.monotonic() < end:
            if self._stop_event.is_set():
                return

            url = page.url
            phase = login_phase_from_url(url)
            if phase != last_phase:
                self.state.update(phase_detail=phase)
                last_phase = phase

            # Success: we've landed on a non-login, non-challenge URL.
            if "/accounts/login" not in url and "/challenge" not in url and "/two_factor" not in url:
                # Quick sanity check - still a valid IG page (not redirected to a non-IG host).
                if "instagram.com" in url:
                    break

            # Surface a 2FA input even if URL is still /accounts/login (older flows).
            try:
                if page.locator(twofa_sel).count() > 0 and last_phase != "2FA - enter the code in the browser":
                    self.state.update(phase_detail="2FA - enter the code in the browser")
                    last_phase = "2FA - enter the code in the browser"
            except Exception:
                pass

            # Inline credential errors → bail immediately, no point waiting.
            try:
                if page.locator(error_sel).count() > 0:
                    try:
                        msg = page.locator(error_sel).first.inner_text(timeout=2000).strip()
                    except Exception:
                        msg = "unknown error"
                    shot = self._screenshot(page, "login_error")
                    raise RuntimeError(f"login rejected: {msg[:200]} (screenshot: {shot})")
            except RuntimeError:
                raise
            except Exception:
                pass

            time.sleep(0.5)
        else:
            shot = self._screenshot(page, "login_timeout")
            raise RuntimeError(
                f"login did not complete within {login_wait_seconds}s "
                f"(stuck at {page.url}, screenshot: {shot})"
            )

        # "Save your login info?" / "Turn on notifications" interstitials.
        for label in (r"^Not now$", r"^Not Now$"):
            try:
                page.get_by_role("button", name=re.compile(label)).first.click(timeout=3000)
                self._jitter(0.6, 1.4)
            except Exception:
                continue

        # Final verification - we should now be off the login / challenge pages.
        time.sleep(1.5)
        if "/accounts/login" in page.url or "/challenge" in page.url:
            shot = self._screenshot(page, "login_stuck")
            raise RuntimeError(
                f"login appeared to submit but page is still {page.url} (screenshot: {shot})"
            )
        self.state.update(phase_detail="login successful")

    # Collect the user hrefs currently rendered in the dialog.
    _COLLECT_JS = (
        '(d) => Array.from(d.querySelectorAll(\'a[role="link"][href^="/"]\'))'
        '.map(a => a.getAttribute("href"))'
    )
    # Scroll the last rendered row into view - this is what fires IG's
    # infinite-scroll observer. Setting scrollTop=scrollHeight does NOT work on
    # the current modal (the container's scrollTop stays pinned at 0).
    # Align-to-top (default scrollIntoView) forces a real scroll and reveals
    # empty space below, which fires the load. {block:"end"} does nothing once
    # the last row is already at the bottom.
    _SCROLL_JS = (
        '(d) => { const a = d.querySelectorAll(\'a[role="link"][href^="/"]\');'
        ' if (a.length) a[a.length - 1].scrollIntoView(); }'
    )

    def _collect_into(self, dialog, my_username: str, seen: set, order: list) -> None:
        for h in dialog.evaluate(self._COLLECT_JS):
            m = USERNAME_HREF_RE.match(h)
            if not m:
                continue
            u = m.group(1).lower()
            if u in RESERVED or u == my_username.lower() or u in seen:
                continue
            seen.add(u)
            order.append(u)

    def _open_following_modal(self, page, my_username: str):
        """Open the 'following' list dialog and return its locator.

        On your OWN profile IG renders the following count as <a href="#"> (JS
        driven), not /{user}/following/, and the /following/ URL no longer opens
        an overlay - so we click the count link by its accessible name.
        """
        page.goto(f"https://www.instagram.com/{my_username}/", wait_until="domcontentloaded")
        self._jitter(2.0, 3.5)
        try:
            page.get_by_role(
                "link", name=re.compile(r"following", re.I)
            ).first.click(timeout=6000)
        except Exception:
            shot = self._screenshot(page, "scrape_no_following_link")
            raise RuntimeError(
                f"could not find a 'following' link on {page.url} (screenshot: {shot})"
            )
        try:
            page.wait_for_selector('div[role="dialog"]', timeout=15000)
        except PWTimeout:
            shot = self._screenshot(page, "scrape_no_dialog")
            raise RuntimeError(f"following dialog never opened (screenshot: {shot})")
        return page.locator('div[role="dialog"]').last

    def _scrape_following(self, page, my_username: str) -> list[str]:
        self.state.update(status="scraping", phase_detail="opening following modal")
        dialog = self._open_following_modal(page, my_username)
        self._jitter(1.5, 3.0)

        seen: set[str] = set()
        order: list[str] = []
        self._collect_into(dialog, my_username, seen, order)

        prev = -1
        stagnant = 0
        # IG loads ~10-12 rows per fetch. Stop once the unique count stops
        # growing across several consecutive scrolls. We accumulate on every
        # step so rows that virtualize out of the DOM are never lost.
        while stagnant < 8:
            if self._stop_event.is_set():
                return order
            dialog.evaluate(self._SCROLL_JS)
            self._jitter(0.8, 1.8)
            self._collect_into(dialog, my_username, seen, order)
            if len(seen) == prev:
                stagnant += 1
            else:
                stagnant = 0
                prev = len(seen)
                self.state.update(phase_detail=f"loaded {len(seen)} so far...")

        return order

    # --- source scraping (growth: fill the candidate pool with strangers) ---

    def _collect_modal(self, dialog, exclude: str, cap: int, on_collect=None) -> list[str]:
        """Scroll a username-list dialog and accumulate usernames up to `cap`.
        Shared by followers/following and likers scraping. Mirrors the
        accumulate-on-every-scroll approach of _scrape_following so rows that
        virtualize out of the DOM aren't lost. `on_collect(order)` (optional) is
        called with the cumulative list as it grows: it ingests the new names live
        (so the backlog count + stop-checks update mid-scroll, not only when the seed
        finishes) and returns a truthy signal when scrolling should stop."""
        seen: set[str] = set()
        order: list[str] = []
        self._collect_into(dialog, exclude, seen, order)
        if on_collect and on_collect(order):
            return order[:cap]
        prev = -1
        stagnant = 0
        while stagnant < 6 and len(order) < cap:
            if self._stop_event.is_set():
                break
            dialog.evaluate(self._SCROLL_JS)
            self._jitter(0.8, 1.8)
            self._collect_into(dialog, exclude, seen, order)
            if len(seen) == prev:
                stagnant += 1
            else:
                stagnant = 0
                prev = len(seen)
                self.state.update(phase_detail=f"collected {len(seen)} so far...")
                if on_collect and on_collect(order):
                    break
        return order[:cap]

    def _open_list_modal(self, page, profile: str, which: str):
        """Open another account's followers/following dialog. `which` is
        'followers' or 'following'. Returns the dialog locator."""
        page.goto(f"https://www.instagram.com/{profile}/", wait_until="domcontentloaded")
        self._jitter(2.0, 3.5)
        clicked = False
        # The count links on OTHER profiles are real hrefs (/{profile}/followers/).
        try:
            page.locator(f'a[href="/{profile}/{which}/"]').first.click(timeout=6000)
            clicked = True
        except Exception:
            try:
                page.get_by_role("link", name=re.compile(which, re.I)).first.click(timeout=4000)
                clicked = True
            except Exception:
                clicked = False
        if not clicked:
            # Last resort: navigate straight to the list URL (still opens the modal).
            try:
                page.goto(f"https://www.instagram.com/{profile}/{which}/",
                          wait_until="domcontentloaded")
            except Exception:
                pass
        try:
            page.wait_for_selector('div[role="dialog"]', timeout=15000)
        except PWTimeout:
            shot = self._screenshot(page, f"scrape_no_dialog_{profile}")
            raise RuntimeError(
                f"{which} dialog never opened for @{profile} - private or rate-limited "
                f"(shot:{shot})"
            )
        return page.locator('div[role="dialog"]').last

    def _scrape_list(self, page, profile: str, which: str, cap: int = 600,
                     on_collect=None) -> list[str]:
        """Return up to `cap` usernames from a profile's followers/following."""
        self.state.update(status="scraping", phase_detail=f"opening @{profile}'s {which}")
        dialog = self._open_list_modal(page, profile, which)
        self._jitter(1.5, 3.0)
        # Exclude the profile owner's own self-links; our own account and
        # already-followed accounts are dropped later by the done-set.
        return self._collect_modal(dialog, profile, cap, on_collect=on_collect)

    def _scrape_likers(self, page, post_url: str, cap: int = 600,
                       on_collect=None) -> list[str]:
        """Return up to `cap` usernames who liked a post. Returns [] when IG
        hides the likers (common on video/reels and very large accounts)."""
        self.state.update(status="scraping", phase_detail=f"opening likers of {post_url}")
        try:
            page.goto(post_url, wait_until="domcontentloaded")
        except Exception:
            return []
        self._jitter(2.0, 3.5)
        # The likers open from a 'liked_by' link or the 'N others'/'likes' text.
        opened = False
        for sel in ('a[href$="/liked_by/"]', 'a:has-text("others")',
                    'a:has-text("likes")', 'button:has-text("likes")'):
            try:
                loc = page.locator(sel).first
                if loc.is_visible(timeout=1500):
                    loc.click(timeout=4000)
                    opened = True
                    break
            except Exception:
                continue
        if not opened:
            return []
        try:
            page.wait_for_selector('div[role="dialog"]', timeout=8000)
        except PWTimeout:
            return []
        dialog = page.locator('div[role="dialog"]').last
        self._jitter(1.0, 2.0)
        my = (os.getenv("IG_USERNAME") or "").lower()
        return self._collect_modal(dialog, my, cap, on_collect=on_collect)

    def _scrape_commenters(self, page, post_url: str, cap: int = 200,
                           on_collect=None) -> list[str]:
        """Return up to `cap` usernames who COMMENTED on a post (higher intent than
        passive likers). Best-effort: IG's comment markup shifts often, so the
        load-more selectors are tried loosely and a miss just yields fewer names."""
        self.state.update(status="scraping", phase_detail=f"opening comments of {post_url}")
        try:
            page.goto(post_url, wait_until="domcontentloaded")
        except Exception:
            return []
        self._jitter(2.0, 3.5)
        my = (os.getenv("IG_USERNAME") or "").lower()
        scope = page.locator("article").last
        if scope.count() == 0:
            scope = page.locator("main").last

        seen: set[str] = set()
        order: list[str] = []
        self._collect_into(scope, my, seen, order)
        stagnant = 0
        while stagnant < 6 and len(order) < cap:
            if self._stop_event.is_set():
                break
            # Reveal more comments: click a load-more control if present, else
            # scroll the page to trigger IG's lazy loading.
            clicked = False
            for sel in ('svg[aria-label="Load more comments"]',
                        'button:has-text("View more comments")',
                        'button:has-text("Load more")'):
                try:
                    loc = page.locator(sel).first
                    if loc.is_visible(timeout=800):
                        loc.click(timeout=3000)
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked:
                try:
                    page.mouse.wheel(0, 2200)
                except Exception:
                    pass
            self._jitter(0.8, 1.8)
            before = len(seen)
            self._collect_into(scope, my, seen, order)
            if len(seen) == before:
                stagnant += 1
            else:
                stagnant = 0
                self.state.update(phase_detail=f"collected {len(seen)} commenters...")
                if on_collect and on_collect(order):
                    break
        return order[:cap]

    def _scrape_hashtag(self, page, tag: str, cap: int = 200,
                        per_post: int = 60, on_collect=None) -> list[str]:
        """Return up to `cap` usernames sourced from a hashtag - the authors and
        commenters of recent posts under #tag. People posting/commenting on a
        niche hashtag are high-intent targets."""
        tag = (tag or "").strip().lstrip("#").lower()
        if not tag:
            return []
        self.state.update(status="scraping", phase_detail=f"opening #{tag}")
        try:
            page.goto(f"https://www.instagram.com/explore/tags/{tag}/",
                      wait_until="domcontentloaded")
        except Exception:
            return []
        self._jitter(2.5, 4.0)

        # Collect recent post URLs from the grid (scroll until it stops growing).
        post_urls: list[str] = []
        seen_posts: set[str] = set()
        stagnant = 0
        while stagnant < 5 and len(post_urls) < 30:
            if self._stop_event.is_set():
                break
            try:
                hrefs = page.eval_on_selector_all(
                    'a[href*="/p/"], a[href*="/reel/"]',
                    'els => els.map(e => e.getAttribute("href"))')
            except Exception:
                hrefs = []
            before = len(seen_posts)
            for h in hrefs or []:
                if not h or h in seen_posts:
                    continue
                seen_posts.add(h)
                post_urls.append(h if h.startswith("http")
                                 else f"https://www.instagram.com{h}")
            stagnant = stagnant + 1 if len(seen_posts) == before else 0
            try:
                page.mouse.wheel(0, 2500)
            except Exception:
                pass
            self._jitter(0.8, 1.6)

        # Pull authors + commenters from each sampled post.
        out: list[str] = []
        seen_users: set[str] = set()
        my = (os.getenv("IG_USERNAME") or "").lower()
        for url in post_urls:
            if self._stop_event.is_set() or len(out) >= cap:
                break
            try:
                people = self._scrape_commenters(page, url, per_post)
            except Exception:
                people = []
            for u in people:
                if u == my or u in seen_users:
                    continue
                seen_users.add(u)
                out.append(u)
            # Ingest + stop-check at post granularity (each post's commenters are
            # collected fully first - they're small - then handed up in one batch).
            if on_collect and on_collect(out):
                break
        return out[:cap]

    def _scrape_profile_commenters(self, page, profile: str, cap: int = 600,
                                   per_post: int = 60, on_collect=None) -> list[str]:
        """Return the COMMENTERS on an influencer profile's recent posts - the active,
        high-intent users engaging with that account. Same machinery as the hashtag
        source: open the profile grid (_collect_post_links) -> sample recent posts ->
        _scrape_commenters each. Public profiles only."""
        profile = (profile or "").strip().lstrip("@").lower()
        if not profile:
            return []
        self.state.update(status="scraping", phase_detail=f"opening @{profile}'s posts")
        posts = self._collect_post_links(page, f"https://www.instagram.com/{profile}/", 12)
        out: list[str] = []
        seen: set[str] = set()
        my = (os.getenv("IG_USERNAME") or "").lower()
        for url in posts:
            if self._stop_event.is_set() or len(out) >= cap:
                break
            try:
                people = self._scrape_commenters(page, url, per_post)
            except Exception:
                people = []
            for u in people:
                if u == my or u in seen:
                    continue
                seen.add(u)
                out.append(u)
            if on_collect and on_collect(out):   # ingest + stop-check per post
                break
        return out[:cap]

    _LIKERS_COOLDOWN = 3600.0   # secs to pause liker scraping after IG hides a profile's likers
    # Burner navigation circuit-breaker: if this many profile page-loads time out in a
    # sweep with ZERO new candidates, the burner can't reach IG (logged out / checkpointed
    # / rate-limited / network) - stop hammering, diagnose, and back off for the cooldown.
    _BURNER_NAV_FAIL_LIMIT = 4
    _BURNER_UNHEALTHY_COOLDOWN = 1800.0   # 30 min - transient throttle / network
    _BURNER_BLOCKED_COOLDOWN = 21600.0    # 6 h - checkpoint / logged out (needs a manual fix)

    def _burner_accounts(self, scr: dict) -> list:
        """Burner profiles to rotate among (multi-burner failover). Each is
        {user_data_dir, label, ...}. Accounts configured in the dashboard carry a
        label/username (no explicit profile dir) - derive a stable dir from it so the
        runtime matches what scraper_login.py created. Falls back to the single
        scraper.user_data_dir (or the CDP/default empty profile) so existing
        single-burner setups are unchanged."""
        out = []
        for a in (scr.get("accounts") or []):
            if isinstance(a, dict) and (a.get("user_data_dir") or a.get("username") or a.get("label")):
                acct = dict(a)   # keep optional per-burner overrides (proxy/user_agent/…)
                acct["user_data_dir"] = burner_profile_dir(a)
                acct["label"] = a.get("label") or a.get("username") or acct["user_data_dir"]
                out.append(acct)
        if not out:
            udd = scr.get("user_data_dir") or ""
            out.append({"user_data_dir": udd, "label": udd or "default"})
        return out

    def _read_burner_cooldowns(self) -> dict:
        try:
            return json.loads(BURNER_COOLDOWNS.read_text(encoding="utf-8")) or {}
        except Exception:
            return {}

    def _mark_burner_cooldown(self, user_data_dir: str, seconds: float, reason: str = "") -> None:
        """Sideline a burner that can't navigate IG for `seconds`, so the scraper rotates
        to another (or idles if it's the only one). Persisted so a restart doesn't
        immediately re-pick a flagged burner."""
        cd = self._read_burner_cooldowns()
        cd[user_data_dir or ""] = {"until": time.time() + seconds, "reason": reason}
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = BURNER_COOLDOWNS.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(cd), encoding="utf-8")
            os.replace(tmp, BURNER_COOLDOWNS)
        except Exception:
            pass

    def _select_burner(self, scr: dict):
        """Pick the first burner not in cooldown. Returns (account|None, total, soonest_ts).
        account=None means every burner is cooling down (soonest_ts = when one frees up).
        Prefers the current active burner when it's healthy, to avoid needless flapping."""
        accts = self._burner_accounts(scr)
        cd = self._read_burner_cooldowns()
        now = time.time()

        def cooling(a):
            return float((cd.get(a["user_data_dir"]) or {}).get("until", 0)) > now

        available = [a for a in accts if not cooling(a)]
        if available:
            cur = getattr(self, "_active_burner_dir", None)
            for a in available:                       # stick with the current one if it's healthy
                if a["user_data_dir"] == cur:
                    return a, len(accts), 0.0
            return available[0], len(accts), 0.0
        soonest = min((float((cd.get(a["user_data_dir"]) or {}).get("until", 0)) for a in accts),
                      default=now)
        return None, len(accts), soonest

    def _burner_health_probe(self, page) -> str:
        """After repeated navigation timeouts, classify WHY the burner can't scrape so the
        dashboard shows a precise cause instead of silently spinning. Returns a short
        human reason. Checks (in order): can it load instagram.com at all → is it
        checkpointed → is it logged out → else IG is throttling its page loads."""
        try:
            page.goto("https://www.instagram.com/", wait_until="domcontentloaded", timeout=20000)
        except Exception:
            return ("burner can't reach Instagram at all - the Pi's network/proxy is down or "
                    "the burner's IP is blocked. Check connectivity / the burner proxy.")
        if self._checkpoint_detected(page):
            return ("burner is CHECKPOINTED / blocked by Instagram - open the burner Chrome and "
                    "clear the challenge, or swap the burner account.")
        if not self._is_logged_in(page):
            return ("burner is LOGGED OUT - run scraper_login.py to log the burner back in.")
        return ("Instagram is rate-limiting the burner (home loads, but profile pages time out) "
                "- likely a temporary throttle on this account/IP. Backing off to let it cool down.")

    def _scrape_profile_likers(self, page, profile: str, cap: int = 600,
                               per_post: int = 60, on_collect=None) -> list[str]:
        """Return the LIKERS on an influencer profile's recent posts - active users who
        engaged with that account. Mirrors _scrape_profile_commenters but calls
        _scrape_likers per post. IG HIDES liker lists on big/clinic accounts (then
        _scrape_likers returns []); if NOT ONE sampled post exposes its likers we set a
        global cooldown (_likers_blocked_until) and the task builder skips likers until it
        elapses - they're plentiful from other sources, no point wasting passes."""
        profile = (profile or "").strip().lstrip("@").lower()
        if not profile:
            return []
        self.state.update(status="scraping", phase_detail=f"opening @{profile}'s posts (likers)")
        posts = self._collect_post_links(page, f"https://www.instagram.com/{profile}/", 12)
        out: list[str] = []
        seen: set[str] = set()
        my = (os.getenv("IG_USERNAME") or "").lower()
        any_exposed = False
        for url in posts:
            if self._stop_event.is_set() or len(out) >= cap:
                break
            try:
                people = self._scrape_likers(page, url, per_post)
            except Exception:
                people = []
            if people:
                any_exposed = True
            for u in people:
                if u == my or u in seen:
                    continue
                seen.add(u)
                out.append(u)
            if on_collect and on_collect(out):   # ingest + stop-check per post
                break
        # Shield: the profile had posts but IG exposed likers on NONE of them → gated.
        # Pause ALL liker scraping for a while (the task builder checks the timestamp).
        if posts and not any_exposed:
            self._likers_blocked_until = time.time() + self._LIKERS_COOLDOWN
            self.state.emit("log", {"level": "info",
                "msg": f"likers hidden for @{profile} - pausing liker scraping for "
                       f"{int(self._LIKERS_COOLDOWN // 60)}m"})
        return out[:cap]

    def _scrape_candidates(self, page, cfg, pool_read=None, pool_write=None,
                           extra_exclude=None, on_progress=None, status_cb=None,
                           done_set=None, pool_min=None) -> int:
        """Run every configured source, dedup against the exclusion set + the
        existing pool (+ extra_exclude), and append new usernames. Stops early once
        the pending pool reaches candidate_pool_min. Returns the count added.

        Defaults write the bot's follow_candidates pool (core-bot self-scrape). The
        scraper service passes pool_read/pool_write for scraper_todo and
        extra_exclude = already-vetted (result) + rejected usernames, so it never
        re-queues accounts it has already evaluated.

        `done_set` overrides the exclusion set. Default (None) = the FOLLOW done-set
        (never re-follow). The reach harvest passes a SMALLER set (just accounts we
        currently follow + self) so it isn't starved of every account the follow side
        already touched - reach can like a prospect's post even if we've followed them."""
        pool_read = pool_read or read_follow_candidates
        pool_write = pool_write or write_follow_candidates
        targeting = cfg.get("targeting", {}) or {}
        sources = targeting.get("sources", {}) or {}
        per_cap = int(targeting.get("scrape_per_source_cap", 600))
        # Backlog target for THIS call. Default = candidate_pool_min; callers (reach) pass
        # a smaller value to scrape a quick batch and start vetting sooner (interleaving).
        pool_min = int(pool_min) if pool_min is not None else int(targeting.get("candidate_pool_min", 300))
        # Per-SEED round-robin chunk: how many of a seed's raw usernames we drain
        # into the pool before rotating to the next seed. Keeps the todo/pool
        # interleaved across seeds instead of one big same-seed block (the filter
        # appends in todo order, so the eligible pool inherits this mixing). 0/neg
        # = no chunking (drain each seed fully, legacy-ish ordering).
        seed_chunk = int(targeting.get("scrape_per_seed_cap", 150))
        # Yield the Pi the instant the bot starts working (scraper service only - the
        # core bot's own self-scrape runs WHILE acting, so it must never yield to itself).
        _coordinate = (cfg.get("scraper", {}) or {}).get("coordinate_with_bot", True)
        def _yield_to_bot():
            return self._scraper_service and _coordinate and self._bot_is_acting()

        my = (os.getenv("IG_USERNAME") or "").lower()
        done = done_set if done_set is not None else self._follow_done_set(load_whitelist(), my)

        pool = pool_read()
        existing = {c["username"] for c in pool} | (extra_exclude or set())
        added = 0

        def pending_count() -> int:
            return sum(1 for c in pool if c["username"] not in done)

        def ingest(users, source) -> int:
            """Dedup + append new usernames under `source` and flush atomically, so
            the backlog file - and thus the scraper's status heartbeat AND the
            candidate_pool_min stop-check - reflects them IMMEDIATELY (mid-scroll),
            not only when a seed finishes. Returns the count newly added."""
            nonlocal added
            before = added
            for u in users:
                if u in done or u in existing:
                    continue
                existing.add(u)
                pool.append({"username": u, "source": source})
                added += 1
            if added != before:
                pool_write(pool)
                self.state.update(candidate_pool=pending_count())
            if on_progress:
                on_progress()
            return added - before

        def _norm_profile(v):
            return (v or "").strip().lstrip("@").lower()

        def _norm_post(v):
            return (v or "").strip()

        def _norm_tag(v):
            return (v or "").strip().lstrip("#").lower()

        # The hook each scrape helper calls as it scrolls: ingest the new names LIVE
        # (so the backlog count climbs in real time), refresh the throttled status,
        # and tell the helper when to stop scrolling - "stop" once the pool hits
        # candidate_pool_min (so we don't scroll a seed to its 600 cap when only a few
        # more were needed), "next" once this seed has handed over its fair per-round
        # chunk so we rotate to the next source and keep the backlog diverse.
        # chunk_box[0] is the per-round chunk; it's lowered below to a FAIR SHARE of
        # the pool target so one big follower list can't fill the pool alone.
        chunk_box = [seed_chunk if seed_chunk > 0 else 10**9]
        _last_status = [0.0]
        def _collector(label, desc):
            state = {"new": 0}
            def hook(order):
                state["new"] += ingest(order, label)
                now = time.monotonic()
                stop_for_bot = False
                if now - _last_status[0] >= 1.5:
                    _last_status[0] = now
                    if status_cb:
                        status_cb(f"scraping {desc}: backlog {pending_count()}")
                    stop_for_bot = _yield_to_bot()   # checked ≤ every 1.5s mid-scroll
                if self._stop_event.is_set() or stop_for_bot or pending_count() >= pool_min:
                    return "stop"
                if state["new"] >= chunk_box[0]:
                    return "next"
                return None
            return hook, state

        tasks = []   # each: {"label","desc","scrape","exhausted"}
        # Liker scraping is paused after IG hides a profile's likers (see the shield in
        # _scrape_profile_likers) - skip building those tasks until the cooldown elapses.
        likers_ok = time.time() >= getattr(self, "_likers_blocked_until", 0)
        for prof in sources.get("profiles", []) or []:
            prof = _norm_profile(prof)
            if prof:
                # Each influencer profile auto-feeds its post COMMENTERS (active, high-
                # intent) + its FOLLOWERS (backfill) + its post LIKERS (active; often
                # gated on big accounts → shielded). The user adds the handle once.
                tasks.append({"label": f"commenters:@{prof}", "desc": f"@{prof} post commenters",
                              "scrape": (lambda hook, p=prof:
                                         self._scrape_profile_commenters(page, p, per_cap, on_collect=hook)),
                              "exhausted": False})
                tasks.append({"label": f"followers:{prof}", "desc": f"@{prof} followers",
                              "scrape": (lambda hook, p=prof:
                                         self._scrape_list(page, p, "followers", per_cap, on_collect=hook)),
                              "exhausted": False})
                if likers_ok:
                    tasks.append({"label": f"likers:@{prof}", "desc": f"@{prof} post likers",
                                  "scrape": (lambda hook, p=prof:
                                             self._scrape_profile_likers(page, p, per_cap, on_collect=hook)),
                                  "exhausted": False})
        for post in sources.get("post_likers", []) or []:
            post = _norm_post(post)
            if post:
                tasks.append({"label": f"likers:{post}", "desc": f"likers of {post}",
                              "scrape": (lambda hook, u=post:
                                         self._scrape_likers(page, u, per_cap, on_collect=hook)),
                              "exhausted": False})
        for post in sources.get("post_commenters", []) or []:
            post = _norm_post(post)
            if post:
                tasks.append({"label": f"commenters:{post}", "desc": f"commenters of {post}",
                              "scrape": (lambda hook, u=post:
                                         self._scrape_commenters(page, u, per_cap, on_collect=hook)),
                              "exhausted": False})
        for tag in sources.get("hashtags", []) or []:
            tag = _norm_tag(tag)
            if tag:
                tasks.append({"label": f"hashtag:{tag}", "desc": f"#{tag}",
                              "scrape": (lambda hook, t=tag:
                                         self._scrape_hashtag(page, t, per_cap, on_collect=hook)),
                              "exhausted": False})

        # --- SOURCE DIVERSITY: balance the backlog across ALL sources ---
        # The round-robin stops the instant the pool hits candidate_pool_min, so if it
        # always led with the same big follower list it would fill the pool from one or
        # two seeds (the observed bias). So: (1) shuffle the seed order each pass and
        # rotate the lead each round so no fixed seed dominates, and (2) cap each seed's
        # per-round chunk to a FAIR SHARE of the pool target (pool_min / #seeds), so
        # every configured source contributes before the pool fills.
        random.shuffle(tasks)
        n_seeds = max(1, len(tasks))
        fair = max(10, pool_min // n_seeds)
        chunk_box[0] = min(chunk_box[0], fair)

        # Surface the source composition so an empty/short sources list is obvious (e.g.
        # only hashtags - which IG gates - because the profiles got cleared).
        n_prof = sum(1 for p in (sources.get("profiles") or []) if _norm_profile(p))
        n_pl = sum(1 for p in (sources.get("post_likers") or []) if _norm_post(p))
        n_pc = sum(1 for p in (sources.get("post_commenters") or []) if _norm_post(p))
        n_tag = sum(1 for t in (sources.get("hashtags") or []) if _norm_tag(t))
        if not tasks:
            self.state.emit("log", {"level": "warn",
                "msg": "scrape: NO sources configured (Config → Targeting → Sources) - nothing to fill the pool"})
        else:
            self.state.emit("log", {"level": "info",
                "msg": f"scrape sweep: {n_prof} profiles, {n_pc} post-commenters, "
                       f"{n_pl} post-likers, {n_tag} hashtags"})

        # Round-robin across seeds: each visit pulls up to the fair chunk then rotates.
        # A seed that handed a full chunk MAY have more, so it's revisited next round
        # (the helper re-scrolls; dedup skips what we already took); one that gave less
        # is drained and marked exhausted. The lead rotates each round so the seeds cut
        # off when the pool fills get to lead next time - even coverage over refills.
        rr = 0
        yielded = False
        # Burner health tracking for this sweep: count profile-load timeouts and whether
        # anything was actually harvested. Many timeouts + zero new = the burner can't
        # navigate IG → trip the circuit-breaker instead of spinning for an hour.
        nav_timeouts = 0
        sweep_progressed = False
        unhealthy = False
        while not self._stop_event.is_set() and pending_count() < pool_min:
            if _yield_to_bot():
                yielded = True
                break
            progressed = False
            order = tasks[rr % n_seeds:] + tasks[:rr % n_seeds]
            rr += 1
            for task in order:
                if self._stop_event.is_set() or pending_count() >= pool_min:
                    break
                if _yield_to_bot():       # bot started working → stop between seeds
                    yielded = True
                    break
                if task["exhausted"]:
                    continue
                # Announce the source the moment this seed starts, so the scraper
                # status names it during grid-load / first post too (not just on the
                # per-batch ingest tick) - the status always reflects what's scraping.
                if status_cb:
                    status_cb(f"scraping {task['desc']}: backlog {pending_count()}")
                    _last_status[0] = time.monotonic()
                hook, state = _collector(task["label"], task["desc"])
                try:
                    task["scrape"](hook)
                except Exception as e:
                    task["exhausted"] = True
                    self.state.emit("log", {"level": "error",
                                            "msg": f"scrape {task['desc']} failed: {e}"})
                    msg = str(e)
                    if "Timeout" in msg and ("goto" in msg or "navigating to" in msg):
                        nav_timeouts += 1
                        # Many page-loads timing out with nothing harvested = the burner
                        # can't reach IG. Stop the sweep early and diagnose (below).
                        if nav_timeouts >= self._BURNER_NAV_FAIL_LIMIT and not sweep_progressed:
                            unhealthy = True
                            break
                    continue
                if state["new"]:
                    progressed = True
                    sweep_progressed = True
                    self.state.emit("log", {"level": "info",
                                            "msg": f"+{state['new']} from {task['desc']} "
                                                   f"(backlog {pending_count()})"})
                elif not task["exhausted"]:
                    # Surface sources that returned NOTHING (gated / drained / all already
                    # excluded), so it's visible WHICH sources were tried and came up empty -
                    # otherwise only yielding sources log and a source looks untried.
                    self.state.emit("log", {"level": "info",
                                            "msg": f"{task['desc']} → 0 new (drained or gated)"})
                if state["new"] < chunk_box[0]:
                    task["exhausted"] = True   # gave less than a fair chunk - drained
            if unhealthy or yielded or not progressed:
                break

        # Burner can't navigate: diagnose the cause + back off so the scraper stops
        # burning passes (and the dashboard shows WHY the pools aren't filling).
        if unhealthy and page is not None:
            reason = self._burner_health_probe(page)
            shot = self._screenshot(page, "burner_unhealthy")
            # Sideline THIS burner so the scraper rotates to another (or idles if it's the
            # only one). Logged out / checkpointed needs a manual fix → cool down longer.
            cool = (self._BURNER_UNHEALTHY_COOLDOWN
                    if "rate-limit" in reason.lower() or "reach instagram" in reason.lower()
                    else self._BURNER_BLOCKED_COOLDOWN)
            self._mark_burner_cooldown(getattr(self, "_active_burner_dir", "") or "", cool, reason)
            self._write_scraper_status(error=reason)
            self.state.emit("log", {"level": "error",
                "msg": f"burner navigation failing ({nav_timeouts} page-load timeouts, 0 new) - "
                       f"{reason} (shot:{shot})"})
            append_event("burner_unhealthy", reason[:160])

        if added:
            pool_write(pool)
            self.state.update(candidate_pool=pending_count())
        return added

    def _find_following_button(self, page):
        """Locate the profile-header 'Following'/'Requested' button.

        IG renders this control as either a real <button> or a div[role=button],
        and occasionally as a plain element whose text is exactly 'Following'. We
        try role-based exact matches first, then a text-is fallback scoped to the
        header so we never grab a 'Following' count link elsewhere on the page.
        """
        # NOTE: substring (not exact) match - IG's button has a chevron icon
        # whose label is appended to the accessible name, so the name is e.g.
        # "Following ..." not exactly "Following". exact=True returns 0 matches.
        # Scoped to <header>: the profile page also renders a 'Suggested for you'
        # carousel with its own Follow/Following buttons we must never grab.
        hdr = page.locator("header")
        for name in ("Following", "Requested"):
            loc = hdr.get_by_role("button", name=name).first
            try:
                loc.wait_for(state="visible", timeout=5000)
                return loc
            except Exception:
                continue
        # Fallback: an element in the header whose visible text is exactly that.
        for name in ("Following", "Requested"):
            loc = page.locator(f'header :text-is("{name}")').first
            try:
                if loc.is_visible(timeout=1500):
                    return loc
            except Exception:
                continue
        return None

    def _click_unfollow_control(self, scope, timeout_s: float = 10.0) -> bool:
        """Wait for, then click, the 'Unfollow' control inside a dialog scope.

        The menu content loads asynchronously - IG first shows a spinner, then
        renders Mute / Restrict / Unfollow / ... So we must POLL until the item
        appears rather than checking once: Playwright's is_visible() returns the
        current state immediately (it does not wait), so a single check against a
        still-spinning dialog always misses. The control is sometimes a button,
        sometimes a div[role=button], and sometimes a styled element whose text
        is exactly 'Unfollow'."""
        end = time.monotonic() + timeout_s
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return False
            for getter in (
                lambda: scope.get_by_role("button", name="Unfollow").first,
                lambda: scope.locator(':text-is("Unfollow")').first,
                # Pending follow-request to a private account → the confirm is a
                # 'Cancel Follow Request' / 'Withdraw' control, not 'Unfollow'.
                lambda: scope.get_by_role("button", name=re.compile(r"cancel follow request|withdraw", re.I)).first,
                lambda: scope.locator(':text-is("Cancel Follow Request")').first,
            ):
                loc = getter()
                try:
                    if loc.is_visible():
                        loc.click(timeout=4000)
                        return True
                except Exception:
                    continue
            time.sleep(0.4)
        return False

    # Instagram's soft block / rate-limit interstitials. When you exceed the
    # unfollow rate it silently rejects the action and pops one of these - the
    # header button stays 'Following', so the unfollow never lands.
    _RATE_LIMIT_RE = re.compile(
        r"try again later|we restrict certain activity|temporarily blocked|"
        r"action blocked|please wait a few minutes|limit",
        re.I,
    )

    def _rate_limited(self, page) -> bool:
        """True if a 'Try Again Later' / action-block dialog is on screen."""
        try:
            dlg = page.locator('div[role="dialog"]').last
            if dlg.count() == 0:
                return False
            return bool(self._RATE_LIMIT_RE.search(dlg.inner_text(timeout=1000)))
        except Exception:
            return False

    # A checkpoint/challenge/suspension interstitial - the moment to STOP, not
    # retry (retrying during an account review hammers IG and deepens the flag).
    _CHECKPOINT_TEXT_RE = re.compile(
        r"we suspended your account|we disabled your account|"
        r"your account has been (disabled|suspended)|confirm it'?s you|"
        r"help us confirm|verify your account|suspicious (login|activity)|"
        r"unusual activity|we detected unusual",
        re.I,
    )

    def _checkpoint_detected(self, page) -> bool:
        """True if IG is showing a checkpoint/challenge/suspension screen."""
        try:
            url = (page.url or "").lower()
        except Exception:
            url = ""
        if any(k in url for k in ("/challenge", "/accounts/suspended", "/accounts/disabled")):
            return True
        try:
            txt = page.evaluate(
                "() => (document.body ? document.body.innerText : '').slice(0, 4000)")
        except Exception:
            txt = ""
        return bool(self._CHECKPOINT_TEXT_RE.search(txt or ""))

    # IG's "this account is gone" interstitial. Distinguishes a genuinely
    # deleted/disabled/blocked profile (permanent skip) from a page that simply
    # hasn't finished loading (transient - should be retried, not skipped).
    _UNAVAILABLE_RE = re.compile(
        r"page isn'?t available|sorry, this page|user not found|"
        r"page not found|content isn'?t available|account.*removed",
        re.I,
    )

    def _profile_truly_unavailable(self, page) -> bool:
        """True only if IG actually says the profile is gone. Reloads once first so
        a slow initial render isn't mistaken for a dead account."""
        for _ in range(2):
            try:
                body = page.locator("body").inner_text(timeout=1500)
            except Exception:
                body = ""
            if self._UNAVAILABLE_RE.search(body):
                return True
            if page.locator("header").count() > 0:
                return False  # header is here after all - not unavailable
            try:
                page.reload(wait_until="domcontentloaded")
                self._jitter(1.5, 3.0)
            except Exception:
                break
        # Header never showed and IG never said "unavailable" - ambiguous, so treat
        # as NOT unavailable (caller returns transient -> retry, never a hard skip).
        return False

    def _still_following(self, page) -> bool:
        """Fast check: is a Following/Requested button still in the header?

        Used to verify an unfollow took effect. Unlike _find_following_button this
        uses a short per-check timeout, so the verify loop can poll many times
        instead of burning ~8s on a single negative result."""
        for name in ("Following", "Requested"):
            try:
                if page.get_by_role("button", name=name).first.is_visible(timeout=600):
                    return True
            except Exception:
                continue
        return False

    # Outcome codes that mean "something transient went wrong" (async-load race,
    # slow header repaint, etc.) rather than a real dead-end. The coordinator
    # _unfollow retries these in the same run instead of logging them failed on
    # the first miss. Matched by prefix because most carry a ' (shot:...)' suffix.
    _TRANSIENT = (
        "profile_not_ready",
        "following_confirmed",
        "following_click_failed",
        "menu_dialog_timeout",
        "unfollow_item_missing",
        "verify_timeout",
        "post_open_failed",
        "post_more_menu_missing",
        "post_unfollow_item_missing",
        "post_verify_failed",
    )

    # Terminal outcomes: no point retrying or trying another surface. (rate_limited
    # is handled out of band by _process_day's cooldown, so it's terminal here too.)
    _TERMINAL = ("ok", "not_following", "no_button_no_posts", "private_or_missing", "checkpoint")

    def _step(self, username: str, label: str, tone: str = "neutral",
              key: Optional[str] = None) -> None:
        """Record one step of a flow: shows in the status bar AND streams a 'step'
        event so the dashboard lists every step under one expandable row. Rows group
        by `key` (defaults to username - per-user for unfollow/follow; per-POST for
        reach, where one user can be liked across many separate posts)."""
        self.state.update(phase_detail=f"@{username or 'post'}: {label}")
        payload = {"username": username, "label": label, "tone": tone,
                   "intent": self._intent}
        if key:
            payload["key"] = key
        self.state.emit("step", payload)

    def _unfollow(self, page, target: str) -> str:
        """Coordinator: run the profile-page unfollow (header -> post menu), and on
        a transient failure fall back to the own-Following-list modal, then retry
        the whole thing a few times. Terminal outcomes short-circuit immediately."""
        self._intent = "unfollow"
        behavior = load_config().get("behavior", {})
        retries = int(behavior.get("unfollow_retries", 2))
        lo, hi = (behavior.get("unfollow_retry_backoff_seconds") or [3, 8])[:2]
        use_modal = behavior.get("use_following_list_fallback", True)

        result = ""
        modal_tried = False
        for attempt in range(retries + 1):
            if self._stop_event.is_set():
                return "stopped"
            result = self._unfollow_once(page, target)
            if result == "not_following":
                result = self._confirm_not_following(page, target)

            if result in self._TERMINAL or result.startswith("rate_limited"):
                return result

            # Transient (or unverified-missing): fall back to the independent modal
            # surface. Try it at most once - it's heavy (navigates to our own
            # profile) and a second go rarely helps; if it rate-limits, bail out.
            if use_modal and self._me and not modal_tried:
                modal_tried = True
                modal = self._unfollow_via_following_list(page, target)
                if modal == "not_following":
                    modal = self._confirm_not_following(page, target)
                # Terminal outcomes from the modal (ok / not_following / rate_limited)
                # are authoritative - the following list is the ground truth for the
                # relationship. Only its own modal_* failures fall through to a retry.
                if modal in self._TERMINAL or modal.startswith("rate_limited"):
                    return modal

            if attempt < retries:
                self._clear_dialogs(page)
                self._step(target, f"{result} - retry {attempt + 1}/{retries}", "bad")
                self._jitter(lo, hi)

        return result

    def _i_follow_them(self, page, target: str):
        """Definitive check: open the TARGET's FOLLOWERS list and search for OUR own
        username - if we follow them, we're in their followers. Returns True / False
        / None (inconclusive). A different surface from the header/post/own-list, so
        it catches relationships the others misread."""
        me = self._me
        if not me:
            return None
        target = target.lstrip("@").lower()
        try:
            dialog = self._open_list_modal(page, target, "followers")
        except Exception:
            return None
        self._jitter(1.0, 2.0)
        box = None
        for getter in (
            lambda: dialog.get_by_placeholder(re.compile("search", re.I)).first,
            lambda: dialog.locator('input[aria-label*="search" i]').first,
            lambda: dialog.locator('input[type="text"]').first,
        ):
            try:
                cand = getter()
                if cand.is_visible(timeout=1500):
                    box = cand
                    break
            except Exception:
                continue
        if box is None:
            return None
        try:
            box.click(timeout=4000)
            box.fill("")
            box.press_sequentially(me, delay=random.randint(60, 160))
        except Exception:
            return None
        row = dialog.locator(f'a[href="/{me}/"]').first
        no_results = dialog.get_by_text(
            re.compile(r"no results|no accounts found|couldn'?t find", re.I)).first
        ans = None
        end = time.monotonic() + 8
        while time.monotonic() < end:
            if self._stop_event.is_set():
                break
            try:
                if row.is_visible(timeout=300):
                    ans = True
                    break
            except Exception:
                pass
            try:
                if no_results.is_visible(timeout=300):
                    ans = False
                    break
            except Exception:
                pass
            time.sleep(0.3)
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return ans

    def _confirm_not_following(self, page, target: str) -> str:
        """Final safety before a PERMANENT 'not_following' skip: confirm via the
        target's followers list. If we're actually in their followers (we follow
        them), return a transient so it's retried - never wrongly skipped."""
        self._step(target, "double-checking via their followers list")
        res = self._i_follow_them(page, target)
        if res is True:
            self._step(target, "we ARE in their followers - keep trying, not a skip", "bad")
            return "following_confirmed"   # transient -> retried, not skipped
        if res is False:
            self._step(target, "confirmed not following", "neutral")
        return "not_following"

    def _withdraw_request(self, page, target: str, btn) -> str:
        """Handle a 'Requested' (pending follow request to a private account) in the
        unfollow/churn path. Clicking it withdraws the request; some IG variants pop
        a confirm dialog, others withdraw on click. Best-effort + terminal: a pending
        request isn't a real follow, so we never loop the full fallback chain on it.
        Returns 'ok' (recorded as churned/done)."""
        self._step(target, "withdrawing pending follow request")
        try:
            btn.click(timeout=5000)
        except Exception:
            self._step(target, "request button stuck - recording as done", "neutral")
            return "ok"
        # If a confirm dialog appears, click its withdraw/unfollow control.
        try:
            dlg = page.locator('div[role="dialog"]').last
            if dlg.count() and dlg.is_visible(timeout=2000):
                self._click_unfollow_control(dlg, timeout_s=5)
        except Exception:
            pass
        self._jitter(0.6, 1.4)
        if self._rate_limited(page):
            shot = self._screenshot(page, f"fail_ratelimit_{target}")
            return f"rate_limited (shot:{shot})"
        if self._find_following_button(page) is None:
            self._step(target, "follow request withdrawn", "good")
        else:
            self._step(target, "request still pending - recording as done", "neutral")
        return "ok"

    def _clear_dialogs(self, page) -> None:
        """Press Escape to dismiss any stray open dialog so a leftover modal from a
        failed attempt doesn't poison the next target / retry."""
        for _ in range(2):
            try:
                if page.locator('div[role="dialog"]').count() == 0:
                    return
                page.keyboard.press("Escape")
                time.sleep(0.3)
            except Exception:
                return

    def _unfollow_once(self, page, target: str) -> str:
        self._step(target, "opening profile")
        try:
            page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded")
        except Exception:
            self._step(target, "page load failed - retry", "bad")
            return "profile_not_ready"
        if self._checkpoint_detected(page):
            return "checkpoint"

        # Wait for the profile header to actually render - IG loads it AFTER
        # domcontentloaded, so checking immediately (the old behaviour) reported a
        # false 'profile_not_ready' on slow loads. Give it real time.
        try:
            page.wait_for_selector("header", timeout=12000)
        except Exception:
            if self._profile_truly_unavailable(page):
                self._step(target, "profile unavailable", "bad")
                return "private_or_missing"
            self._step(target, "header didn't load - retry", "bad")
            return "profile_not_ready"
        self._jitter(2.0, 4.5)
        self._random_mouse(page)

        btn = self._find_following_button(page)
        if btn is None:
            # No Following/Requested button found. Only treat as 'not following' if an
            # EXACT 'Follow' button is showing IN THE HEADER. Both qualifiers matter:
            #   - exact=True: substring 'Follow' would also match 'Following'.
            #   - header scope: the profile page also shows a 'Suggested for you'
            #     carousel full of OTHER accounts' Follow buttons - matching those
            #     was causing false 'not following' skips on people we do follow.
            hdr = page.locator("header")
            if (hdr.get_by_role("button", name="Follow", exact=True).count() > 0
                    or hdr.get_by_role("button", name="Follow Back", exact=True).count() > 0):
                self._step(target, "not following (Follow button in header)", "neutral")
                return "not_following"
            # Neither a Following nor an exact Follow button rendered - either the
            # known IG action-row bug, or the header was still loading. Fall back to
            # the post '...' menu (and then the following-list modal), which read the
            # real relationship instead of guessing from a half-rendered header.
            self._step(target, "no header button - trying post menu", "neutral")
            return self._unfollow_via_post(page, target)

        # Pending follow-request (private account we requested earlier, not yet
        # accepted) shows a 'Requested' button. Clicking it doesn't open the normal
        # Unfollow menu - it withdraws the request - so the regular path fails and
        # then burns the whole fallback chain + retries. Handle it directly and
        # return terminal so we don't waste minutes on it.
        try:
            btn_label = (btn.inner_text(timeout=1000) or "").strip().lower()
        except Exception:
            btn_label = ""
        if "request" in btn_label:
            return self._withdraw_request(page, target, btn)

        self._step(target, "opening header menu")
        try:
            btn.click(timeout=5000)
        except Exception as e:
            shot = self._screenshot(page, f"fail_btnclick_{target}")
            return f"following_click_failed:{type(e).__name__} (shot:{shot})"

        # Wait for the options dialog that lists Mute / Restrict / Unfollow / ...
        try:
            page.wait_for_selector('div[role="dialog"]', timeout=6000)
        except PWTimeout:
            shot = self._screenshot(page, f"fail_nomenu_{target}")
            return f"menu_dialog_timeout (shot:{shot})"
        self._jitter(0.8, 2.2)

        self._step(target, "clicking Unfollow")
        dialog = page.locator('div[role="dialog"]').last
        if not self._click_unfollow_control(dialog):
            shot = self._screenshot(page, f"fail_noitem_{target}")
            return f"unfollow_item_missing (shot:{shot})"
        self._jitter(0.6, 1.5)

        # Some IG variants pop a SECOND "Unfollow @user?" confirmation dialog.
        # If one is present and still offers an Unfollow control, click it too.
        try:
            confirm = page.locator('div[role="dialog"]').last
            if confirm.count() > 0 and confirm.get_by_role(
                "button", name="Unfollow"
            ).count() > 0:
                self._click_unfollow_control(confirm)
                self._jitter(0.6, 1.5)
        except Exception:
            pass

        # A soft block shows up here: the click is accepted but IG refuses it and
        # pops a 'Try Again Later' dialog. Detect it explicitly so the caller can
        # back off instead of hammering (which extends the block).
        if self._rate_limited(page):
            shot = self._screenshot(page, f"fail_ratelimit_{target}")
            return f"rate_limited (shot:{shot})"

        # Verify success by absence: the Following/Requested button should be gone
        # from the header (it flips to 'Follow'). Poll with a short per-check
        # timeout so a slow header repaint isn't logged as a false failure.
        self._step(target, "verifying via header")
        end = time.monotonic() + 12
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return "ok"
            if self._rate_limited(page):
                shot = self._screenshot(page, f"fail_ratelimit_{target}")
                return f"rate_limited (shot:{shot})"
            if not self._still_following(page):
                return "ok"
            time.sleep(0.4)

        # Last resort: the click may have landed but the SPA header didn't repaint.
        # Reload the profile once and re-check before calling it a failure.
        try:
            page.reload(wait_until="domcontentloaded")
            self._jitter(1.5, 3.0)
            if not self._still_following(page):
                return "ok"
        except Exception:
            pass

        shot = self._screenshot(page, f"fail_verify_{target}")
        return f"verify_timeout (shot:{shot})"

    def _open_post_more_menu(self, page) -> bool:
        """Open an opened post's '...' (More options) menu. Returns True on click."""
        selectors = (
            'svg[aria-label="More options"]',
            'button[aria-label="More options"]',
            '[aria-label="More options"]',
        )
        for sel in selectors:
            loc = page.locator(sel).last
            try:
                if loc.is_visible():
                    loc.click(timeout=3000)
                    return True
            except Exception:
                continue
        try:
            btn = page.get_by_role("button", name="More options").last
            if btn.is_visible():
                btn.click(timeout=3000)
                return True
        except Exception:
            pass
        return False

    def _post_menu_still_following(self, page) -> bool:
        """After unfollowing via a post, reopen its '...' menu and report whether
        it STILL offers 'Unfollow' (i.e. the action didn't land). Best-effort -
        ambiguity is treated as success by the caller, since the header button is
        broken for these profiles and this is the only signal we have."""
        if not self._open_post_more_menu(page):
            return False
        menu = page.locator('div[role="dialog"]').last
        still = False
        try:
            still = (
                menu.get_by_role("button", name="Unfollow").count() > 0
                or menu.locator(':text-is("Unfollow")').count() > 0
            )
        except Exception:
            still = False
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return still

    def _unfollow_via_post(self, page, target: str) -> str:
        """Fallback for profiles whose header Following button never renders (an
        IG web bug). Open one of the user's posts and unfollow from the post's
        '...' (More options) menu, which still works - this mirrors the manual
        workaround. Accounts with no posts can't use this path."""
        # Read the first post's URL and navigate straight to it instead of
        # clicking the thumbnail - clicking the grid is flaky because IG's hover
        # overlay (likes/comments) intercepts the click, which is what caused
        # 'post_open_failed'. A direct goto always lands on the post page.
        post = page.locator('a[href*="/p/"], a[href*="/reel/"]').first
        try:
            post.wait_for(state="attached", timeout=5000)
            href = post.get_attribute("href")
        except Exception:
            href = None
        if not href:
            # No header button AND no posts to open - nothing we can do here.
            shot = self._screenshot(page, f"fail_noposts_{target}")
            return f"no_button_no_posts (shot:{shot})"

        post_url = href if href.startswith("http") else f"https://www.instagram.com{href}"
        self._step(target, "opening a post")
        try:
            page.goto(post_url, wait_until="domcontentloaded")
        except Exception:
            shot = self._screenshot(page, f"fail_postopen_{target}")
            return f"post_open_failed (shot:{shot})"
        self._jitter(1.5, 3.0)

        self._step(target, "opening post '...' menu")
        if not self._open_post_more_menu(page):
            shot = self._screenshot(page, f"fail_postmenu_{target}")
            return f"post_more_menu_missing (shot:{shot})"
        self._jitter(0.5, 1.2)

        # The options menu now lists Unfollow (poll for it - it loads async too).
        self._step(target, "clicking Unfollow in post menu")
        menu = page.locator('div[role="dialog"]').last
        if not self._click_unfollow_control(menu):
            shot = self._screenshot(page, f"fail_postnoitem_{target}")
            return f"post_unfollow_item_missing (shot:{shot})"
        self._jitter(0.8, 1.5)

        # Clicking 'Unfollow' in the post menu opens a confirmation dialog
        # ('Unfollow @user?') that MUST be clicked to finish the action. Poll for
        # it and click it - a one-shot presence check races the dialog's open
        # animation and, when it loses, silently leaves the account followed
        # (that was the cause of post_verify_failed). If no confirmation appears
        # (some variants unfollow on the first click), the poll just no-ops.
        self._click_unfollow_control(page.locator('div[role="dialog"]').last, timeout_s=6)
        self._jitter(0.6, 1.2)

        if self._rate_limited(page):
            shot = self._screenshot(page, f"fail_ratelimit_{target}")
            return f"rate_limited (shot:{shot})"

        # Verify by reopening the '...' menu. The relationship state can lag a
        # beat after the click, so poll a few times; only a persistent 'Unfollow'
        # option means it really failed.
        self._step(target, "verifying via post menu")
        for _ in range(3):
            if not self._post_menu_still_following(page):
                return "ok"
            time.sleep(1.0)
        shot = self._screenshot(page, f"fail_postverify_{target}")
        return f"post_verify_failed (shot:{shot})"

    def _unfollow_via_following_list(self, page, target: str) -> str:
        """Last-resort surface: open OUR OWN following list, search the target, and
        click the row's 'Following' button. This DOM path still works on profiles
        whose own page is bugged (broken header button / flaky post menu), which is
        what produces almost all of the post_* failures.

        Doubles as a cross-check: if the target isn't in our following list at all,
        we're already not following them (e.g. a 'post_verify_failed' that actually
        landed) -> report success rather than a spurious failure."""
        target = target.lstrip("@").lower()
        self._step(target, "trying your following-list modal")
        try:
            dialog = self._open_following_modal(page, self._me)
        except Exception:
            shot = self._screenshot(page, f"fail_modalopen_{target}")
            return f"modal_open_failed (shot:{shot})"
        self._jitter(1.0, 2.0)

        # Type the target into the modal's search box (human-paced). Try a few
        # selectors - IG's input markup shifts and a missed box is the main reason
        # this path used to silently fail and mark followed users 'not_following'.
        box = None
        for getter in (
            lambda: dialog.get_by_placeholder(re.compile("search", re.I)).first,
            lambda: dialog.locator('input[aria-label*="search" i]').first,
            lambda: dialog.locator('input[type="text"]').first,
        ):
            try:
                cand = getter()
                if cand.is_visible(timeout=1500):
                    box = cand
                    break
            except Exception:
                continue
        if box is None:
            shot = self._screenshot(page, f"fail_modalnobox_{target}")
            return f"modal_search_failed (shot:{shot})"
        try:
            box.click(timeout=4000)
            box.fill("")
            box.press_sequentially(target, delay=random.randint(60, 160))
        except Exception:
            shot = self._screenshot(page, f"fail_modalsearch_{target}")
            return f"modal_search_failed (shot:{shot})"

        # Poll for ONE of three outcomes (the filter is async + debounced):
        #   - the target's row appears        -> we follow them, unfollow below
        #   - IG shows a 'no results' message -> we genuinely don't follow them
        #   - neither within the window       -> INCONCLUSIVE (search likely didn't
        #     take) -> return transient so it's RETRIED, never a permanent skip.
        row_link = dialog.locator(f'a[href="/{target}/"]').first
        no_results = dialog.get_by_text(
            re.compile(r"no results|no accounts found|couldn'?t find", re.I)).first
        end = time.monotonic() + 9
        found = absent = False
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return "stopped"
            try:
                if row_link.is_visible(timeout=300):
                    found = True
                    break
            except Exception:
                pass
            try:
                if no_results.is_visible(timeout=300):
                    absent = True
                    break
            except Exception:
                pass
            time.sleep(0.3)

        if absent:
            self._step(target, "not in your following list", "neutral")
            return "not_following"            # IG confirmed no match - we don't follow them
        if not found:
            shot = self._screenshot(page, f"fail_modalinconclusive_{target}")
            return f"modal_inconclusive (shot:{shot})"   # transient -> retried, NOT skipped

        # Click the 'Following' button WITHIN the target's row (not the first one in
        # the dialog, which could be a different account if the list isn't filtered).
        self._step(target, "found in list - clicking Unfollow")
        row = row_link.locator("xpath=ancestor::div[.//button or .//*[@role='button']][1]")
        try:
            btn = row.get_by_role(
                "button", name=re.compile(r"^(Following|Requested)", re.I)).first
            btn.wait_for(state="visible", timeout=4000)
            btn.click(timeout=4000)
        except Exception:
            shot = self._screenshot(page, f"fail_modalbtn_{target}")
            return f"modal_button_missing (shot:{shot})"
        self._jitter(0.6, 1.5)

        # Confirmation dialog ('Unfollow @user?') -> reuse the polling clicker.
        self._click_unfollow_control(page.locator('div[role="dialog"]').last, timeout_s=6)
        self._jitter(0.6, 1.2)

        if self._rate_limited(page):
            shot = self._screenshot(page, f"fail_ratelimit_{target}")
            return f"rate_limited (shot:{shot})"

        # Verify by absence within the SAME row: its 'Following' button should be
        # gone (flips to 'Follow') or the row drops out of the filtered list.
        end = time.monotonic() + 8
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return "ok"
            try:
                still = row.get_by_role(
                    "button", name=re.compile(r"^(Following|Requested)", re.I)
                ).first.is_visible(timeout=500)
            except Exception:
                still = False
            if not still:
                return "ok"
            time.sleep(0.4)

        shot = self._screenshot(page, f"fail_modalverify_{target}")
        return f"modal_verify_failed (shot:{shot})"

    # --- follow side ---

    # Read the three profile-header stats. IG shows the numbers ABBREVIATED in visible
    # text ("15.2K") but keeps the EXACT value in a child span's `title` attribute
    # (e.g. <span title="15,262">). On the OWN profile the count is NOT an <a href>
    # (clicking opens a modal, no URL), so we can't target a link - instead we find
    # every numeric `title` node and classify it by the label text of a nearby ancestor
    # (… followers / following / posts). Abbreviated text + a regex are the fallbacks.
    _COUNTS_JS = """
    () => {
      const root = document.body || document.documentElement;
      const out = { posts: null, followers: null, following: null, _items: [] };
      const numTitle = (s) => /^[\\d.,]+\\s*[kmb]?$/i.test((s || "").trim());
      // Which count this title belongs to, from the label text of a nearby ancestor.
      // Climb a few levels but bail once the text is too broad (whole-page).
      const labelOf = (n) => {
        let el = n.parentElement;
        for (let i = 0; i < 4 && el; i++, el = el.parentElement) {
          const t = (el.textContent || "").toLowerCase();
          if (t.length > 80) break;
          if (t.includes("follower"))  return "followers";
          if (t.includes("following")) return "following";
          if (t.includes("post"))      return "posts";
        }
        return null;
      };
      // 1) exact counts from numeric title tooltips, classified by nearby label. Searched
      //    document-wide (the header counts come first in DOM order, so they win), since
      //    the profile header isn't always inside a <header> element.
      Array.from(root.querySelectorAll("[title]")).forEach(n => {
        const title = (n.getAttribute("title") || "").trim();
        if (!numTitle(title)) return;
        const key = labelOf(n);
        if (key && !out[key]) out[key] = { title };
      });
      // 2) abbreviated text fallback: short "<count> <label>" wrappers
      Array.from(root.querySelectorAll("a, span, li, button")).forEach(el => {
        const txt = (el.innerText || "").trim();
        if (txt && txt.length <= 40 && /\\d/.test(txt) && /(post|follower|following)/i.test(txt))
          out._items.push(txt);
      });
      return out;
    }
    """

    def _read_profile_counts(self, page) -> dict:
        """Return {'posts', 'followers', 'following'} as ints (or None each).

        Prefers the EXACT count from the title tooltip (e.g. '15,262') over the
        abbreviated visible text ('15.2K'). Falls back to the abbreviated wrapper text,
        then a regex over the header/main text."""
        counts = {"posts": None, "followers": None, "following": None}

        # Poll briefly: IG renders the header counts (and the exact-value title tooltip)
        # a moment AFTER domcontentloaded, so a single immediate read can miss it and
        # fall back to the abbreviated number. Re-evaluate until the exact followers
        # title appears (or ~6s), so we capture '15,262' not '15.2K'.
        data = {}
        for _ in range(6):
            try:
                data = page.evaluate(self._COUNTS_JS) or {}
            except Exception:
                data = {}
            f = data.get("followers")
            if isinstance(f, dict) and f.get("title"):
                break
            if self._stop_event.is_set():
                break
            self._interruptible_sleep(1.0)

        # Exact value from the title tooltips.
        for key in ("posts", "followers", "following"):
            d = data.get(key)
            if isinstance(d, dict) and d.get("title"):
                counts[key] = parse_count(d["title"])

        # Abbreviated text fallback for anything the titles missed.
        for txt in data.get("_items") or []:
            low = txt.lower()
            if "post" in low:
                key = "posts"
            elif "follower" in low:
                key = "followers"
            elif "following" in low:
                key = "following"
            else:
                continue
            if counts[key] is None:
                counts[key] = parse_count(txt)

        # Strategy 2: regex over the header text for anything still missing.
        if any(v is None for v in counts.values()):
            try:
                text = page.evaluate(
                    "() => (document.querySelector('header') || document.querySelector('main')"
                    " || document.body).innerText || ''"
                )
            except Exception:
                text = ""
            for key, kw in (("posts", "posts?"), ("followers", "followers?"),
                            ("following", "following")):
                if counts[key] is None:
                    m = re.search(r"([\d.,]+\s*[kmb]?)\s*" + kw, text, re.I)
                    if m:
                        counts[key] = parse_count(m.group(1))
        return counts

    def _refresh_account_counts(self, page) -> None:
        """Fetch our OWN follower/following counts from our profile and publish
        them to the status bar. Used to seed the numbers on start and to re-sync
        periodically (correcting any drift from the per-action adjustments)."""
        if not self._me:
            return
        try:
            page.goto(f"https://www.instagram.com/{self._me}/", wait_until="domcontentloaded")
            self._jitter(1.5, 3.0)
            counts = self._read_profile_counts(page)
        except Exception as e:
            self.state.emit("log", {"level": "warn",
                "msg": f"account count refresh failed (could not open @{self._me}): {e}"})
            return
        # Visibility: log what we actually read, so it's clear whether the EXACT count
        # was extracted (e.g. 15,262) or it fell back to the abbreviated value (15,200).
        self.state.emit("log", {"level": "info",
            "msg": f"account counts read - followers={counts.get('followers')} "
                   f"following={counts.get('following')} posts={counts.get('posts')}"})
        fields = {}
        if counts.get("followers") is not None:
            fields["account_followers"] = counts["followers"]
        if counts.get("following") is not None:
            fields["account_following"] = counts["following"]
        if fields:
            self.state.update(**fields)
            snap = self.state.snapshot()
            f, g = snap.get("account_followers"), snap.get("account_following")
            write_account_stats(f, g)
            # Append to the growth time-series (throttled to ~30 min so it stays small).
            if f is not None and g is not None and \
                    time.time() - getattr(self, "_last_history_write", 0) > 1800:
                self._last_history_write = time.time()
                append_log(ACCOUNT_HISTORY, f"{time.strftime('%Y-%m-%d %H:%M:%S')}\t{f}\t{g}")
        else:
            # Both parses returned nothing - the bar would silently freeze. Surface
            # it so a DOM/locale/login problem on the Pi is visible, not invisible.
            self.state.emit("log", {"level": "warn",
                "msg": f"account count refresh: could not read follower/following from @{self._me} "
                       "(not logged in, or Instagram changed the profile header)"})
        self._actions_since_resync = 0

    def fetch_account_now(self) -> bool:
        """One-shot: connect, read our own counts, persist, disconnect - so the
        status bar can refresh while the bot is idle (dashboard open). No-op if a
        run/scrape is active (those update the counts on their own)."""
        if self.is_running or getattr(self, "_account_fetching", False):
            return False
        self._account_fetching = True
        try:
            load_dotenv(ROOT / ".env", override=True)
            self._me = (os.getenv("IG_USERNAME") or "").lstrip("@").lower()
            if not self._me:
                return False
            cfg = load_config()
            with sync_playwright() as p:
                browser, context, page, using_cdp, using_persistent = self._connect(p, cfg["browser"])
                try:
                    if self._is_logged_in(page):
                        self._refresh_account_counts(page)
                finally:
                    try:
                        if using_persistent:
                            context.close()
                        elif not using_cdp:
                            browser.close()
                    except Exception:
                        pass
            return True
        except Exception:
            return False
        finally:
            self._account_fetching = False

    # ---- standalone scraper+filter service (separate process / 2nd Chrome) ----

    def _pool_ready(self, cfg) -> int:
        """Count of eligible candidates ready for the bot to follow (result list
        minus the done-set). Drives the scraper's high/low-water gating."""
        try:
            done = self._follow_done_set(load_whitelist(), self._me)
            return sum(1 for c in read_follow_candidates() if c["username"] not in done)
        except Exception:
            return 0

    def _reach_pool_ready(self) -> int:
        """Count of harvested post links not yet visited/liked (what the bot can
        still like). reach_liked.log records every OPENED post, so it doubles as the
        consumed-set - the bot never has to rewrite the pool file."""
        try:
            liked = self._reach_liked_set()
            return sum(1 for e in read_reach_pool() if e["url"] not in liked)
        except Exception:
            return 0

    def _reach_likes_left(self, cfg) -> int:
        """The reach pool's LOW-water mark = reach likes still allowed today. Uses the
        same per-day 'likes' cap the bot enforces (seeded from limits.likes_per_day),
        so once the day's likes are spent an empty reach pool no longer blocks the bot."""
        try:
            L = self._ensure_ledger(cfg)
            limits = cfg.get("limits", {}) or {}
            cap = int(L.get("caps", {}).get("likes") or limits.get("likes_per_day", 100) or 0)
            return max(0, cap - int(L.get("likes", 0)))
        except Exception:
            return 0

    def _reach_tags(self, cfg) -> list:
        """The niche hashtags reach harvests from (reach_hashtags, else the shared
        follow.sources.hashtags). Empty = reach can't be filled, so it never gates."""
        eng = cfg.get("engagement", {}) or {}
        sources = (cfg.get("targeting", {}) or {}).get("sources", {}) or {}
        return [t.strip().lstrip("#").lower()
                for t in (eng.get("reach_hashtags") or sources.get("hashtags") or [])
                if t and t.strip()]

    def _reach_harvest_on(self, cfg) -> bool:
        """True when the burner should fill the reach pool and the main account should
        consume-only. Needs reach enabled + external harvest, plus something to harvest
        FROM: in 'prospects' mode that's any scrape source (profiles/hashtags/posts);
        in 'hashtags' mode it's at least one reach hashtag."""
        eng = cfg.get("engagement", {}) or {}
        if not (eng.get("reach_enabled", False) and eng.get("reach_external_harvest", True)):
            return False
        if (eng.get("reach_source") or "hashtags").lower() == "prospects":
            srcs = (cfg.get("targeting", {}) or {}).get("sources", {}) or {}
            return bool((srcs.get("profiles") or []) or (srcs.get("hashtags") or [])
                        or (srcs.get("post_commenters") or []) or (srcs.get("post_likers") or []))
        return bool(self._reach_tags(cfg))

    _REACH_WARM_STALL = 300.0   # secs to wait for reach to keep filling before proceeding anyway

    def _pools_warm(self, cfg, mode):
        """Unified pool gate: (warm, detail). The bot stays fully idle (browser closed)
        until EVERY pool it will consume holds at least its low-water, so the scraper
        fills them first (true mutual exclusion). A pool whose feature is off - or when
        the scraper toggle itself is off (nothing would fill it) - is auto-satisfied so
        the bot never deadlocks waiting on a pool nothing feeds.
          follow low-water: a full daily cap of eligible candidates (follow/churn only)
          reach  low-water: today's remaining like budget (reach enabled only)"""
        scr = cfg.get("scraper", {}) or {}
        if not scr.get("enabled", False):
            return True, ""   # scraper off → don't wait on pools it isn't filling
        warm, parts = True, []
        if mode in ("follow", "marketing"):
            # Low-water = only what the bot still needs TODAY (remaining follow room),
            # NOT a full daily cap. So once it's mid-day the bot keeps consuming a
            # shrinking pool as long as there's enough left to finish today's follows,
            # instead of yielding to the scraper (which must never run while the bot is
            # working). It only waits when the pool can't even cover the remaining room.
            fc = int((cfg.get("limits", {}) or {}).get("follows_per_day", 30)) or 30
            need = min(fc, max(1, self._day_room("follows", cfg)))
            ready = self._pool_ready(cfg)
            parts.append(f"follow {ready}/{need}")
            # Only WAIT on the follow pool if the scraper can actually fill it. With no
            # follow sources configured, blocking here would hang the bot forever waiting
            # on a pool nothing feeds - so proceed and let the normal "pool empty" path
            # surface it instead (warn once in a while so the cause is visible).
            srcs = (cfg.get("targeting", {}) or {}).get("sources", {}) or {}
            has_sources = any(srcs.get(k) for k in
                              ("profiles", "hashtags", "post_likers", "post_commenters"))
            if ready < need:
                if has_sources:
                    warm = False
                elif time.time() - getattr(self, "_warned_no_sources", 0) > 600:
                    self._warned_no_sources = time.time()
                    self.state.emit("log", {"level": "warn",
                        "msg": "scraper is on but no follow sources are configured (Config → "
                               "Targeting → Sources) - the follow pool can't be filled"})
        if self._reach_harvest_on(cfg):
            need = self._reach_likes_left(cfg)
            ready = self._reach_pool_ready()
            parts.append(f"reach {ready}/{need}")
            # Wait for reach to reach low-water like follow does - but never DEADLOCK.
            # Keep waiting while the pool is still making progress (the scraper is filling
            # it); only give up if it's STALLED (no growth) for a while, which means the
            # scraper genuinely can't fill it (too few public/posting prospects, or it's
            # down). Tracking progress (not just "scraper idle right now") avoids both the
            # old premature-proceed on a transient idle AND a permanent hang.
            if ready >= need:
                self._reach_warm = None                 # satisfied → reset the stall clock
            else:
                now = time.monotonic()
                w = getattr(self, "_reach_warm", None)
                if w is None or ready != w[1]:
                    w = (now, ready)                     # first sight / pool changed → (re)start clock
                    self._reach_warm = w
                if now - w[0] <= self._REACH_WARM_STALL:
                    warm = False                         # still filling → keep waiting
                # else: stalled past _REACH_WARM_STALL → proceed (don't deadlock)
        return warm, ", ".join(parts)

    def _scraper_phase(self) -> str:
        """The scraper service's current phase (from its status file) if it's alive,
        for surfacing progress on the bot's warm-up line. '' if stale/absent."""
        try:
            d = json.loads(SCRAPER_STATUS.read_text(encoding="utf-8"))
            if time.time() - float(d.get("ts", 0)) < 180:
                return str(d.get("phase") or "")
        except Exception:
            pass
        return ""

    def _await_scraper_idle(self, max_wait: float = 10.0) -> None:
        """Called by the BOT right after it announces it's acting: block briefly until
        the scraper has actually released the Pi (closed its Chrome), so the two never
        hold a browser at once. The scraper bails its current pass within ~1.5s of
        seeing the bot act and reports an idle/paused phase; we poll for that. Bounded
        so a stuck/absent scraper can never deadlock the bot."""
        if not scraper_running():
            return
        end = time.monotonic() + max_wait
        while time.monotonic() < end and not self._stop_event.is_set():
            if not self._scraper_active():
                return   # scraper released (or status stale/absent) → safe to proceed
            self.state.update(phase_detail="waiting for the scraper to free the Pi…")
            self._interruptible_sleep(0.5)

    _SCRAPER_IDLE_MARKS = ("idle", "pools at target", "paused", "stopped", "disabled", "starting")

    def _scraper_active(self) -> bool:
        """True when the burner scraper is actively working right now (scraping / vetting
        / harvesting), per its fresh status phase. Lets the bot surface the whole system
        as 'scraping' while it's idle, rather than 'sleeping' - one system, not two."""
        if not scraper_running():
            return False
        ph = self._scraper_phase().lower()
        return bool(ph) and not any(ph.startswith(m) for m in self._SCRAPER_IDLE_MARKS)

    def _sleep_reflecting_scraper(self, seconds: float, sleeping_detail: str) -> None:
        """Sleep in short ticks, but show the system as 'scraping' (with the live burner
        phase) whenever the scraper is working, else 'sleeping' with `sleeping_detail`.
        Leaves next_action_at as the caller set it. Makes a bot-idle/scraper-busy stretch
        read as one system doing work."""
        end = time.monotonic() + seconds
        while not self._stop_event.is_set():
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            if self._scraper_active():
                # Keep the WHY visible even while the burner works, so a short re-check
                # or an overnight cap wait never reads as a bare, unexplained countdown.
                phase = "scraping — " + (self._scraper_phase() or "filling pools")
                if sleeping_detail:
                    phase += f" · {sleeping_detail}"
                self.state.update(status="scraping", current_target=None, phase_detail=phase)
            else:
                self.state.update(status="sleeping", current_target=None,
                                  phase_detail=sleeping_detail)
            self._interruptible_sleep(min(5.0, remaining))

    def _set_working_pool(self, p: str) -> None:
        """Set which pipeline the scraper is working ('follow' / 'reach' / ''), used for
        BOTH the dashboard pipeline border (status file) and the per-line log badges
        (the StateManager stamps each scraper log with this)."""
        self._working_pool = p
        try:
            self.state._log_pool = p
        except Exception:
            pass

    def _write_scraper_status(self, error: Optional[str] = None,
                              phase: str = "") -> None:
        """Lightweight heartbeat for the dashboard's Scraper card: timestamp + current
        phase + any error. Atomic write so the server never reads a half-written file.
        Counts are NOT computed here - the server derives live pool counts from disk
        itself (TTL-cached), so recomputing them on every vetted profile (a done-set +
        several full log reads) was pure wasted Pi work in the hottest loop."""
        status = {"ts": time.time(), "phase": phase, "error": error,
                  "pool": getattr(self, "_working_pool", "")}   # which pipeline is working now
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = SCRAPER_STATUS.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(status), encoding="utf-8")
            os.replace(tmp, SCRAPER_STATUS)
        except Exception:
            pass

    def _filter_one(self, page, target: str, filters: dict):
        """Browser-navigate a profile and apply the ACCOUNT-AGNOSTIC filters
        (posts / followers / following ranges + private). Returns
        (reject_reason_or_None, meta) where meta carries the vetted profile data
        (followers/posts/following/private/vetted_at) so the bot/dashboard can use
        it without re-reading. Relationship filters are NOT applied here (the burner
        can't judge them; the core bot handles those at follow time)."""
        try:
            page.goto(f"https://www.instagram.com/{target}/",
                      wait_until="domcontentloaded", timeout=15000)
            page.wait_for_selector("header", timeout=9000)
        except Exception:
            return "unavailable", {}
        self._jitter(0.2, 0.5)   # tiny settle (read-only burner - vetting can be brisk)
        private = self._is_private(page)
        counts = self._read_profile_counts(page)
        meta = {
            "followers": counts.get("followers"),
            "posts": counts.get("posts"),
            "following": counts.get("following"),
            "private": private,
            "vetted_at": int(time.time()),
        }
        if filters.get("skip_private", True) and private:
            return "private", meta
        return self._passes_filters(counts, filters), meta   # 'no_posts'/'filtered'/None

    def _filter_pool(self, page, cfg, target: int = None) -> None:
        """Vet the scraper_todo backlog. Each account: pass → append to the ELIGIBLE
        result list (follow_candidates, what the bot consumes); fail → rejected
        ledger. Either way it leaves the todo. Persists both lists atomically as it
        goes, so the bot reads a clean eligible-only list with no comparison. Stops
        once the eligible pool reaches `target` (default the follow high-water mark)
        so it can hand the burner over to reach harvesting instead of over-filling."""
        filters = (cfg.get("targeting", {}) or {}).get("filters", {}) or {}
        scr = cfg.get("scraper", {}) or {}
        min_d = float(scr.get("filter_delay_min", 1.5))
        max_d = float(scr.get("filter_delay_max", 4))
        long_every = int(scr.get("long_break_every", 40))
        long_min = float(scr.get("long_break_min", 60))
        long_max = float(scr.get("long_break_max", 180))
        rejected_log = _log_path("filter_rejected_log", "data/filter_rejected.log")
        checked_log = _log_path("filter_checked_log", "data/filter_checked.log")

        done = self._follow_done_set(load_whitelist(), self._me)
        todo = read_scraper_todo()
        result = read_follow_candidates()
        result_set = {c["username"] for c in result}
        resolved: set[str] = set()   # vetted this pass (kept or rejected) → leave todo

        def _persist():
            # result = eligible & not already-followed; todo = the unresolved remainder.
            write_follow_candidates([c for c in result if c["username"] not in done])
            write_scraper_todo([c for c in todo
                                if c["username"] not in resolved
                                and c["username"] not in done
                                and c["username"] not in result_set])

        pending = [c for c in todo if c["username"] not in done
                   and c["username"] not in result_set]
        self.state.emit("log", {"level": "info", "msg": f"vetting {len(pending)} candidate(s)"})
        self._write_scraper_status(phase=f"vetting {len(pending)}")
        coordinate = scr.get("coordinate_with_bot", True)
        if target is None:
            follows_cap = int((cfg.get("limits", {}) or {}).get("follows_per_day", 30))
            target = max(1, follows_cap * int(scr.get("follow_pool_mult", 5)))
        # Yield the Pi the instant the bot starts working - checked up front (before any
        # profile) AND every few profiles - so vetting never overlaps an active bot.
        if coordinate and self._bot_is_acting():
            self._write_scraper_status(phase="paused - bot became active")
            return
        processed = 0
        for c in todo:
            if self._stop_event.is_set():
                break
            # Yield the Pi the instant the bot starts working, and stop once the
            # ready pool reaches the target (checked every few profiles), so reach
            # harvesting gets a turn instead of vetting all the way to high-water.
            if processed and processed % 3 == 0:
                if coordinate and self._bot_is_acting():
                    self._write_scraper_status(phase="paused - bot became active")
                    break
                if self._pool_ready(cfg) >= target:
                    break
            u = c["username"]
            if u in resolved or u in done or u in result_set:
                continue
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            try:
                reason, meta = self._filter_one(page, u, filters)
            except Exception as e:
                self.state.emit("log", {"level": "error", "msg": f"filter @{u} failed: {e}"})
                continue   # not resolved → stays in todo, retried next pass
            if reason is None:
                result.append({"username": u, "source": c.get("source", ""), **meta})
                result_set.add(u)
                append_log(checked_log, f"{ts}\t{u}")   # vetted-KEPT series (analytics)
            else:
                append_log(rejected_log, f"{ts}\t{u}\t{reason}")
            resolved.add(u)
            processed += 1
            self.state.emit("log", {"level": "info",
                "msg": f"[{processed}/{len(pending)}] @{u} → "
                       f"{'KEEP' if reason is None else 'reject (' + reason + ')'}"})
            if processed % 5 == 0:
                _persist()
            self._write_scraper_status(phase=f"vetted {processed}/{len(pending)}")
            self._jitter(min_d, max_d)
            if long_every and processed % long_every == 0:
                self._interruptible_sleep(random.uniform(long_min, long_max))

        _persist()
        self._write_scraper_status(phase="idle")

    def run_scraper(self) -> None:
        """Entry point for the standalone scraper service (separate process, its
        own Chrome / burner account). Scrapes sources, browser-filters candidates,
        and atomically publishes the cleaned pool for the core bot to consume.
        Never follows/unfollows. Browser navigation only - no IG API."""
        self._scraper_service = True   # arms the in-pass "yield to the bot" checks
        try:
            load_dotenv(ROOT / ".env", override=True)
            # The done-set excludes accounts the MAIN account already follows/handled
            # (shared data dir), so _me is the main handle even though the browser is
            # the burner's session.
            self._me = (os.getenv("IG_USERNAME") or "").lstrip("@").lower()
            cfg = load_config()
            scr = cfg.get("scraper", {}) or {}
            # Build the scraper's own browser config: it must point at the BURNER's
            # Chrome, never the main account's. Default is a 2nd CDP endpoint
            # (:9223). If running on a persistent profile instead, set
            # scraper.user_data_dir; we then blank the inherited main profile so two
            # processes never open the same persistent dir.
            browser_cfg = dict(cfg["browser"])
            ep = scr.get("cdp_endpoint")
            browser_cfg["cdp_endpoint"] = ep if ep is not None else browser_cfg.get("cdp_endpoint")
            if scr.get("user_data_dir"):
                browser_cfg["user_data_dir"] = scr["user_data_dir"]
            elif browser_cfg.get("cdp_endpoint"):
                browser_cfg["user_data_dir"] = ""   # CDP wins; don't touch main profile
            if scr.get("executable_path"):
                browser_cfg["executable_path"] = scr["executable_path"]
            # Distinct fingerprint from the main account (anti-association): overlay
            # any scraper.browser.* overrides (UA/viewport/locale/timezone/proxy).
            for k, v in (scr.get("browser") or {}).items():
                if v not in (None, ""):
                    browser_cfg[k] = v
            # Base fingerprint snapshot: on a burner SWITCH we reset these from the base
            # then apply that burner's overrides, so a previous burner's proxy/UA never
            # leaks onto the next one.
            base_browser_cfg = dict(browser_cfg)
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                SCRAPER_PID.write_text(str(os.getpid()), encoding="utf-8")
            except Exception:
                pass
            self._write_scraper_status(phase="starting")

            with sync_playwright() as p:
                # Lazy browser: the scraper's Chrome is open ONLY while it's actively
                # scraping/vetting, and fully closed whenever it's paused (bot active,
                # pool full, disabled). True mutual exclusion with the main bot.
                conn = {"browser": None, "context": None, "page": None,
                        "cdp": False, "persistent": False, "ok": False}

                def ensure_connected():
                    if conn["ok"]:
                        return conn["page"]
                    b, c, pg, ucdp, upers = self._connect(p, browser_cfg)
                    conn.update(browser=b, context=c, page=pg, cdp=ucdp, persistent=upers, ok=True)
                    if not self._is_logged_in(pg):
                        self._write_scraper_status(
                            error="scraper Chrome is not logged in - run scraper_login.py "
                                  "to log the burner account in")
                        self.state.emit("log", {"level": "error",
                                                "msg": "burner not logged in - run scraper_login.py"})
                        disconnect()
                        return None
                    # Health check: a checkpointed / suspended / disabled burner stays
                    # "logged in" (cookies present) but every navigation redirects to a
                    # challenge, so scrapes return nothing and the scraper would idle
                    # SILENTLY. Surface it so the dashboard says WHY reach/follow won't fill.
                    if self._checkpoint_detected(pg):
                        self._write_scraper_status(
                            error="burner account is checkpointed / blocked by Instagram - open the "
                                  "burner Chrome and clear the challenge, or swap the burner account. "
                                  "Scraping is paused until then.")
                        self.state.emit("log", {"level": "error",
                            "msg": "burner checkpointed/blocked - can't harvest (clear the challenge "
                                   "on the burner or swap accounts)"})
                        disconnect()
                        return None
                    return pg

                def disconnect():
                    if not conn["ok"]:
                        return
                    try:
                        if conn["persistent"]:
                            conn["context"].close()
                        elif not conn["cdp"]:
                            conn["browser"].close()
                        else:
                            try:
                                conn["page"].close()
                            except Exception:
                                pass
                    except Exception:
                        pass
                    conn.update(browser=None, context=None, page=None, ok=False)

                append_event("scraper_start")
                while not self._stop_event.is_set():
                    self._set_working_pool("")   # cleared each cycle; the fill_* set it when working
                    day_cfg = load_config()
                    scr = day_cfg.get("scraper", {}) or {}
                    idle = float(scr.get("idle_seconds", 600))
                    # While idle the scraper must still wake often enough to (a) keep its
                    # status heartbeat fresh - the bot treats it as OFF past 180s stale -
                    # and (b) re-read config + re-evaluate the pool gate, so a pool that
                    # just became needed (reach toggled on, or the bot drained one) is
                    # filled within a minute instead of after a full idle_seconds rest.
                    idle_poll = min(idle, 60.0)
                    # Dashboard pause switch: keep the service alive but idle.
                    if not scr.get("enabled", False):
                        disconnect()
                        self._write_scraper_status(phase="disabled (toggle off)")
                        self._interruptible_sleep(30)
                        continue
                    # Multi-burner failover: pick a burner that isn't cooling down (the
                    # breaker sidelines one that can't navigate IG). If every burner is
                    # cooling, idle with the cause visible until the soonest one frees up.
                    active, n_burners, soonest = self._select_burner(scr)
                    if active is None:
                        disconnect()
                        wait_m = max(0.0, (soonest - time.time()) / 60)
                        self._write_scraper_status(
                            phase=f"paused - all {n_burners} burner(s) cooling down (retry in ~{wait_m:.0f}m)",
                            error="all burner accounts are rate-limited / blocked - see the latest "
                                  "log lines for each cause (logged out / checkpointed / throttled).")
                        self._interruptible_sleep(min(idle_poll, 60))
                        continue
                    # Switch the active burner profile when it changed (close the old
                    # Chrome first so two profiles never open at once).
                    if active["user_data_dir"] != self._active_burner_dir:
                        disconnect()
                        if self._active_burner_dir is not None and n_burners > 1:
                            self.state.emit("log", {"level": "info",
                                "msg": f"scraper switching burner → {active['label']}"})
                        self._active_burner_dir = active["user_data_dir"]
                        browser_cfg["user_data_dir"] = active["user_data_dir"]
                        if active["user_data_dir"]:
                            browser_cfg["cdp_endpoint"] = ""   # persistent profile wins over any CDP default
                        # Optional per-burner fingerprint/proxy: rotating accounts on the
                        # SAME IP won't dodge an IP-level limit, so a per-burner proxy makes
                        # failover actually effective. Reset from base first so the previous
                        # burner's overrides don't leak, then apply this burner's own.
                        for k in ("proxy", "user_agent", "viewport_width",
                                  "viewport_height", "locale", "timezone_id", "executable_path"):
                            browser_cfg[k] = (active[k] if active.get(k) not in (None, "")
                                              else base_browser_cfg.get(k))
                    # Coordinate with the bot so they don't fight over the Pi: only
                    # scrape during the bot's DEAD TIME. Two-stage fill - always bring
                    # both pools to LOW-water (so the bot can start), but only build the
                    # high-water buffer in the bot's downtime (outside active hours, or
                    # once it's used today's room). Idle once both pools hit their target.
                    coordinate = scr.get("coordinate_with_bot", True)
                    mode = day_cfg.get("mode", "unfollow")
                    limits = day_cfg.get("limits", {}) or {}
                    mult = int(scr.get("follow_pool_mult", 5))
                    # Fill whichever pool the bot will actually consume: the FOLLOW pool
                    # only in follow/marketing, the REACH pool only when reach is enabled.
                    follow_need = mode in ("follow", "marketing")
                    follow_low = int(limits.get("follows_per_day", 30))
                    follow_high = max(1, follow_low * mult)
                    follow_ready = self._pool_ready(day_cfg)
                    reach_need = self._reach_harvest_on(day_cfg)
                    # Low-water = a FULL day's likes, mirroring follow_low = a full day's
                    # follows. (NOT _reach_likes_left / remaining-today: that shrinks as the
                    # bot likes, so reach would look "at low-water" while still nearly empty
                    # and the scraper would skip ahead to follow's high-water buffer.)
                    reach_low = int(limits.get("likes_per_day", 100) or 100) if reach_need else 0
                    reach_mult = int(scr.get("reach_pool_mult", mult) or mult)
                    reach_high = max(1, int(limits.get("likes_per_day", 100) or 100) * reach_mult)
                    reach_ready = self._reach_pool_ready()
                    # STAGE 2 gate - may this pool build past low-water? Build the
                    # high-water buffer either when the bot is DONE for the day (used up
                    # today's room) OR when the bot is STOPPED entirely (nothing will
                    # consume the pools, so fill them up and then idle). While the bot is
                    # alive WITH room left we stop at low-water and let it work the Pi.
                    bot_running = self._bot_is_running()
                    follow_build_high = follow_need and (
                        not bot_running or self._day_room("follows", day_cfg) <= 0)
                    reach_build_high = reach_need and (
                        not bot_running or self._day_room("likes", day_cfg) <= 0)
                    # STRICT fill ORDER (the whole ladder runs in ONE pass, in this
                    # sequence, so the bot can always START before either pool spends time
                    # on its deep buffer, and follow's buffer is built before reach's):
                    #   1) follow → low-water   2) reach → low-water
                    #   3) follow → high-water  4) reach → high-water
                    # The high-water phases only run when allowed to build past low-water
                    # (follow_build_high / reach_build_high = bot done for the day / stopped).
                    def _follow_short(t):
                        return follow_need and self._pool_ready(day_cfg) < t
                    def _reach_short(t):
                        return reach_need and self._reach_pool_ready() < t

                    work = (_follow_short(follow_low) or _reach_short(reach_low)
                            or (follow_build_high and _follow_short(follow_high))
                            or (reach_build_high and _reach_short(reach_high)))
                    if coordinate and self._bot_is_acting():
                        disconnect()   # bot is working → free the Pi, close our Chrome
                        self._write_scraper_status(
                            phase=f"idle - bot active (follow {follow_ready}, reach {reach_ready})")
                        self._interruptible_sleep(60)
                        continue
                    if not work:
                        disconnect()
                        self._write_scraper_status(
                            phase=f"pools at target - follow {follow_ready}, reach {reach_ready}; idle")
                        self._interruptible_sleep(idle_poll)
                        continue

                    # --- ACTIVE: bring up the burner browser and do a pass ---
                    page = ensure_connected()
                    if page is None:
                        self._interruptible_sleep(30)   # not logged in - retry later
                        continue

                    def fill_follow(target):
                        """Vet-first: drain the already-scraped backlog into the eligible
                        list before scraping anything new (re-scraped names are mostly
                        dupes), then scrape fresh sources only if still short."""
                        if not _follow_short(target):
                            return
                        self._set_working_pool("follow")   # so the UI border + log badges track THIS pipeline
                        self._filter_pool(page, day_cfg, target=target)
                        if self._stop_event.is_set() or self._pool_ready(day_cfg) >= target:
                            return
                        self._write_scraper_status(phase="scraping sources")
                        try:
                            exclude = {c["username"] for c in read_follow_candidates()}
                            exclude |= {r["username"] for r in read_filter_rejected_log()}
                            self._scrape_candidates(
                                page, day_cfg,
                                pool_read=read_scraper_todo,
                                pool_write=write_scraper_todo,
                                extra_exclude=exclude,
                                status_cb=lambda ph: self._write_scraper_status(phase=ph))
                        except Exception as e:
                            self.state.emit("log", {"level": "error", "msg": f"scrape pass failed: {e}"})
                        if not self._stop_event.is_set():
                            self._filter_pool(page, day_cfg, target=target)

                    def fill_reach(target):
                        if not _reach_short(target):
                            return
                        self._set_working_pool("reach")   # so the UI border + log badges track THIS pipeline
                        rsrc = ((day_cfg.get("engagement", {}) or {}).get("reach_source")
                                or "hashtags").lower()
                        try:
                            if rsrc == "prospects":
                                self._harvest_reach_prospects(page, day_cfg, target=target)
                            else:
                                self._harvest_reach_links(page, day_cfg, target=target)
                        except Exception as e:
                            self.state.emit("log", {"level": "error", "msg": f"reach harvest failed: {e}"})

                    def _bot_took_over():
                        """Release the Pi the instant the bot starts working mid-ladder."""
                        if coordinate and self._bot_is_acting():
                            disconnect()
                            self._set_working_pool("")
                            self._write_scraper_status(phase="idle - bot active")
                            self._interruptible_sleep(60)
                            return True
                        return self._stop_event.is_set()

                    # ---- run the ladder, in order, yielding to the bot between phases ----
                    # _bot_took_over() handles its own disconnect + back-off, so on a
                    # yield we just `continue` to the next loop iteration (skip the tail).
                    fill_follow(follow_low)                       # 1) follow → low
                    if _bot_took_over():
                        continue
                    fill_reach(reach_low)                         # 2) reach → low
                    if _bot_took_over():
                        continue
                    if follow_build_high:
                        fill_follow(follow_high)                  # 3) follow → high
                        if _bot_took_over():
                            continue
                    if reach_build_high:
                        fill_reach(reach_high)                    # 4) reach → high
                    if self._stop_event.is_set():
                        break
                    # 4. idle until the next pass → close the browser, free the Pi.
                    #    Short poll (not the full idle_seconds) so the heartbeat stays
                    #    fresh and a newly-needed pool is picked up within a minute.
                    disconnect()
                    self._set_working_pool("")
                    self._write_scraper_status(phase="idle")
                    self._interruptible_sleep(idle_poll * random.uniform(0.85, 1.15))
                disconnect()
        except Exception as e:
            self._write_scraper_status(error=str(e))
            append_event("scraper_error", str(e)[:200])
        finally:
            self._write_scraper_status(phase="stopped")
            append_event("scraper_stop")
            try:
                if scraper_pid() == os.getpid():
                    SCRAPER_PID.unlink()
            except Exception:
                pass

    def _adjust_following(self, delta: int) -> None:
        """Nudge the live following count by ±1 after a real follow/unfollow, so the
        status bar tracks between full re-syncs. No-op until the first fetch seeds it."""
        cur = self.state.snapshot().get("account_following")
        if cur is not None:
            self.state.update(account_following=max(0, cur + delta))

    def _tick_resync(self, page) -> None:
        """Count an action and do a full re-sync every N actions (configurable), or
        immediately when a manual force-refresh was queued from the dashboard (the bot
        owns the browser while running, so the click can't fetch directly - it re-syncs
        here, on the next action, instead)."""
        n = int((load_config().get("behavior", {}) or {}).get("account_resync_every", 40))
        self._actions_since_resync = getattr(self, "_actions_since_resync", 0) + 1
        if getattr(self, "_force_account_refresh", False) or (n > 0 and self._actions_since_resync >= n):
            self._force_account_refresh = False
            self._refresh_account_counts(page)

    _PRIVATE_RE = re.compile(r"This account is private|This Account is Private", re.I)

    def _is_private(self, page) -> bool:
        try:
            return page.locator(f'text=/{self._PRIVATE_RE.pattern}/').count() > 0
        except Exception:
            return False

    def _follows_you(self, page) -> bool:
        """True if the profile shows the 'Follows you' chip (they already follow
        us). IG renders it as a small label next to the username in the header.
        Used both to skip them on the follow side and for the churn reciprocity
        check (stage 3)."""
        try:
            if page.locator('header :text-is("Follows you")').count() > 0:
                return True
            return page.locator(':text-is("Follows you")').count() > 0
        except Exception:
            return False

    def _passes_filters(self, counts: dict, filters: dict) -> Optional[str]:
        """Return a skip reason ('no_posts' / 'filtered') if the account fails the
        configured filters, else None. Private is handled separately."""
        if filters.get("skip_no_posts", True) and counts.get("posts") == 0:
            return "no_posts"
        followers = counts.get("followers")
        if followers is not None:
            min_f = filters.get("min_followers", 0) or 0
            max_f = filters.get("max_followers", 0) or 0
            if min_f and followers < min_f:
                return "filtered"
            if max_f and followers > max_f:
                return "filtered"
        following = counts.get("following")
        if following is not None:
            max_g = filters.get("max_following", 0) or 0
            if max_g and following > max_g:
                return "filtered"
        return None

    def _profile_text(self, page) -> str:
        """Lowercased header/main innerText of the loaded profile - used for
        bio-keyword matching during source discovery. Same eval the Strategy-2
        path of _read_profile_counts uses, so no extra page round-trip cost."""
        try:
            return (page.evaluate(
                "() => (document.querySelector('header') || document.querySelector('main')"
                " || document.body).innerText || ''") or "").lower()
        except Exception:
            return ""

    def _maybe_discover_source(self, page, target: str, counts: dict,
                              disc_cfg: dict) -> None:
        """If the loaded profile looks like a niche INFLUENCER - bio matches a
        keyword, no negative keyword, follower count in the influencer range -
        queue it for review in discovered_sources.json. Runs on profiles we're
        already visiting during the follow pass, so it's near-free. The dashboard
        promotes queued entries into follow.sources (review queue, not auto-add)."""
        if not disc_cfg or not disc_cfg.get("enabled"):
            return
        keywords = [k.lower() for k in (disc_cfg.get("keywords") or []) if k]
        if not keywords:
            return
        followers = counts.get("followers")
        if followers is None:
            return
        min_f = int(disc_cfg.get("min_followers", 5000) or 0)
        max_f = int(disc_cfg.get("max_followers", 500000) or 0)
        if (min_f and followers < min_f) or (max_f and followers > max_f):
            return

        text = self._profile_text(page)
        if not text:
            return
        if any(neg.lower() in text for neg in (disc_cfg.get("negative_keywords") or []) if neg):
            return
        matched = next((k for k in keywords if k in text), None)
        if not matched:
            return

        target = target.lstrip("@").lower()
        my = (os.getenv("IG_USERNAME") or "").lower()
        if target == my:
            return
        sources = (load_config().get("targeting", {}) or {}).get("sources", {}) or {}
        if target in {s.lstrip("@").lower() for s in (sources.get("profiles") or [])}:
            return
        queue = read_discovered_sources()
        if any(r["username"] == target for r in queue):
            return
        queue.append({
            "username": target, "followers": followers, "matched": matched,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        write_discovered_sources(queue)
        self.state.emit("source_discovered",
                        {"username": target, "followers": followers, "matched": matched})
        self.state.emit("log", {"level": "info",
                                "msg": f"discovered niche source @{target} "
                                       f"({matched}, {followers} followers)"})

    def _find_follow_button(self, page):
        """Locate the profile-header 'Follow' / 'Follow Back' button.

        Exact match (unlike the unfollow side): a substring 'Follow' would also
        match the 'Following' button, and we've already ruled that out by the
        time we call this."""
        for name in ("Follow", "Follow Back"):
            loc = page.get_by_role("button", name=name, exact=True).first
            try:
                loc.wait_for(state="visible", timeout=4000)
                return loc
            except Exception:
                continue
        return None

    def _find_post_follow_button(self, page, target: str):
        """The 'Follow' button next to the author's name in an OPEN post's header.
        Scoped to the author's profile link so we never grab a 'Follow' from a
        suggested-accounts strip elsewhere on the page. None if not present."""
        target = target.lstrip("@").lower()
        sel = (f'xpath=(//a[@href="/{target}/"])[1]/following::*'
               f'[(self::button or @role="button") and '
               f'(normalize-space()="Follow" or normalize-space()="Follow Back")][1]')
        end = time.monotonic() + 6
        while time.monotonic() < end:
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible(timeout=400):
                    return loc.first
            except Exception:
                pass
            # Generic fallback: the first Follow button anywhere on the post page
            # (the author's header button is first in DOM order).
            try:
                g = page.get_by_role("button", name="Follow", exact=True).first
                if g.is_visible(timeout=300):
                    return g
            except Exception:
                pass
            time.sleep(0.3)
        return None

    def _post_header_following(self, page, target: str) -> bool:
        """Did the post-header button flip to Following/Requested? Used to verify
        a follow done via the post fallback."""
        target = target.lstrip("@").lower()
        sel = (f'xpath=(//a[@href="/{target}/"])[1]/following::*'
               f'[(self::button or @role="button") and '
               f'(normalize-space()="Following" or normalize-space()="Requested")][1]')
        try:
            loc = page.locator(sel)
            return loc.count() > 0 and loc.first.is_visible(timeout=500)
        except Exception:
            return False

    def _follow_via_post(self, page, target: str) -> str:
        """Fallback for profiles whose header Follow button never renders (an IG
        web bug). Open one of the user's posts and click the 'Follow' button next
        to the author's name in the post header, which still works. Mirrors
        _unfollow_via_post."""
        post = page.locator('a[href*="/p/"], a[href*="/reel/"]').first
        try:
            post.wait_for(state="attached", timeout=5000)
            href = post.get_attribute("href")
        except Exception:
            href = None
        if not href:
            shot = self._screenshot(page, f"fail_fnoposts_{target}")
            return f"no_button_no_posts (shot:{shot})"

        post_url = href if href.startswith("http") else f"https://www.instagram.com{href}"
        self._step(target, "opening a post to follow")
        try:
            page.goto(post_url, wait_until="domcontentloaded")
        except Exception:
            shot = self._screenshot(page, f"fail_fpostopen_{target}")
            return f"post_open_failed (shot:{shot})"
        self._jitter(1.5, 3.0)

        # Maybe the post header already shows Following (header was just bugged).
        if self._post_header_following(page, target):
            return "already_following"

        btn = self._find_post_follow_button(page, target)
        if btn is None:
            shot = self._screenshot(page, f"fail_fpostnobtn_{target}")
            return f"post_follow_item_missing (shot:{shot})"

        self._step(target, "clicking Follow in post header")
        try:
            btn.click(timeout=5000)
        except Exception as e:
            shot = self._screenshot(page, f"fail_fpostclick_{target}")
            return f"follow_click_failed:{type(e).__name__} (shot:{shot})"

        if self._rate_limited(page):
            shot = self._screenshot(page, f"fail_ratelimit_{target}")
            return f"rate_limited (shot:{shot})"

        # Verify: the post-header button should flip to Following/Requested.
        self._step(target, "verifying follow (post)")
        end = time.monotonic() + 10
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return "ok"
            if self._post_header_following(page, target):
                self._step(target, "followed (via post)", "good")
                return "ok"
            time.sleep(0.4)

        # Cross-check on the profile page before calling it a failure.
        try:
            page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded")
            self._jitter(1.5, 3.0)
            if self._still_following(page):
                self._step(target, "followed (via post)", "good")
                return "ok"
        except Exception:
            pass

        shot = self._screenshot(page, f"fail_fpostverify_{target}")
        return f"post_follow_verify_failed (shot:{shot})"

    def _follow(self, page, target: str, filters: Optional[dict] = None,
                disc_cfg: Optional[dict] = None, lean: bool = False) -> str:
        """Visit a profile and follow it, applying filters. Mirrors _unfollow.

        Returns one of: 'ok', 'already_following', 'unavailable',
        'skipped_follows_you', 'skipped_private', 'skipped_no_posts',
        'skipped_filtered', 'rate_limited (...)', or a transient failure string.

        lean=True (external-scraper mode): the scraper already vetted this account,
        so skip the redundant filter reads (private / counts / discovery) - just
        confirm we're not already following, then follow cleanly."""
        self._intent = "follow"
        filters = filters or {}
        self._step(target, "opening profile")
        page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded")
        # Wait for the header to render before deciding (avoids false 'unavailable'
        # on a slow load - same fix as the unfollow side).
        try:
            page.wait_for_selector("header", timeout=12000)
        except Exception:
            self._step(target, "profile unavailable", "bad")
            return "unavailable"
        if self._checkpoint_detected(page):
            return "checkpoint"
        self._jitter(1.0, 2.5) if lean else self._jitter(2.0, 4.5)
        self._random_mouse(page)

        # Already following (or request pending)? Nothing to do. (always - "I'm
        # already following him" → skip.)
        if self._find_following_button(page) is not None:
            self._step(target, "already following", "neutral")
            return "already_following"

        # They already follow us → skip (no net-new reach). This is the ONE
        # relationship check we always do, including lean mode, because the burner
        # can't judge it - it's relative to the main account. Read from the header
        # we already loaded, so it's nearly free.
        if filters.get("skip_already_follows_me", True) and self._follows_you(page):
            self._step(target, "they already follow you - skip", "neutral")
            return "skipped_follows_you"

        if not lean:
            if filters.get("skip_private", True) and self._is_private(page):
                self._step(target, "private account - skip", "neutral")
                return "skipped_private"

            counts = self._read_profile_counts(page)
            # Discovery runs BEFORE the follow filters so niche influencers (who are
            # usually filtered out by max_followers) still get queued as sources.
            self._maybe_discover_source(page, target, counts, disc_cfg or {})
            reason = self._passes_filters(counts, filters)
            if reason == "no_posts":
                self._step(target, "no posts - skip", "neutral")
                return "skipped_no_posts"
            if reason == "filtered":
                self._step(target, "filtered out (followers/following limits)", "neutral")
                return "skipped_filtered"

        btn = self._find_follow_button(page)
        if btn is None:
            # No header Follow button (the same IG web bug the unfollow side hits).
            # Fall back to opening a post and using the Follow button next to the
            # author's name in the post header, which still works.
            self._step(target, "no header Follow button - trying via a post", "neutral")
            return self._follow_via_post(page, target)

        self._step(target, "clicking Follow")
        try:
            btn.click(timeout=5000)
        except Exception as e:
            shot = self._screenshot(page, f"fail_followclick_{target}")
            return f"follow_click_failed:{type(e).__name__} (shot:{shot})"

        if self._rate_limited(page):
            shot = self._screenshot(page, f"fail_ratelimit_{target}")
            return f"rate_limited (shot:{shot})"

        # Verify the button flipped to Following/Requested. Poll with a short
        # per-check timeout so a slow header repaint isn't a false failure.
        self._step(target, "verifying follow")
        end = time.monotonic() + 12
        while time.monotonic() < end:
            if self._stop_event.is_set():
                return "ok"
            if self._rate_limited(page):
                shot = self._screenshot(page, f"fail_ratelimit_{target}")
                return f"rate_limited (shot:{shot})"
            if self._still_following(page):
                self._step(target, "followed", "good")
                return "ok"
            time.sleep(0.4)

        # Last resort: reload once and re-check before calling it a failure.
        try:
            page.reload(wait_until="domcontentloaded")
            self._jitter(1.5, 3.0)
            if self._still_following(page):
                return "ok"
        except Exception:
            pass

        shot = self._screenshot(page, f"fail_followverify_{target}")
        return f"follow_verify_failed (shot:{shot})"

    # --- engagement (extra exposure touches) ---

    def _view_story(self, page, target: str) -> bool:
        """View a target's ACTIVE story (if any), then close. Returns True ONLY when
        a real story was actually playing.

        Detection: a viewable story makes IG redirect to
        /stories/<user>/<numeric-media-id>/. If that id never appears - no story, or
        the account is private and we don't follow them, or it bounced back to the
        profile - there's nothing to view, so we return False (skip). The old check
        (just 'still on a /stories/ URL') falsely counted those as views."""
        target = target.lstrip("@").lower()
        try:
            page.goto(f"https://www.instagram.com/stories/{target}/",
                      wait_until="domcontentloaded")
        except Exception:
            return False
        story_re = re.compile(rf"/stories/{re.escape(target)}/\d+")
        # Poll briefly for the media id (the redirect can lag the initial load).
        end = time.monotonic() + 5
        playing = False
        while time.monotonic() < end:
            if self._stop_event.is_set() or self._story_stop.is_set():
                return False
            url = page.url or ""
            if story_re.search(url):
                playing = True
                break
            if "/stories/" not in url:
                return False   # bounced to profile/login -> no viewable story
            time.sleep(0.3)
        if not playing:
            return False
        # Actually watch a segment or two (interruptible sleeps so we don't clobber
        # the main loop's countdown). Stop if the viewer closes.
        for _ in range(random.randint(1, 2)):
            if self._stop_event.is_set() or self._story_stop.is_set():
                break
            self._interruptible_sleep(random.uniform(2.0, 4.5))   # human dwell
            try:
                page.keyboard.press("ArrowRight")
            except Exception:
                break
            if not story_re.search(page.url or ""):
                break   # advanced past the last segment / viewer closed
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return True

    def _like_recent_posts(self, page, target: str, n: int) -> int:
        """Like up to `n` of the target's most recent posts. Returns how many were
        liked. Best-effort. Each like is LEDGERED (counts toward limits.likes_per_day)
        and the loop stops the moment the like cap / active-hours gate closes, so
        after-follow likes can't silently blow past the daily like budget. A rate-limit
        sets self._like_rate_limited so the caller can back off (soft-block)."""
        self._like_rate_limited = False
        if n <= 0:
            return 0
        cfg = load_config()
        target = target.lstrip("@").lower()
        try:
            page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded")
            self._interruptible_sleep(random.uniform(2.0, 3.5))
        except Exception:
            return 0
        # Wait for the post grid to load. Private / no-posts -> none -> 0 likes.
        try:
            page.wait_for_selector('a[href*="/p/"], a[href*="/reel/"]', timeout=6000)
        except Exception:
            return 0
        try:
            hrefs = page.eval_on_selector_all(
                'a[href*="/p/"], a[href*="/reel/"]',
                'els => els.map(e => e.getAttribute("href"))')
        except Exception:
            hrefs = []
        # Collect extra candidates (n*3) so already-liked / unlikeable posts don't
        # stop us short of the target count.
        urls, seen = [], set()
        for h in hrefs or []:
            if not h or h in seen:
                continue
            seen.add(h)
            urls.append(h if h.startswith("http") else f"https://www.instagram.com{h}")
            if len(urls) >= n * 3:
                break

        liked = 0
        for url in urls:
            if liked >= n or self._stop_event.is_set() or self._story_stop.is_set():
                break
            if not self._can_act("likes", cfg):
                break   # daily like cap reached / outside active hours
            try:
                page.goto(url, wait_until="domcontentloaded")
                self._interruptible_sleep(random.uniform(1.5, 3.0))
                if self._click_like(page):
                    liked += 1
                    self._day_record("likes", cfg)   # ledger it against the like cap
                    self._interruptible_sleep(random.uniform(1.0, 2.5))
                    if self._rate_limited(page):
                        self._like_rate_limited = True
                        break
            except Exception:
                continue
        return liked

    def _action_bar(self, page):
        """Locator for the OPEN post's action bar. Anchors on ANY action-bar-only
        icon (Comment / Save / Share - reels lack a 'Comment' svg, so we can't rely
        on it alone) and returns the nearest ancestor that holds the Like/Unlike.
        That like is the POST's, never a comment's heart. None if not found."""
        end = time.monotonic() + 5
        while time.monotonic() < end:
            for label in ("Comment", "Save", "Remove", "Share"):
                try:
                    anchor = page.locator(f'svg[aria-label="{label}"]').first
                    if anchor.is_visible(timeout=300):
                        bar = anchor.locator(
                            "xpath=ancestor::*[.//*[@aria-label='Like' or @aria-label='Unlike']][1]")
                        if bar.count() > 0:
                            return bar
                except Exception:
                    continue
            time.sleep(0.3)
        return None

    def _click_like(self, page) -> bool:
        """Like the currently open post (photo OR reel). Clicks the Like in the post's
        action bar; falls back to double-clicking the media and confirming a NEW
        'Unlike' appeared (count delta). Returns False if already liked or unconfirmed
        (then logs diagnostics + screenshot)."""
        bar = self._action_bar(page)
        if bar is not None:
            try:
                if bar.locator('svg[aria-label="Unlike"]').first.is_visible(timeout=500):
                    return False   # already liked - don't toggle off
            except Exception:
                pass
            try:
                like = bar.locator('svg[aria-label="Like"]').first
                btn = like.locator("xpath=ancestor::*[self::button or @role='button'][1]")
                try:
                    btn.click(timeout=3000)
                except Exception:
                    like.click(timeout=3000)
            except Exception:
                pass
            self._interruptible_sleep(random.uniform(0.7, 1.4))
            try:
                if bar.locator('svg[aria-label="Unlike"]').first.is_visible(timeout=2500):
                    return True
            except Exception:
                pass

        # Fallback (works when the bar can't be located, e.g. odd reel layouts):
        # double-click the media and confirm a NEW Unlike appeared.
        try:
            before = page.locator('svg[aria-label="Unlike"]').count()
        except Exception:
            before = 0
        for sel in ('article video', 'article img', 'main video', 'main img', 'video'):
            try:
                media = page.locator(sel).first
                if media.is_visible(timeout=800):
                    media.dblclick(timeout=3000)
                    break
            except Exception:
                continue
        self._interruptible_sleep(random.uniform(0.8, 1.6))
        try:
            after = page.locator('svg[aria-label="Unlike"]').count()
        except Exception:
            after = before
        if after > before:
            return True

        # Couldn't confirm - capture state so we can fix it precisely.
        try:
            likes = page.locator('svg[aria-label="Like"]').count()
        except Exception:
            likes = -1
        shot = self._screenshot(page, "reach_like_fail")
        self.state.emit("log", {"level": "error", "msg":
            f"reach like failed (shot:{shot}) bar={'y' if bar is not None else 'n'} "
            f"Like={likes} Unlike {before}->{after}"})
        return False

    def _engage_after_follow(self, page, target: str, eng_cfg: dict) -> str:
        """Optional touches right after a follow (still on/near the profile): view
        their story and/or like a couple posts. Controlled by follow.engagement.
        Never raises - engagement is best-effort and must not abort the follow that
        already happened. Returns 'block' if after-follow likes tripped a soft-block
        that should stop the run for the day, else ''."""
        if not eng_cfg:
            return ""
        try:
            if eng_cfg.get("on_follow_view_story", False):
                self._view_story(page, target)
            n = int(eng_cfg.get("on_follow_like_posts", 0) or 0)
            if n > 0:
                self._like_recent_posts(page, target, n)
                if self._like_rate_limited:
                    # A soft-block on the after-follow likes is a real IG signal -
                    # record + back off (and stop for the day past the daily max).
                    return self._handle_soft_block(load_config())
        except Exception as e:
            self.state.emit("log", {"level": "error",
                                    "msg": f"engagement on @{target} failed: {e}"})
        return ""

    def _build_story_queue(self) -> list[str]:
        """Candidate-pool members eligible for a story check: not in the follow done
        set, and not checked within the last `story_recheck_hours`. The recheck
        window means we cycle back to accounts later (to catch new stories) instead
        of checking each only once - so story-reach keeps running, but we don't spam
        the same account in a tight loop."""
        my = (os.getenv("IG_USERNAME") or "").lower()
        recheck_h = float((load_config().get("engagement", {}) or {})
                          .get("story_recheck_hours", 20) or 0)
        cutoff = (time.time() - recheck_h * 3600) if recheck_h > 0 else None
        story_log = _log_path("story_viewed_log", "data/story_viewed.log")
        last_checked: dict = {}
        if story_log.exists():
            for line in story_log.read_text(encoding="utf-8").splitlines():
                parts = line.split("\t")
                if len(parts) >= 2:
                    ts = parse_log_ts(parts[0])
                    if ts is not None:
                        u = parts[1].lower()
                        last_checked[u] = max(last_checked.get(u, 0.0), ts)
        done = self._follow_done_set(load_whitelist(), my)
        out = []
        for c in read_follow_candidates():
            u = c["username"]
            if u in done:
                continue
            if cutoff is not None and last_checked.get(u, 0.0) >= cutoff:
                continue   # checked recently - revisit after the window
            out.append(u)
        return out

    def _reset_story_reach(self) -> None:
        """Reset the per-batch story-reach counters/queue (called each batch start)."""
        self._story_tick = 0
        self._story_today = 0
        self._story_queue = None
        self._reach_checked = 0
        self._reach_acted = 0
        self._reach_pool = []
        self._reach_last_scrape = 0.0
        self._reach_scrape_cooldown = 0.0
        self._story_next = 1
        self.state.update(reach_scraped=0, reach_liked=0, reach_pool=0)

    # ---- per-CALENDAR-DAY action ledger + active-hours (ban safety) ----
    # Caps in config are now true per-day totals (persisted across restarts /
    # keep_running re-runs), randomized ±jitter daily, so the account can't blow
    # past a safe daily volume the way per-batch caps allowed.

    @staticmethod
    def _cap_bases(cfg) -> dict:
        """The configured per-day base caps (pre-jitter). Used to detect when the
        dashboard changed a limit so today's rolled caps can be re-rolled to match."""
        limits = cfg.get("limits", {}) or {}
        return {
            "follows": int(limits.get("follows_per_day", 30) or 0),
            "unfollows": int(limits.get("unfollows_per_day", 30) or 0),
            "likes": int(limits.get("likes_per_day", 50) or 0),
            "combined": int(limits.get("combined_per_day", 0) or 0),
        }

    def _roll_daily_caps(self, cfg) -> dict:
        jit = float((cfg.get("limits", {}) or {}).get("daily_jitter", 0.3))

        def r(base):
            base = float(base or 0)
            if base <= 0:
                return 0
            return max(1, int(round(base * random.uniform(1 - jit, 1 + jit))))

        return {k: r(v) for k, v in self._cap_bases(cfg).items()}

    def _ensure_ledger(self, cfg) -> dict:
        today = time.strftime("%Y-%m-%d")
        bases = self._cap_bases(cfg)
        L = getattr(self, "_ledger", None)
        if L is None:
            try:
                L = json.loads(DAILY_COUNTS.read_text(encoding="utf-8"))
            except Exception:
                L = {}
        if L.get("date") != today:
            L = {"date": today, "follows": 0, "unfollows": 0, "likes": 0,
                 "stories": 0, "soft_blocks": 0, "last_block_ts": 0,
                 "follow_rests": 0, "caps": self._roll_daily_caps(cfg), "caps_base": bases}
            self._ledger = L
            self._save_ledger()
        else:
            self._ledger = L
            # Re-roll today's caps if the configured limits changed (dashboard edit),
            # so a lowered cap applies the SAME day instead of waiting for midnight.
            if "caps" not in L or L.get("caps_base") != bases:
                L["caps"] = self._roll_daily_caps(cfg)
                L["caps_base"] = bases
                self._save_ledger()
        return L

    def _save_ledger(self) -> None:
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = DAILY_COUNTS.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._ledger, indent=2), encoding="utf-8")
            os.replace(tmp, DAILY_COUNTS)
        except Exception:
            pass

    def _day_room(self, kind, cfg) -> int:
        """Remaining room today for a kind (follows/unfollows/likes), honoring the
        per-kind cap AND the combined follow+unfollow cap. Large = effectively no cap."""
        L = self._ensure_ledger(cfg)
        caps = L.get("caps", {})
        room = 10**9
        cap = caps.get(kind)
        if cap:
            room = min(room, cap - int(L.get(kind, 0)))
        if kind in ("follows", "unfollows"):
            comb = caps.get("combined")
            if comb:
                used = int(L.get("follows", 0)) + int(L.get("unfollows", 0))
                room = min(room, comb - used)
        return room

    def _publish_day_counts(self, cfg) -> None:
        """Push today's action counts AND their rolled caps to the dashboard, so the
        top bar can show 'done / cap' for follows/unfollows/likes."""
        L = self._ensure_ledger(cfg)
        caps = L.get("caps", {}) or {}
        self.state.update(day_follows=L.get("follows", 0),
                          day_unfollows=L.get("unfollows", 0),
                          day_likes=L.get("likes", 0),
                          day_follows_cap=caps.get("follows", 0),
                          day_unfollows_cap=caps.get("unfollows", 0),
                          day_likes_cap=caps.get("likes", 0))

    def _day_record(self, kind, cfg, n=1) -> None:
        L = self._ensure_ledger(cfg)
        L[kind] = int(L.get(kind, 0)) + n
        self._save_ledger()
        self._publish_day_counts(cfg)

    def _record_soft_block(self, cfg) -> int:
        L = self._ensure_ledger(cfg)
        L["soft_blocks"] = int(L.get("soft_blocks", 0)) + 1
        L["last_block_ts"] = time.time()
        self._save_ledger()
        return int(L["soft_blocks"])

    def _active_window(self, cfg):
        safety = cfg.get("safety", {}) or {}
        if not safety.get("active_hours_enabled", False):
            return None
        return (int(safety.get("active_hours_start", 8)),
                int(safety.get("active_hours_end", 24)))

    def _in_active_hours(self, cfg) -> bool:
        win = self._active_window(cfg)
        if not win:
            return True
        start, end = win
        hour = time.localtime().tm_hour
        if start <= end:
            return start <= hour < end
        return hour >= start or hour < end   # window wraps midnight

    def _seconds_until_active(self, cfg) -> float:
        """0 if in the active window, else seconds until it next opens."""
        if self._in_active_hours(cfg):
            return 0.0
        win = self._active_window(cfg)
        if not win:
            return 0.0
        start = win[0]
        now = time.localtime()
        secs_now = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
        start_secs = start * 3600
        if start_secs > secs_now:
            return float(start_secs - secs_now)
        return float((86400 - secs_now) + start_secs)

    def _seconds_until_tomorrow(self) -> float:
        now = time.localtime()
        return float(86400 - (now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec) + 5)

    def _can_act(self, kind, cfg) -> bool:
        """Gate every real action: must be inside active hours AND under today's cap."""
        return self._in_active_hours(cfg) and self._day_room(kind, cfg) > 0

    def _handle_soft_block(self, cfg) -> str:
        """On an IG soft-block: record it (persisted), back off with a long,
        exponential cooldown, and stop for the day after too many. IG action-blocks
        last hours-to-days, so resuming after 15 min just re-arms the block.
        Returns 'block' (stop the run) or 'cooldown' (backed off, resume)."""
        safety = cfg.get("safety", {}) or {}
        n = self._record_soft_block(cfg)
        append_event("soft_block", f"#{n} today")
        if n >= int(safety.get("soft_block_max_per_day", 2)):
            self.state.update(phase_detail=f"{n} soft-blocks today - stopping for the day")
            return "block"
        lo = float(safety.get("rate_limit_cooldown_min", 3600))
        hi = float(safety.get("rate_limit_cooldown_max", 7200))
        cooldown = random.uniform(lo, hi) * (2 ** (n - 1))   # exponential per hit
        self.state.update(phase_detail=f"soft block #{n} - cooling down {cooldown / 3600:.1f}h")
        self._interruptible_sleep(cooldown)
        return "cooldown"

    def _reach_finishable(self, cfg) -> bool:
        """Can the LIKES cap realistically still be worked? True only if reach is enabled
        AND there's actually a way to like more right now: the reach pool already holds
        unliked posts, OR the scraper can fill it. Used so the day stays 'open' for likes
        only when likes can progress - never deadlocking when reach is on but unfillable
        (no sources, scraper off/broken)."""
        eng = cfg.get("engagement", {}) or {}
        if not eng.get("reach_enabled", False):
            return False
        return self._reach_harvest_on(cfg) or self._reach_pool_ready() > 0

    def _day_capped_for_mode(self, cfg, mode) -> bool:
        if mode == "follow":
            return self._day_room("follows", cfg) <= 0
        if mode == "marketing":
            fu_done = (self._day_room("follows", cfg) <= 0
                       and self._day_room("unfollows", cfg) <= 0)
            # The day isn't done until LIKES are also capped - reach likes only fire
            # interleaved with follow/unfollow actions, so when those caps are hit the
            # like budget is usually still short. Keep working until likes are done too,
            # but only while likes can actually progress (else we'd never end the day).
            if fu_done and self._reach_finishable(cfg) and self._day_room("likes", cfg) > 0:
                return False
            return fu_done
        if mode == "unfollow":
            return self._day_room("unfollows", cfg) <= 0
        return False

    def _do_reach(self, page, u: str) -> str:
        """Do one marketing 'reach' touch on a pool member per `engagement.reach_mode`
        ('likes' | 'story' | 'both'): like a recent post and/or view their story.
        Likes work on any PUBLIC account (high hit rate); stories need an active,
        viewable story (rare). Logs + feeds only on a real action. Returns
        'liked' / 'viewed' / 'ratelimit' / '' (nothing) so the caller can pace."""
        self._intent = "like"
        eng = load_config().get("engagement", {}) or {}
        mode = (eng.get("reach_mode") or "likes").lower()
        self.state.update(phase_detail=f"reach: checking @{u}")
        parts, rate_limited = [], False
        try:
            if mode in ("likes", "both"):
                n = int(eng.get("reach_like_posts", 1) or 0)
                if n > 0:
                    liked = self._like_recent_posts(page, u, n)
                    if liked:
                        parts.append(f"liked {liked} post" + ("s" if liked > 1 else ""))
                    if self._rate_limited(page):
                        rate_limited = True
            if not rate_limited and mode in ("story", "both"):
                if self._view_story(page, u):
                    parts.append("viewed story")
        except Exception:
            pass

        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        detail = ", ".join(parts)
        append_log(_log_path("story_viewed_log", "data/story_viewed.log"),
                   f"{ts}\t{u}\t{detail or 'nothing'}")
        if parts:
            self._story_today += 1
            new_count = self.state.snapshot().get("story_viewed_count", 0) + 1
            self.state.update(story_viewed_count=new_count,
                              last_message=f"reach @{u}: {detail}")
            self.state.emit("story_viewed",
                            {"timestamp": ts, "username": u, "detail": detail})
        # Heartbeat so you can see reach is alive even when most accounts have
        # nothing to like (private / no posts). Works for both the interleave and
        # the background worker.
        self._reach_checked += 1
        if parts:
            self._reach_acted += 1
        if self._reach_checked % 5 == 0:
            self.state.emit("log", {"level": "info",
                "msg": f"reach: checked {self._reach_checked} accounts · acted on {self._reach_acted}"})
        if rate_limited:
            return "ratelimit"
        if any("liked" in p for p in parts):
            return "liked"
        return "viewed" if parts else ""

    # ---- hashtag reach (standalone: like public posts under niche hashtags) ----

    def _collect_post_links(self, page, url: str, cap: int) -> list[str]:
        """Open `url` and accumulate post/reel links, polling + scrolling for up to
        ~10s so a lazily-rendered grid isn't missed. Returns [] if none appear."""
        try:
            page.goto(url, wait_until="domcontentloaded")
        except Exception:
            return []
        self._interruptible_sleep(random.uniform(2.0, 3.5))
        urls, seen, stagnant = [], set(), 0
        end = time.monotonic() + 10
        while time.monotonic() < end and len(urls) < cap and stagnant < 5:
            if (self._stop_event.is_set() or self._story_stop.is_set()
                    or (self._scraper_service and self._bot_is_acting())):
                break
            try:
                hrefs = page.eval_on_selector_all(
                    'a[href*="/p/"], a[href*="/reel/"]',
                    'els => els.map(e => e.getAttribute("href"))')
            except Exception:
                hrefs = []
            before = len(urls)
            for h in hrefs or []:
                if not h:
                    continue
                full = h if h.startswith("http") else f"https://www.instagram.com{h}"
                if full in seen:
                    continue
                seen.add(full)
                urls.append(full)
            stagnant = stagnant + 1 if len(urls) == before else 0
            try:
                page.mouse.wheel(0, 2500)
            except Exception:
                pass
            self._interruptible_sleep(random.uniform(1.0, 2.0))
        return urls[:cap]

    def _first_post_link_on_page(self, page) -> Optional[str]:
        """Return the first post/reel URL from the profile page ALREADY loaded in `page`
        (no navigation). Used to grab a reach prospect's recent post during the same load
        that vetted them, instead of re-opening the profile. Polls briefly (~3s) for a
        lazily-rendered grid; the grid anchors exist even with images blocked. Returns
        None if no post link shows up (caller can fall back to _collect_post_links)."""
        end = time.monotonic() + 3.0
        while time.monotonic() < end:
            if (self._stop_event.is_set() or self._story_stop.is_set()
                    or (self._scraper_service and self._bot_is_acting())):
                break
            try:
                hrefs = page.eval_on_selector_all(
                    'a[href*="/p/"], a[href*="/reel/"]',
                    'els => els.map(e => e.getAttribute("href"))')
            except Exception:
                hrefs = []
            for h in hrefs or []:
                if not h:
                    continue
                return h if h.startswith("http") else f"https://www.instagram.com{h}"
            self._interruptible_sleep(random.uniform(0.6, 1.0))
        return None

    def _hashtag_post_urls(self, page, tag: str, cap: int = 60) -> list[str]:
        """Public post URLs for #tag. Tries the hashtag grid first, then the keyword
        search results page as a fallback (IG renders these differently and one
        often works when the other doesn't). Screenshots if BOTH come up empty."""
        urls = self._collect_post_links(
            page, f"https://www.instagram.com/explore/tags/{tag}/", cap)
        if not urls:
            urls = self._collect_post_links(
                page, f"https://www.instagram.com/explore/search/keyword/?q=%23{tag}", cap)
        if not urls:
            shot = self._screenshot(page, f"reach_no_posts_{tag}")
            self.state.emit("log", {"level": "error",
                "msg": f"reach: no posts found for #{tag} (shot:{shot}) - IG may be gating "
                       "the hashtag page"})
        return urls

    def _harvest_reach_links(self, page, cfg, target: int = None) -> None:
        """Burner-side: top up the persistent reach pool (post links) the main account
        likes from. Browser navigation only - round-robins the niche hashtags (tag grid
        + keyword-search fallback via _hashtag_post_urls) so the pool is tag-diverse,
        fills to `target` (default the reach high-water mark), and prunes already-visited
        posts. The scraper is
        the SOLE writer of reach_pool.json (the bot dedups via reach_liked.log), so these
        atomic writes never race the consumer. Backs off naturally: a full tag cycle that
        adds nothing ends the pass, and the outer loop idles before retrying."""
        if not self._reach_harvest_on(cfg):
            return
        eng = cfg.get("engagement", {}) or {}
        tags = self._reach_tags(cfg)
        scr = cfg.get("scraper", {}) or {}
        mult = int(scr.get("reach_pool_mult", scr.get("follow_pool_mult", 5)) or 5)
        if target is None:
            likes_cap = int((cfg.get("limits", {}) or {}).get("likes_per_day", 100) or 100)
            target = max(1, likes_cap * mult)
        per_tag = int(eng.get("reach_scrape_per_tag", 60) or 60)
        lo, hi = float(scr.get("filter_delay_min", 1)), float(scr.get("filter_delay_max", 3))

        liked = self._reach_liked_set()
        pool = [e for e in read_reach_pool() if e["url"] not in liked]   # prune visited
        have = {e["url"] for e in pool}
        random.shuffle(tags)   # vary which tag leads each pass
        self._write_scraper_status(phase=f"harvesting reach links ({len(pool)}/{target})")

        progress = True
        while progress and len(pool) < target and not self._stop_event.is_set():
            progress = False
            for tag in tags:
                if self._stop_event.is_set() or len(pool) >= target:
                    break
                if self._bot_is_acting():   # bot woke up → yield the Pi immediately
                    write_reach_pool(pool)
                    return
                try:
                    urls = self._hashtag_post_urls(page, tag, per_tag)
                except Exception as e:
                    self.state.emit("log", {"level": "error",
                                            "msg": f"reach harvest #{tag} failed: {e}"})
                    urls = []
                added = 0
                for u in urls:
                    if u in have or u in liked:
                        continue
                    have.add(u)
                    pool.append({"url": u, "tag": tag, "source": f"hashtag:{tag}",
                                 "added_at": int(time.time())})
                    added += 1
                if added:
                    progress = True
                    write_reach_pool(pool)   # sole writer, atomic
                    self.state.update(
                        reach_scraped=self.state.snapshot().get("reach_scraped", 0) + added,
                        reach_pool=len(pool))
                    self._write_scraper_status(phase=f"harvesting reach links ({len(pool)}/{target})")
                self._jitter(lo, hi)
        write_reach_pool(pool)

    def _harvest_reach_prospects(self, page, cfg, target: int = None) -> None:
        """Burner-side (prospects mode): fill the reach pool with PROSPECTS' post links -
        the commenters/influencer-followers behind the niche - so the main account's likes
        land on real prospects instead of the clinics who post under hashtags. Scrapes
        usernames from the SAME sources as follow into a separate reach TODO, vets each
        PUBLIC (mandatory) + the follow filters, grabs one recent post URL, and stores
        {url, username, source} in reach_pool. Independent of the follow pool. Yields the
        Pi the instant the bot starts acting; the main account consume path is unchanged."""
        if not self._reach_harvest_on(cfg):
            return
        targeting = cfg.get("targeting", {}) or {}
        scr = cfg.get("scraper", {}) or {}
        coordinate = scr.get("coordinate_with_bot", True)
        mult = int(scr.get("reach_pool_mult", scr.get("follow_pool_mult", 5)) or 5)
        if target is None:
            likes_cap = int((cfg.get("limits", {}) or {}).get("likes_per_day", 100) or 100)
            target = max(1, likes_cap * mult)
        # Mandatory public + has-posts on top of the configured follow filters, so a like
        # can actually land (private accounts have no visible posts to like).
        filters = dict(targeting.get("filters", {}) or {})
        filters["skip_private"] = True
        filters["skip_no_posts"] = True
        lo, hi = float(scr.get("filter_delay_min", 1)), float(scr.get("filter_delay_max", 3))

        liked = self._reach_liked_set()
        pool = [e for e in read_reach_pool() if e.get("url") not in liked]
        have_users = {(e.get("username") or "").lower() for e in pool if e.get("username")}
        have_urls = {e.get("url") for e in pool}
        # Reach excludes ONLY accounts we currently follow (no point reach-liking someone
        # who already sees our feed) + own handle - NOT the whole follow done-set. Reusing
        # the full done-set starved reach: from the shared sources the follow side has
        # already consumed almost everything, so reach was left only the unusable dregs.
        # With this narrower set, reach can target the same active prospects the follow
        # side draws from (incl. churned / not-yet-followed) and actually fill its pool.
        done = {u.lower() for u in read_following_cache()}
        if self._me:
            done.add(self._me.lower())
        self._write_scraper_status(phase=f"harvesting reach prospects ({len(pool)}/{target})")
        if len(pool) >= target:
            return

        def _yield():
            return self._stop_event.is_set() or (coordinate and self._bot_is_acting())

        def vet_backlog() -> bool:
            """Drain the current reach TODO into the pool: vet each (public + filters),
            grab a post URL, append to the pool. Grows the reach POOL incrementally (each
            KEEP), so the bot's progress gate keeps seeing it climb. Returns True if it
            made any progress (vetted at least one)."""
            resolved: list[str] = []
            progressed = False
            for c in read_reach_todo():
                if _yield() or len(pool) >= target:
                    break
                u = (c.get("username") or "").lower()
                if not u or u in have_users or u in done:
                    resolved.append(u)
                    continue
                self._write_scraper_status(phase=f"vetting reach prospect @{u} ({len(pool)}/{target})")
                try:
                    reason, _meta = self._filter_one(page, u, filters)
                except Exception as e:
                    self.state.emit("log", {"level": "error", "msg": f"reach vet @{u} failed: {e}"})
                    continue   # transient - leave in todo, retry next pass
                resolved.append(u)        # vetted (kept or rejected) → drop from todo
                progressed = True
                if reason is not None:
                    self.state.emit("log", {"level": "info", "msg": f"reach @{u} → reject ({reason})"})
                    continue              # private / filtered out
                # _filter_one just loaded this profile - grab the recent post link from
                # that same page (no re-navigation); fall back to a fresh load if needed.
                try:
                    url = self._first_post_link_on_page(page)
                    if not url:
                        posts = self._collect_post_links(page, f"https://www.instagram.com/{u}/", 1)
                        url = posts[0] if posts else None
                except Exception:
                    url = None
                if not url or url in have_urls or url in liked:
                    self.state.emit("log", {"level": "info", "msg": f"reach @{u} → no usable post"})
                    continue
                have_urls.add(url)
                have_users.add(u)
                pool.append({"url": url, "username": u,
                             "source": c.get("source", "") or "reach", "added_at": int(time.time())})
                write_reach_pool(pool)    # sole writer, atomic
                self.state.emit("log", {"level": "info",
                    "msg": f"reach [{len(pool)}/{target}] @{u} → KEEP (post linked)"})
                self.state.update(
                    reach_scraped=self.state.snapshot().get("reach_scraped", 0) + 1,
                    reach_pool=len(pool))
                self._jitter(lo, hi)
            if resolved:   # drop vetted/excluded usernames from the todo; keep the rest
                rset = set(resolved)
                write_reach_todo([c for c in read_reach_todo()
                                  if (c.get("username") or "").lower() not in rset])
            return progressed

        # INTERLEAVE: vet the queued backlog first (so the pool climbs immediately), then
        # scrape a SMALL fresh batch and vet that, repeat. This keeps the reach POOL growing
        # steadily - instead of scraping a big backlog for minutes (pool flat) while the bot
        # gives up waiting and takes over before any vetting happens. Also, one source
        # failing just ends that scrape batch; the next round vets what we already have.
        BATCH = 40
        for _round in range(50):                       # safety bound
            if _yield() or len(pool) >= target:
                break
            vetted = vet_backlog()                     # drain whatever's queued → pool grows
            if _yield() or len(pool) >= target:
                break
            before = len(read_reach_todo())
            try:                                       # top up the backlog with a small batch
                self._scrape_candidates(
                    page, cfg,
                    pool_read=read_reach_todo, pool_write=write_reach_todo,
                    extra_exclude=have_users, done_set=done, pool_min=BATCH,
                    status_cb=lambda ph: self._write_scraper_status(phase=ph))
            except Exception as e:
                self.state.emit("log", {"level": "error", "msg": f"reach prospect scrape failed: {e}"})
            scraped = len(read_reach_todo()) - before
            if not vetted and scraped <= 0:
                break   # nothing left to vet and no fresh prospects → done this pass
        write_reach_pool(pool)

    def _reach_liked_set(self) -> set:
        path = _log_path("reach_liked_log", "data/reach_liked.log")
        out = set()
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                parts = line.split("\t")
                if len(parts) >= 2:
                    out.add(parts[1])
        return out

    def _reach_post_author(self, page):
        for sel in ('article header a[href^="/"]', 'header a[href^="/"]'):
            try:
                href = page.eval_on_selector(sel, 'e => e.getAttribute("href")')
            except Exception:
                href = None
            if href:
                m = USERNAME_HREF_RE.match(href)
                if m and m.group(1).lower() not in RESERVED:
                    return m.group(1).lower()
        return None

    def _next_reach_post(self, page, eng) -> Optional[str]:
        """Return the next un-liked post URL to like.

        Default (reach_external_harvest): CONSUME-ONLY from the persistent reach pool
        that the scraper/burner fills - the main account never loads a gated explore/
        hashtag grid itself, so likes keep flowing at the daily target from cache.

        Legacy fallback (reach_external_harvest off, hashtags mode only): refill an
        in-memory pool by scraping ONE random hashtag grid on the main account, rarely +
        with backoff. 'prospects' mode is burner-only (the main account never scrapes
        prospects itself), so it always consumes the harvested pool."""
        if (eng.get("reach_external_harvest", True)
                or (eng.get("reach_source") or "").lower() == "prospects"):
            return self._next_reach_post_from_pool(eng)
        sources = (load_config().get("targeting", {}) or {}).get("sources", {}) or {}
        tags = [t.strip().lstrip("#").lower()
                for t in (eng.get("reach_hashtags") or sources.get("hashtags") or [])
                if t and t.strip()]
        if not tags:
            return None
        liked = self._reach_liked_set()
        self._reach_pool = [(u, t) for (u, t) in self._reach_pool if u not in liked]

        now = time.monotonic()
        if (len(self._reach_pool) < 5
                and now >= self._reach_last_scrape + self._reach_scrape_cooldown):
            tag = random.choice(tags)
            self.state.update(phase_detail=f"reach: loading #{tag} posts")
            fresh = [u for u in self._hashtag_post_urls(page, tag) if u not in liked]
            self._reach_last_scrape = time.monotonic()
            if fresh:
                self._reach_pool.extend((u, tag) for u in fresh)
                self._reach_scrape_cooldown = random.uniform(90, 180)
                self.state.update(
                    reach_scraped=self.state.snapshot().get("reach_scraped", 0) + len(fresh))
            else:
                # No posts -> almost certainly gated. Back off well before the next
                # grid load so we don't hammer it; likes pause until the pool refills.
                self._reach_scrape_cooldown = random.uniform(600, 1200)
                self.state.emit("log", {"level": "info", "msg":
                    f"reach: #{tag} grid returned nothing (likely gated) - "
                    f"pausing hashtag loads ~{self._reach_scrape_cooldown / 60:.0f}m"})

        self.state.update(reach_pool=len(self._reach_pool))
        if self._reach_pool:
            url, tag = self._reach_pool.pop(random.randrange(len(self._reach_pool)))
            self._reach_tag = tag
            return url
        return None

    @staticmethod
    def _pick_diverse_reach(pool, recent_tags, max_streak):
        """Pick a reach entry whose tag isn't over-represented in the recent window so
        consecutive likes spread across hashtags. The pool is already harvested in
        round-robin tag order, so first-fit on top of that keeps it mixed. <=0 disables."""
        if not pool:
            return None
        if max_streak <= 0:
            return pool[0]
        recent = list(recent_tags)
        for e in pool:
            tag = e.get("source") or e.get("tag", "")
            if recent[-max_streak:].count(tag) < max_streak:
                return e
        return pool[0]

    def _next_reach_post_from_pool(self, eng) -> Optional[str]:
        """Consume-only reach: pop a tag-diverse, not-yet-visited post URL from the
        persistent reach pool (filled by the scraper). No grid navigation here, so the
        main account is never exposed to hashtag-page gating. Returns None when the pool
        is empty (reach idles this tick until the scraper refills it). reach_liked.log
        records every opened post, so it's the cross-run de-dupe; _reach_consumed guards
        the brief pick→log window within a run."""
        liked = self._reach_liked_set()
        pool = [e for e in read_reach_pool()
                if e["url"] not in liked and e["url"] not in self._reach_consumed]
        self.state.update(reach_pool=len(pool))
        if not pool:
            return None
        e = self._pick_diverse_reach(pool, self._recent_reach_tags,
                                     int(eng.get("reach_max_same_tag_streak", 2)))
        url = e["url"]
        self._reach_tag = e.get("tag", "")
        self._recent_reach_tags.append(e.get("source") or e.get("tag", ""))
        self._reach_consumed.add(url)
        return url

    def _do_reach_post(self, page, url: str, eng: dict) -> str:
        """Open a hashtag post, like it, and (optionally) view the poster's story.
        Emits a per-poster STEP for each stage (like the unfollow flow) so the feed
        shows the full flow under one expandable row. Returns
        'liked' / 'viewed' / 'ratelimit' / ''."""
        self._intent = "like"
        tag = getattr(self, "_reach_tag", "")
        # Per-POST grouping key, so each like is its OWN expandable row (one user can
        # be liked across many posts; we don't want them merged like the user-keyed
        # unfollow rows).
        m = re.search(r"/(?:p|reel|tv)/([^/?#]+)", url)
        pkey = "post:" + (m.group(1) if m else url)
        try:
            page.goto(url, wait_until="domcontentloaded")
            self._interruptible_sleep(random.uniform(1.5, 3.0))
        except Exception:
            return ""
        # Identify the poster (display only). None -> the row shows no @handle (and
        # never a bogus '@post' link).
        author = self._reach_post_author(page)
        disp = author or ""
        self._step(disp, f"opened #{tag} post" if tag else "opened post", key=pkey)

        self._step(disp, "liking post", key=pkey)
        liked = self._click_like(page)
        rate_limited = self._rate_limited(page)
        parts = []
        if liked:
            self._step(disp, "liked the post", "good", key=pkey)
            parts.append("liked post")
            self.state.update(reach_liked=self.state.snapshot().get("reach_liked", 0) + 1)
            self._day_record("likes", load_config())
        elif rate_limited:
            self._step(disp, "rate-limited", "bad", key=pkey)
        else:
            self._step(disp, "couldn't like", "bad", key=pkey)

        if not rate_limited and eng.get("reach_view_story", False) and author:
            self._step(disp, "checking story", key=pkey)
            try:
                if self._view_story(page, author):
                    self._step(disp, "viewed story", "good", key=pkey)
                    parts.append("viewed story")
                else:
                    self._step(disp, "no active story", key=pkey)
            except Exception:
                pass

        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        # Record the post so we never reopen/re-like it.
        append_log(_log_path("reach_liked_log", "data/reach_liked.log"),
                   f"{ts}\t{url}\t{author or ''}")
        self._reach_checked += 1
        if parts:
            self._reach_acted += 1
            self._story_today += 1
            new_count = self.state.snapshot().get("story_viewed_count", 0) + 1
            self.state.update(story_viewed_count=new_count,
                              last_message=f"reach @{author or '?'}: {', '.join(parts)}")
            self.state.emit("story_viewed", {
                "timestamp": ts, "username": disp, "key": pkey,
                "detail": ", ".join(parts) + (f" (#{tag})" if tag else "")})
        if self._reach_checked % 5 == 0:
            self.state.emit("log", {"level": "info",
                "msg": f"reach: checked {self._reach_checked} posts · acted on {self._reach_acted}"})
        if rate_limited:
            return "ratelimit"
        return "liked" if liked else ("viewed" if parts else "")

    def _reach_one(self, page) -> str:
        """One reach action, per `engagement.reach_source`:
        'prospects' (like a recent post of commenters/influencer-followers) or 'hashtags'
        (like public posts under niche hashtags) - both consume the burner-harvested
        reach_pool of post URLs - or 'pool' (legacy - engage candidate-pool members)."""
        cfg = load_config()
        eng = cfg.get("engagement", {}) or {}
        cap = int((cfg.get("limits", {}) or {}).get("likes_per_day", 100) or 0)
        if cap and self._story_today >= cap:
            return "cap"
        if (eng.get("reach_source") or "hashtags").lower() in ("hashtags", "prospects"):
            url = self._next_reach_post(page, eng)
            if not url:
                return ""
            return self._do_reach_post(page, url, eng)
        # legacy pool path
        if self._story_queue is None:
            self._story_queue = self._build_story_queue()
        if not self._story_queue:
            return ""
        return self._do_reach(page, self._story_queue.pop(0))

    def _drain_reach_to_cap(self, page, cfg, max_actions: int = None) -> str:
        """Dedicated reach-liking phase: keep liking pool posts until the daily LIKES cap
        is met, the reach pool empties, a rate-limit hits, or `max_actions` is reached.
        Run when follow+unfollow are done for the day but likes still have room, so the
        bot finishes ALL THREE caps instead of ending the day with likes short (reach
        otherwise only fires interleaved with follow/unfollow actions). Each like is
        paced + ledgered exactly like the interleaved tick.
        Returns 'stopped' / 'block' / 'cap' / 'empty' / 'progress'."""
        eng = cfg.get("engagement", {}) or {}
        if not eng.get("reach_enabled", False):
            return "cap"   # reach off → nothing to drain, treat as satisfied
        # Active work, not warmup: the churn cycle may have returned before any action
        # method set 'running' (both caps already met), leaving a stale 'warmup' status.
        self.state.update(status="running", phase_detail="finishing today's likes")
        did = 0
        misses = 0   # consecutive posts that couldn't be liked (button race / unlikeable)
        MAX_MISSES = 10
        while not self._stop_event.is_set():
            cfg = load_config()           # re-read so a pause / cap edit takes effect live
            if not self._can_act("likes", cfg):
                return "cap"              # likes capped (or outside active hours)
            if max_actions and did >= max_actions:
                return "progress"
            res = self._reach_one(page)
            if res == "cap":
                return "cap"
            if res == "ratelimit":
                # A reach rate-limit is a real IG signal - back off like the follow path.
                rc = self._handle_soft_block(cfg)
                if rc == "block":
                    return "block"
                continue                  # 'cooldown' → backed off, keep going
            if res in ("", None):
                # No like happened. A like that FAILED (button race / unlikeable post) is
                # consumed (logged), so the next pick differs - don't mistake one failure
                # for an empty pool. Only give up when the pool is actually drained, or
                # after a run of misses (so we don't spin if every post fails).
                if self._reach_pool_ready() <= 0:
                    return "empty"
                misses += 1
                if misses >= MAX_MISSES:
                    return "empty"
                self._interruptible_sleep(random.uniform(3.0, 7.0))   # brief gap, try next post
                continue
            misses = 0
            did += 1
            self.state.update(status="running",
                              phase_detail=f"finishing likes ({self._day_room('likes', cfg)} left)")
            eng = cfg.get("engagement", {}) or {}
            lo = float(eng.get("reach_like_min_delay", 30))
            hi = float(eng.get("reach_like_max_delay", 90))
            self._interruptible_sleep(random.uniform(lo, max(lo, hi)))
        return "stopped"

    def _maybe_story_reach_tick(self, page) -> str:
        """Interleaved story-reach for NON-CDP modes (where a 2nd concurrent tab
        isn't safe). In CDP mode the background worker handles it, so this no-ops.
        Likes one pool post every `reach_cadence` actions. Returns 'block' if a reach
        rate-limit escalated to a soft-block that should stop the run, else ''."""
        if self._story_worker_active:
            # Defer to the background worker - but self-heal if its thread died
            # (e.g. the 2nd CDP connection failed), so we don't silently stop
            # doing story-reach entirely.
            if self._story_thread is not None and self._story_thread.is_alive():
                return ""
            self._story_worker_active = False
            self.state.emit("log", {"level": "info",
                                    "msg": "story-reach: background worker gone - using interleaved mode"})
        cfg = load_config()
        eng = cfg.get("engagement", {}) or {}
        if not eng.get("reach_enabled", False):
            return ""
        # Respect the per-day like cap + active hours (reach is mostly likes).
        if not self._can_act("likes", cfg):
            return ""
        # Fire after a RANDOM number of actions (not every single one) so a like
        # doesn't follow every unfollow like clockwork - looks less robotic.
        self._story_tick += 1
        if self._story_tick < self._story_next:
            return ""
        self._story_tick = 0
        self._story_next = self._roll_reach_interval(eng)
        # A reach like that gets rate-limited is a real IG signal - back off like the
        # follow/unfollow paths do, instead of silently liking into the block.
        if self._reach_one(page) == "ratelimit":
            return self._handle_soft_block(cfg)
        return ""

    def _roll_reach_interval(self, eng) -> int:
        lo = int(eng.get("reach_cadence_min",
                         eng.get("reach_cadence_fallback", 1)) or 1)
        hi = int(eng.get("reach_cadence_max", lo + 3) or lo)
        lo = max(1, lo)
        return random.randint(lo, max(lo, hi))

    def _story_worker_loop(self) -> None:
        """Background story-reach: its OWN CDP connection + tab, running truly
        concurrently with the main run on its own cadence (independent of the main
        loop's pacing, pauses and long breaks). CDP-only - a second independent
        connection to the same Chrome; other connection models can't safely open a
        parallel session, so they use the interleaved tick instead."""
        browser_cfg = load_config().get("browser", {}) or {}
        endpoint = browser_cfg.get("cdp_endpoint") or ""
        if not endpoint:
            self._story_worker_active = False
            return
        try:
            with sync_playwright() as p:
                # timeout so a flaky 2nd CDP connection can't hang the worker forever
                sp_browser = p.chromium.connect_over_cdp(endpoint, timeout=15000)
                ctx = sp_browser.contexts[0] if sp_browser.contexts else sp_browser.new_context()
                page = ctx.new_page()   # our own dedicated tab
                n = len(self._build_story_queue())
                mode0 = (load_config().get("engagement", {}) or {}).get("reach_mode", "likes")
                self.state.emit("log", {"level": "info",
                    "msg": f"reach worker live ({mode0}) - {n} candidates queued"})
                try:
                    while not self._stop_event.is_set() and not self._story_stop.is_set():
                        # honor pause
                        if self._pause_event.is_set():
                            time.sleep(0.5)
                            continue
                        rc = load_config()
                        eng = rc.get("engagement", {}) or {}
                        if not eng.get("reach_enabled", False):
                            self._story_sleep(5)
                            continue
                        cap = int((rc.get("limits", {}) or {}).get("likes_per_day", 100) or 0)
                        if cap and self._story_today >= cap:
                            self._story_sleep(60)
                            continue
                        res = self._reach_one(page)
                        if res == "":
                            self._story_sleep(120)   # nothing available - wait, retry
                            continue
                        # Pace by action type: likes are rate-limited (slowest),
                        # story views are cheap, misses scan fast.
                        if res == "cap":
                            self._story_sleep(60)
                        elif res == "ratelimit":
                            self.state.emit("log", {"level": "info",
                                "msg": "reach: soft-blocked on likes - backing off"})
                            self._story_sleep(random.uniform(600, 1200))
                        elif res == "liked":
                            lo = float(eng.get("reach_like_min_delay", 30))
                            hi = float(eng.get("reach_like_max_delay", 90))
                            self._story_sleep(random.uniform(lo, max(lo, hi)))
                        elif res == "viewed":
                            lo = float(eng.get("story_min_delay", 8))
                            hi = float(eng.get("story_max_delay", 25))
                            self._story_sleep(random.uniform(lo, max(lo, hi)))
                        else:
                            self._story_sleep(random.uniform(2.0, 4.0))
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass
        except Exception as e:
            # Could not open/keep the 2nd CDP tab - hand story-reach back to the
            # interleaved fallback so it doesn't silently stop.
            self.state.emit("log", {"level": "error",
                "msg": f"story-reach worker failed ({e}) - falling back to interleaved mode"})
        finally:
            self._story_worker_active = False

    def _story_sleep(self, seconds: float) -> None:
        """Interruptible sleep for the story worker (own stop event, no state writes)."""
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if self._stop_event.is_set() or self._story_stop.is_set():
                return
            time.sleep(min(0.5, end - time.monotonic()))

    # --- run loop ---

    def _process_day(self, page, following, pacing, unfollowed_log, failed_log,
                     skipped_log, cap=None, set_gauge=True, max_actions=None,
                     cfg=None) -> str:
        """Run one daily batch of unfollows.

        Re-reads the whitelist and the done set (unfollowed + skipped) fresh, so
        exclusions added via the dashboard and prior progress are always honored.
        Returns one of: 'cap' (daily cap hit), 'exhausted' (nothing left),
        'stopped' (user stop), 'block' (5 consecutive failures).

        `cap` overrides the per-batch unfollow limit (defaults to pacing daily
        cap). `set_gauge=False` skips the follow/unfollow header gauge updates so
        this can run as a churn add-on (see `_process_list_trim`) without
        clobbering the churn progress display."""
        if cfg is None:
            cfg = load_config()
        if cap is None:
            cap = int((cfg.get("limits", {}) or {}).get("unfollows_per_day", 80))
        else:
            cap = int(cap)
        whitelist = load_whitelist()
        done_set = {row["username"].lower() for row in read_unfollowed_log()}
        done_set |= {row["username"].lower() for row in read_skipped_log()}
        # Permanently give up on 'poison' accounts that keep failing to unfollow (IG
        # missing-button bug) so they aren't retried every run and slow the bot.
        give_up_after = int((cfg.get("behavior", {}) or {}).get("unfollow_give_up_after", 3))
        done_set |= self._give_up_set(read_failed_log(), give_up_after)
        targets = [u for u in following if u not in whitelist and u not in done_set
                   and u not in self._run_skip]

        if set_gauge:
            self.state.update(
                status="running",
                total_targets=min(len(targets), cap),
                progress_index=0,
                phase_detail=f"{len(targets)} remaining in list",
            )
        else:
            self.state.update(
                status="running",
                phase_detail=f"list trim: {len(targets)} non-whitelisted left (cap {cap})",
            )
        if not targets:
            return "exhausted"

        processed = 0
        attempts = 0            # accounts touched this call (for the batch limit)
        consecutive_errors = 0  # only real unfollow failures, reset on success
        rate_limit_hits = 0     # soft-block ('Try Again Later') hits this batch
        for target in targets:
            if self._stop_event.is_set():
                return "stopped"
            if processed >= cap:
                self.state.update(phase_detail=f"daily cap {cap} reached")
                return "cap"
            if max_actions is not None and attempts >= max_actions:
                return "cap"   # batch limit - more may remain
            if not self._can_act("unfollows", cfg):
                return "cap"   # per-DAY cap reached or outside active hours
            attempts += 1

            if set_gauge:
                self.state.update(current_target=target, progress_index=processed + 1)
            else:
                self.state.update(current_target=target)

            try:
                result = self._unfollow(page, target)
            except Exception as e:
                result = "error"
                self.state.emit("log", {"level": "error", "msg": f"exception on {target}: {e}"})

            if result == "checkpoint":
                return "checkpoint"
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            if result == "ok":
                append_log(unfollowed_log, f"{ts}\t{target}")
                new_count = self.state.snapshot()["unfollowed_count"] + 1
                self.state.update(unfollowed_count=new_count, last_message=f"unfollowed @{target}")
                self.state.emit("unfollowed", {"timestamp": ts, "username": target, "note": ""})
                processed += 1
                self._day_record("unfollows", cfg)
                consecutive_errors = 0
                self._adjust_following(-1)
                self._tick_resync(page)
            elif result == "not_following":
                append_log(skipped_log, f"{ts}\t{target}\tnot_following")
                self.state.emit("skipped", {"timestamp": ts, "username": target, "reason": "not_following"})
                self.state.update(last_message=f"already not following @{target} (skipped)")
                consecutive_errors = 0
            elif result == "private_or_missing":
                # Deleted/unavailable account (common among very old follows).
                # Recorded as skipped (done, not retried); not a block signal and
                # NOT a real unfollow, so it stays out of the unfollowed list.
                append_log(skipped_log, f"{ts}\t{target}\tunavailable")
                self.state.emit("skipped", {"timestamp": ts, "username": target, "reason": "unavailable"})
                self.state.update(last_message=f"unavailable @{target} (skipped)")
            elif result.startswith("no_button_no_posts"):
                # Header button is broken (IG bug) AND there are no posts to use
                # the fallback '...' menu on - there's no UI path to unfollow, so
                # mark it skipped (done) instead of failing it forever.
                append_log(skipped_log, f"{ts}\t{target}\tno_unfollow_path")
                self.state.emit("skipped", {"timestamp": ts, "username": target, "reason": "no_unfollow_path"})
                self.state.update(last_message=f"no unfollow path for @{target} (skipped)")
                consecutive_errors = 0
            elif result.startswith("rate_limited"):
                # IG soft-blocked the unfollow action ('Try Again Later'). Hitting
                # it again immediately just extends the block, so we back off for a
                # cooldown before continuing, and give up the batch after a few hits
                # so we don't burn the whole window against a wall.
                append_log(failed_log, f"{ts}\t{target}\t{result}")
                new_failed = self.state.snapshot()["failed_count"] + 1
                consecutive_errors = 0  # not a selector failure - handled separately
                self.state.update(failed_count=new_failed,
                                  last_message=f"rate-limited @{target} (soft block)")
                self.state.emit("failed", {"timestamp": ts, "username": target, "reason": "rate_limited"})
                if self._handle_soft_block(cfg) == "block":
                    return "block"
                if self._stop_event.is_set():
                    return "stopped"
                continue
            else:
                append_log(failed_log, f"{ts}\t{target}\t{result}")
                self._run_skip.add(target)   # don't re-attempt this run (poison guard)
                new_failed = self.state.snapshot()["failed_count"] + 1
                consecutive_errors += 1
                self.state.update(failed_count=new_failed, last_message=f"failed @{target}: {result}")
                self.state.emit("failed", {"timestamp": ts, "username": target, "reason": result})
                if consecutive_errors >= 5:
                    return "block"

            if self._stop_event.is_set():
                return "stopped"

            # Interleaved story-reach marketing - ticks on EVERY processed target
            # (not just real unfollows), so it runs even through a stretch of skips.
            if self._maybe_story_reach_tick(page) == "block":
                return "block"

            # Pacing applies to REAL unfollows only - Instagram rate-limits the
            # unfollow ACTION, not page visits. Skips/failures (deleted,
            # not-following) continue after just a brief pause so we blow through
            # dead accounts quickly instead of waiting minutes.
            if result == "ok":
                self._jitter(
                    pacing["action_delay_min"],
                    pacing["action_delay_max"],
                    pacing.get("distraction_chance", 0),
                    pacing.get("distraction_min", 0),
                    pacing.get("distraction_max", 0),
                )
                if processed > 0 and processed % pacing["long_break_every"] == 0:
                    pause = random.uniform(
                        pacing["long_break_min"],
                        pacing["long_break_max"],
                    )
                    self.state.update(phase_detail=f"long break {pause:.0f}s")
                    self._interruptible_sleep(pause)
            else:
                self._jitter(1.0, 3.0)

        return "exhausted"

    _DONE_SET_TTL = 10.0   # seconds; the log-derived part is memoized this long

    def _follow_done_set(self, whitelist: set[str], my_username: str) -> set[str]:
        """Accounts we must never (re-)follow: already followed, permanently
        skipped, churned off, currently in our following list, whitelisted, or
        ourselves. Transient skips/failures are intentionally left out so they
        get retried.

        The log-derived part (4 logs + the following cache) is MEMOIZED for a few
        seconds: this set is recomputed up to 4-5x per scraper cycle AND once per
        follow, and re-reading those (growing) files each time is a major Pi cost.
        Re-following within a run is already prevented by the loop's in-run `seen`
        set, so a short staleness window here is safe; the whitelist + own handle are
        re-unioned fresh on every call (cheap)."""
        now = time.monotonic()
        cache = getattr(self, "_done_logs_cache", None)
        if cache is None or now - cache[0] > self._DONE_SET_TTL:
            logs = {row["username"].lower() for row in read_followed_log()}
            logs |= {
                row["username"].lower() for row in read_follow_skipped_log()
                if row["reason"] in PERMANENT_FOLLOW_SKIPS
            }
            logs |= {row["username"].lower() for row in read_churn_unfollowed_log()}
            logs |= {u.lower() for u in read_following_cache()}
            self._done_logs_cache = (now, logs)
        done = set(self._done_logs_cache[1])   # copy so the union below can't mutate it
        done |= set(whitelist)
        if my_username:
            done.add(my_username.lower())
        return done

    @staticmethod
    def _give_up_set(rows, threshold: int, reason_prefix: Optional[str] = None) -> set:
        """Usernames that have FAILED >= `threshold` times in a failure log (optionally
        only failures whose reason starts with `reason_prefix`). These are 'poison'
        accounts - usually IG's missing-unfollow-button bug - that fail the whole
        fallback chain every run; we stop retrying them so they don't waste a minute
        each and slow the bot. Persisted via the log, so it survives restarts."""
        if threshold <= 0:
            return set()
        counts: dict = {}
        for r in rows:
            if reason_prefix and not str(r.get("reason") or "").startswith(reason_prefix):
                continue
            u = (r.get("username") or "").lower()
            if u:
                counts[u] = counts.get(u, 0) + 1
        return {u for u, n in counts.items() if n >= threshold}

    @staticmethod
    def _seed_tier_weight(source: str) -> int:
        """Intent tier weight for follow priority (higher = preferred). Commenters are
        ACTIVE users engaging in the niche -> top priority; hashtag posters / post
        likers are engaged; followers are passive backfill."""
        s = (source or "").lower()
        if s.startswith("commenters"):
            return 4
        if s.startswith("hashtag") or s.startswith("likers"):
            return 2
        return 1   # followers / other

    @staticmethod
    def _wilson_lower(k: int, n: int, z: float = 1.64) -> float:
        """Lower bound of the Wilson score interval for a binomial rate (one-sided
        ~90%). A conservative follow-back rate that doesn't over-trust small samples."""
        if n <= 0:
            return 0.0
        phat = k / n
        denom = 1.0 + z * z / n
        centre = phat + z * z / (2 * n)
        margin = z * ((phat * (1 - phat) + z * z / (4 * n)) / n) ** 0.5
        return max(0.0, (centre - margin) / denom)

    _PERF_MIN_SAMPLE = 12     # measured outcomes needed before a source is steered by data
    _PERF_TTL = 60.0

    def _source_perf(self) -> dict:
        """Per-source follow-back PERFORMANCE weight from measured outcomes (memoized).
        A source with >= _PERF_MIN_SAMPLE measured follow-backs gets a weight derived
        from its Wilson-lower-bound rate - high converters favored, poor ones still get
        occasional exploration (floor 0.25). Under that sample it's left out (treated as
        neutral 1.0 by the picker) so new/unproven sources are explored, not judged on
        noise. Closes the loop: measured follow-back rate steers future follow priority."""
        now = time.monotonic()
        cache = getattr(self, "_perf_cache", None)
        if cache and now - cache[0] < self._PERF_TTL:
            return cache[1]
        agg: dict = {}
        for r in read_follow_outcomes():
            a = agg.setdefault(r.get("source") or "", [0, 0])
            a[1] += 1
            if r.get("followed_back"):
                a[0] += 1
        perf = {s: round(0.25 + self._wilson_lower(k, n) * 4.0, 3)
                for s, (k, n) in agg.items() if n >= self._PERF_MIN_SAMPLE}
        self._perf_cache = (now, perf)
        return perf

    @staticmethod
    def _pick_diverse_candidate(eligible: list[dict], recent_seeds, max_streak: int,
                                perf: dict = None) -> dict:
        """Pick the next account to follow: PRIORITY by source type, RANDOM within it.

        Seeds are grouped into intent tiers (commenters > hashtag/likers > followers).
        We pick a TIER weighted by `_seed_tier_weight` - so commenters are preferred
        regardless of how many follower seeds exist - then a RANDOM seed in that tier,
        then a RANDOM member. Type-priority gives active users the lead; the random
        seed/member within a tier keeps any single source (e.g. one big follower or
        commenter list) from dominating. The recent-seed window still blocks a seed
        that's appeared `max_streak` times in a row so runs stay mixed.

        `eligible` is the pending list; `recent_seeds` is the last few seeds picked
        (most recent last). max_streak<=0 disables the streak guard."""
        if not eligible:
            return None
        groups = {}
        for c in eligible:
            groups.setdefault(c.get("source", ""), []).append(c)
        seeds = list(groups.keys())
        if max_streak > 0 and len(seeds) > 1:
            recent = list(recent_seeds)[-max_streak:]
            avail = [s for s in seeds if recent.count(s) < max_streak]
            if avail:
                seeds = avail
        # Bucket the available seeds by tier weight, pick a tier weighted (commenters
        # favored), then random within - priority by type, no single-seed bias.
        tiers: dict[int, list] = {}
        for s in seeds:
            tiers.setdefault(Bot._seed_tier_weight(s), []).append(s)
        weights = list(tiers.keys())
        chosen = random.choices(weights, weights=weights, k=1)[0]
        tier_seeds = tiers[chosen]
        # Within the chosen tier, favor seeds that historically convert better (measured
        # follow-back rate); unproven seeds are neutral (1.0). Weighted-random, so it
        # steers toward winners without ever fully starving the others - and it's WITHIN
        # a tier, so it never reintroduces cross-type (count) bias.
        if perf:
            seed = random.choices(tier_seeds,
                                  weights=[perf.get(s, 1.0) for s in tier_seeds], k=1)[0]
        else:
            seed = random.choice(tier_seeds)
        return random.choice(groups[seed])

    def _process_follow_day(self, page, cfg, max_actions=None) -> str:
        """Run one daily batch of follows pulled from the candidate pool.

        Mirrors _process_day: re-reads the whitelist + done set fresh, paces only
        real follows, backs off on soft blocks, and aborts on a run of failures.
        Returns 'cap' / 'exhausted' / 'stopped' / 'block' / 'rest'. max_actions caps
        the accounts touched this call (for interleaving) - 'cap' when that batch
        limit is hit (more may remain), 'exhausted' only when the pool is empty.
        'rest' = a sustained run of follow failures (IG gating) - the caller backs
        off and resumes (the streak is tracked across batches, not just this call)."""
        pacing = cfg["pacing"]
        targeting = cfg.get("targeting", {}) or {}
        filters = targeting.get("filters", {}) or {}
        disc_cfg = targeting.get("discovery", {}) or {}
        eng_cfg = cfg.get("engagement", {}) or {}
        daily_cap = int((cfg.get("limits", {}) or {}).get("follows_per_day", 80))
        min_delay = pacing.get("action_delay_min", 60)
        max_delay = pacing.get("action_delay_max", 200)

        followed_log = _log_path("followed_log", "data/followed.log")
        skipped_log = _log_path("follow_skipped_log", "data/follow_skipped.log")
        failed_log = _log_path("follow_failed_log", "data/follow_failed.log")

        whitelist = load_whitelist()
        my_username = (os.getenv("IG_USERNAME") or "").lower()
        done_set = self._follow_done_set(whitelist, my_username)

        # Source diversity: don't follow the same seed (target's followers / one
        # hashtag / one post) more than this many times in a row while another
        # seed is available, so any window of follows is spread across sources.
        # 0/1 = strongest spread; <=0 disables (pure intent-priority order).
        max_same_seed = int(targeting.get("max_same_seed_streak", 2))

        processed = 0
        attempts = 0             # accounts touched this call (for the batch limit)
        consecutive_errors = 0
        rate_limit_hits = 0
        # Seed from the run-wide skip set so a poison account that hard-failed
        # earlier this run isn't re-attempted every interleaved batch.
        seen: set[str] = set(self._run_skip)

        # follow_candidates is the ELIGIBLE result list (scraper-vetted). The bot is
        # consume-only - the burner scraper service is the sole source - so we just
        # re-read it every follow to pick up newly-vetted accounts immediately.
        while not self._stop_event.is_set():
            if processed >= daily_cap:
                self.state.update(phase_detail=f"daily cap {daily_cap} reached")
                return "cap"
            if max_actions is not None and attempts >= max_actions:
                return "cap"   # batch limit - more may remain, caller re-invokes
            if not self._can_act("follows", cfg):
                return "cap"   # per-DAY cap reached or outside active hours

            done_set = self._follow_done_set(whitelist, my_username)
            candidates = read_follow_candidates()
            eligible = [c for c in candidates
                        if c["username"] not in done_set and c["username"] not in seen]

            self.state.update(
                status="running", daily_cap=daily_cap, candidate_pool=len(eligible),
                total_targets=processed + min(len(eligible), max(0, daily_cap - processed)),
                progress_index=processed + 1,
                phase_detail=f"{len(eligible)} eligible candidate(s) to follow",
            )
            if not eligible:
                if processed == 0:
                    self.state.update(
                        phase_detail="no eligible candidates yet"
                                     + ("" if candidates else " (pool empty)"),
                        last_message="nothing to follow - waiting for the scraper / sources",
                    )
                return "exhausted"

            # Pick for SOURCE DIVERSITY: random seed, then random member of it, so
            # follows spread evenly across all sources (equal odds per seed) instead
            # of draining one target's followers - the anti-bias picker.
            cand = self._pick_diverse_candidate(
                eligible, self._recent_follow_seeds, max_same_seed, self._source_perf())
            target = cand["username"]
            source = cand.get("source", "")
            seen.add(target)
            attempts += 1
            self.state.update(current_target=target, progress_index=processed + 1)

            try:
                result = self._follow(page, target, filters, disc_cfg, lean=True)
            except Exception as e:
                result = "error"
                self.state.emit("log", {"level": "error", "msg": f"exception on {target}: {e}"})

            if result == "checkpoint":
                return "checkpoint"
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            if result == "ok":
                append_log(followed_log, f"{ts}\t{target}\t{source}")
                # Record the seed so the next pick steers away from it (diversity
                # tracks real follows only — the scarce, IG-visible action).
                self._recent_follow_seeds.append(source)
                new_count = self.state.snapshot()["followed_count"] + 1
                self.state.update(followed_count=new_count, last_message=f"followed @{target}")
                self.state.emit("followed", {"timestamp": ts, "username": target, "source": source})
                processed += 1
                self._day_record("follows", cfg)
                consecutive_errors = 0
                self._follow_fail_streak = 0   # a real follow clears the gated-rest streak
                self._adjust_following(+1)
                self._tick_resync(page)
                # Extra exposure touches while we're still on the profile.
                if self._engage_after_follow(page, target, eng_cfg) == "block":
                    return "block"
            elif result in ("already_following", "skipped_follows_you", "skipped_private",
                            "skipped_no_posts", "skipped_filtered", "unavailable"):
                reason = {
                    "already_following": "already_following",
                    "skipped_follows_you": "follows_you",
                    "skipped_private": "private",
                    "skipped_no_posts": "no_posts",
                    "skipped_filtered": "filtered",
                    "unavailable": "unavailable",
                }[result]
                append_log(skipped_log, f"{ts}\t{target}\t{reason}")
                self.state.emit("follow_skipped", {"timestamp": ts, "username": target, "reason": reason})
                self.state.update(last_message=f"skipped @{target} ({reason})")
                consecutive_errors = 0
                self._follow_fail_streak = 0   # a clean skip means follows still work
            elif result.startswith("rate_limited"):
                append_log(failed_log, f"{ts}\t{target}\t{result}")
                new_failed = self.state.snapshot()["follow_failed_count"] + 1
                consecutive_errors = 0
                self._follow_fail_streak = 0   # handled by the soft-block cooldown below
                self.state.update(follow_failed_count=new_failed,
                                  last_message=f"rate-limited @{target} (soft block)")
                self.state.emit("follow_failed", {"timestamp": ts, "username": target, "reason": "rate_limited"})
                if self._handle_soft_block(cfg) == "block":
                    return "block"
                if self._stop_event.is_set():
                    return "stopped"
                continue
            else:
                append_log(failed_log, f"{ts}\t{target}\t{result}")
                self._run_skip.add(target)   # don't re-attempt this run (poison guard)
                new_failed = self.state.snapshot()["follow_failed_count"] + 1
                consecutive_errors += 1
                # Streak persists ACROSS interleaved batches (unlike the per-call
                # consecutive_errors), so a churn cycle that follows 4 at a time can't
                # mask a sustained gating run. A run of failures = IG is gating follows;
                # rest (close the browser, let the scraper work + IG cool down) instead
                # of hammering. The caller turns this into a backed-off resume.
                self._follow_fail_streak += 1
                rest_at = int((cfg.get("safety", {}) or {}).get("follow_fail_rest_threshold", 5))
                self.state.update(follow_failed_count=new_failed,
                                  last_message=f"failed @{target}: {result} "
                                               f"({self._follow_fail_streak}/{rest_at})")
                self.state.emit("follow_failed", {"timestamp": ts, "username": target, "reason": result})
                if rest_at > 0 and self._follow_fail_streak >= rest_at:
                    return "rest"

            if self._stop_event.is_set():
                return "stopped"

            if self._maybe_story_reach_tick(page) == "block":   # interleaved story-reach, every target
                return "block"

            # Pace real follows only - IG rate-limits the follow ACTION, not page
            # visits. Skips/failures continue after a brief pause so we blow
            # through dead/ineligible accounts quickly.
            if result == "ok":
                self._jitter(
                    min_delay,
                    max_delay,
                    pacing.get("distraction_chance", 0),
                    pacing.get("distraction_min", 0),
                    pacing.get("distraction_max", 0),
                )
                if processed > 0 and processed % pacing["long_break_every"] == 0:
                    pause = random.uniform(
                        pacing["long_break_min"],
                        pacing["long_break_max"],
                    )
                    self.state.update(phase_detail=f"long break {pause:.0f}s")
                    self._interruptible_sleep(pause)
            else:
                self._jitter(1.0, 3.0)

        return "stopped"

    # --- churn (follow -> wait -> unfollow non-followers-back) ---

    def _process_churn_cycle(self, page, cfg) -> str:
        """One churn cycle, INTERLEAVED: instead of doing all unfollows then all
        follows, it alternates them in small batches by a ratio (churn.
        interleave_unfollows : interleave_follows), so both progress together and
        the activity reads as steady mixed marketing. The story-reach layer keeps
        firing inside each batch (every action calls the reach tick), so reach runs
        alongside automatically. Returns 'cap'/'exhausted'/'stopped'/'block'/'rest'."""
        mkt = cfg.get("marketing", {}) or {}
        limits = cfg.get("limits", {}) or {}
        unf_per = max(1, int(mkt.get("ratio_unfollows", 2)))
        fol_per = max(1, int(mkt.get("ratio_follows", 1)))
        also_trim = bool(mkt.get("also_trim_following", False))

        # Approximate per-cycle budgets (rounds × batch). Real actions may be fewer
        # when accounts skip, which only makes us gentler - safe.
        u_cap = int(limits.get("unfollows_per_day", 80))
        t_cap = int(mkt.get("list_trim_cap", 40))
        f_cap = int(limits.get("follows_per_day", 80))
        u_used = t_used = f_used = 0
        # 'dead' = that source ran out of accounts (exhausted) this cycle.
        u_dead = False
        t_dead = not also_trim
        f_dead = False
        # Ratio gate baseline: count REAL actions this cycle (state deltas), so follows
        # can't outrun unfollows while unfollow supply remains. churn unfollows bump
        # churn_unfollowed_count, list-trim bumps unfollowed_count, follows bump
        # followed_count. When BOTH unfollow sources are exhausted we drop the gate and
        # let follows run to the cap (keep-following-to-cap when there's nothing to churn).
        base = self.state.snapshot()
        base_f = base.get("followed_count", 0) or 0
        base_u = (base.get("churn_unfollowed_count", 0) or 0) + (base.get("unfollowed_count", 0) or 0)

        while not self._stop_event.is_set():
            progressed = False
            # Once today's UNFOLLOW cap is hit there's no unfollow work left today, so
            # skip the unfollow/trim batches AND drop the follow ratio gate - otherwise
            # follows stay throttled to their ratio share of unfollows that can no longer
            # happen, dripping ~1 follow per cycle and repeatedly yielding to the scraper.
            # With the gate dropped, follows run straight to their own daily cap.
            unf_capped = self._day_room("unfollows", cfg) <= 0
            # Both follow + unfollow caps met → nothing left for churn this day. Return
            # so the caller can drain the reach pool to finish today's LIKES (the day
            # isn't "done" until all three caps are, when reach can still progress).
            if unf_capped and self._day_room("follows", cfg) <= 0:
                return "exhausted"

            # --- unfollow batch: aged-review first ---
            if not u_dead and u_used < u_cap and not unf_capped:
                out = self._process_churn_unfollows(page, cfg, max_actions=unf_per)
                if out in ("stopped", "block", "checkpoint"):
                    return out
                u_used += unf_per
                u_dead = (out == "exhausted")
                progressed = progressed or not u_dead
            # --- unfollow batch: list-trim (shrink the existing following list) ---
            if not t_dead and t_used < t_cap and not unf_capped:
                out = self._process_list_trim(page, cfg, max_actions=unf_per)
                if out in ("stopped", "block", "checkpoint"):
                    return out
                t_used += unf_per
                t_dead = (out == "exhausted")
                progressed = progressed or not t_dead
            # --- follow batch (ratio-gated while unfollows still have work today) ---
            if not f_dead and f_used < f_cap:
                # While any unfollow source still has accounts AND the unfollow cap isn't
                # spent, hold follows to their ratio share of REAL unfollows so a refilled
                # follow pool can't balloon the following count ahead of churn (the
                # overnight follow-only drift). A small starter allowance (fol_per) lets
                # the first follows run at u=0. Once unfollows are capped, the gate drops.
                unfollow_supply = (not (u_dead and t_dead)) and not unf_capped
                snap = self.state.snapshot()
                real_f = (snap.get("followed_count", 0) or 0) - base_f
                real_u = ((snap.get("churn_unfollowed_count", 0) or 0)
                          + (snap.get("unfollowed_count", 0) or 0)) - base_u
                allowed_f = (real_u * fol_per) // unf_per + fol_per
                if unfollow_supply and real_f >= allowed_f:
                    self.state.update(phase_detail=(
                        f"holding follows for {fol_per}:{unf_per} ratio "
                        f"({real_f} follows : {real_u} unfollows this cycle)"))
                    # don't touch f_used/progressed - let the next round catch unfollows up
                else:
                    out = self._process_follow_day(page, cfg, max_actions=fol_per)
                    if out in ("stopped", "block", "checkpoint", "rest"):
                        return out
                    f_used += fol_per
                    f_dead = (out == "exhausted")
                    progressed = progressed or not f_dead

            u_busy = (not u_dead and u_used < u_cap and not unf_capped)
            t_busy = (not t_dead and t_used < t_cap and not unf_capped)
            f_busy = (not f_dead and f_used < f_cap)
            if not (u_busy or t_busy or f_busy):
                return "exhausted"   # every source capped or drained
            if not progressed:
                return "exhausted"   # a full round did nothing - back off (keep_running re-checks)
        return "stopped"

    def _process_list_trim(self, page, cfg, max_actions=None) -> str:
        """Churn add-on: unfollow non-whitelisted accounts from the existing
        following cache (data/following.json), reusing the full unfollow
        fallback chain. Uses its own small cap (churn.list_unfollow_cap) so it
        doesn't starve the follow/churn budget or trip rate limits."""
        if not FOLLOWING_CACHE.exists():
            return "exhausted"
        try:
            following = json.loads(FOLLOWING_CACHE.read_text(encoding="utf-8"))
        except Exception:
            following = []
        if not following:
            return "exhausted"
        cap = int((cfg.get("marketing", {}) or {}).get("list_trim_cap", 40))
        self.state.update(phase_detail=f"churn add-on: trimming following list (cap {cap})")
        return self._process_day(
            page, following, cfg["pacing"],
            _log_path("unfollowed_log", "data/unfollowed.log"),
            _log_path("failed_log", "data/failed.log"),
            _log_path("skipped_log", "data/skipped.log"),
            cap=cap, set_gauge=False, max_actions=max_actions, cfg=cfg,
        )

    def _process_churn_unfollows(self, page, cfg, max_actions=None) -> str:
        """Visit follows older than unfollow_after_days; keep the ones who
        followed back, unfollow the rest (up to daily_unfollow_cap).

        max_actions caps how many accounts are touched this call (for interleaving)
        - 'cap' is returned when that batch limit is hit (more may remain),
        'exhausted' only when nothing is left to review."""
        mkt = cfg.get("marketing", {}) or {}
        pacing = cfg["pacing"]
        after_days = float(mkt.get("unfollow_after_days", 4))
        keep_back = bool(mkt.get("keep_followers_back", True))
        daily_unfollow_cap = int((cfg.get("limits", {}) or {}).get("unfollows_per_day", 80))
        min_delay = pacing.get("action_delay_min", 60)
        max_delay = pacing.get("action_delay_max", 200)

        churn_log = _log_path("churn_unfollowed_log", "data/churn_unfollowed.log")
        kept_log = _log_path("follow_kept_log", "data/follow_kept.log")
        failed_log = _log_path("follow_failed_log", "data/follow_failed.log")
        outcomes_log = _log_path("follow_outcomes_log", "data/follow_outcomes.log")

        # Stop re-checking accounts we've already resolved.
        resolved = {r["username"].lower() for r in read_follow_kept_log()}
        resolved |= {r["username"].lower() for r in read_churn_unfollowed_log()}
        whitelist = load_whitelist()
        # Permanently give up on 'poison' accounts that have failed to unfollow this many
        # times (IG missing-button bug) - persisted in the log, so they're never retried
        # across runs and stop wasting a minute each.
        give_up_after = int((cfg.get("behavior", {}) or {}).get("unfollow_give_up_after", 3))
        gave_up = self._give_up_set(read_follow_failed_log(), give_up_after, reason_prefix="churn:")
        # username -> source, for per-source conversion analytics recorded below.
        source_map = {r["username"].lower(): r.get("source", "")
                      for r in read_followed_log()}

        now = time.time()
        cutoff = after_days * 86400
        due, seen = [], set()
        for row in read_followed_log():
            u = row["username"].lower()
            if (u in resolved or u in whitelist or u in seen or u in self._run_skip
                    or u in gave_up):
                continue
            ts = parse_log_ts(row["timestamp"])
            if ts is None or (now - ts) < cutoff:
                continue
            seen.add(u)
            due.append(u)

        self.state.update(status="running", current_target=None,
                          phase_detail=(f"churn: {len(due)} follow(s) due for review"
                                        + (f" ({len(gave_up)} poison skipped)" if gave_up else "")))
        if not due:
            return "exhausted"

        processed = 0          # real unfollows (counts toward the cap)
        attempts = 0           # accounts touched this call (for the batch limit)
        consecutive_errors = 0
        rate_limit_hits = 0
        for u in due:
            self._intent = "unfollow"   # churn review → unfollow; tags the review steps
            if self._stop_event.is_set():
                return "stopped"
            if processed >= daily_unfollow_cap:
                self.state.update(phase_detail=f"churn cap {daily_unfollow_cap} reached")
                return "cap"
            if max_actions is not None and attempts >= max_actions:
                return "cap"   # batch limit - more may remain, caller re-invokes
            if not self._can_act("unfollows", cfg):
                return "cap"   # per-DAY cap reached or outside active hours
            attempts += 1

            self.state.update(current_target=u)
            ts = time.strftime("%Y-%m-%d %H:%M:%S")

            # Reciprocity check on the loaded profile before deciding.
            self._step(u, "churn review - checking follow-back")
            try:
                page.goto(f"https://www.instagram.com/{u}/", wait_until="domcontentloaded")
                self._jitter(2.0, 4.0)
            except Exception:
                pass

            # Measure reciprocity for per-source analytics regardless of the
            # keep_back setting - the churn visit is the natural measurement point.
            follows_back = self._follows_you(page)
            src = source_map.get(u, "")

            if keep_back and follows_back:
                self._step(u, "followed back - keeping", "good")
                append_log(kept_log, f"{ts}\t{u}")
                append_log(outcomes_log, f"{ts}\t{u}\t{src}\t1")
                self.state.emit("follow_kept", {"timestamp": ts, "username": u})
                self.state.update(last_message=f"@{u} followed back - kept")
                self._jitter(1.0, 3.0)
                continue
            self._step(u, "no follow-back - unfollowing" if not follows_back
                       else "followed back (keep off) - unfollowing")

            try:
                result = self._unfollow(page, u)
            except Exception as e:
                result = "error"
                self.state.emit("log", {"level": "error", "msg": f"churn exception on {u}: {e}"})

            if result == "checkpoint":
                return "checkpoint"

            if result == "ok" or result == "not_following" or result == "private_or_missing" \
                    or result.startswith("no_button_no_posts"):
                # Either we unfollowed them, or there's nothing left to unfollow -
                # resolved either way, so record it (done-set) and stop re-checking.
                append_log(churn_log, f"{ts}\t{u}")
                append_log(outcomes_log, f"{ts}\t{u}\t{src}\t{'1' if follows_back else '0'}")
                if result == "ok":
                    # A REAL unfollow happened - count it + feed the live churn gauge.
                    new_count = self.state.snapshot()["churn_unfollowed_count"] + 1
                    self.state.emit("churn_unfollowed", {"timestamp": ts, "username": u})
                    self.state.update(churn_unfollowed_count=new_count,
                                      last_message=f"churned @{u} (didn't follow back)")
                    processed += 1
                    self._day_record("unfollows", cfg)
                    consecutive_errors = 0
                    self._adjust_following(-1)
                    self._tick_resync(page)
                    self._jitter(
                        min_delay, max_delay,
                        pacing.get("distraction_chance", 0),
                        pacing.get("distraction_min", 0),
                        pacing.get("distraction_max", 0),
                    )
                    if processed > 0 and processed % pacing["long_break_every"] == 0:
                        pause = random.uniform(pacing["long_break_min"],
                                               pacing["long_break_max"])
                        self.state.update(phase_detail=f"long break {pause:.0f}s")
                        self._interruptible_sleep(pause)
                else:
                    # Nothing was actually unfollowed - don't report it as a churn.
                    self.state.update(last_message=f"@{u} already not followed (resolved)")
                    self._jitter(1.0, 3.0)
            elif result.startswith("rate_limited"):
                consecutive_errors = 0
                self.state.update(last_message=f"rate-limited churning @{u} (soft block)")
                if self._handle_soft_block(cfg) == "block":
                    return "block"
                if self._stop_event.is_set():
                    return "stopped"
            else:
                append_log(failed_log, f"{ts}\t{u}\tchurn:{result}")
                self._run_skip.add(u)   # don't re-attempt this run (poison guard)
                consecutive_errors += 1
                # Churn IS an unfollow, so count it toward the run's unfollow-fail
                # tally (the Overview failures tile shows follow / unfollow).
                self.state.update(failed_count=self.state.snapshot().get("failed_count", 0) + 1,
                                  last_message=f"churn failed @{u}: {result}")
                self.state.emit("follow_failed", {"timestamp": ts, "username": u, "reason": f"churn:{result}"})
                if consecutive_errors >= 5:
                    return "block"
                self._jitter(1.0, 3.0)

            if self._maybe_story_reach_tick(page) == "block":   # interleaved story-reach, every target
                return "block"

        return "exhausted"

    # --- browser connection (shared by the run loop and one-shot scrapes) ---

    _WEBDRIVER_MASK = "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"

    def _setup_context(self, context, browser_cfg):
        """Shared post-creation hardening: webdriver mask. (Image blocking is done
        via a native Chromium launch flag instead of per-request routing - routing
        every request through Python is too slow on the Pi and was causing header
        load timeouts.)"""
        try:
            context.add_init_script(self._WEBDRIVER_MASK)
        except Exception:
            pass

    def _connect(self, p, browser_cfg):
        """Open/attach a browser per config and return
        (browser, context, page, using_cdp, using_persistent).

        Mirrors the three connection models: CDP (attach to a Chrome the user
        started with --remote-debugging-port; never closed), persistent profile
        (Pi-native login dir), or ephemeral browser + session.json. Does NOT log
        in or warm up - the caller handles that."""
        cdp_endpoint = browser_cfg.get("cdp_endpoint") or ""
        using_cdp = bool(cdp_endpoint)
        user_data_dir = browser_cfg.get("user_data_dir") or ""
        using_persistent = bool(user_data_dir) and not using_cdp

        # Common Chromium launch flags (shared by persistent + ephemeral).
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--no-default-browser-check",
            "--no-first-run",
            "--no-sandbox",   # required when running as a service on the Pi
            "--disable-gpu",  # headless Pi has no GPU/display; silences ANGLE/EGL errors
        ]
        # Block images natively (no per-request Python overhead) - big speed/RAM win
        # on the Pi. Only for launched browsers; a user-launched CDP Chrome must set
        # this flag itself. We need only header text/buttons/action-bar, not pixels.
        if browser_cfg.get("block_media", True):
            launch_args.append("--blink-settings=imagesEnabled=false")

        def _browser_binary(kwargs: dict) -> None:
            # On ARM/Pi, use the distro Chromium via executable_path or channel.
            if browser_cfg.get("executable_path"):
                kwargs["executable_path"] = browser_cfg["executable_path"]
            elif browser_cfg.get("channel"):
                kwargs["channel"] = browser_cfg["channel"]

        browser = None  # set only for cdp/ephemeral; persistent has no separate browser

        if using_cdp:
            # Connect to a Chrome instance the user already started with
            # --remote-debugging-port=<port>. We DO NOT close this browser.
            self.state.update(phase_detail=f"connecting to Chrome at {cdp_endpoint}")
            browser = p.chromium.connect_over_cdp(cdp_endpoint)
            contexts = browser.contexts
            context = contexts[0] if contexts else browser.new_context()
            # Reuse an existing instagram.com tab if one is open; otherwise
            # open our own tab. Never hijack an arbitrary tab (e.g. the
            # dashboard) by blindly grabbing pages[0].
            page = None
            for pg in context.pages:
                if "instagram.com" in (pg.url or ""):
                    page = pg
                    break
            if page is None:
                page = context.new_page()
            # CDP Chrome is user-launched; we can't set proxy/UA here, but we can
            # still add the webdriver mask + heavy-request blocking on its context.
            self._setup_context(context, browser_cfg)
        elif using_persistent:
            # Permanent Pi-native login: a persistent profile dir that
            # keeps you logged in across runs/reboots. Log in ONCE on the
            # Pi (see DEPLOY.md) and the bot reuses it headlessly forever.
            self.state.update(phase_detail=f"opening persistent profile {user_data_dir}")
            pkwargs = {
                "user_data_dir": user_data_dir,
                "headless": browser_cfg["headless"],
                "args": launch_args,
                "viewport": {
                    "width": browser_cfg["viewport_width"],
                    "height": browser_cfg["viewport_height"],
                },
                "locale": browser_cfg["locale"],
                "user_agent": browser_cfg["user_agent"],
            }
            if browser_cfg.get("timezone_id"):
                pkwargs["timezone_id"] = browser_cfg["timezone_id"]
            if browser_cfg.get("proxy"):
                pkwargs["proxy"] = {"server": browser_cfg["proxy"]}
            _browser_binary(pkwargs)
            context = p.chromium.launch_persistent_context(**pkwargs)
            self._setup_context(context, browser_cfg)
            page = context.pages[0] if context.pages else context.new_page()
        else:
            # Ephemeral browser + storage_state (session.json) - the
            # copy-session model.
            launch_kwargs = {"headless": browser_cfg["headless"], "args": launch_args}
            if browser_cfg.get("proxy"):
                launch_kwargs["proxy"] = {"server": browser_cfg["proxy"]}
            _browser_binary(launch_kwargs)
            browser = p.chromium.launch(**launch_kwargs)
            ctx_args = {
                "viewport": {
                    "width": browser_cfg["viewport_width"],
                    "height": browser_cfg["viewport_height"],
                },
                "locale": browser_cfg["locale"],
                "user_agent": browser_cfg["user_agent"],
            }
            if browser_cfg.get("timezone_id"):
                ctx_args["timezone_id"] = browser_cfg["timezone_id"]
            if SESSION_PATH.exists():
                ctx_args["storage_state"] = str(SESSION_PATH)
            context = browser.new_context(**ctx_args)
            self._setup_context(context, browser_cfg)
            page = context.new_page()

        return browser, context, page, using_cdp, using_persistent

    def _run(self) -> None:
        try:
            load_dotenv(ROOT / ".env", override=True)
            username = os.getenv("IG_USERNAME")
            password = os.getenv("IG_PASSWORD")
            self._me = (username or "").lstrip("@").lower()
            self._run_skip = set()   # fresh each run; poison accounts get another chance next run
            self._follow_fail_streak = 0   # reset the gated-rest streak each run
            self._set_acting(False, running=True)   # publish "alive, not yet acting" so a
                                                     # restart isn't read as stopped by the scraper
            self._recent_follow_seeds.clear()  # fresh source-diversity window each run
            self._recent_reach_tags.clear()    # fresh reach tag-diversity window each run
            self._reach_consumed.clear()       # fresh reach pick-guard each run
            self._warmed = False     # one-time pool warm-up gate (external scraper)
            if not username or not password:
                self.state.update(status="error", error="IG_USERNAME / IG_PASSWORD not set")
                return

            cfg = load_config()
            mode = (cfg.get("mode") or "unfollow").lower()
            append_event("bot_start", mode)
            whitelist = load_whitelist()
            DATA_DIR.mkdir(exist_ok=True)
            pacing = cfg["pacing"]
            behavior = cfg["behavior"]
            browser_cfg = cfg["browser"]
            unfollowed_log = ROOT / cfg["logging"]["unfollowed_log"]
            failed_log = ROOT / cfg["logging"]["failed_log"]
            skipped_log = ROOT / cfg["logging"].get("skipped_log", "data/skipped.log")

            # The follow daily cap drives the header gauge in follow/marketing modes.
            _limits = cfg.get("limits", {}) or {}
            start_cap = (
                int(_limits.get("follows_per_day", 80))
                if mode in ("follow", "marketing")
                else int(_limits.get("unfollows_per_day", 80))
            )
            self.state.update(
                status="starting", started_at=time.time(),
                unfollowed_count=0, failed_count=0, total_targets=0,
                followed_count=0, follow_failed_count=0, candidate_pool=0,
                churn_unfollowed_count=0,
                progress_index=0, daily_cap=start_cap, error=None,
                current_target=None, phase_detail=f"launching browser ({mode} mode)",
                next_action_at=None,
            )

            with sync_playwright() as p:
                # The browser is brought up only while the bot is ACTIVELY working and
                # fully torn down whenever it goes idle (warm-up wait, day-cap sleep,
                # off-hours, between cycles). That frees the Pi entirely for the
                # scraper during the bot's dead time - true mutual exclusion, so the
                # two Chromes never contend. conn holds the live handles.
                conn = {"browser": None, "context": None, "page": None,
                        "cdp": False, "persistent": False, "ok": False}
                did_warmup = [False]
                following: list[str] = []   # unfollow-mode list, loaded lazily

                def ensure_connected():
                    if conn["ok"]:
                        return conn["page"]
                    self.state.update(phase_detail=f"launching browser ({mode} mode)")
                    b, c, pg, ucdp, upers = self._connect(p, browser_cfg)
                    conn.update(browser=b, context=c, page=pg, cdp=ucdp, persistent=upers, ok=True)
                    if self._stop_event.is_set():
                        raise _Stopped()
                    if self._is_logged_in(pg):
                        self.state.update(phase_detail="reusing saved session")
                    elif upers:
                        raise RuntimeError(
                            "Not logged in on this profile. Log into Instagram once on "
                            "the Pi (DEPLOY.md 'permanent login'), then restart.")
                    else:
                        self._login(pg, username, password)
                        if self._stop_event.is_set():
                            raise _Stopped()
                        if not ucdp:
                            c.storage_state(path=str(SESSION_PATH))
                    self._refresh_account_counts(pg)
                    # One-time human warmup browse, only on the first connect.
                    if not did_warmup[0]:
                        did_warmup[0] = True
                        ws = behavior.get("warmup_browse_seconds", 0)
                        if ws > 0:
                            self.state.update(status="warmup", phase_detail=f"browsing feed for {ws}s")
                            loaded = False
                            for attempt in range(2):
                                try:
                                    pg.goto("https://www.instagram.com/",
                                            wait_until="domcontentloaded", timeout=60000)
                                    loaded = True
                                    break
                                except Exception as e:
                                    self.state.emit("log", {"level": "info",
                                        "msg": f"warmup load slow ({type(e).__name__}); "
                                               f"{'retrying' if attempt == 0 else 'skipping warmup'}"})
                                    if self._stop_event.is_set():
                                        raise _Stopped()
                            if loaded:
                                end = time.time() + ws
                                while time.time() < end and not self._stop_event.is_set():
                                    self._random_mouse(pg)
                                    try:
                                        pg.mouse.wheel(0, random.randint(200, 700))
                                    except Exception:
                                        pass
                                    time.sleep(random.uniform(1.5, 4.0))
                    return pg

                def disconnect():
                    if not conn["ok"]:
                        return
                    try:
                        if conn["persistent"]:
                            conn["context"].close()   # frees Chromium + persists the profile
                        elif not conn["cdp"]:
                            try:
                                conn["context"].storage_state(path=str(SESSION_PATH))
                            except Exception:
                                pass
                            conn["browser"].close()
                        else:
                            try:
                                conn["page"].close()   # CDP Chrome is user-owned; close our tab
                            except Exception:
                                pass
                    except Exception:
                        pass
                    conn.update(browser=None, context=None, page=None, ok=False)

                # Reach-enabled confirmation log (no browser needed).
                eng_cfg0 = cfg.get("engagement", {}) or {}
                if eng_cfg0.get("reach_enabled", False):
                    srcs = (cfg.get("targeting", {}) or {}).get("sources", {}) or {}
                    tags = [t for t in (eng_cfg0.get("reach_hashtags") or srcs.get("hashtags") or []) if t]
                    self.state.emit("log", {"level": "info" if tags else "error",
                        "msg": ("reach enabled" if tags
                                else "reach is ON but NO hashtags configured - add some in Sources → Hashtags")})

                daily_loop = bool(behavior.get("daily_loop", False))
                loop_hours = float(behavior.get("daily_loop_hours", 24))

                while not self._stop_event.is_set():
                    # Re-read config each day so dashboard edits (e.g. the daily
                    # cap or whitelist) take effect on the next batch.
                    day_cfg = load_config()
                    pacing = day_cfg["pacing"]
                    self._publish_day_counts(day_cfg)   # top bar: done / cap (also re-rolls at midnight)

                    # --- ban-safety gates (NO browser needed → keep the bot's Chrome
                    #     CLOSED so the scraper gets the whole Pi) ---
                    # Outside the configured active hours → sleep until the window opens.
                    gate_secs = self._seconds_until_active(day_cfg)
                    if gate_secs > 0:
                        disconnect(); self._set_acting(False)
                        self.state.update(
                            status="sleeping", current_target=None,
                            phase_detail=f"outside active hours - resuming in ~{gate_secs / 3600:.1f}h",
                            next_action_at=time.time() + gate_secs)
                        self._interruptible_sleep(gate_secs)
                        self.state.update(next_action_at=None)
                        continue
                    # Hit today's caps → sleep to the next local day (caps re-roll then).
                    # The burner keeps building tomorrow's pools meanwhile, so reflect
                    # 'scraping' while it works rather than a flat 'sleeping'.
                    if self._day_capped_for_mode(day_cfg, mode):
                        disconnect(); self._set_acting(False)
                        # Sleep toward tomorrow in short chunks, re-checking the cap each
                        # chunk - so a manual "reset daily tasks" wakes the bot within ~a
                        # minute instead of waiting for midnight. The burner keeps filling
                        # pools meanwhile (reflected as 'scraping').
                        while not self._stop_event.is_set() and \
                                self._day_capped_for_mode(load_config(), mode):
                            gate_secs = self._seconds_until_tomorrow()
                            resume_clk = time.strftime("%H:%M", time.localtime(time.time() + gate_secs))
                            self.state.update(next_action_at=time.time() + gate_secs)
                            self._sleep_reflecting_scraper(
                                min(gate_secs, 60.0),
                                f"daily caps reached - resuming ~{resume_clk} (in ~{gate_secs / 3600:.1f}h)")
                        self.state.update(next_action_at=None)
                        continue

                    # Pool coordination gate: stay fully idle (browser CLOSED) until every
                    # pool the bot will consume holds its low-water (follow: a daily cap of
                    # candidates; reach: today's remaining like budget), so the scraper
                    # fills them first (true mutual exclusion). Re-checked each cycle, so if
                    # a pool later drops the bot yields again and the scraper refills.
                    warm, detail = self._pools_warm(day_cfg, mode)
                    if not warm:
                        disconnect(); self._set_acting(False)
                        self._warmed = False
                        # Re-check in short ticks so the numbers track the scraper live;
                        # surface its phase so it's clear it's working.
                        for _ in range(6):   # ~30s, polled every 5s
                            if self._stop_event.is_set():
                                break
                            warm, detail = self._pools_warm(day_cfg, mode)
                            if warm:
                                break
                            # One system: while the burner fills the pool, show the whole
                            # system as 'scraping' (live phase), not 'sleeping'.
                            if self._scraper_active():
                                self.state.update(
                                    status="scraping", current_target=None, next_action_at=None,
                                    phase_detail=f"scraping — {self._scraper_phase()} · need {detail}")
                            else:
                                self.state.update(
                                    status="sleeping", current_target=None, next_action_at=None,
                                    phase_detail=f"waiting for pool - {detail} (scraper idle/off - start it?)")
                            self._interruptible_sleep(5)
                        continue
                    if not self._warmed:
                        self._warmed = True
                        if detail:
                            self.state.emit("log", {"level": "info",
                                "msg": f"pools ready ({detail}) - starting"})

                    # --- ACTIVE: bring the browser up, verify the session, then work ---
                    self._set_acting(True)   # scraper pauses + closes its Chrome
                    self._await_scraper_idle()   # wait for it to actually release the Pi first
                    page = ensure_connected()
                    # Session still valid? (only meaningful for non-CDP models)
                    if not conn["cdp"] and not self._is_logged_in(page):
                        fix = ("log into Instagram again on the Pi profile"
                               if conn["persistent"]
                               else "refresh session.json (run export_session.py)")
                        self.state.update(status="error", current_target=None,
                                          next_action_at=None,
                                          error=f"Instagram session expired - {fix}.")
                        break
                    # Lazy-load the following list for unfollow mode (needs the page).
                    if mode == "unfollow" and not following:
                        if behavior.get("use_following_cache", True) and FOLLOWING_CACHE.exists():
                            following = json.loads(FOLLOWING_CACHE.read_text(encoding="utf-8"))
                            self.state.update(phase_detail=f"loaded {len(following)} from cache")
                        else:
                            scraped = self._scrape_following(page, username)
                            scraped.reverse()
                            following = scraped
                            FOLLOWING_CACHE.write_text(json.dumps(following, indent=2), encoding="utf-8")
                            self.state.emit("following_cached", {"count": len(following)})

                    # (account counts already refreshed on connect in ensure_connected)
                    self._reset_story_reach()

                    _day_limits = day_cfg.get("limits", {}) or {}
                    if mode == "follow":
                        outcome = self._process_follow_day(page, day_cfg)
                    elif mode == "marketing":
                        self.state.update(
                            daily_cap=int(_day_limits.get("follows_per_day", 80))
                        )
                        outcome = self._process_churn_cycle(page, day_cfg)
                        # Finish the LIKES cap before ending the day. Reach likes only fire
                        # interleaved with follow/unfollow actions, so when those caps are
                        # done likes are usually still short - drain the reach pool now
                        # (browser still up) until likes are capped / the pool empties.
                        if (outcome not in ("stopped", "checkpoint", "block", "rest")
                                and (day_cfg.get("engagement") or {}).get("reach_enabled", False)
                                and self._day_room("likes", day_cfg) > 0):
                            rc = self._drain_reach_to_cap(page, day_cfg)
                            if rc == "stopped":
                                outcome = "stopped"
                            elif rc == "block":
                                outcome = "block"
                    else:
                        self.state.update(daily_cap=int(_day_limits.get("unfollows_per_day", 80)))
                        outcome = self._process_day(
                            page, following, pacing, unfollowed_log, failed_log, skipped_log
                        )

                    # Copy-session mode: persist refreshed cookies so the session
                    # survives restarts. (Persistent/CDP need nothing here.)
                    if conn["ok"] and not conn["cdp"] and not conn["persistent"]:
                        try:
                            conn["context"].storage_state(path=str(SESSION_PATH))
                        except Exception:
                            pass

                    if outcome == "stopped":
                        break
                    if outcome == "checkpoint":
                        self.state.update(
                            status="error", current_target=None, next_action_at=None,
                            error="Instagram CHECKPOINT/challenge detected - STOPPED immediately. "
                                  "Open Instagram on this account and clear the challenge "
                                  "(confirm it's you), then start again. Do NOT keep running.",
                        )
                        self.state.emit("log", {"level": "error",
                            "msg": "checkpoint/challenge detected - hard stop (no retries)"})
                        append_event("checkpoint")
                        break
                    if outcome == "block":
                        self.state.update(
                            status="error", current_target=None, next_action_at=None,
                            error="Action block suspected. Stopped - lower the daily caps and "
                                  "start again later (after several hours).",
                        )
                        break
                    if outcome == "rest":
                        # A run of follow failures = IG is gating follows. Don't hammer:
                        # close the browser (frees the Pi so the scraper fills pools) and
                        # back off so IG cools down, then resume. After too many rests in
                        # one day, give up - it's a hard block, not a transient gate.
                        L = self._ensure_ledger(day_cfg)
                        L["follow_rests"] = int(L.get("follow_rests", 0)) + 1
                        self._save_ledger()
                        rests = int(L["follow_rests"])
                        _safety = day_cfg.get("safety", {}) or {}
                        max_rests = int(_safety.get("follow_rest_max_per_day", 3))
                        append_event("follow_rest", f"#{rests} today")
                        if max_rests > 0 and rests >= max_rests:
                            self.state.update(
                                status="error", current_target=None, next_action_at=None,
                                error=f"Follows gated - rested {rests} time(s) today and "
                                      "they're still failing. Stopped for the day. Lower the "
                                      "follow cap and try again later (several hours).")
                            self.state.emit("log", {"level": "error",
                                "msg": f"follow gating persisted after {rests} rests - hard stop"})
                            break
                        lo = float(_safety.get("follow_fail_rest_min", 1200))
                        hi = float(_safety.get("follow_fail_rest_max", 2400))
                        rest_s = random.uniform(min(lo, hi), max(lo, hi))
                        disconnect(); self._set_acting(False)
                        self._follow_fail_streak = 0
                        self.state.update(
                            status="sleeping", current_target=None,
                            phase_detail=(f"follows gated - resting ~{rest_s / 60:.0f}m "
                                          f"(rest {rests}/{max_rests}; scraper filling pools)"),
                            next_action_at=time.time() + rest_s)
                        self.state.emit("log", {"level": "info",
                            "msg": f"follows gated - rest #{rests} for ~{rest_s / 60:.0f}m"})
                        self._interruptible_sleep(rest_s)
                        self.state.update(next_action_at=None)
                        continue
                    day_scraper = day_cfg.get("scraper", {}) or {}
                    keep_running = (bool(day_scraper.get("keep_running", False))
                                    and mode in ("follow", "marketing"))

                    if not daily_loop and not keep_running:
                        break  # one-shot mode: a single batch then stop

                    if keep_running:
                        # If THIS cycle just exhausted the daily cap(s), don't do a short
                        # re-check - loop straight back so the cap gate at the top of the
                        # loop sleeps us until tomorrow. The gate is only evaluated at the
                        # top, so a cycle that hits the cap mid-way must yield here or it
                        # would otherwise idle one pointless re-check interval first.
                        if self._day_capped_for_mode(day_cfg, mode):
                            continue
                        # Explain WHY we're re-checking rather than acting, so a short
                        # countdown right after a cap is hit reads sensibly. In marketing
                        # one cap can be reached while the other still has room (e.g.
                        # follows done for the day, but unfollows mature over time).
                        f_room = self._day_room("follows", day_cfg)
                        u_room = self._day_room("unfollows", day_cfg)
                        # Likes still short with follow+unfollow done = finishing today's
                        # likes off the reach pool (the drain backed off on a run of
                        # unlikeable posts, or is waiting for a refill).
                        likes_only = (mode == "marketing" and f_room <= 0 and u_room <= 0
                                      and self._reach_finishable(day_cfg)
                                      and self._day_room("likes", day_cfg) > 0)
                        # Don't hard-stop when the pool is empty - sleep a short,
                        # randomized interval and re-check. A background scraper service
                        # refills the pool; churn-unfollows keep maturing. When ONLY likes
                        # remain, re-check soon (~2-3m) instead of the full idle window -
                        # the reach pool usually still has posts to grind through.
                        if likes_only:
                            sleep_s = random.uniform(120, 180)
                        else:
                            lo = float(day_scraper.get("idle_recheck_min", 15))
                            hi = float(day_scraper.get("idle_recheck_max", 30))
                            sleep_s = random.uniform(min(lo, hi), max(lo, hi)) * 60
                        if likes_only:
                            note = "follow + unfollow done - finishing today's likes"
                        elif mode == "marketing" and f_room <= 0 and u_room > 0:
                            note = "follow cap reached for today - waiting for unfollows to become due"
                        elif mode == "marketing" and u_room <= 0 and f_room > 0:
                            note = "unfollow cap reached for today - following more as the pool refills"
                        elif outcome == "exhausted":
                            note = "pool empty - waiting for fresh candidates"
                        else:
                            note = "cycle done - re-checking"
                    else:
                        # Daily-loop: sleep ~loop_hours before the next batch.
                        sleep_s = loop_hours * 3600 * random.uniform(0.9, 1.1)
                        note = ("list fully processed - re-checking"
                                if outcome == "exhausted" else "daily batch done")

                    wake = time.time() + sleep_s
                    human = (f"~{sleep_s / 60:.0f}m" if sleep_s < 3600
                             else f"~{sleep_s / 3600:.1f}h")
                    # Going idle between cycles → CLOSE the browser, free the Pi. The
                    # burner refills the pool meanwhile, so show 'scraping' while it works.
                    disconnect(); self._set_acting(False)
                    self.state.update(next_action_at=wake)
                    self._sleep_reflecting_scraper(sleep_s, f"{note}; next cycle in {human}")
                    self.state.update(next_action_at=None)

                disconnect()   # run ended → tear the browser down

            # Don't clobber an error/block status set inside the loop.
            if self.state.snapshot()["status"] != "error":
                self.state.update(
                    status="stopped" if self._stop_event.is_set() else "idle",
                    current_target=None,
                    phase_detail="stopped by user" if self._stop_event.is_set() else "completed",
                    next_action_at=None,
                )
        except _Stopped:
            self.state.update(
                status="stopped",
                current_target=None,
                phase_detail="stopped by user",
                next_action_at=None,
            )
        except Exception as e:
            self.state.update(status="error", error=str(e), next_action_at=None)
            append_event("error", str(e)[:200])
        finally:
            self._set_acting(False, running=False)   # bot stopped → scraper builds to high-water
            try:
                st = self.state.snapshot().get("status")
                append_event("bot_stop", "stopped" if self._stop_event.is_set() else (st or ""))
            except Exception:
                pass
            # Always tear down the background story worker when the run ends.
            self._story_stop.set()
            self._story_worker_active = False
            if self._story_thread:
                try:
                    self._story_thread.join(timeout=15)
                except Exception:
                    pass
                self._story_thread = None

    def _scrape_once(self) -> None:
        """One-shot scrape of all configured sources into the candidate pool,
        then disconnect. Backs the dashboard 'Scrape now' button."""
        try:
            load_dotenv(ROOT / ".env", override=True)
            self._me = (os.getenv("IG_USERNAME") or "").lstrip("@").lower()
            cfg = load_config()
            browser_cfg = cfg["browser"]
            DATA_DIR.mkdir(exist_ok=True)
            sources = (cfg.get("targeting", {}) or {}).get("sources", {}) or {}
            if not ((sources.get("profiles") or []) or (sources.get("post_likers") or [])):
                self.state.update(status="error",
                                  error="No sources configured - add follower profiles or post URLs.")
                return

            self.state.update(status="scraping", started_at=time.time(), error=None,
                              current_target=None, next_action_at=None,
                              phase_detail="launching browser (scrape)")

            with sync_playwright() as p:
                browser, context, page, using_cdp, using_persistent = self._connect(p, browser_cfg)
                if self._stop_event.is_set():
                    raise _Stopped()
                if not self._is_logged_in(page):
                    fix = ("log into Instagram on the Pi profile" if using_persistent
                           else "refresh session.json / log in" if not using_cdp
                           else "log into Instagram in the Chrome you're debugging")
                    self.state.update(status="error",
                                      error=f"Not logged in - {fix}, then scrape again.")
                    return

                # Seed the account status bar before scraping so it's visible
                # during the scrape too (the run loop seeds it on its own path).
                self._refresh_account_counts(page)

                added = self._scrape_candidates(page, cfg)

                if using_persistent:
                    try:
                        context.close()
                    except Exception:
                        pass
                elif not using_cdp:
                    try:
                        context.storage_state(path=str(SESSION_PATH))
                    except Exception:
                        pass
                    browser.close()

            if self.state.snapshot()["status"] != "error":
                self.state.update(
                    status="stopped" if self._stop_event.is_set() else "idle",
                    phase_detail=("stopped by user" if self._stop_event.is_set()
                                  else f"scrape done - added {added} new candidate(s)"),
                    last_message=f"scraped {added} new candidate(s)",
                    next_action_at=None,
                )
        except _Stopped:
            self.state.update(status="stopped", phase_detail="scrape stopped by user",
                              next_action_at=None)
        except Exception as e:
            self.state.update(status="error", error=f"scrape failed: {e}", next_action_at=None)


class _Stopped(Exception):
    pass
