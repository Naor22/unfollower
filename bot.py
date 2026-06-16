"""
Bot core: stateful Instagram unfollow worker that runs in a background thread.

Exposes:
  - StateManager: thread-safe state + event broadcaster (with asyncio bridge).
  - Bot: start/stop/pause controller wrapping the Playwright flow.
  - Module-level helpers for reading/writing config, whitelist, and logs
    (consumed by both server.py and main.py).
"""

import collections
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
FOLLOW_CANDIDATES = DATA_DIR / "follow_candidates.json"
DISCOVERED_SOURCES = DATA_DIR / "discovered_sources.json"
ACCOUNT_STATS = DATA_DIR / "account_stats.json"
SCRAPER_STATUS = DATA_DIR / "scraper_status.json"   # separate scraper service heartbeat/counts
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


class StateManager:
    """Thread-safe state container + asyncio event broadcaster.

    The bot thread calls update()/emit() synchronously; WebSocket subscribers
    receive messages via asyncio queues attached to the FastAPI event loop.
    """

    def __init__(self, persist_events: bool = True) -> None:
        self._lock = threading.Lock()
        self._state = BotState()
        self._subscribers: list = []
        self._loop = None
        # The separate scraper service uses its own StateManager but must NOT
        # touch the shared activity.json (the server's StateManager owns it) — two
        # processes writing the same file would corrupt the feed. persist_events=
        # False keeps emit/update working (in-memory) without disk I/O.
        self._persist = persist_events
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
        msg = {"type": event_type, "data": payload}
        # Record to the shared feed (everything except high-frequency 'state').
        self._events.append(msg)
        self._events_since_save += 1
        if self._persist and self._events_since_save >= 25:
            self._events_since_save = 0
            self._save_events()
        self._broadcast(msg)

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
    same way — used by the churn timer to age out follows."""
    try:
        return time.mktime(time.strptime(s.strip(), "%Y-%m-%d %H:%M:%S"))
    except Exception:
        return None


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_config(data: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)


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
            out.append({
                "username": str(item["username"]).lstrip("@").lower(),
                "source": item.get("source", ""),
            })
    return out


def write_follow_candidates(entries: list[dict]) -> None:
    """Atomically replace the candidate pool (temp file + os.replace) so a reader
    in another process (the core bot) never sees a half-written file while the
    scraper service republishes it."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = FOLLOW_CANDIDATES.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    os.replace(tmp, FOLLOW_CANDIDATES)


def read_filter_rejected_log() -> list[dict]:
    """Candidates the scraper service filtered OUT (failed a filter). Excluded
    from the follow done-set so the core bot never visits them."""
    return _read_reason_log(_log_path("filter_rejected_log", "data/filter_rejected.log"))


def read_filter_checked_log() -> list[dict]:
    """Candidates the scraper service already evaluated and KEPT — tracked only so
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


# ---------- Bot ----------

class Bot:
    def __init__(self, state: StateManager) -> None:
        self.state = state
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()  # set = paused
        self._lock = threading.Lock()
        self._me = ""  # logged-in handle; set in _run, used by the modal fallback
        self._actions_since_resync = 0  # drives periodic account-count re-sync
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

    def _interruptible_sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while True:
            self.state.touch()   # heartbeat: a sleeping bot is alive, a hung one isn't
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
        # Timed out with neither clear signal — fall back to "logged in only if
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
            # Click username first — IG sometimes lazy-mounts the password input on focus.
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
            # Fall back: press Enter on the password field — the form submits natively.
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
                return "captcha / challenge — solve it in the browser"
            if "/two_factor" in url or "two-factor" in url:
                return "2FA — enter the code in the browser"
            if "/accounts/login" in url:
                return "waiting for login response (solve captcha/2FA in browser — up to 1 min)"
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
                # Quick sanity check — still a valid IG page (not redirected to a non-IG host).
                if "instagram.com" in url:
                    break

            # Surface a 2FA input even if URL is still /accounts/login (older flows).
            try:
                if page.locator(twofa_sel).count() > 0 and last_phase != "2FA — enter the code in the browser":
                    self.state.update(phase_detail="2FA — enter the code in the browser")
                    last_phase = "2FA — enter the code in the browser"
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

        # Final verification — we should now be off the login / challenge pages.
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
    # Scroll the last rendered row into view — this is what fires IG's
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
        an overlay — so we click the count link by its accessible name.
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

    def _collect_modal(self, dialog, exclude: str, cap: int) -> list[str]:
        """Scroll a username-list dialog and accumulate usernames up to `cap`.
        Shared by followers/following and likers scraping. Mirrors the
        accumulate-on-every-scroll approach of _scrape_following so rows that
        virtualize out of the DOM aren't lost."""
        seen: set[str] = set()
        order: list[str] = []
        self._collect_into(dialog, exclude, seen, order)
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
                f"{which} dialog never opened for @{profile} — private or rate-limited "
                f"(shot:{shot})"
            )
        return page.locator('div[role="dialog"]').last

    def _scrape_list(self, page, profile: str, which: str, cap: int = 600) -> list[str]:
        """Return up to `cap` usernames from a profile's followers/following."""
        self.state.update(status="scraping", phase_detail=f"opening @{profile}'s {which}")
        dialog = self._open_list_modal(page, profile, which)
        self._jitter(1.5, 3.0)
        # Exclude the profile owner's own self-links; our own account and
        # already-followed accounts are dropped later by the done-set.
        return self._collect_modal(dialog, profile, cap)

    def _scrape_likers(self, page, post_url: str, cap: int = 600) -> list[str]:
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
        return self._collect_modal(dialog, my, cap)

    def _scrape_commenters(self, page, post_url: str, cap: int = 200) -> list[str]:
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
        return order[:cap]

    def _scrape_hashtag(self, page, tag: str, cap: int = 200,
                        per_post: int = 60) -> list[str]:
        """Return up to `cap` usernames sourced from a hashtag — the authors and
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
        return out[:cap]

    def _scrape_candidates(self, page, cfg) -> int:
        """Run every configured source, dedup against the follow done-set and the
        existing pool, and append new usernames to follow_candidates.json. Stops
        early once the pending pool reaches candidate_pool_min. Returns the count
        added."""
        follow_cfg = cfg.get("follow", {}) or {}
        sources = follow_cfg.get("sources", {}) or {}
        per_cap = int(follow_cfg.get("scrape_per_source_cap", 600))
        pool_min = int(follow_cfg.get("candidate_pool_min", 300))

        my = (os.getenv("IG_USERNAME") or "").lower()
        done = self._follow_done_set(load_whitelist(), my)

        pool = read_follow_candidates()
        existing = {c["username"] for c in pool}
        added = 0

        def pending_count() -> int:
            return sum(1 for c in pool if c["username"] not in done)

        def ingest(users, source):
            nonlocal added
            for u in users:
                if u in done or u in existing:
                    continue
                existing.add(u)
                pool.append({"username": u, "source": source})
                added += 1

        for prof in sources.get("follower_profiles", []) or []:
            if self._stop_event.is_set() or pending_count() >= pool_min:
                break
            prof = (prof or "").strip().lstrip("@").lower()
            if not prof:
                continue
            try:
                users = self._scrape_list(page, prof, "followers", per_cap)
                ingest(users, f"followers:{prof}")
                self.state.emit("log", {"level": "info",
                                        "msg": f"scraped {len(users)} from @{prof} followers"})
            except Exception as e:
                self.state.emit("log", {"level": "error",
                                        "msg": f"scrape @{prof} followers failed: {e}"})

        for post in sources.get("liker_posts", []) or []:
            if self._stop_event.is_set() or pending_count() >= pool_min:
                break
            post = (post or "").strip()
            if not post:
                continue
            try:
                users = self._scrape_likers(page, post, per_cap)
                ingest(users, "likers")
                self.state.emit("log", {"level": "info",
                                        "msg": f"scraped {len(users)} likers from {post}"})
            except Exception as e:
                self.state.emit("log", {"level": "error",
                                        "msg": f"scrape likers {post} failed: {e}"})

        for post in sources.get("commenter_posts", []) or []:
            if self._stop_event.is_set() or pending_count() >= pool_min:
                break
            post = (post or "").strip()
            if not post:
                continue
            try:
                users = self._scrape_commenters(page, post, per_cap)
                ingest(users, "commenters")
                self.state.emit("log", {"level": "info",
                                        "msg": f"scraped {len(users)} commenters from {post}"})
            except Exception as e:
                self.state.emit("log", {"level": "error",
                                        "msg": f"scrape commenters {post} failed: {e}"})

        for tag in sources.get("hashtags", []) or []:
            if self._stop_event.is_set() or pending_count() >= pool_min:
                break
            tag = (tag or "").strip().lstrip("#").lower()
            if not tag:
                continue
            try:
                users = self._scrape_hashtag(page, tag, per_cap)
                ingest(users, f"hashtag:{tag}")
                self.state.emit("log", {"level": "info",
                                        "msg": f"scraped {len(users)} from #{tag}"})
            except Exception as e:
                self.state.emit("log", {"level": "error",
                                        "msg": f"scrape #{tag} failed: {e}"})

        if added:
            write_follow_candidates(pool)
            self.state.update(candidate_pool=pending_count())
        return added

    def _find_following_button(self, page):
        """Locate the profile-header 'Following'/'Requested' button.

        IG renders this control as either a real <button> or a div[role=button],
        and occasionally as a plain element whose text is exactly 'Following'. We
        try role-based exact matches first, then a text-is fallback scoped to the
        header so we never grab a 'Following' count link elsewhere on the page.
        """
        # NOTE: substring (not exact) match — IG's button has a chevron icon
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

        The menu content loads asynchronously — IG first shows a spinner, then
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
    # unfollow rate it silently rejects the action and pops one of these — the
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

    # IG's "this account is gone" interstitial. Distinguishes a genuinely
    # deleted/disabled/blocked profile (permanent skip) from a page that simply
    # hasn't finished loading (transient — should be retried, not skipped).
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
                return False  # header is here after all — not unavailable
            try:
                page.reload(wait_until="domcontentloaded")
                self._jitter(1.5, 3.0)
            except Exception:
                break
        # Header never showed and IG never said "unavailable" — ambiguous, so treat
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
    _TERMINAL = ("ok", "not_following", "no_button_no_posts", "private_or_missing")

    def _step(self, username: str, label: str, tone: str = "neutral",
              key: Optional[str] = None) -> None:
        """Record one step of a flow: shows in the status bar AND streams a 'step'
        event so the dashboard lists every step under one expandable row. Rows group
        by `key` (defaults to username — per-user for unfollow/follow; per-POST for
        reach, where one user can be liked across many separate posts)."""
        self.state.update(phase_detail=f"@{username or 'post'}: {label}")
        payload = {"username": username, "label": label, "tone": tone}
        if key:
            payload["key"] = key
        self.state.emit("step", payload)

    def _unfollow(self, page, target: str) -> str:
        """Coordinator: run the profile-page unfollow (header -> post menu), and on
        a transient failure fall back to the own-Following-list modal, then retry
        the whole thing a few times. Terminal outcomes short-circuit immediately."""
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
            # surface. Try it at most once — it's heavy (navigates to our own
            # profile) and a second go rarely helps; if it rate-limits, bail out.
            if use_modal and self._me and not modal_tried:
                modal_tried = True
                modal = self._unfollow_via_following_list(page, target)
                if modal == "not_following":
                    modal = self._confirm_not_following(page, target)
                # Terminal outcomes from the modal (ok / not_following / rate_limited)
                # are authoritative — the following list is the ground truth for the
                # relationship. Only its own modal_* failures fall through to a retry.
                if modal in self._TERMINAL or modal.startswith("rate_limited"):
                    return modal

            if attempt < retries:
                self._clear_dialogs(page)
                self._step(target, f"{result} — retry {attempt + 1}/{retries}", "bad")
                self._jitter(lo, hi)

        return result

    def _i_follow_them(self, page, target: str):
        """Definitive check: open the TARGET's FOLLOWERS list and search for OUR own
        username — if we follow them, we're in their followers. Returns True / False
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
        them), return a transient so it's retried — never wrongly skipped."""
        self._step(target, "double-checking via their followers list")
        res = self._i_follow_them(page, target)
        if res is True:
            self._step(target, "we ARE in their followers — keep trying, not a skip", "bad")
            return "following_confirmed"   # transient -> retried, not skipped
        if res is False:
            self._step(target, "confirmed not following", "neutral")
        return "not_following"

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
            self._step(target, "page load failed — retry", "bad")
            return "profile_not_ready"

        # Wait for the profile header to actually render — IG loads it AFTER
        # domcontentloaded, so checking immediately (the old behaviour) reported a
        # false 'profile_not_ready' on slow loads. Give it real time.
        try:
            page.wait_for_selector("header", timeout=12000)
        except Exception:
            if self._profile_truly_unavailable(page):
                self._step(target, "profile unavailable", "bad")
                return "private_or_missing"
            self._step(target, "header didn't load — retry", "bad")
            return "profile_not_ready"
        self._jitter(2.0, 4.5)
        self._random_mouse(page)

        btn = self._find_following_button(page)
        if btn is None:
            # No Following/Requested button found. Only treat as 'not following' if an
            # EXACT 'Follow' button is showing IN THE HEADER. Both qualifiers matter:
            #   - exact=True: substring 'Follow' would also match 'Following'.
            #   - header scope: the profile page also shows a 'Suggested for you'
            #     carousel full of OTHER accounts' Follow buttons — matching those
            #     was causing false 'not following' skips on people we do follow.
            hdr = page.locator("header")
            if (hdr.get_by_role("button", name="Follow", exact=True).count() > 0
                    or hdr.get_by_role("button", name="Follow Back", exact=True).count() > 0):
                self._step(target, "not following (Follow button in header)", "neutral")
                return "not_following"
            # Neither a Following nor an exact Follow button rendered — either the
            # known IG action-row bug, or the header was still loading. Fall back to
            # the post '...' menu (and then the following-list modal), which read the
            # real relationship instead of guessing from a half-rendered header.
            self._step(target, "no header button — trying post menu", "neutral")
            return self._unfollow_via_post(page, target)

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
        it STILL offers 'Unfollow' (i.e. the action didn't land). Best-effort —
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
        '...' (More options) menu, which still works — this mirrors the manual
        workaround. Accounts with no posts can't use this path."""
        # Read the first post's URL and navigate straight to it instead of
        # clicking the thumbnail — clicking the grid is flaky because IG's hover
        # overlay (likes/comments) intercepts the click, which is what caused
        # 'post_open_failed'. A direct goto always lands on the post page.
        post = page.locator('a[href*="/p/"], a[href*="/reel/"]').first
        try:
            post.wait_for(state="attached", timeout=5000)
            href = post.get_attribute("href")
        except Exception:
            href = None
        if not href:
            # No header button AND no posts to open — nothing we can do here.
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

        # The options menu now lists Unfollow (poll for it — it loads async too).
        self._step(target, "clicking Unfollow in post menu")
        menu = page.locator('div[role="dialog"]').last
        if not self._click_unfollow_control(menu):
            shot = self._screenshot(page, f"fail_postnoitem_{target}")
            return f"post_unfollow_item_missing (shot:{shot})"
        self._jitter(0.8, 1.5)

        # Clicking 'Unfollow' in the post menu opens a confirmation dialog
        # ('Unfollow @user?') that MUST be clicked to finish the action. Poll for
        # it and click it — a one-shot presence check races the dialog's open
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
        # selectors — IG's input markup shifts and a missed box is the main reason
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
            return "not_following"            # IG confirmed no match — we don't follow them
        if not found:
            shot = self._screenshot(page, f"fail_modalinconclusive_{target}")
            return f"modal_inconclusive (shot:{shot})"   # transient -> retried, NOT skipped

        # Click the 'Following' button WITHIN the target's row (not the first one in
        # the dialog, which could be a different account if the list isn't filtered).
        self._step(target, "found in list — clicking Unfollow")
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

    # Read the three profile-header stats. IG renders them as a <ul> of <li>,
    # each with the number and a label (Posts / Followers / Following). The
    # number is often abbreviated in visible text ('1,234', '12.3k') but the
    # exact value is in a child span's title attribute, so we capture both.
    _COUNTS_JS = (
        '() => { const h = document.querySelector("header") || document.body;'
        ' return Array.from(h.querySelectorAll("li")).map(li => {'
        '   const t = li.querySelector("span[title]");'
        '   return { text: li.innerText || "", title: t ? t.getAttribute("title") : "" };'
        ' }); }'
    )

    def _read_profile_counts(self, page) -> dict:
        """Return {'posts', 'followers', 'following'} as ints (or None each).

        Two strategies: (1) structured <li> items, preferring the exact value in
        a span[title] (e.g. '1,234,567' behind the abbreviated '1.2M'); (2) a
        regex over the header/main text as a fallback, since IG reshuffles the
        header markup often but the visible 'X posts / Y followers / Z following'
        wording is stable."""
        counts = {"posts": None, "followers": None, "following": None}

        # Strategy 1: structured list items (gives the exact follower count).
        try:
            items = page.evaluate(self._COUNTS_JS)
        except Exception:
            items = []
        for it in items or []:
            low = (it.get("text") or "").lower()
            if "post" in low:
                key = "posts"
            elif "follower" in low:
                key = "followers"
            elif "following" in low:
                key = "following"
            else:
                continue
            if counts[key] is not None:
                continue
            counts[key] = parse_count(it.get("title") or "") or parse_count(it.get("text") or "")

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
        except Exception:
            return
        fields = {}
        if counts.get("followers") is not None:
            fields["account_followers"] = counts["followers"]
        if counts.get("following") is not None:
            fields["account_following"] = counts["following"]
        if fields:
            self.state.update(**fields)
            snap = self.state.snapshot()
            write_account_stats(snap.get("account_followers"), snap.get("account_following"))
        self._actions_since_resync = 0

    def fetch_account_now(self) -> bool:
        """One-shot: connect, read our own counts, persist, disconnect — so the
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

    def _write_scraper_status(self, error: Optional[str] = None,
                              phase: str = "") -> None:
        """Heartbeat + counts for the dashboard's Scraper card. Atomic write so the
        server never reads a half-written file. The status FILE (not the WS state)
        is how this separate process reports to the dashboard."""
        try:
            pool = read_follow_candidates()
            done = self._follow_done_set(load_whitelist(), self._me)
            ready = sum(1 for c in pool if c["username"] not in done)
        except Exception:
            pool, ready = [], 0
        status = {
            "ts": time.time(),
            "phase": phase,
            "pool": len(pool),
            "ready": ready,
            "checked": len(read_filter_checked_log()),
            "rejected": len(read_filter_rejected_log()),
            "error": error,
        }
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = SCRAPER_STATUS.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(status, indent=2), encoding="utf-8")
            os.replace(tmp, SCRAPER_STATUS)
        except Exception:
            pass

    def _filter_one(self, page, target: str, filters: dict) -> Optional[str]:
        """Browser-navigate a profile and apply the ACCOUNT-AGNOSTIC filters
        (posts / followers / following ranges + private). Returns a reject reason
        or None (passes). Relationship filters (already-follows-me / already-
        following) are NOT applied here — the burner can't judge them; the core
        bot handles those via its done-set + follow-time check."""
        try:
            page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded")
            page.wait_for_selector("header", timeout=12000)
        except Exception:
            return "unavailable"
        self._jitter(1.0, 2.5)
        if filters.get("skip_private", True) and self._is_private(page):
            return "private"
        counts = self._read_profile_counts(page)
        return self._passes_filters(counts, filters)   # 'no_posts' / 'filtered' / None

    def _filter_pool(self, page, cfg) -> None:
        """Evaluate not-yet-checked candidates, logging keep/reject, then republish
        the pool (atomic) with rejected + already-consumed accounts removed."""
        follow_cfg = cfg.get("follow", {}) or {}
        filters = follow_cfg.get("filters", {}) or {}
        scr = cfg.get("scraper", {}) or {}
        min_d = float(scr.get("min_delay", 3))
        max_d = float(scr.get("max_delay", 8))
        long_every = int(scr.get("long_break_every", 40))
        long_min = float(scr.get("long_break_min", 60))
        long_max = float(scr.get("long_break_max", 180))

        checked_log = _log_path("filter_checked_log", "data/filter_checked.log")
        rejected_log = _log_path("filter_rejected_log", "data/filter_rejected.log")

        done = self._follow_done_set(load_whitelist(), self._me)
        checked = {r["username"] for r in read_filter_checked_log()}
        rejected = {r["username"] for r in read_filter_rejected_log()}
        pool = read_follow_candidates()
        todo = [c["username"] for c in pool
                if c["username"] not in done
                and c["username"] not in checked
                and c["username"] not in rejected]

        self._write_scraper_status(phase=f"filtering {len(todo)} candidate(s)")
        processed = 0
        for u in todo:
            if self._stop_event.is_set():
                break
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            try:
                reason = self._filter_one(page, u, filters)
            except Exception as e:
                self.state.emit("log", {"level": "error", "msg": f"filter @{u} failed: {e}"})
                continue   # leave unchecked, retry next pass
            if reason is None:
                append_log(checked_log, f"{ts}\t{u}")
                checked.add(u)
            else:
                append_log(rejected_log, f"{ts}\t{u}\t{reason}")
                rejected.add(u)
            processed += 1
            if processed % 10 == 0:
                self._write_scraper_status(phase=f"filtered {processed}/{len(todo)}")
            self._jitter(min_d, max_d)
            if long_every and processed % long_every == 0:
                self._interruptible_sleep(random.uniform(long_min, long_max))

        # Republish: drop rejected + already-consumed, keep good + still-unchecked.
        new_pool = [c for c in pool
                    if c["username"] not in rejected and c["username"] not in done]
        if len(new_pool) != len(pool):
            write_follow_candidates(new_pool)
        self._write_scraper_status(phase="idle")

    def run_scraper(self) -> None:
        """Entry point for the standalone scraper service (separate process, its
        own Chrome / burner account). Scrapes sources, browser-filters candidates,
        and atomically publishes the cleaned pool for the core bot to consume.
        Never follows/unfollows. Browser navigation only — no IG API."""
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
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            self._write_scraper_status(phase="starting")

            with sync_playwright() as p:
                browser, context, page, using_cdp, using_persistent = self._connect(p, browser_cfg)
                if not self._is_logged_in(page):
                    self._write_scraper_status(
                        error="scraper Chrome is not logged in — log the burner account "
                              "in once on the 2nd Chrome (port 9223), then restart")
                    return
                while not self._stop_event.is_set():
                    day_cfg = load_config()
                    scr = day_cfg.get("scraper", {}) or {}
                    idle = float(scr.get("idle_seconds", 600))
                    # Dashboard pause switch: keep the service alive but idle.
                    if not scr.get("enabled", False):
                        self._write_scraper_status(phase="disabled (toggle off)")
                        self._interruptible_sleep(30)
                        continue
                    # 1. scrape raw usernames (reuses every source scraper)
                    self._write_scraper_status(phase="scraping sources")
                    try:
                        self._scrape_candidates(page, day_cfg)
                    except Exception as e:
                        self.state.emit("log", {"level": "error", "msg": f"scrape pass failed: {e}"})
                    if self._stop_event.is_set():
                        break
                    # 2. browser-filter the pool
                    self._filter_pool(page, day_cfg)
                    if self._stop_event.is_set():
                        break
                    # 3. idle until the next pass (keeps the pool topped + clean)
                    self._write_scraper_status(phase="idle")
                    self._interruptible_sleep(idle * random.uniform(0.85, 1.15))
        except Exception as e:
            self._write_scraper_status(error=str(e))
        finally:
            self._write_scraper_status(phase="stopped")

    def _adjust_following(self, delta: int) -> None:
        """Nudge the live following count by ±1 after a real follow/unfollow, so the
        status bar tracks between full re-syncs. No-op until the first fetch seeds it."""
        cur = self.state.snapshot().get("account_following")
        if cur is not None:
            self.state.update(account_following=max(0, cur + delta))

    def _tick_resync(self, page) -> None:
        """Count an action and do a full re-sync every N actions (configurable)."""
        n = int((load_config().get("behavior", {}) or {}).get("account_resync_every", 40))
        self._actions_since_resync = getattr(self, "_actions_since_resync", 0) + 1
        if n > 0 and self._actions_since_resync >= n:
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
        """Lowercased header/main innerText of the loaded profile — used for
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
        """If the loaded profile looks like a niche INFLUENCER — bio matches a
        keyword, no negative keyword, follower count in the influencer range —
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
        sources = (load_config().get("follow", {}) or {}).get("sources", {}) or {}
        if target in {s.lstrip("@").lower() for s in (sources.get("follower_profiles") or [])}:
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
                disc_cfg: Optional[dict] = None) -> str:
        """Visit a profile and follow it, applying filters. Mirrors _unfollow.

        Returns one of: 'ok', 'already_following', 'unavailable',
        'skipped_follows_you', 'skipped_private', 'skipped_no_posts',
        'skipped_filtered', 'rate_limited (...)', or a transient failure string."""
        filters = filters or {}
        self._step(target, "opening profile")
        page.goto(f"https://www.instagram.com/{target}/", wait_until="domcontentloaded")
        # Wait for the header to render before deciding (avoids false 'unavailable'
        # on a slow load — same fix as the unfollow side).
        try:
            page.wait_for_selector("header", timeout=12000)
        except Exception:
            self._step(target, "profile unavailable", "bad")
            return "unavailable"
        self._jitter(2.0, 4.5)
        self._random_mouse(page)

        # Already following (or request pending)? Nothing to do.
        if self._find_following_button(page) is not None:
            self._step(target, "already following", "neutral")
            return "already_following"

        # They already follow us — skip if configured (no point spending a follow
        # on someone already in our followers when the goal is net-new reach).
        if filters.get("skip_already_follows_me", True) and self._follows_you(page):
            self._step(target, "they already follow you — skip", "neutral")
            return "skipped_follows_you"

        if filters.get("skip_private", True) and self._is_private(page):
            self._step(target, "private account — skip", "neutral")
            return "skipped_private"

        counts = self._read_profile_counts(page)
        # Discovery runs BEFORE the follow filters so niche influencers (who are
        # usually filtered out by max_followers) still get queued as sources.
        self._maybe_discover_source(page, target, counts, disc_cfg or {})
        reason = self._passes_filters(counts, filters)
        if reason == "no_posts":
            self._step(target, "no posts — skip", "neutral")
            return "skipped_no_posts"
        if reason == "filtered":
            self._step(target, "filtered out (followers/following limits)", "neutral")
            return "skipped_filtered"

        btn = self._find_follow_button(page)
        if btn is None:
            # No header Follow button (the same IG web bug the unfollow side hits).
            # Fall back to opening a post and using the Follow button next to the
            # author's name in the post header, which still works.
            self._step(target, "no header Follow button — trying via a post", "neutral")
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
        /stories/<user>/<numeric-media-id>/. If that id never appears — no story, or
        the account is private and we don't follow them, or it bounced back to the
        profile — there's nothing to view, so we return False (skip). The old check
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
        liked. Best-effort. Waits for the grid + the like control to actually render
        (the previous version evaluated too early and found nothing)."""
        if n <= 0:
            return 0
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
            try:
                page.goto(url, wait_until="domcontentloaded")
                self._interruptible_sleep(random.uniform(1.5, 3.0))
                if self._click_like(page):
                    liked += 1
                    self._interruptible_sleep(random.uniform(1.0, 2.5))
                    if self._rate_limited(page):
                        break
            except Exception:
                continue
        return liked

    def _action_bar(self, page):
        """Locator for the OPEN post's action bar. Anchors on ANY action-bar-only
        icon (Comment / Save / Share — reels lack a 'Comment' svg, so we can't rely
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
                    return False   # already liked — don't toggle off
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

        # Couldn't confirm — capture state so we can fix it precisely.
        try:
            likes = page.locator('svg[aria-label="Like"]').count()
        except Exception:
            likes = -1
        shot = self._screenshot(page, "reach_like_fail")
        self.state.emit("log", {"level": "error", "msg":
            f"reach like failed (shot:{shot}) bar={'y' if bar is not None else 'n'} "
            f"Like={likes} Unlike {before}->{after}"})
        return False

    def _engage_after_follow(self, page, target: str, eng_cfg: dict) -> None:
        """Optional touches right after a follow (still on/near the profile): view
        their story and/or like a couple posts. Controlled by follow.engagement.
        Never raises — engagement is best-effort and must not abort a follow."""
        if not eng_cfg:
            return
        try:
            if eng_cfg.get("on_follow_view_story", False):
                self._view_story(page, target)
            n = int(eng_cfg.get("on_follow_like_posts", 0) or 0)
            if n > 0:
                self._like_recent_posts(page, target, n)
        except Exception as e:
            self.state.emit("log", {"level": "error",
                                    "msg": f"engagement on @{target} failed: {e}"})

    def _build_story_queue(self) -> list[str]:
        """Candidate-pool members eligible for a story check: not in the follow done
        set, and not checked within the last `story_recheck_hours`. The recheck
        window means we cycle back to accounts later (to catch new stories) instead
        of checking each only once — so story-reach keeps running, but we don't spam
        the same account in a tight loop."""
        my = (os.getenv("IG_USERNAME") or "").lower()
        recheck_h = float((load_config().get("follow", {}) or {})
                          .get("engagement", {}).get("story_recheck_hours", 20) or 0)
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
                continue   # checked recently — revisit after the window
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

    def _do_reach(self, page, u: str) -> str:
        """Do one marketing 'reach' touch on a pool member per `engagement.reach_mode`
        ('likes' | 'story' | 'both'): like a recent post and/or view their story.
        Likes work on any PUBLIC account (high hit rate); stories need an active,
        viewable story (rare). Logs + feeds only on a real action. Returns
        'liked' / 'viewed' / 'ratelimit' / '' (nothing) so the caller can pace."""
        eng = (load_config().get("follow", {}) or {}).get("engagement", {}) or {}
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
            if self._stop_event.is_set() or self._story_stop.is_set():
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
                "msg": f"reach: no posts found for #{tag} (shot:{shot}) — IG may be gating "
                       "the hashtag page"})
        return urls

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
        """Return a random un-liked post URL from a COMBINED pool spanning all tags.
        The pool is refilled by scraping ONE random hashtag grid, but only when it
        runs low AND a cooldown has elapsed — IG gates the explore/hashtag grid pages
        when hit too often, so we load them rarely and back off hard if one returns
        nothing (the gating signal). Likes themselves keep going from the cache."""
        sources = (load_config().get("follow", {}) or {}).get("sources", {}) or {}
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
                    f"reach: #{tag} grid returned nothing (likely gated) — "
                    f"pausing hashtag loads ~{self._reach_scrape_cooldown / 60:.0f}m"})

        self.state.update(reach_pool=len(self._reach_pool))
        if self._reach_pool:
            url, tag = self._reach_pool.pop(random.randrange(len(self._reach_pool)))
            self._reach_tag = tag
            return url
        return None

    def _do_reach_post(self, page, url: str, eng: dict) -> str:
        """Open a hashtag post, like it, and (optionally) view the poster's story.
        Emits a per-poster STEP for each stage (like the unfollow flow) so the feed
        shows the full flow under one expandable row. Returns
        'liked' / 'viewed' / 'ratelimit' / ''."""
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
        'hashtags' (default — like public posts under niche hashtags) or 'pool'
        (legacy — engage candidate-pool members)."""
        eng = (load_config().get("follow", {}) or {}).get("engagement", {}) or {}
        cap = int(eng.get("story_reach_daily_cap", 100) or 0)
        if cap and self._story_today >= cap:
            return "cap"
        if (eng.get("reach_source") or "hashtags").lower() == "hashtags":
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

    def _maybe_story_reach_tick(self, page) -> None:
        """Interleaved story-reach for NON-CDP modes (where a 2nd concurrent tab
        isn't safe). In CDP mode the background worker handles it, so this no-ops.
        Views one pool story every `story_reach_every_actions` actions."""
        if self._story_worker_active:
            # Defer to the background worker — but self-heal if its thread died
            # (e.g. the 2nd CDP connection failed), so we don't silently stop
            # doing story-reach entirely.
            if self._story_thread is not None and self._story_thread.is_alive():
                return
            self._story_worker_active = False
            self.state.emit("log", {"level": "info",
                                    "msg": "story-reach: background worker gone — using interleaved mode"})
        eng = (load_config().get("follow", {}) or {}).get("engagement", {}) or {}
        if not eng.get("story_reach_enabled", False):
            return
        # Fire after a RANDOM number of actions (not every single one) so a like
        # doesn't follow every unfollow like clockwork — looks less robotic.
        self._story_tick += 1
        if self._story_tick < self._story_next:
            return
        self._story_tick = 0
        self._story_next = self._roll_reach_interval(eng)
        self._reach_one(page)

    def _roll_reach_interval(self, eng) -> int:
        lo = int(eng.get("story_reach_every_min",
                         eng.get("story_reach_every_actions", 1)) or 1)
        hi = int(eng.get("story_reach_every_max", lo + 3) or lo)
        lo = max(1, lo)
        return random.randint(lo, max(lo, hi))

    def _story_worker_loop(self) -> None:
        """Background story-reach: its OWN CDP connection + tab, running truly
        concurrently with the main run on its own cadence (independent of the main
        loop's pacing, pauses and long breaks). CDP-only — a second independent
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
                mode0 = ((load_config().get("follow", {}) or {}).get("engagement", {}) or {}).get("reach_mode", "likes")
                self.state.emit("log", {"level": "info",
                    "msg": f"reach worker live ({mode0}) — {n} candidates queued"})
                try:
                    while not self._stop_event.is_set() and not self._story_stop.is_set():
                        # honor pause
                        if self._pause_event.is_set():
                            time.sleep(0.5)
                            continue
                        eng = (load_config().get("follow", {}) or {}).get("engagement", {}) or {}
                        if not eng.get("story_reach_enabled", False):
                            self._story_sleep(5)
                            continue
                        cap = int(eng.get("story_reach_daily_cap", 100) or 0)
                        if cap and self._story_today >= cap:
                            self._story_sleep(60)
                            continue
                        res = self._reach_one(page)
                        if res == "":
                            self._story_sleep(120)   # nothing available — wait, retry
                            continue
                        # Pace by action type: likes are rate-limited (slowest),
                        # story views are cheap, misses scan fast.
                        if res == "cap":
                            self._story_sleep(60)
                        elif res == "ratelimit":
                            self.state.emit("log", {"level": "info",
                                "msg": "reach: soft-blocked on likes — backing off"})
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
            # Could not open/keep the 2nd CDP tab — hand story-reach back to the
            # interleaved fallback so it doesn't silently stop.
            self.state.emit("log", {"level": "error",
                "msg": f"story-reach worker failed ({e}) — falling back to interleaved mode"})
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
                     skipped_log, cap=None, set_gauge=True) -> str:
        """Run one daily batch of unfollows.

        Re-reads the whitelist and the done set (unfollowed + skipped) fresh, so
        exclusions added via the dashboard and prior progress are always honored.
        Returns one of: 'cap' (daily cap hit), 'exhausted' (nothing left),
        'stopped' (user stop), 'block' (5 consecutive failures).

        `cap` overrides the per-batch unfollow limit (defaults to pacing daily
        cap). `set_gauge=False` skips the follow/unfollow header gauge updates so
        this can run as a churn add-on (see `_process_list_trim`) without
        clobbering the churn progress display."""
        cap = pacing["daily_cap"] if cap is None else int(cap)
        whitelist = load_whitelist()
        done_set = {row["username"].lower() for row in read_unfollowed_log()}
        done_set |= {row["username"].lower() for row in read_skipped_log()}
        targets = [u for u in following if u not in whitelist and u not in done_set]

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
        consecutive_errors = 0  # only real unfollow failures, reset on success
        rate_limit_hits = 0     # soft-block ('Try Again Later') hits this batch
        for target in targets:
            if self._stop_event.is_set():
                return "stopped"
            if processed >= cap:
                self.state.update(phase_detail=f"daily cap {cap} reached")
                return "cap"

            if set_gauge:
                self.state.update(current_target=target, progress_index=processed + 1)
            else:
                self.state.update(current_target=target)

            try:
                result = self._unfollow(page, target)
            except Exception as e:
                result = "error"
                self.state.emit("log", {"level": "error", "msg": f"exception on {target}: {e}"})

            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            if result == "ok":
                append_log(unfollowed_log, f"{ts}\t{target}")
                new_count = self.state.snapshot()["unfollowed_count"] + 1
                self.state.update(unfollowed_count=new_count, last_message=f"unfollowed @{target}")
                self.state.emit("unfollowed", {"timestamp": ts, "username": target, "note": ""})
                processed += 1
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
                # the fallback '...' menu on — there's no UI path to unfollow, so
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
                rate_limit_hits += 1
                consecutive_errors = 0  # not a selector failure — handled separately
                self.state.update(failed_count=new_failed,
                                  last_message=f"rate-limited @{target} (soft block)")
                self.state.emit("failed", {"timestamp": ts, "username": target, "reason": "rate_limited"})
                if rate_limit_hits >= pacing.get("rate_limit_max_hits", 3):
                    return "block"
                cooldown = random.uniform(
                    pacing.get("rate_limit_cooldown_min_seconds", 900),
                    pacing.get("rate_limit_cooldown_max_seconds", 1800),
                )
                self.state.update(phase_detail=f"soft block — cooling down {cooldown / 60:.0f}m")
                self._interruptible_sleep(cooldown)
                if self._stop_event.is_set():
                    return "stopped"
                continue
            else:
                append_log(failed_log, f"{ts}\t{target}\t{result}")
                new_failed = self.state.snapshot()["failed_count"] + 1
                consecutive_errors += 1
                self.state.update(failed_count=new_failed, last_message=f"failed @{target}: {result}")
                self.state.emit("failed", {"timestamp": ts, "username": target, "reason": result})
                if consecutive_errors >= 5:
                    return "block"

            if self._stop_event.is_set():
                return "stopped"

            # Interleaved story-reach marketing — ticks on EVERY processed target
            # (not just real unfollows), so it runs even through a stretch of skips.
            self._maybe_story_reach_tick(page)

            # Pacing applies to REAL unfollows only — Instagram rate-limits the
            # unfollow ACTION, not page visits. Skips/failures (deleted,
            # not-following) continue after just a brief pause so we blow through
            # dead accounts quickly instead of waiting minutes.
            if result == "ok":
                self._jitter(
                    pacing["min_delay_seconds"],
                    pacing["max_delay_seconds"],
                    pacing.get("distraction_chance", 0),
                    pacing.get("distraction_min_seconds", 0),
                    pacing.get("distraction_max_seconds", 0),
                )
                if processed > 0 and processed % pacing["long_break_every_n"] == 0:
                    pause = random.uniform(
                        pacing["long_break_min_seconds"],
                        pacing["long_break_max_seconds"],
                    )
                    self.state.update(phase_detail=f"long break {pause:.0f}s")
                    self._interruptible_sleep(pause)
            else:
                self._jitter(1.0, 3.0)

        return "exhausted"

    def _follow_done_set(self, whitelist: set[str], my_username: str) -> set[str]:
        """Accounts we must never (re-)follow: already followed, permanently
        skipped, churned off, currently in our following list, whitelisted, or
        ourselves. Transient skips/failures are intentionally left out so they
        get retried."""
        done = {row["username"].lower() for row in read_followed_log()}
        done |= {
            row["username"].lower() for row in read_follow_skipped_log()
            if row["reason"] in PERMANENT_FOLLOW_SKIPS
        }
        done |= {row["username"].lower() for row in read_churn_unfollowed_log()}
        done |= {u.lower() for u in read_following_cache()}
        done |= set(whitelist)
        if my_username:
            done.add(my_username.lower())
        return done

    def _process_follow_day(self, page, cfg) -> str:
        """Run one daily batch of follows pulled from the candidate pool.

        Mirrors _process_day: re-reads the whitelist + done set fresh, paces only
        real follows, backs off on soft blocks, and aborts on a run of failures.
        Returns 'cap' / 'exhausted' / 'stopped' / 'block'."""
        follow_cfg = cfg.get("follow", {}) or {}
        pacing = cfg["pacing"]
        filters = follow_cfg.get("filters", {}) or {}
        disc_cfg = follow_cfg.get("discovery", {}) or {}
        eng_cfg = follow_cfg.get("engagement", {}) or {}
        daily_cap = int(follow_cfg.get("daily_cap", 80))
        min_delay = follow_cfg.get("min_delay_seconds", 60)
        max_delay = follow_cfg.get("max_delay_seconds", 200)

        followed_log = _log_path("followed_log", "data/followed.log")
        skipped_log = _log_path("follow_skipped_log", "data/follow_skipped.log")
        failed_log = _log_path("follow_failed_log", "data/follow_failed.log")

        whitelist = load_whitelist()
        my_username = (os.getenv("IG_USERNAME") or "").lower()
        done_set = self._follow_done_set(whitelist, my_username)

        candidates = read_follow_candidates()
        targets = [c for c in candidates if c["username"] not in done_set]

        # Auto top-up: if the eligible pool is below candidate_pool_min and any
        # sources are configured, scrape more strangers before following.
        # Skipped entirely when an external scraper service owns the pool — the
        # core bot then only consumes the cleaned pool it publishes.
        sources = follow_cfg.get("sources", {}) or {}
        has_sources = bool((sources.get("follower_profiles") or [])
                           or (sources.get("liker_posts") or []))
        external_scraper = bool(follow_cfg.get("external_scraper", False))
        pool_min = int(follow_cfg.get("candidate_pool_min", 300))
        if (not external_scraper and has_sources and len(targets) < pool_min
                and not self._stop_event.is_set()):
            self.state.update(phase_detail=f"pool low ({len(targets)}) — scraping sources")
            try:
                self._scrape_candidates(page, cfg)
            except Exception as e:
                self.state.emit("log", {"level": "error", "msg": f"scrape failed: {e}"})
            candidates = read_follow_candidates()
            targets = [c for c in candidates if c["username"] not in done_set]

        self.state.update(
            status="running",
            daily_cap=daily_cap,
            candidate_pool=len(targets),
            total_targets=min(len(targets), daily_cap),
            progress_index=0,
            phase_detail=f"{len(targets)} eligible candidate(s) to follow",
        )
        if not targets:
            extra = (f" ({len(candidates)} in pool, all already followed/skipped/filtered)"
                     if candidates else " (pool is empty)")
            self.state.update(
                phase_detail=f"no eligible candidates to follow{extra}",
                last_message="nothing to follow — add new accounts to the candidate pool",
            )
            return "exhausted"

        processed = 0
        consecutive_errors = 0
        rate_limit_hits = 0
        for cand in targets:
            target = cand["username"]
            source = cand.get("source", "")
            if self._stop_event.is_set():
                return "stopped"
            if processed >= daily_cap:
                self.state.update(phase_detail=f"daily cap {daily_cap} reached")
                return "cap"

            self.state.update(current_target=target, progress_index=processed + 1)

            try:
                result = self._follow(page, target, filters, disc_cfg)
            except Exception as e:
                result = "error"
                self.state.emit("log", {"level": "error", "msg": f"exception on {target}: {e}"})

            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            if result == "ok":
                append_log(followed_log, f"{ts}\t{target}\t{source}")
                new_count = self.state.snapshot()["followed_count"] + 1
                self.state.update(followed_count=new_count, last_message=f"followed @{target}")
                self.state.emit("followed", {"timestamp": ts, "username": target, "source": source})
                processed += 1
                consecutive_errors = 0
                self._adjust_following(+1)
                self._tick_resync(page)
                # Extra exposure touches while we're still on the profile.
                self._engage_after_follow(page, target, eng_cfg)
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
            elif result.startswith("rate_limited"):
                append_log(failed_log, f"{ts}\t{target}\t{result}")
                new_failed = self.state.snapshot()["follow_failed_count"] + 1
                rate_limit_hits += 1
                consecutive_errors = 0
                self.state.update(follow_failed_count=new_failed,
                                  last_message=f"rate-limited @{target} (soft block)")
                self.state.emit("follow_failed", {"timestamp": ts, "username": target, "reason": "rate_limited"})
                if rate_limit_hits >= pacing.get("rate_limit_max_hits", 3):
                    return "block"
                cooldown = random.uniform(
                    pacing.get("rate_limit_cooldown_min_seconds", 900),
                    pacing.get("rate_limit_cooldown_max_seconds", 1800),
                )
                self.state.update(phase_detail=f"soft block — cooling down {cooldown / 60:.0f}m")
                self._interruptible_sleep(cooldown)
                if self._stop_event.is_set():
                    return "stopped"
                continue
            else:
                append_log(failed_log, f"{ts}\t{target}\t{result}")
                new_failed = self.state.snapshot()["follow_failed_count"] + 1
                consecutive_errors += 1
                self.state.update(follow_failed_count=new_failed,
                                  last_message=f"failed @{target}: {result}")
                self.state.emit("follow_failed", {"timestamp": ts, "username": target, "reason": result})
                if consecutive_errors >= 5:
                    return "block"

            if self._stop_event.is_set():
                return "stopped"

            self._maybe_story_reach_tick(page)   # interleaved story-reach, every target

            # Pace real follows only — IG rate-limits the follow ACTION, not page
            # visits. Skips/failures continue after a brief pause so we blow
            # through dead/ineligible accounts quickly.
            if result == "ok":
                self._jitter(
                    min_delay,
                    max_delay,
                    pacing.get("distraction_chance", 0),
                    pacing.get("distraction_min_seconds", 0),
                    pacing.get("distraction_max_seconds", 0),
                )
                if processed > 0 and processed % pacing["long_break_every_n"] == 0:
                    pause = random.uniform(
                        pacing["long_break_min_seconds"],
                        pacing["long_break_max_seconds"],
                    )
                    self.state.update(phase_detail=f"long break {pause:.0f}s")
                    self._interruptible_sleep(pause)
            else:
                self._jitter(1.0, 3.0)

        return "exhausted"

    # --- churn (follow -> wait -> unfollow non-followers-back) ---

    def _process_churn_cycle(self, page, cfg) -> str:
        """One churn cycle: follow new strangers (with auto top-up), then review
        old follows and unfollow the ones who didn't follow back. Returns the
        usual 'cap'/'exhausted'/'stopped'/'block'."""
        # Phase 1 — follow (reuses scraping top-up, follow cap, pacing, backoff).
        follow_outcome = self._process_follow_day(page, cfg)
        if follow_outcome in ("stopped", "block"):
            return follow_outcome
        if self._stop_event.is_set():
            return "stopped"
        # Phase 2 — unfollow non-followers-back among aged follows.
        churn_outcome = self._process_churn_unfollows(page, cfg)
        if churn_outcome in ("stopped", "block"):
            return churn_outcome
        if self._stop_event.is_set():
            return "stopped"

        # Phase 3 (optional) — also trim the EXISTING following list: unfollow
        # non-whitelisted accounts from data/following.json so the user can keep
        # shrinking their following toward a target while marketing runs. Off by
        # default; controlled by churn.also_unfollow_following.
        churn_cfg = (cfg.get("follow", {}) or {}).get("churn", {}) or {}
        if churn_cfg.get("also_unfollow_following", False):
            trim_outcome = self._process_list_trim(page, cfg)
            if trim_outcome in ("stopped", "block"):
                return trim_outcome
        return churn_outcome

    def _process_list_trim(self, page, cfg) -> str:
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
        churn_cfg = (cfg.get("follow", {}) or {}).get("churn", {}) or {}
        cap = int(churn_cfg.get("list_unfollow_cap", 40))
        self.state.update(phase_detail=f"churn add-on: trimming following list (cap {cap})")
        return self._process_day(
            page, following, cfg["pacing"],
            _log_path("unfollowed_log", "data/unfollowed.log"),
            _log_path("failed_log", "data/failed.log"),
            _log_path("skipped_log", "data/skipped.log"),
            cap=cap, set_gauge=False,
        )

    def _process_churn_unfollows(self, page, cfg) -> str:
        """Visit follows older than unfollow_after_days; keep the ones who
        followed back, unfollow the rest (up to daily_unfollow_cap)."""
        follow_cfg = cfg.get("follow", {}) or {}
        churn_cfg = follow_cfg.get("churn", {}) or {}
        pacing = cfg["pacing"]
        after_days = float(churn_cfg.get("unfollow_after_days", 4))
        keep_back = bool(churn_cfg.get("keep_followers_back", True))
        daily_unfollow_cap = int(churn_cfg.get("daily_unfollow_cap", 80))
        min_delay = follow_cfg.get("min_delay_seconds", 60)
        max_delay = follow_cfg.get("max_delay_seconds", 200)

        churn_log = _log_path("churn_unfollowed_log", "data/churn_unfollowed.log")
        kept_log = _log_path("follow_kept_log", "data/follow_kept.log")
        failed_log = _log_path("follow_failed_log", "data/follow_failed.log")
        outcomes_log = _log_path("follow_outcomes_log", "data/follow_outcomes.log")

        # Stop re-checking accounts we've already resolved.
        resolved = {r["username"].lower() for r in read_follow_kept_log()}
        resolved |= {r["username"].lower() for r in read_churn_unfollowed_log()}
        whitelist = load_whitelist()
        # username -> source, for per-source conversion analytics recorded below.
        source_map = {r["username"].lower(): r.get("source", "")
                      for r in read_followed_log()}

        now = time.time()
        cutoff = after_days * 86400
        due, seen = [], set()
        for row in read_followed_log():
            u = row["username"].lower()
            if u in resolved or u in whitelist or u in seen:
                continue
            ts = parse_log_ts(row["timestamp"])
            if ts is None or (now - ts) < cutoff:
                continue
            seen.add(u)
            due.append(u)

        self.state.update(status="running", current_target=None,
                          phase_detail=f"churn: {len(due)} follow(s) due for review")
        if not due:
            return "exhausted"

        processed = 0          # real unfollows (counts toward the cap)
        consecutive_errors = 0
        rate_limit_hits = 0
        for u in due:
            if self._stop_event.is_set():
                return "stopped"
            if processed >= daily_unfollow_cap:
                self.state.update(phase_detail=f"churn cap {daily_unfollow_cap} reached")
                return "cap"

            self.state.update(current_target=u)
            ts = time.strftime("%Y-%m-%d %H:%M:%S")

            # Reciprocity check on the loaded profile before deciding.
            self._step(u, "churn review — checking follow-back")
            try:
                page.goto(f"https://www.instagram.com/{u}/", wait_until="domcontentloaded")
                self._jitter(2.0, 4.0)
            except Exception:
                pass

            # Measure reciprocity for per-source analytics regardless of the
            # keep_back setting — the churn visit is the natural measurement point.
            follows_back = self._follows_you(page)
            src = source_map.get(u, "")

            if keep_back and follows_back:
                self._step(u, "followed back — keeping", "good")
                append_log(kept_log, f"{ts}\t{u}")
                append_log(outcomes_log, f"{ts}\t{u}\t{src}\t1")
                self.state.emit("follow_kept", {"timestamp": ts, "username": u})
                self.state.update(last_message=f"@{u} followed back — kept")
                self._jitter(1.0, 3.0)
                continue
            self._step(u, "no follow-back — unfollowing" if not follows_back
                       else "followed back (keep off) — unfollowing")

            try:
                result = self._unfollow(page, u)
            except Exception as e:
                result = "error"
                self.state.emit("log", {"level": "error", "msg": f"churn exception on {u}: {e}"})

            if result == "ok" or result == "not_following" or result == "private_or_missing" \
                    or result.startswith("no_button_no_posts"):
                # Either we unfollowed them, or there's nothing left to unfollow —
                # done either way, so record it and stop re-checking.
                append_log(churn_log, f"{ts}\t{u}")
                append_log(outcomes_log, f"{ts}\t{u}\t{src}\t{'1' if follows_back else '0'}")
                new_count = self.state.snapshot()["churn_unfollowed_count"] + 1
                self.state.emit("churn_unfollowed", {"timestamp": ts, "username": u})
                if result == "ok":
                    self.state.update(churn_unfollowed_count=new_count,
                                      last_message=f"churned @{u} (didn't follow back)")
                    processed += 1
                    consecutive_errors = 0
                    self._adjust_following(-1)
                    self._tick_resync(page)
                    self._jitter(
                        min_delay, max_delay,
                        pacing.get("distraction_chance", 0),
                        pacing.get("distraction_min_seconds", 0),
                        pacing.get("distraction_max_seconds", 0),
                    )
                    if processed > 0 and processed % pacing["long_break_every_n"] == 0:
                        pause = random.uniform(pacing["long_break_min_seconds"],
                                               pacing["long_break_max_seconds"])
                        self.state.update(phase_detail=f"long break {pause:.0f}s")
                        self._interruptible_sleep(pause)
                else:
                    self.state.update(churn_unfollowed_count=new_count,
                                      last_message=f"@{u} already not followed (resolved)")
                    self._jitter(1.0, 3.0)
            elif result.startswith("rate_limited"):
                rate_limit_hits += 1
                consecutive_errors = 0
                self.state.update(last_message=f"rate-limited churning @{u} (soft block)")
                if rate_limit_hits >= pacing.get("rate_limit_max_hits", 3):
                    return "block"
                cooldown = random.uniform(
                    pacing.get("rate_limit_cooldown_min_seconds", 900),
                    pacing.get("rate_limit_cooldown_max_seconds", 1800),
                )
                self.state.update(phase_detail=f"soft block — cooling down {cooldown / 60:.0f}m")
                self._interruptible_sleep(cooldown)
                if self._stop_event.is_set():
                    return "stopped"
            else:
                append_log(failed_log, f"{ts}\t{u}\tchurn:{result}")
                consecutive_errors += 1
                self.state.emit("follow_failed", {"timestamp": ts, "username": u, "reason": f"churn:{result}"})
                self.state.update(last_message=f"churn failed @{u}: {result}")
                if consecutive_errors >= 5:
                    return "block"
                self._jitter(1.0, 3.0)

            self._maybe_story_reach_tick(page)   # interleaved story-reach, every target

        return "exhausted"

    # --- browser connection (shared by the run loop and one-shot scrapes) ---

    def _connect(self, p, browser_cfg):
        """Open/attach a browser per config and return
        (browser, context, page, using_cdp, using_persistent).

        Mirrors the three connection models: CDP (attach to a Chrome the user
        started with --remote-debugging-port; never closed), persistent profile
        (Pi-native login dir), or ephemeral browser + session.json. Does NOT log
        in or warm up — the caller handles that."""
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
            _browser_binary(pkwargs)
            context = p.chromium.launch_persistent_context(**pkwargs)
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.pages[0] if context.pages else context.new_page()
        else:
            # Ephemeral browser + storage_state (session.json) — the
            # copy-session model.
            launch_kwargs = {"headless": browser_cfg["headless"], "args": launch_args}
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
            if SESSION_PATH.exists():
                ctx_args["storage_state"] = str(SESSION_PATH)
            context = browser.new_context(**ctx_args)
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.new_page()

        return browser, context, page, using_cdp, using_persistent

    def _run(self) -> None:
        try:
            load_dotenv(ROOT / ".env", override=True)
            username = os.getenv("IG_USERNAME")
            password = os.getenv("IG_PASSWORD")
            self._me = (username or "").lstrip("@").lower()
            if not username or not password:
                self.state.update(status="error", error="IG_USERNAME / IG_PASSWORD not set")
                return

            cfg = load_config()
            mode = (cfg.get("mode") or "unfollow").lower()
            whitelist = load_whitelist()
            DATA_DIR.mkdir(exist_ok=True)
            pacing = cfg["pacing"]
            behavior = cfg["behavior"]
            browser_cfg = cfg["browser"]
            unfollowed_log = ROOT / cfg["logging"]["unfollowed_log"]
            failed_log = ROOT / cfg["logging"]["failed_log"]
            skipped_log = ROOT / cfg["logging"].get("skipped_log", "data/skipped.log")

            # The follow daily cap drives the header gauge in follow/churn modes.
            start_cap = (
                int((cfg.get("follow", {}) or {}).get("daily_cap", 80))
                if mode in ("follow", "churn") else pacing["daily_cap"]
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
                browser, context, page, using_cdp, using_persistent = self._connect(p, browser_cfg)

                if self._stop_event.is_set():
                    raise _Stopped()

                if self._is_logged_in(page):
                    self.state.update(phase_detail="reusing saved session")
                elif using_persistent:
                    # Permanent-profile mode never auto-logs-in with credentials
                    # (a headless first login would hit 2FA/captcha). Log in once
                    # on the Pi instead — see DEPLOY.md.
                    raise RuntimeError(
                        "Not logged in on this profile. Log into Instagram once on "
                        "the Pi (DEPLOY.md 'permanent login'), then restart."
                    )
                else:
                    self._login(page, username, password)
                    if self._stop_event.is_set():
                        raise _Stopped()
                    if not using_cdp:
                        context.storage_state(path=str(SESSION_PATH))

                # Seed the account status bar as soon as we're logged in, before
                # warmup/scrape, so the followers/following show right away.
                self._refresh_account_counts(page)

                # Start the concurrent background story-reach worker (CDP only — it
                # opens its own independent connection + tab to the same Chrome and
                # runs on its own cadence, regardless of what the main loop does).
                # Default OFF: the separate-tab worker needs a 2nd CDP connection
                # which proved flaky. Reach runs reliably in the main loop instead
                # (interleaved every story_reach_every_actions actions). Opt in with
                # story_reach_background: true to use the concurrent tab.
                eng_cfg0 = (cfg.get("follow", {}) or {}).get("engagement", {}) or {}
                if eng_cfg0.get("story_reach_enabled", False):
                    if using_cdp and eng_cfg0.get("story_reach_background", False):
                        self._story_stop.clear()
                        self._story_worker_active = True
                        self._story_thread = threading.Thread(
                            target=self._story_worker_loop, daemon=True)
                        self._story_thread.start()
                        self.state.emit("log", {"level": "info",
                                                "msg": "reach worker started (background tab)"})
                    else:
                        # In-loop reach (default). Confirm it's on so it's not a guess.
                        src = (eng_cfg0.get("reach_source") or "hashtags").lower()
                        if src == "hashtags":
                            srcs = (cfg.get("follow", {}) or {}).get("sources", {}) or {}
                            tags = [t for t in (eng_cfg0.get("reach_hashtags")
                                                or srcs.get("hashtags") or []) if t]
                            if tags:
                                self.state.emit("log", {"level": "info",
                                    "msg": f"reach enabled (in-loop, hashtags) — {len(tags)} tag(s) loaded"})
                            else:
                                self.state.emit("log", {"level": "error",
                                    "msg": "reach is ON but NO hashtags configured — add some in "
                                           "Sources → Hashtags, otherwise it has nothing to like"})
                        else:
                            try:
                                qn = len(self._build_story_queue())
                            except Exception:
                                qn = 0
                            self.state.emit("log", {"level": "info",
                                "msg": f"reach enabled (in-loop, pool) — {qn} candidates "
                                       "(note: many strangers are private and can't be liked)"})

                ws = behavior.get("warmup_browse_seconds", 0)
                if ws > 0:
                    self.state.update(status="warmup", phase_detail=f"browsing feed for {ws}s")
                    page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
                    end = time.time() + ws
                    while time.time() < end and not self._stop_event.is_set():
                        self._random_mouse(page)
                        try:
                            page.mouse.wheel(0, random.randint(200, 700))
                        except Exception:
                            pass
                        time.sleep(random.uniform(1.5, 4.0))
                    if self._stop_event.is_set():
                        raise _Stopped()

                # The cache (data/following.json) is stored OLDEST-FIRST — either
                # produced by import_following.py from Instagram's data export
                # (sorted by true follow date) or by a fresh scrape below. We use
                # it in that order directly, no reverse. Only the unfollow mode
                # needs it; follow/churn pull from the candidate pool instead.
                following: list[str] = []
                if mode == "unfollow":
                    if behavior.get("use_following_cache", True) and FOLLOWING_CACHE.exists():
                        following = json.loads(FOLLOWING_CACHE.read_text(encoding="utf-8"))
                        self.state.update(phase_detail=f"loaded {len(following)} from cache")
                    else:
                        scraped = self._scrape_following(page, username)
                        scraped.reverse()  # scrape returns newest-first -> store oldest-first
                        following = scraped
                        FOLLOWING_CACHE.write_text(json.dumps(following, indent=2), encoding="utf-8")
                        self.state.emit("following_cached", {"count": len(following)})

                daily_loop = bool(behavior.get("daily_loop", False))
                loop_hours = float(pacing.get("daily_loop_hours", 24))

                while not self._stop_event.is_set():
                    # Re-read config each day so dashboard edits (e.g. the daily
                    # cap or whitelist) take effect on the next batch.
                    day_cfg = load_config()
                    pacing = day_cfg["pacing"]

                    # Verify the session is still valid each day; if it lapsed,
                    # persistent mode needs a re-login on the Pi, copy-session
                    # mode needs a fresh session.json (export_session.py).
                    if not using_cdp and not self._is_logged_in(page):
                        fix = ("log into Instagram again on the Pi profile"
                               if using_persistent
                               else "refresh session.json (run export_session.py)")
                        self.state.update(
                            status="error", current_target=None, next_action_at=None,
                            error=f"Instagram session expired — {fix}.",
                        )
                        break

                    # Seed/re-sync our own follower & following counts for the
                    # status bar at the start of every batch (per-action ±1 keeps
                    # it live between these full fetches).
                    self._refresh_account_counts(page)
                    # Reset the interleaved story-reach budget for this batch (it
                    # runs inside every mode's loop now, not as a churn-only phase).
                    self._reset_story_reach()

                    if mode == "follow":
                        outcome = self._process_follow_day(page, day_cfg)
                    elif mode == "churn":
                        self.state.update(
                            daily_cap=int((day_cfg.get("follow", {}) or {}).get("daily_cap", 80))
                        )
                        outcome = self._process_churn_cycle(page, day_cfg)
                    else:
                        self.state.update(daily_cap=pacing["daily_cap"])
                        outcome = self._process_day(
                            page, following, pacing, unfollowed_log, failed_log, skipped_log
                        )

                    # Copy-session mode: persist refreshed cookies so the session
                    # survives restarts. Persistent mode saves to its profile dir
                    # automatically, so nothing to do there.
                    if not using_cdp and not using_persistent:
                        try:
                            context.storage_state(path=str(SESSION_PATH))
                        except Exception:
                            pass

                    if outcome == "stopped":
                        break
                    if outcome == "block":
                        self.state.update(
                            status="error", current_target=None, next_action_at=None,
                            error="Action block suspected (5 consecutive failures). "
                                  "Stopped — lower the daily cap and start again later.",
                        )
                        break
                    day_behavior = day_cfg.get("behavior", {}) or {}
                    keep_running = (bool(day_behavior.get("keep_running", False))
                                    and mode in ("follow", "churn"))

                    if not daily_loop and not keep_running:
                        break  # one-shot mode: a single batch then stop

                    if keep_running:
                        # Don't hard-stop when the pool is empty — sleep a short,
                        # randomized interval and re-check. A background scraper
                        # service refills the pool; churn-unfollows keep maturing.
                        lo = float(day_behavior.get("idle_recheck_min", 15))
                        hi = float(day_behavior.get("idle_recheck_max", 30))
                        sleep_s = random.uniform(min(lo, hi), max(lo, hi)) * 60
                        note = ("pool empty — waiting for fresh candidates"
                                if outcome == "exhausted" else "cycle done — re-checking")
                    else:
                        # Daily-loop: sleep ~loop_hours before the next batch.
                        sleep_s = loop_hours * 3600 * random.uniform(0.9, 1.1)
                        note = ("list fully processed — re-checking"
                                if outcome == "exhausted" else "daily batch done")

                    wake = time.time() + sleep_s
                    human = (f"~{sleep_s / 60:.0f}m" if sleep_s < 3600
                             else f"~{sleep_s / 3600:.1f}h")
                    self.state.update(
                        status="sleeping", current_target=None,
                        phase_detail=f"{note}; next cycle in {human}",
                        next_action_at=wake,
                    )
                    self._interruptible_sleep(sleep_s)
                    self.state.update(next_action_at=None)

                if using_persistent:
                    try:
                        context.close()  # persists the profile to disk
                    except Exception:
                        pass
                elif not using_cdp:
                    try:
                        context.storage_state(path=str(SESSION_PATH))
                    except Exception:
                        pass
                    browser.close()

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
        finally:
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
            sources = (cfg.get("follow", {}) or {}).get("sources", {}) or {}
            if not ((sources.get("follower_profiles") or []) or (sources.get("liker_posts") or [])):
                self.state.update(status="error",
                                  error="No sources configured — add follower profiles or post URLs.")
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
                                      error=f"Not logged in — {fix}, then scrape again.")
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
                                  else f"scrape done — added {added} new candidate(s)"),
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
