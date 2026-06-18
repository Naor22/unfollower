"""FastAPI server: REST + WebSocket for the dashboard UI."""

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import dotenv_values, set_key
from fastapi import (BackgroundTasks, FastAPI, HTTPException, WebSocket,
                     WebSocketDisconnect)
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import bot

ROOT = Path(__file__).parent
STATIC_DIR = ROOT / "static"
ENV_PATH = ROOT / ".env"

state_manager = bot.StateManager()
bot_instance = bot.Bot(state_manager)


class SystemStats:
    """Dependency-free host metrics from /proc + os + shutil (Linux/Pi). Each
    metric degrades to None where unavailable (e.g. on the Windows dev box)."""

    def __init__(self) -> None:
        self._prev_cpu = None  # (idle, total) for delta-based CPU%

    def _cpu_percent(self):
        try:
            with open("/proc/stat") as f:
                nums = [int(x) for x in f.readline().split()[1:]]
            idle = nums[3] + (nums[4] if len(nums) > 4 else 0)  # idle + iowait
            total = sum(nums)
            prev, self._prev_cpu = self._prev_cpu, (idle, total)
            if prev:
                di, dt = idle - prev[0], total - prev[1]
                if dt > 0:
                    return round(100 * (1 - di / dt), 1)
        except Exception:
            return None
        return None

    def _memory(self):
        try:
            info = {}
            with open("/proc/meminfo") as f:
                for line in f:
                    k, _, v = line.partition(":")
                    info[k.strip()] = int(v.strip().split()[0])  # kB
            total = info.get("MemTotal", 0)
            avail = info.get("MemAvailable", info.get("MemFree", 0))
            used = total - avail
            return {"total_mb": round(total / 1024), "used_mb": round(used / 1024),
                    "percent": round(100 * used / total, 1) if total else None}
        except Exception:
            return None

    def _temp(self):
        try:
            zone = "/sys/class/thermal/thermal_zone0/temp"
            if os.path.exists(zone):
                with open(zone) as f:
                    return round(int(f.read().strip()) / 1000, 1)
        except Exception:
            pass
        return None

    def _disk(self):
        try:
            u = shutil.disk_usage(str(ROOT))
            return {"total_gb": round(u.total / 1e9, 1), "used_gb": round(u.used / 1e9, 1),
                    "percent": round(100 * u.used / u.total, 1)}
        except Exception:
            return None

    def _uptime(self):
        try:
            with open("/proc/uptime") as f:
                return int(float(f.read().split()[0]))
        except Exception:
            return None

    def sample(self) -> dict:
        try:
            load = [round(x, 2) for x in os.getloadavg()]
        except Exception:
            load = None
        return {
            "cpu_percent": self._cpu_percent(),
            "memory": self._memory(),
            "disk": self._disk(),
            "temp_c": self._temp(),
            "load": load,
            "cpu_count": os.cpu_count(),
            "uptime_seconds": self._uptime(),
        }


_sys_stats = SystemStats()


def _restart_cmd(service: str):
    # --no-block hands the restart job to systemd (PID 1) and returns immediately,
    # so this process can finish its HTTP response before systemd stops it, and the
    # restart still completes (the job lives in systemd, not in our process). Much
    # simpler to authorize in sudoers than the old systemd-run form.
    return ["sudo", "-n", "systemctl", "--no-block", "restart", service]


def _watchdog_loop():
    """Restart the service if the bot hangs. 'Stuck' = running but no heartbeat for
    `stuck_after_seconds`. The heartbeat ticks during legitimate long sleeps, so
    only a real freeze trips it. Gated on autostart so a restart actually resumes
    the bot, and rate-limited to avoid restart loops."""
    last_restart = 0.0
    while True:
        time.sleep(30)
        try:
            cfg = bot.load_config()
            wd = cfg.get("watchdog", {}) or {}
            if not wd.get("enabled") or not bot_instance.is_running:
                continue
            age = time.time() - state_manager.last_heartbeat
            if age < float(wd.get("stuck_after_seconds", 600)):
                continue
            if time.time() - last_restart < float(wd.get("min_restart_interval_seconds", 900)):
                continue
            srv = cfg.get("server", {}) or {}
            if not srv.get("autostart"):
                state_manager.emit("log", {"level": "error", "msg":
                    f"watchdog: bot stuck {age:.0f}s but server.autostart is off - "
                    "not restarting (it wouldn't resume). Enable autostart."})
                last_restart = time.time()
                continue
            if sys.platform != "linux":
                continue
            service = srv.get("service_name", "unfollower")
            state_manager.emit("log", {"level": "error", "msg":
                f"watchdog: no heartbeat for {age:.0f}s - restarting {service}"})
            try:
                subprocess.Popen(_restart_cmd(service))
                last_restart = time.time()
                bot.append_event("watchdog_restart", f"stuck {age:.0f}s")
            except Exception as e:
                state_manager.emit("log", {"level": "error",
                                           "msg": f"watchdog restart failed: {e}"})
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    state_manager.attach_loop(asyncio.get_running_loop())
    threading.Thread(target=_watchdog_loop, daemon=True).start()
    try:
        bot.append_event("server_start")
    except Exception:
        pass
    # Seed the account status bar from the last-known counts so it shows on open
    # even when idle / right after a restart, before any live fetch.
    try:
        stats = bot.read_account_stats()
        if stats:
            state_manager.update(account_followers=stats.get("followers"),
                                 account_following=stats.get("following"))
    except Exception:
        pass
    # Seed today's per-day action counts (if any) so the bar shows them while idle.
    try:
        import json as _json
        L = _json.loads(bot.DAILY_COUNTS.read_text(encoding="utf-8"))
        if L.get("date") == time.strftime("%Y-%m-%d"):
            state_manager.update(day_follows=L.get("follows", 0),
                                 day_unfollows=L.get("unfollows", 0),
                                 day_likes=L.get("likes", 0))
    except Exception:
        pass
    # Pi mode: auto-start the bot on launch so it resumes after a reboot
    # (combined with behavior.daily_loop this runs unattended forever).
    try:
        srv = bot.load_config().get("server", {}) or {}
        env = dotenv_values(ENV_PATH) if ENV_PATH.exists() else {}
        if srv.get("autostart") and env.get("IG_USERNAME"):
            bot_instance.start()
            _maybe_autostart_scraper()   # boot: bring the scraper up with the bot
    except Exception:
        pass
    yield


app = FastAPI(lifespan=lifespan)


# ---------- models ----------

class ConfigUpdate(BaseModel):
    data: dict


class WhitelistUpdate(BaseModel):
    text: str


class WhitelistAdd(BaseModel):
    username: str


class CredentialsUpdate(BaseModel):
    username: str
    password: str


class CandidatesUpdate(BaseModel):
    text: str


class LogRemove(BaseModel):
    log: str
    username: str


class LogClear(BaseModel):
    log: str


class DataClear(BaseModel):
    target: str


class DeployFile(BaseModel):
    name: str
    content: str


class DeployUpload(BaseModel):
    files: list[DeployFile]
    restart: bool = True


class SourcesUpdate(BaseModel):
    follower_profiles: list[str]
    liker_posts: list[str]
    commenter_posts: list[str] = []
    hashtags: list[str] = []


class DiscoveredAction(BaseModel):
    username: str


# ---------- bot lifecycle ----------

@app.get("/api/state")
async def get_state():
    return {
        "state": state_manager.snapshot(),
        "is_running": bot_instance.is_running,
    }


@app.post("/api/start")
async def start_bot():
    if not ENV_PATH.exists():
        raise HTTPException(400, "No credentials configured. POST /api/credentials first.")
    env = dotenv_values(ENV_PATH)
    if not env.get("IG_USERNAME") or not env.get("IG_PASSWORD"):
        raise HTTPException(400, "IG_USERNAME / IG_PASSWORD not set.")
    if not bot_instance.start():
        raise HTTPException(409, "Bot already running.")
    _maybe_autostart_scraper()   # follow/churn + scraper.enabled → start it too
    return {"ok": True}


@app.post("/api/stop")
async def stop_bot():
    bot_instance.stop()
    return {"ok": True}


@app.post("/api/pause")
async def pause_bot():
    bot_instance.pause()
    return {"ok": True}


@app.post("/api/resume")
async def resume_bot():
    bot_instance.resume()
    return {"ok": True}


# ---------- config / whitelist / credentials ----------

@app.get("/api/config")
async def read_config():
    return bot.load_config()


@app.put("/api/config")
async def write_config(payload: ConfigUpdate):
    bot.save_config(payload.data)
    return {"ok": True}


@app.get("/api/whitelist")
async def read_whitelist():
    if not bot.WHITELIST_PATH.exists():
        return {"text": ""}
    return {"text": bot.WHITELIST_PATH.read_text(encoding="utf-8")}


@app.put("/api/whitelist")
async def write_whitelist(payload: WhitelistUpdate):
    bot.save_whitelist(payload.text)
    return {"ok": True}


@app.post("/api/whitelist/add")
async def add_to_whitelist(payload: WhitelistAdd):
    """Append a single username to the whitelist (the per-row 'Exclude' button)."""
    u = payload.username.strip().lstrip("@")
    if not u:
        raise HTTPException(400, "empty username")
    if u.lower() in bot.load_whitelist():
        return {"ok": True, "already": True}
    current = bot.WHITELIST_PATH.read_text(encoding="utf-8") if bot.WHITELIST_PATH.exists() else ""
    if current and not current.endswith("\n"):
        current += "\n"
    bot.save_whitelist(current + u + "\n")
    return {"ok": True}


@app.post("/api/skip/add")
async def add_to_skip(payload: WhitelistAdd):
    """Append a username to skipped.log (the per-row 'Skip' button). Skipped
    accounts are treated as done, so the bot won't try to unfollow them - unlike
    Exclude/whitelist this isn't permanent protection, just 'leave this one'."""
    u = payload.username.strip().lstrip("@").lower()
    if not u:
        raise HTTPException(400, "empty username")
    if u in {r["username"].lower() for r in bot.read_skipped_log()}:
        return {"ok": True, "already": True}
    path = bot.ROOT / bot.load_config()["logging"].get("skipped_log", "data/skipped.log")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    bot.append_log(path, f"{ts}\t{u}\tmanual_skip")
    return {"ok": True}


@app.get("/api/credentials")
async def get_credentials():
    if not ENV_PATH.exists():
        return {"has_username": False, "has_password": False, "username": ""}
    env = dotenv_values(ENV_PATH)
    return {
        "has_username": bool(env.get("IG_USERNAME")),
        "has_password": bool(env.get("IG_PASSWORD")),
        "username": env.get("IG_USERNAME", ""),
    }


@app.post("/api/credentials")
async def update_credentials(payload: CredentialsUpdate):
    ENV_PATH.touch(exist_ok=True)
    set_key(str(ENV_PATH), "IG_USERNAME", payload.username, quote_mode="never")
    set_key(str(ENV_PATH), "IG_PASSWORD", payload.password, quote_mode="never")
    return {"ok": True}


# ---------- lists ----------

@app.get("/api/lists")
async def get_lists():
    following = bot.read_following_cache()  # cache is already oldest-first
    whitelist = bot.load_whitelist()
    unfollowed = bot.read_unfollowed_log()
    failed = bot.read_failed_log()
    skipped = bot.read_skipped_log()

    done_set = {row["username"].lower() for row in unfollowed}
    skipped_set = {row["username"].lower() for row in skipped}
    failed_set = {row["username"].lower() for row in failed}
    snap = state_manager.snapshot()
    current = (snap.get("current_target") or "").lower()

    rows = []
    for u in following:
        u_low = u.lower()
        if u_low in done_set:
            status = "unfollowed"
        elif u_low in skipped_set:
            status = "skipped"
        elif u_low in failed_set:
            status = "failed"
        elif u_low in whitelist:
            status = "whitelisted"
        elif u_low == current:
            status = "current"
        else:
            status = "pending"
        rows.append({"username": u, "status": status})

    return {
        "following": rows,
        "unfollowed": unfollowed,
        "failed": failed,
        "skipped": skipped,
        "totals": {
            "following": len(following),
            "unfollowed": len(unfollowed),
            "failed": len(failed),
            "skipped": len(skipped),
            "whitelisted": sum(1 for u in following if u.lower() in whitelist),
        },
    }


# ---------- follow / growth ----------

@app.get("/api/follow-lists")
async def get_follow_lists():
    """Follow-side counterpart to /api/lists: the candidate pool plus the
    followed / skipped / failed / churn logs."""
    followed = bot.read_followed_log()
    skipped = bot.read_follow_skipped_log()
    failed = bot.read_follow_failed_log()
    churn_unfollowed = bot.read_churn_unfollowed_log()
    kept = bot.read_follow_kept_log()
    candidates = bot.read_follow_candidates()

    # The pending pool excludes anyone already actioned (mirrors _follow_done_set
    # for the parts that come from logs; following.json/whitelist are applied at
    # run time, so this is an upper bound on what the next run will attempt).
    done = {r["username"].lower() for r in followed}
    done |= {r["username"].lower() for r in skipped
             if r["reason"] in bot.PERMANENT_FOLLOW_SKIPS}
    done |= {r["username"].lower() for r in churn_unfollowed}
    # follow_candidates is the eligible result list (scraper-vetted, or self-scraped
    # when no external scraper), so "pending" is just it minus already-actioned.
    pending = [c for c in candidates if c["username"] not in done]

    return {
        "candidates": candidates,
        "pending": pending,
        "followed": followed,
        "follow_skipped": skipped,
        "follow_failed": failed,
        "churn_unfollowed": churn_unfollowed,
        "follow_kept": kept,
        "totals": {
            "candidates": len(candidates),
            "pending": len(pending),
            "followed": len(followed),
            "follow_skipped": len(skipped),
            "follow_failed": len(failed),
            "churn_unfollowed": len(churn_unfollowed),
            "follow_kept": len(kept),
            "reach_pool": len(bot.read_reach_pool()),   # harvested post links waiting to be liked
        },
    }


@app.get("/api/sources")
async def get_sources():
    sources = (bot.load_config().get("targeting", {}) or {}).get("sources", {}) or {}
    return {
        "follower_profiles": sources.get("profiles", []) or [],
        "liker_posts": sources.get("post_likers", []) or [],
        "commenter_posts": sources.get("post_commenters", []) or [],
        "hashtags": sources.get("hashtags", []) or [],
    }


def _dedup_clean(items, strip_at=False):
    out, seen = [], set()
    for s in items:
        u = (s or "").strip()
        if strip_at:
            u = u.lstrip("@").lower()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


@app.put("/api/sources")
async def put_sources(payload: SourcesUpdate):
    """Update the scrape sources in config.yaml (reads from disk first so other
    config keys aren't clobbered). Profiles/hashtags are normalized; URLs kept as-is."""
    fp = _dedup_clean(payload.follower_profiles, strip_at=True)
    lp = _dedup_clean(payload.liker_posts)
    cp = _dedup_clean(payload.commenter_posts)
    ht = [h.lstrip("#").lower() for h in _dedup_clean(payload.hashtags, strip_at=False)]
    cfg = bot.load_config()
    cfg.setdefault("targeting", {})
    cfg["targeting"].setdefault("sources", {})
    cfg["targeting"]["sources"]["profiles"] = fp
    cfg["targeting"]["sources"]["post_likers"] = lp
    cfg["targeting"]["sources"]["post_commenters"] = cp
    cfg["targeting"]["sources"]["hashtags"] = ht
    bot.save_config(cfg)
    return {"ok": True, "follower_profiles": fp, "liker_posts": lp,
            "commenter_posts": cp, "hashtags": ht}


@app.post("/api/scrape")
async def scrape_now():
    """Kick off a one-shot source scrape in the background."""
    sources = (bot.load_config().get("targeting", {}) or {}).get("sources", {}) or {}
    if not any(sources.get(k) for k in
               ("profiles", "post_likers", "post_commenters", "hashtags")):
        raise HTTPException(400, "No sources configured. Add follower profiles, post URLs, or hashtags first.")
    if not bot_instance.start_scrape():
        raise HTTPException(409, "Bot is busy (already running or scraping).")
    return {"ok": True}


@app.get("/api/discovered-sources")
async def get_discovered_sources():
    """Niche-influencer accounts the bot flagged for review (bio-keyword match).
    Already-added ones are filtered out."""
    current = {s.lstrip("@").lower() for s in
               ((bot.load_config().get("targeting", {}) or {}).get("sources", {}) or {})
               .get("profiles", []) or []}
    rows = [r for r in bot.read_discovered_sources()
            if r["username"].lower() not in current]
    return {"rows": rows, "count": len(rows)}


@app.post("/api/discovered-sources/add")
async def add_discovered_source(payload: DiscoveredAction):
    """Promote a discovered account into targeting.sources.profiles and drop
    it from the review queue."""
    u = payload.username.strip().lstrip("@").lower()
    if not u:
        raise HTTPException(400, "empty username")
    cfg = bot.load_config()
    cfg.setdefault("targeting", {}).setdefault("sources", {})
    fp = cfg["targeting"]["sources"].get("profiles", []) or []
    if u not in {s.lstrip("@").lower() for s in fp}:
        fp.append(u)
        cfg["targeting"]["sources"]["profiles"] = fp
        bot.save_config(cfg)
    bot.write_discovered_sources(
        [r for r in bot.read_discovered_sources() if r["username"].lower() != u])
    return {"ok": True}


@app.post("/api/discovered-sources/dismiss")
async def dismiss_discovered_source(payload: DiscoveredAction):
    """Remove a discovered account from the review queue without adding it."""
    u = payload.username.strip().lstrip("@").lower()
    if not u:
        raise HTTPException(400, "empty username")
    bot.write_discovered_sources(
        [r for r in bot.read_discovered_sources() if r["username"].lower() != u])
    return {"ok": True}


@app.get("/api/source-analytics")
async def get_source_analytics():
    """Per-source conversion: how many we followed from each source vs. how many
    followed us back (measured at churn time). Sorted by follow-back rate."""
    followed = bot.read_followed_log()
    outcomes = bot.read_follow_outcomes()
    agg: dict[str, dict] = {}
    for r in followed:
        src = r.get("source") or "(none)"
        agg.setdefault(src, {"source": src, "followed": 0, "measured": 0, "followed_back": 0})
        agg[src]["followed"] += 1
    # Dedup outcomes by username (last measurement wins) before aggregating.
    last: dict[str, dict] = {}
    for o in outcomes:
        last[o["username"].lower()] = o
    for o in last.values():
        src = o.get("source") or "(none)"
        agg.setdefault(src, {"source": src, "followed": 0, "measured": 0, "followed_back": 0})
        agg[src]["measured"] += 1
        if o["followed_back"]:
            agg[src]["followed_back"] += 1
    rows = []
    for a in agg.values():
        a["rate"] = round(100 * a["followed_back"] / a["measured"], 1) if a["measured"] else 0.0
        rows.append(a)
    rows.sort(key=lambda a: (a["rate"], a["measured"]), reverse=True)
    return {"rows": rows}


@app.get("/api/analytics")
async def get_analytics():
    """Everything the Analytics page graphs: growth, daily action volumes, totals
    & rates, failure/skip/reject reason breakdowns, and runtime/uptime/restart/error
    stats derived from the lifecycle event log."""
    import collections as _c
    import json as _json

    def day(ts):
        return (ts or "")[:10]

    def reason_counts(rows, n=12):
        c = _c.Counter((r.get("reason") or "?") for r in rows)
        return [{"reason": k, "count": v} for k, v in c.most_common(n)]

    def ts_rows(path):
        """Read a 'ts\\t...' log into just timestamps (for daily counting)."""
        p = bot.ROOT / path
        if not p.exists():
            return []
        out = []
        for line in p.read_text(encoding="utf-8").splitlines():
            parts = line.split("\t")
            if parts and parts[0]:
                out.append(parts[0])
        return out

    followed = bot.read_followed_log()
    kept = bot.read_follow_kept_log()
    churned = bot.read_churn_unfollowed_log()
    f_skipped = bot.read_follow_skipped_log()
    f_failed = bot.read_follow_failed_log()
    unfollowed = bot.read_unfollowed_log()
    failed = bot.read_failed_log()
    checked = bot.read_filter_checked_log()
    rejected = bot.read_filter_rejected_log()
    outcomes = bot.read_follow_outcomes()
    history = bot.read_account_history()
    events = bot.read_runtime_events()
    likes_ts = ts_rows("data/reach_liked.log")
    stories_ts = ts_rows("data/story_viewed.log")

    totals = {
        "followed": len(followed), "kept": len(kept), "churned": len(churned),
        "follow_skipped": len(f_skipped), "follow_failed": len(f_failed),
        "unfollowed": len(unfollowed), "failed": len(failed),
        "checked": len(checked), "rejected": len(rejected),
        "likes": len(likes_ts), "stories": len(stories_ts),
    }
    measured = len({o["username"].lower(): o for o in outcomes})
    back = sum(1 for o in {o["username"].lower(): o for o in outcomes}.values() if o["followed_back"])
    chk_rej = len(checked) + len(rejected)
    rates = {
        "follow_back_rate": round(100 * back / measured, 1) if measured else None,
        "reject_rate": round(100 * len(rejected) / chk_rej, 1) if chk_rej else None,
        "measured": measured, "followed_back": back,
    }

    # daily series
    daily = _c.defaultdict(lambda: {"follows": 0, "unfollows": 0, "churn": 0,
                                    "likes": 0, "checked": 0, "rejected": 0})
    for r in followed:
        daily[day(r["timestamp"])]["follows"] += 1
    for r in unfollowed:
        daily[day(r["timestamp"])]["unfollows"] += 1
    for r in churned:
        daily[day(r["timestamp"])]["churn"] += 1
    for r in checked:
        daily[day(r["timestamp"])]["checked"] += 1
    for r in rejected:
        daily[day(r["timestamp"])]["rejected"] += 1
    for t in likes_ts:
        daily[day(t)]["likes"] += 1
    daily_list = [dict(date=d, **v) for d, v in sorted(daily.items()) if d][-30:]

    # growth (downsample to keep the payload small)
    growth = [{"ts": r["timestamp"], "followers": r["followers"], "following": r["following"]}
              for r in history]
    if len(growth) > 300:
        step = len(growth) // 300 + 1
        growth = growth[::step]

    # runtime / lifecycle
    def intervals(start_kind, stop_kind):
        total, open_start, count = 0.0, None, 0
        for e in events:
            t = bot.parse_log_ts(e["timestamp"])
            if t is None:
                continue
            if e["kind"] == start_kind:
                open_start, count = t, count + 1
            elif e["kind"] == stop_kind and open_start is not None:
                total += max(0.0, t - open_start)
                open_start = None
        cur = None
        if open_start is not None:
            cur = max(0.0, time.time() - open_start)
            total += cur
        return total, count, cur

    bot_total, bot_runs, bot_cur = intervals("bot_start", "bot_stop")
    scr_total, scr_runs, scr_cur = intervals("scraper_start", "scraper_stop")
    kinds = _c.Counter(e["kind"] for e in events)
    first_ts = bot.parse_log_ts(events[0]["timestamp"]) if events else None
    span = (time.time() - first_ts) if first_ts else 0.0
    days = max(1.0, span / 86400)
    runtime = {
        "bot_runtime_total": int(bot_total), "scraper_runtime_total": int(scr_total),
        "bot_runs": bot_runs, "scraper_runs": scr_runs,
        "bot_current_uptime": int(bot_cur) if bot_cur else None,
        "scraper_current_uptime": int(scr_cur) if scr_cur else None,
        "errors": kinds.get("error", 0) + kinds.get("scraper_error", 0),
        "checkpoints": kinds.get("checkpoint", 0),
        "soft_blocks": kinds.get("soft_block", 0),
        "watchdog_restarts": kinds.get("watchdog_restart", 0),
        "server_starts": kinds.get("server_start", 0),
        "avg_daily_bot_runtime": int(bot_total / days),
        "avg_daily_scraper_runtime": int(scr_total / days),
        "observed_days": round(days, 1),
        "bot_uptime_pct": round(100 * bot_total / span, 1) if span else None,
        "downtime_total": int(max(0.0, span - bot_total)),
        "last_error": next((e["detail"] for e in reversed(events)
                            if e["kind"] in ("error", "scraper_error")), None),
        "last_checkpoint": next((e["timestamp"] for e in reversed(events)
                                 if e["kind"] == "checkpoint"), None),
    }

    today = {}
    try:
        L = _json.loads(bot.DAILY_COUNTS.read_text(encoding="utf-8"))
        if L.get("date") == time.strftime("%Y-%m-%d"):
            today = {"follows": L.get("follows", 0), "unfollows": L.get("unfollows", 0),
                     "likes": L.get("likes", 0), "caps": L.get("caps", {})}
    except Exception:
        pass

    # throughput / speed: events per ACTIVE 30-min window. Idle time is excluded
    # (a quiet overnight shouldn't drag the rate toward 0), so this reads as "while
    # it's working, it does ~N per half hour". Also peak window + last-24h rate.
    def _epochs(rows):
        out = []
        for r in rows:
            t = bot.parse_log_ts(r["timestamp"] if isinstance(r, dict) else r)
            if t is not None:
                out.append(t)
        return out

    def rate_stats(epochs):
        if not epochs:
            return {"total": 0, "per_30m": 0, "peak_30m": 0,
                    "active_windows": 0, "recent_per_30m": None}
        buckets = _c.Counter(int(t // 1800) for t in epochs)
        active = len(buckets)
        now_b = int(time.time() // 1800)
        recent = [v for b, v in buckets.items() if b > now_b - 48]   # last ~24h
        return {
            "total": sum(buckets.values()),
            "per_30m": round(sum(buckets.values()) / active, 1),
            "peak_30m": max(buckets.values()),
            "active_windows": active,
            "recent_per_30m": round(sum(recent) / len(recent), 1) if recent else None,
        }

    throughput = {
        "vetted": rate_stats(_epochs(checked) + _epochs(rejected)),
        "follows": rate_stats(_epochs(followed)),
        "unfollows": rate_stats(_epochs(unfollowed) + _epochs(churned)),
        "likes": rate_stats(_epochs(likes_ts)),
    }

    return {
        "totals": totals, "rates": rates, "daily": daily_list, "growth": growth,
        "fail_reasons": reason_counts(f_failed + failed),
        "skip_reasons": reason_counts(f_skipped),
        "reject_reasons": reason_counts(rejected),
        "runtime": runtime, "today": today, "throughput": throughput,
    }


@app.get("/api/activity")
async def get_activity():
    """The shared live-activity feed (same on every device). Replayed by the
    dashboard on load to rebuild the grouped log."""
    return {"events": state_manager.recent_events()}


@app.post("/api/activity/clear")
async def clear_activity():
    state_manager.clear_events()
    return {"ok": True}


@app.post("/api/account/refresh")
async def refresh_account(force: bool = False):
    """Refresh the account follower/following counts. While running, the bot
    already updates them, so this is a no-op then. While idle, it triggers a
    one-shot live fetch (background) unless we fetched very recently. `force=true`
    (a manual click) skips the freshness throttle so the user can re-sync now."""
    if bot_instance.is_running:
        return {"ok": True, "running": True}
    ts = bot.read_account_stats().get("ts", 0)
    if not force and time.time() - ts < 300:
        return {"ok": True, "fresh": True}
    threading.Thread(target=bot_instance.fetch_account_now, daemon=True).start()
    return {"ok": True, "started": True}


@app.get("/api/account/history")
async def account_history():
    """Lightweight follower/following time-series for the Overview sparkline
    (downsampled). The full series with timestamps is on /api/analytics."""
    rows = bot.read_account_history()
    pts = [{"f": r["followers"], "g": r["following"]} for r in rows]
    if len(pts) > 60:
        step = len(pts) // 60 + 1
        pts = pts[::step]
    return {"points": pts}


@app.get("/api/system")
async def get_system():
    """Host metrics for the System page, plus bot liveness for the watchdog UI."""
    s = _sys_stats.sample()
    wd = (bot.load_config().get("watchdog", {}) or {})
    s["bot_running"] = bot_instance.is_running
    s["heartbeat_age"] = round(time.time() - state_manager.last_heartbeat, 1)
    s["watchdog_enabled"] = bool(wd.get("enabled"))
    s["watchdog_threshold"] = float(wd.get("stuck_after_seconds", 600))
    s["autostart"] = bool((bot.load_config().get("server", {}) or {}).get("autostart"))
    return s


def _profile_rows(entries):
    """username + source rows linking to the profile (backlogs & follow pool)."""
    return [{"username": e["username"], "source": e.get("source", "") or "—",
             "link": f"https://www.instagram.com/{e['username']}/"} for e in entries]


@app.get("/api/pool/{kind}")
async def get_pool(kind: str):
    """Browseable contents for the dashboard's click-to-view list. Each row carries
    its scrape source (or, for `rejected`, the reason) and a link:
      follow         - vetted candidates the bot follows (→ profile)
      reach          - harvested posts to like (→ the post)
      follow_backlog - raw scraped accounts awaiting vetting (→ profile)
      reach_backlog  - raw reach prospects awaiting vetting (→ profile)
      rejected       - accounts the vetting filtered out, with the reason (→ profile)"""
    if kind == "follow":
        rows = _profile_rows(bot.read_follow_candidates())
        return {"kind": kind, "count": len(rows), "rows": rows}
    if kind == "follow_backlog":
        rows = _profile_rows(bot.read_scraper_todo())
        return {"kind": kind, "count": len(rows), "rows": rows}
    if kind == "reach_backlog":
        rows = _profile_rows(bot.read_reach_todo())
        return {"kind": kind, "count": len(rows), "rows": rows}
    if kind == "rejected":
        rows = [{"username": r["username"], "source": r.get("reason", "") or "—",
                 "link": f"https://www.instagram.com/{r['username']}/"}
                for r in reversed(bot.read_filter_rejected_log())]   # newest first
        return {"kind": kind, "count": len(rows), "rows": rows}
    if kind == "reach":
        rows = []
        for e in bot.read_reach_pool():
            u = e.get("username", "")
            rows.append({
                "username": u or "post",
                "source": e.get("source") or e.get("tag", "") or "—",
                "link": e.get("url") or (f"https://www.instagram.com/{u}/" if u else ""),
            })
        return {"kind": kind, "count": len(rows), "rows": rows}
    return {"kind": kind, "count": 0, "rows": []}


@app.get("/api/candidates")
async def get_candidates():
    """Return the candidate pool as newline-joined usernames for the editor."""
    entries = bot.read_follow_candidates()
    return {"text": "\n".join(e["username"] for e in entries), "count": len(entries)}


@app.put("/api/candidates")
async def put_candidates(payload: CandidatesUpdate):
    """Replace the candidate pool from newline-separated usernames (deduped).
    Manual entries get source 'manual'; '#' lines and blanks are ignored."""
    seen: set[str] = set()
    out = []
    for line in payload.text.splitlines():
        u = line.strip().lstrip("@").lower()
        if not u or u.startswith("#") or u in seen:
            continue
        seen.add(u)
        out.append({"username": u, "source": "manual"})
    bot.write_follow_candidates(out)
    return {"ok": True, "count": len(out)}


# Maps an API log name -> (logging config key, default path). Removing a row
# from a permanent-skip log (e.g. follow_skipped) lets the bot re-attempt that
# account on the next run.
_LOG_KEYS = {
    "skipped": ("skipped_log", "data/skipped.log"),
    "failed": ("failed_log", "data/failed.log"),
    "follow_skipped": ("follow_skipped_log", "data/follow_skipped.log"),
    "follow_failed": ("follow_failed_log", "data/follow_failed.log"),
    "followed": ("followed_log", "data/followed.log"),
}


@app.post("/api/log/remove")
async def remove_from_log(payload: LogRemove):
    """Delete every row for a username from one of the log files."""
    if payload.log not in _LOG_KEYS:
        raise HTTPException(400, f"unknown log '{payload.log}'")
    cfg_key, default = _LOG_KEYS[payload.log]
    path = bot.ROOT / bot.load_config()["logging"].get(cfg_key, default)
    u = payload.username.strip().lstrip("@").lower()
    if not u:
        raise HTTPException(400, "empty username")
    if not path.exists():
        return {"ok": True, "removed": 0}
    kept, removed = [], 0
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[1].strip().lower() == u:
            removed += 1
            continue
        kept.append(line)
    path.write_text(("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
    return {"ok": True, "removed": removed}


@app.post("/api/log/clear")
async def clear_log(payload: LogClear):
    """Wipe a skipped/failed log AND remove those users from the following cache
    so the bot won't revisit them. Backs the per-list 'Clean' buttons: clearing
    the log alone would make skipped users 'pending' again (and failed users are
    already retried), so we also prune them from data/following.json."""
    if payload.log not in ("skipped", "failed"):
        raise HTTPException(400, f"clear not supported for '{payload.log}'")
    cfg_key, default = _LOG_KEYS[payload.log]
    path = bot.ROOT / bot.load_config()["logging"].get(cfg_key, default)

    users = set()
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = line.split("\t")
            if len(parts) >= 2 and parts[1].strip():
                users.add(parts[1].strip().lower())

    pruned = 0
    if users:
        following = bot.read_following_cache()
        kept = [u for u in following if u.lower() not in users]
        pruned = len(following) - len(kept)
        if pruned:
            bot.write_following_cache(kept)

    cleared = len(users)
    if path.exists():
        path.write_text("", encoding="utf-8")
    return {"ok": True, "cleared": cleared, "pruned": pruned}


# Rebuildable pools the System page can wipe on demand (the scraper refills them).
# Permanent record logs (followed/unfollowed/etc.) are intentionally NOT here.
_CLEARABLE = {
    "reach_pool":        ("reach pool",       bot.read_reach_pool,        bot.write_reach_pool),
    "reach_todo":        ("reach backlog",    bot.read_reach_todo,        bot.write_reach_todo),
    "follow_candidates": ("candidate pool",   bot.read_follow_candidates, bot.write_follow_candidates),
    "scraper_todo":      ("scrape backlog",   bot.read_scraper_todo,      bot.write_scraper_todo),
}


@app.post("/api/data/clear")
async def clear_data(payload: DataClear):
    """Wipe a rebuildable pool (reach pool / backlogs / candidate pool). Safe: the
    scraper refills them; permanent logs are not clearable here."""
    entry = _CLEARABLE.get(payload.target)
    if not entry:
        raise HTTPException(400, f"unknown target '{payload.target}'")
    label, reader, writer = entry
    try:
        removed = len(reader())
    except Exception:
        removed = 0
    writer([])
    return {"ok": True, "label": label, "removed": removed}


# ---------- scraper service (server-managed subprocess) ----------

_scraper_proc = None   # subprocess.Popen handle for the server-managed scraper


def _start_scraper_proc() -> bool:
    """Launch the scraper service as a child process (same venv). No-op if one is
    already alive (managed, orphaned after a server restart, or systemd) - the PID
    file makes that check work across all three."""
    global _scraper_proc
    if bot.scraper_running():
        return False
    try:
        _scraper_proc = subprocess.Popen(
            [sys.executable, str(bot.ROOT / "scraper.py")],
            cwd=str(bot.ROOT),
            start_new_session=True,   # outlive a server restart; tracked via PID file
        )
        return True
    except Exception:
        _scraper_proc = None
        return False


def _stop_scraper_proc() -> bool:
    """Gracefully stop the running scraper (it handles SIGTERM → clean shutdown)."""
    global _scraper_proc
    pid = bot.scraper_pid()
    if not bot.scraper_running():
        _scraper_proc = None
        return False
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
    _scraper_proc = None
    return True


def _maybe_autostart_scraper() -> None:
    """Start the scraper alongside the bot when in follow/marketing mode and enabled."""
    try:
        cfg = bot.load_config()
        mode = (cfg.get("mode") or "").lower()
        scr = cfg.get("scraper", {}) or {}
        if scr.get("enabled") and mode in ("follow", "marketing"):
            _start_scraper_proc()
    except Exception:
        pass


_scraper_counts_cache = {"ts": 0.0, "data": {}}


def _live_scraper_counts() -> dict:
    """Pool sizes read straight from the on-disk pools (NOT the scraper's status
    snapshot), so they stay current even while the scraper is idle and the bot is
    consuming. TTL-cached (~2.5s) so rapid dashboard polling can't hammer the Pi -
    the done-set read (followed/churn/following logs) is the only non-trivial part."""
    now = time.time()
    if now - _scraper_counts_cache["ts"] < 2.5:
        return _scraper_counts_cache["data"]
    d = {}
    try:
        cfg = bot.load_config()
        limits = cfg.get("limits", {}) or {}
        scr = cfg.get("scraper", {}) or {}
        mult = int(scr.get("follow_pool_mult", 5) or 5)
        reach_mult = int(scr.get("reach_pool_mult", mult) or mult)
        d = {
            "follow_backlog": len(bot.read_scraper_todo()),
            "follow_pool": bot_instance._pool_ready({}),
            "reach_backlog": len(bot.read_reach_todo()),
            "reach_pool": bot_instance._reach_pool_ready(),
            "rejected": len(bot.read_filter_rejected_log()),
            "checked": len(bot.read_filter_checked_log()),
            # High-water marks = the most the scraper will build each pool to.
            "follow_high": max(1, int(limits.get("follows_per_day", 30) or 30) * mult),
            "reach_high": max(1, int(limits.get("likes_per_day", 100) or 100) * reach_mult),
        }
    except Exception:
        pass
    _scraper_counts_cache.update(ts=now, data=d)
    return d


def _scraper_working(phase: str, active: bool):
    """Which pipeline the scraper is on RIGHT NOW (for the active-border), from its
    live phase: 'reach' / 'follow' / None when idle."""
    if not active:
        return None
    p = (phase or "").lower()
    if "reach" in p:
        return "reach"
    if any(k in p for k in ("vet", "scrap", "candidate", "follow")):
        return "follow"
    return None


@app.get("/api/scraper")
async def scraper_status():
    """Status of the scraper service. `running` comes from the PID file's process
    liveness (authoritative); pool counts are read live from disk; the status file
    adds the current phase + last-update age."""
    import json as _json
    running = bot.scraper_running()
    path = bot.SCRAPER_STATUS
    data = {"present": False, "running": running}
    if path.exists():
        try:
            data = _json.loads(path.read_text(encoding="utf-8"))
            data["present"] = True
        except Exception:
            data = {"present": False}
        ts = float(data.get("ts", 0) or 0)
        data["age"] = int(time.time() - ts) if ts else None
        data["running"] = running   # PID liveness is authoritative (covers all launchers)
    # `active` = process alive AND actually scraping/vetting right now (fresh non-idle
    # phase). It's False while the scraper is up but idle/paused (e.g. the bot is acting),
    # so the UI can show "idle" rather than "on". Same logic the bot uses internally.
    data["active"] = bool(running) and bot_instance._scraper_active()
    # Which pipeline is being worked right now (for the UI's active highlight).
    data["working"] = _scraper_working(data.get("phase"), data["active"])
    # Live pool counts (always current). Keep legacy keys in sync for any old reader.
    counts = _live_scraper_counts()
    data.update(counts)
    if "follow_backlog" in counts:
        data["backlog"] = counts["follow_backlog"]
        data["ready"] = counts["follow_pool"]
        data["reach_ready"] = counts["reach_pool"]
    return data


@app.get("/api/scraper/log")
async def scraper_log():
    """Recent scraper log lines for the live feed (newest last). Written by the
    scraper process to its own ring file, so it works cross-process."""
    return {"events": bot.read_scraper_activity()}


@app.post("/api/scraper/start")
async def scraper_start():
    started = _start_scraper_proc()
    return {"ok": True, "started": started, "running": bot.scraper_running()}


@app.post("/api/scraper/stop")
async def scraper_stop():
    stopped = _stop_scraper_proc()
    return {"ok": True, "stopped": stopped, "running": bot.scraper_running()}


# ---------- deploy (browser file upload) ----------

# Web assets land in static/; everything else routes by extension below. Anything
# not matched is rejected so the endpoint can't write arbitrary paths.
_DEPLOY_STATIC_EXT = (".html", ".css", ".js", ".svg", ".png", ".ico", ".webmanifest")


def _deploy_dest(filename: str):
    """Map an uploaded filename to its on-disk destination, or None if it isn't
    a deployable file. basename strips any path/traversal from the upload."""
    name = os.path.basename((filename or "").replace("\\", "/")).strip()
    if not name or name.startswith("."):
        return None
    if name.endswith(".py"):
        return bot.ROOT / name                 # code -> project root
    if name.endswith(_DEPLOY_STATIC_EXT):
        return bot.ROOT / "static" / name       # web assets -> static/
    return None


def _do_restart(service: str):
    try:
        subprocess.run(_restart_cmd(service), capture_output=True, text=True, timeout=10)
    except Exception:
        pass


@app.post("/api/deploy/upload")
async def deploy_upload(payload: DeployUpload, background_tasks: BackgroundTasks):
    """Write uploaded source files into place (bot.py/server.py -> root,
    index.html & web assets -> static/) and, by default, restart the service.
    Backs the dashboard Deploy button so changes ship without scp. Files are sent
    as JSON text (no multipart dep). The restart runs as a background task so this
    response flushes before systemd stops the process."""
    written, rejected = [], []
    for f in payload.files:
        dest = _deploy_dest(f.name)
        if dest is None or not f.content:
            rejected.append(f.name)
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        # write_bytes preserves the exact content the browser read (no newline
        # translation), so a Windows-edited file lands byte-identical on the Pi.
        dest.write_bytes(f.content.encode("utf-8"))
        written.append(str(dest.relative_to(bot.ROOT)).replace("\\", "/"))

    result = {"ok": bool(written), "written": written, "rejected": rejected}
    if payload.restart and written:
        if sys.platform != "linux":
            result["restart_skipped"] = "restart only on the Pi (Linux) deployment"
        else:
            srv = bot.load_config().get("server", {}) or {}
            service = srv.get("service_name", "unfollower")
            background_tasks.add_task(_do_restart, service)
            result["restarting"] = service
    return result


# ---------- git deploy (phone-friendly: Pi pulls from GitHub) ----------

def _git(args, timeout=60):
    """Run a git command in the project dir. The service runs as the repo owner,
    so fetch/pull use that user's SSH deploy key (~/.ssh/config) with no sudo."""
    return subprocess.run(["git", "-C", str(bot.ROOT)] + args,
                          capture_output=True, text=True, timeout=timeout)


def _git_head(ref="HEAD"):
    r = _git(["log", "-1", "--pretty=%h\t%s\t%cr", ref])
    if r.returncode != 0 or not r.stdout.strip():
        return None
    parts = (r.stdout.strip().split("\t") + ["", "", ""])[:3]
    return {"hash": parts[0], "subject": parts[1], "when": parts[2]}


@app.get("/api/deploy/status")
async def deploy_status():
    """Current local commit + whether origin is ahead (an update is available).
    Does a best-effort fetch so the dashboard can show 'update available'."""
    if _git(["rev-parse", "--is-inside-work-tree"]).returncode != 0:
        return {"is_repo": False}
    branch = (_git(["rev-parse", "--abbrev-ref", "HEAD"]).stdout or "main").strip() or "main"
    info = {"is_repo": True, "branch": branch, "local": _git_head()}
    fr = _git(["fetch", "--quiet", "origin", branch], timeout=40)
    info["fetch_ok"] = fr.returncode == 0
    if fr.returncode != 0:
        info["fetch_error"] = (fr.stderr or "").strip()[:300]
        return info
    info["remote"] = _git_head(f"origin/{branch}")
    counts = _git(["rev-list", "--left-right", "--count", f"HEAD...origin/{branch}"])
    if counts.returncode == 0 and counts.stdout.strip():
        try:
            ahead, behind = counts.stdout.split()[:2]
            info["ahead"] = int(ahead); info["behind"] = int(behind)
        except Exception:
            pass
    return info


@app.post("/api/deploy/pull")
async def deploy_pull(background_tasks: BackgroundTasks):
    """Fetch origin and hard-reset the tracked code to its latest, then restart.
    Untracked/gitignored files (.env, config.yaml, data/) are left untouched, so
    pulling never disturbs the Pi's own config or live data. Restart runs as a
    background task so this response flushes first."""
    if _git(["rev-parse", "--is-inside-work-tree"]).returncode != 0:
        raise HTTPException(400, "Not a git repository on the server - finish the git deploy setup first.")
    branch = (_git(["rev-parse", "--abbrev-ref", "HEAD"]).stdout or "main").strip() or "main"
    fr = _git(["fetch", "origin", branch], timeout=90)
    if fr.returncode != 0:
        raise HTTPException(500, f"git fetch failed: {(fr.stderr or fr.stdout or '').strip()[:400]}")
    rs = _git(["reset", "--hard", f"origin/{branch}"], timeout=60)
    if rs.returncode != 0:
        raise HTTPException(500, f"git reset failed: {(rs.stderr or rs.stdout or '').strip()[:400]}")

    result = {"ok": True, "branch": branch, "head": _git_head()}
    if sys.platform == "linux":
        srv = bot.load_config().get("server", {}) or {}
        service = srv.get("service_name", "unfollower")
        background_tasks.add_task(_do_restart, service)
        result["restarting"] = service
    else:
        result["restart_skipped"] = "restart only on the Pi (Linux) deployment"
    return result


@app.post("/api/cache/clear")
async def clear_cache():
    """Delete the cached following list so the next run re-scrapes."""
    if bot.FOLLOWING_CACHE.exists():
        bot.FOLLOWING_CACHE.unlink()
    return {"ok": True}


@app.post("/api/restart")
async def restart_service():
    """Restart the systemd service that runs this dashboard+bot (Pi deployment).

    The dashboard process IS the service, so we use `systemctl --no-block restart`:
    systemd (PID 1) owns the job, so it completes even though this process is the
    one being restarted, and --no-block lets this HTTP response flush first. Needs
    one passwordless-sudo line (see DEPLOY.md)."""
    if sys.platform != "linux":
        raise HTTPException(400, "Restart is only supported on the Linux (Pi) deployment.")
    srv = bot.load_config().get("server", {}) or {}
    service = srv.get("service_name", "unfollower")
    try:
        proc = subprocess.run(_restart_cmd(service), capture_output=True, text=True, timeout=10)
    except Exception as e:
        raise HTTPException(500, f"Could not run restart: {e}")
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}"
        raise HTTPException(500, f"Restart failed: {detail}")
    return {"ok": True, "service": service}


# ---------- websocket ----------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue()
    state_manager.subscribe(q)
    await ws.send_json({"type": "state", "data": state_manager.snapshot()})
    try:
        while True:
            msg = await q.get()
            await ws.send_json(msg)
    except WebSocketDisconnect:
        pass
    finally:
        state_manager.unsubscribe(q)


# ---------- static UI ----------

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    # no-store so the iPhone PWA / Safari always fetches the latest dashboard
    # instead of serving a stale cached copy after a deploy.
    return FileResponse(str(STATIC_DIR / "index.html"),
                        headers={"Cache-Control": "no-store, max-age=0"})


if __name__ == "__main__":
    import uvicorn
    srv = bot.load_config().get("server", {}) or {}
    host = srv.get("host", "127.0.0.1")
    port = int(srv.get("port", 8000))
    print(f"dashboard on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)
