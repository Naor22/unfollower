"""FastAPI server: REST + WebSocket for the dashboard UI."""

import asyncio
import os
import shutil
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
                    f"watchdog: bot stuck {age:.0f}s but server.autostart is off — "
                    "not restarting (it wouldn't resume). Enable autostart."})
                last_restart = time.time()
                continue
            if sys.platform != "linux":
                continue
            service = srv.get("service_name", "unfollower")
            state_manager.emit("log", {"level": "error", "msg":
                f"watchdog: no heartbeat for {age:.0f}s — restarting {service}"})
            try:
                subprocess.Popen(_restart_cmd(service))
                last_restart = time.time()
            except Exception as e:
                state_manager.emit("log", {"level": "error",
                                           "msg": f"watchdog restart failed: {e}"})
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    state_manager.attach_loop(asyncio.get_running_loop())
    threading.Thread(target=_watchdog_loop, daemon=True).start()
    # Seed the account status bar from the last-known counts so it shows on open
    # even when idle / right after a restart, before any live fetch.
    try:
        stats = bot.read_account_stats()
        if stats:
            state_manager.update(account_followers=stats.get("followers"),
                                 account_following=stats.get("following"))
    except Exception:
        pass
    # Pi mode: auto-start the bot on launch so it resumes after a reboot
    # (combined with behavior.daily_loop this runs unattended forever).
    try:
        srv = bot.load_config().get("server", {}) or {}
        env = dotenv_values(ENV_PATH) if ENV_PATH.exists() else {}
        if srv.get("autostart") and env.get("IG_USERNAME"):
            bot_instance.start()
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
    accounts are treated as done, so the bot won't try to unfollow them — unlike
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
        },
    }


@app.get("/api/sources")
async def get_sources():
    sources = (bot.load_config().get("follow", {}) or {}).get("sources", {}) or {}
    return {
        "follower_profiles": sources.get("follower_profiles", []) or [],
        "liker_posts": sources.get("liker_posts", []) or [],
        "commenter_posts": sources.get("commenter_posts", []) or [],
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
    cfg.setdefault("follow", {})
    cfg["follow"].setdefault("sources", {})
    cfg["follow"]["sources"]["follower_profiles"] = fp
    cfg["follow"]["sources"]["liker_posts"] = lp
    cfg["follow"]["sources"]["commenter_posts"] = cp
    cfg["follow"]["sources"]["hashtags"] = ht
    bot.save_config(cfg)
    return {"ok": True, "follower_profiles": fp, "liker_posts": lp,
            "commenter_posts": cp, "hashtags": ht}


@app.post("/api/scrape")
async def scrape_now():
    """Kick off a one-shot source scrape in the background."""
    sources = (bot.load_config().get("follow", {}) or {}).get("sources", {}) or {}
    if not any(sources.get(k) for k in
               ("follower_profiles", "liker_posts", "commenter_posts", "hashtags")):
        raise HTTPException(400, "No sources configured. Add follower profiles, post URLs, or hashtags first.")
    if not bot_instance.start_scrape():
        raise HTTPException(409, "Bot is busy (already running or scraping).")
    return {"ok": True}


@app.get("/api/discovered-sources")
async def get_discovered_sources():
    """Niche-influencer accounts the bot flagged for review (bio-keyword match).
    Already-added ones are filtered out."""
    current = {s.lstrip("@").lower() for s in
               ((bot.load_config().get("follow", {}) or {}).get("sources", {}) or {})
               .get("follower_profiles", []) or []}
    rows = [r for r in bot.read_discovered_sources()
            if r["username"].lower() not in current]
    return {"rows": rows, "count": len(rows)}


@app.post("/api/discovered-sources/add")
async def add_discovered_source(payload: DiscoveredAction):
    """Promote a discovered account into follow.sources.follower_profiles and drop
    it from the review queue."""
    u = payload.username.strip().lstrip("@").lower()
    if not u:
        raise HTTPException(400, "empty username")
    cfg = bot.load_config()
    cfg.setdefault("follow", {}).setdefault("sources", {})
    fp = cfg["follow"]["sources"].get("follower_profiles", []) or []
    if u not in {s.lstrip("@").lower() for s in fp}:
        fp.append(u)
        cfg["follow"]["sources"]["follower_profiles"] = fp
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
async def refresh_account():
    """Refresh the account follower/following counts. While running, the bot
    already updates them, so this is a no-op then. While idle, it triggers a
    one-shot live fetch (background) unless we fetched very recently."""
    if bot_instance.is_running:
        return {"ok": True, "running": True}
    ts = bot.read_account_stats().get("ts", 0)
    if time.time() - ts < 300:
        return {"ok": True, "fresh": True}
    threading.Thread(target=bot_instance.fetch_account_now, daemon=True).start()
    return {"ok": True, "started": True}


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


# ---------- scraper service status ----------

@app.get("/api/scraper")
async def scraper_status():
    """Status of the standalone scraper service (a separate process). Reads the
    heartbeat/counts file it publishes and derives `running` from heartbeat
    freshness (the service writes its status every pass / every few profiles)."""
    import json as _json
    path = bot.SCRAPER_STATUS
    if not path.exists():
        return {"present": False, "running": False}
    try:
        data = _json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"present": False, "running": False}
    ts = float(data.get("ts", 0) or 0)
    data["present"] = True
    data["age"] = int(time.time() - ts) if ts else None
    data["running"] = bool(ts and (time.time() - ts) < 180)
    return data


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
        raise HTTPException(400, "Not a git repository on the server — finish the git deploy setup first.")
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
