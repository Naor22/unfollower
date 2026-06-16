"""Merge two data/ directories without losing anything.

Use when you have logs/lists in two places (e.g. the Pi's existing history and a
PC's newer follow data) and want the UNION on one machine. Run it on the
destination, pointing at the incoming copy:

    python merge_data.py <incoming_dir>            # merges <incoming_dir> -> ./data
    python merge_data.py <incoming_dir> --base DIR # merge into a different base
    python merge_data.py <incoming_dir> --dry-run  # show what would change

What it does, per file (only known files are touched — session.json, config,
screenshots, chrome profiles are left alone):

  *.log (ts \\t username \\t ...)  union, deduped by username, keeping the
                                  EARLIEST timestamp per username, sorted by ts.
                                  (Earliest matters for followed.log: the churn
                                  timer ages from the first follow.)
  following.json (list[str])      union, base order first then new appended.
  follow_candidates.json          union of {username, source}, deduped by name.
  whitelist.txt (if present)      union of entries (comments/order preserved).

The base files are backed up to data/_backup_<timestamp>/ before anything is
written.
"""

import json
import shutil
import sys
import time
from pathlib import Path

LOG_FILES = (
    "unfollowed.log", "failed.log", "skipped.log",
    "followed.log", "follow_skipped.log", "follow_failed.log",
    "churn_unfollowed.log", "follow_kept.log",
)


def _read_lines(p: Path) -> list[str]:
    if not p.exists():
        return []
    return [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]


def merge_log(base: Path, incoming: Path) -> tuple[list[str], int, int]:
    """Union two tab-separated logs, dedup by username (col 1), keep earliest
    timestamp (col 0). Returns (lines, base_count, added)."""
    by_user: dict[str, tuple[str, str]] = {}  # user -> (ts, full_line)
    base_lines = _read_lines(base)
    for src in (base_lines, _read_lines(incoming)):
        for ln in src:
            parts = ln.split("\t")
            if len(parts) < 2:
                continue
            user = parts[1].strip().lower()
            ts = parts[0].strip()
            cur = by_user.get(user)
            if cur is None or ts < cur[0]:
                by_user[user] = (ts, ln)
    merged = [v[1] for v in by_user.values()]
    merged.sort(key=lambda ln: ln.split("\t")[0])
    return merged, len(base_lines), len(by_user) - _unique_users(base_lines)


def _unique_users(lines: list[str]) -> int:
    return len({ln.split("\t")[1].strip().lower() for ln in lines if len(ln.split("\t")) >= 2})


def merge_following(base: Path, incoming: Path):
    def load(p):
        if not p.exists():
            return []
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
    base_list = [str(u).lower() for u in load(base)]
    seen = set(base_list)
    out = list(base_list)
    added = 0
    for u in load(incoming):
        u = str(u).lower()
        if u not in seen:
            seen.add(u)
            out.append(u)
            added += 1
    return out, len(base_list), added


def merge_candidates(base: Path, incoming: Path):
    def load(p):
        if not p.exists():
            return []
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
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
    base_list = load(base)
    seen = {c["username"] for c in base_list}
    out = list(base_list)
    added = 0
    for c in load(incoming):
        if c["username"] not in seen:
            seen.add(c["username"])
            out.append(c)
            added += 1
    return out, len(base_list), added


def merge_whitelist(base: Path, incoming: Path):
    """Union usernames; keep base's comments/order, append new usernames."""
    base_lines = _read_lines(base)
    have = {ln.strip().lstrip("@").lower() for ln in base_lines
            if ln.strip() and not ln.strip().startswith("#")}
    out = list(base_lines)
    added = 0
    for ln in _read_lines(incoming):
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        u = s.lstrip("@").lower()
        if u not in have:
            have.add(u)
            out.append(u)
            added += 1
    return out, len(have) - added, added


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    base_dir = Path("data")
    if "--base" in sys.argv:
        base_dir = Path(sys.argv[sys.argv.index("--base") + 1])
    if not args:
        print("usage: python merge_data.py <incoming_dir> [--base DIR] [--dry-run]")
        return 2
    incoming_dir = Path(args[0])
    if not incoming_dir.exists():
        print(f"incoming dir not found: {incoming_dir}")
        return 2
    base_dir.mkdir(parents=True, exist_ok=True)

    plan = []  # (path, content_writer, summary)

    for name in LOG_FILES:
        merged, base_n, added = merge_log(base_dir / name, incoming_dir / name)
        if merged or (base_dir / name).exists() or (incoming_dir / name).exists():
            plan.append((base_dir / name, "\n".join(merged) + ("\n" if merged else ""),
                         f"{name}: {base_n} base + {added} new = {len(merged)}"))

    fl, base_n, added = merge_following(base_dir / "following.json", incoming_dir / "following.json")
    if fl:
        plan.append((base_dir / "following.json", json.dumps(fl, indent=2),
                     f"following.json: {base_n} base + {added} new = {len(fl)}"))

    cands, base_n, added = merge_candidates(base_dir / "follow_candidates.json",
                                            incoming_dir / "follow_candidates.json")
    if cands:
        plan.append((base_dir / "follow_candidates.json", json.dumps(cands, indent=2),
                     f"follow_candidates.json: {base_n} base + {added} new = {len(cands)}"))

    # whitelist.txt normally lives in the project root next to data/. For the
    # incoming copy, accept it either inside the dir (data_pc/whitelist.txt) or
    # beside it (data_pc/../whitelist.txt).
    wl_base = base_dir.parent / "whitelist.txt"
    wl_in = incoming_dir / "whitelist.txt"
    if not wl_in.exists():
        wl_in = incoming_dir.parent / "whitelist.txt"
    if wl_base.exists() or wl_in.exists():
        lines, base_n, added = merge_whitelist(wl_base, wl_in)
        plan.append((wl_base, "\n".join(lines) + ("\n" if lines else ""),
                     f"whitelist.txt: +{added} new"))

    print(f"Merging {incoming_dir}  ->  {base_dir}\n")
    for _, _, summary in plan:
        print("  " + summary)

    if dry:
        print("\n(dry run — nothing written)")
        return 0

    # Back up the existing base files we're about to overwrite.
    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup = base_dir / f"_backup_{stamp}"
    backup.mkdir(parents=True, exist_ok=True)
    for path, _, _ in plan:
        if path.exists():
            shutil.copy2(path, backup / path.name)
    print(f"\nBacked up existing files to {backup}")

    for path, content, _ in plan:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    print("Merge complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
