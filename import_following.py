"""Import your following list from Instagram's official data export.

This avoids scraping entirely (Instagram throttles the web following list). The
export also records WHEN you followed each account, so we can sort by true
oldest-first.

How to get the export:
  Instagram -> Settings and activity -> Accounts Center
    -> Your information and permissions -> Download your information
    -> Download or transfer information -> (pick your IG account)
    -> "Some of your information" -> under "Connections" check
       "Followers and following"
    -> Date range: All time;  Format: JSON
    -> Create files. Instagram emails a download link (minutes to hours).

The ZIP contains:  connections/followers_and_following/following.json

Usage (point at the zip, the json, or the unzipped folder):
  python import_following.py "C:\\path\\to\\instagram_export.zip"
  python import_following.py "C:\\path\\to\\following.json"

Writes data/following.json (oldest-first) — the bot's cache.
"""

import json
import re
import sys
import time
import zipfile
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).parent
OUT = ROOT / "data" / "following.json"
RESERVED = {"explore", "reels", "direct", "accounts", "p", "stories", "tv",
            "about", "api", "web", "graphql", "challenge"}
INNER_NAME = "following.json"


def _load_export_json(arg: str) -> dict:
    p = Path(arg)
    if not p.exists():
        sys.exit(f"path not found: {p}")

    if p.is_dir():
        # find following.json somewhere under the folder
        for cand in p.rglob(INNER_NAME):
            return json.loads(cand.read_text(encoding="utf-8"))
        sys.exit(f"no {INNER_NAME} found under {p}")

    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p) as z:
            names = [n for n in z.namelist() if n.endswith(INNER_NAME)
                     and "followers_and_following" in n]
            if not names:
                names = [n for n in z.namelist() if n.endswith(INNER_NAME)]
            if not names:
                sys.exit(f"no {INNER_NAME} inside {p}")
            with z.open(names[0]) as f:
                print(f"[*] reading {names[0]} from zip")
                return json.loads(f.read().decode("utf-8"))

    # assume it's the json file itself
    return json.loads(p.read_text(encoding="utf-8"))


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: python import_following.py <export.zip | following.json | folder>")

    data = _load_export_json(sys.argv[1])

    # The export wraps entries under "relationships_following"; each entry has a
    # "string_list_data" list with {value (username), href, timestamp}.
    entries = data.get("relationships_following")
    if entries is None and isinstance(data, list):
        entries = data  # some exports are already a bare list
    if not entries:
        sys.exit("could not find 'relationships_following' in the export JSON")

    def href_user(href: str) -> str:
        # e.g. https://www.instagram.com/_u/username  or  .../username
        if not href:
            return ""
        tail = href.rstrip("/").split("/")[-1]
        return tail

    rows = []  # (timestamp, username)
    deleted_placeholders = 0
    for e in entries:
        sld = e.get("string_list_data") or []
        item = sld[0] if sld else {}
        # Username can live in item.value, the entry "title", or the href tail.
        user = (item.get("value") or e.get("title") or href_user(item.get("href", "")))
        user = user.strip().lstrip("@").lower()
        ts = item.get("timestamp") or 0
        if not user or user in RESERVED:
            continue
        # Instagram replaces deleted accounts with a "__deleted__<hash>"
        # placeholder — drop these outright, they can never be unfollowed.
        if user.startswith("__deleted__"):
            deleted_placeholders += 1
            continue
        if not re.match(r"^[a-z0-9._]+$", user):
            continue
        rows.append((ts, user))

    # de-dup keeping earliest timestamp, then sort oldest-first.
    by_user: dict[str, int] = {}
    for ts, user in rows:
        if user not in by_user or ts < by_user[user]:
            by_user[user] = ts
    ordered = sorted(by_user.items(), key=lambda kv: kv[1])  # oldest follow first
    usernames = [u for u, _ in ordered]

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(usernames, indent=2), encoding="utf-8")

    def fmt(ts):
        return time.strftime("%Y-%m-%d", time.localtime(ts)) if ts else "unknown"

    print(f"[+] parsed {len(usernames)} followings")
    if deleted_placeholders:
        print(f"[+] dropped {deleted_placeholders} __deleted__ placeholder accounts")
    print(f"[+] wrote {OUT} (oldest-first)")
    print("\nOldest 10 (unfollowed first):")
    for u, ts in ordered[:10]:
        print(f"   {fmt(ts)}  @{u}")
    print("\nNewest 10 (unfollowed last):")
    for u, ts in ordered[-10:]:
        print(f"   {fmt(ts)}  @{u}")


if __name__ == "__main__":
    main()
