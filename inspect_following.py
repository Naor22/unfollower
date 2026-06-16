"""Scrape-verification harness for the 'following' modal.

Read-only: opens YOUR following list, scrolls it while accumulating usernames
into a set on every step (so virtualized rows can't be lost), and reports the
trend. Pass --full to scroll all the way; otherwise it does --max scrolls.

  python inspect_following.py <username> [--max 40] [--full]
"""

import argparse
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).parent
CDP = "http://localhost:9222"
USERNAME_HREF_RE = re.compile(r"^/([A-Za-z0-9._]+)/?$")
RESERVED = {"explore", "reels", "direct", "accounts", "p", "stories", "tv",
            "about", "api", "web", "graphql", "challenge"}

COLLECT_JS = """(d) => Array.from(
    d.querySelectorAll('a[role="link"][href^="/"]')
).map(a => a.getAttribute('href'))"""

FIND_SCROLLER_JS = """(d) => {
  let best = null, area = 0;
  for (const el of d.querySelectorAll('div')) {
    const cs = getComputedStyle(el);
    if ((cs.overflowY === 'auto' || cs.overflowY === 'scroll') &&
        el.scrollHeight > el.clientHeight) {
      const r = el.getBoundingClientRect();
      const a = r.width * r.height;
      if (a > area) { best = el; area = a; }
    }
  }
  return best;
}"""


def collect(dialog, seen, order, me):
    for h in dialog.evaluate(COLLECT_JS):
        m = USERNAME_HREF_RE.match(h)
        if not m:
            continue
        u = m.group(1).lower()
        if u in RESERVED or u == me or u in seen:
            continue
        seen.add(u)
        order.append(u)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("username")
    ap.add_argument("--max", type=int, default=40, help="max scroll steps (ignored with --full)")
    ap.add_argument("--full", action="store_true", help="scroll until the list stops growing")
    ap.add_argument("--save", action="store_true",
                    help="write data/following.json (bot cache format: scrape order)")
    args = ap.parse_args()
    me = args.username.lower()

    with sync_playwright() as p:
        b = p.chromium.connect_over_cdp(CDP)
        ctx = b.contexts[0]
        pg = ctx.pages[0]
        pg.goto(f"https://www.instagram.com/{args.username}/", wait_until="domcontentloaded")
        time.sleep(5)

        pg.get_by_role("link", name=re.compile(r"following", re.I)).first.click(timeout=8000)
        pg.wait_for_selector('div[role="dialog"]', timeout=15000)
        time.sleep(2)
        dialog = pg.locator('div[role="dialog"]').last
        scroller = dialog.evaluate_handle(FIND_SCROLLER_JS)
        if not scroller:
            print("[!] scroller not found")
            return

        seen, order = set(), []
        collect(dialog, seen, order, me)
        print(f"[*] start: {len(seen)} users in DOM")

        scroll_js = ('(d) => { const a = d.querySelectorAll(\'a[role="link"][href^="/"]\');'
                     ' if (a.length) a[a.length - 1].scrollIntoView(); }')
        jiggle_js = ('(d) => { const a = d.querySelectorAll(\'a[role="link"][href^="/"]\');'
                     ' if (a.length > 5) a[a.length - 5].scrollIntoView(); }')
        prev, stagnant, step, recoveries = -1, 0, 0, 0
        MAX_RECOVERIES = 6
        while True:
            step += 1
            dialog.evaluate(scroll_js)
            time.sleep(1.2)
            collect(dialog, seen, order, me)
            dom = dialog.evaluate("(d) => d.querySelectorAll('a[role=\"link\"][href^=\"/\"]').length")
            if step % 5 == 0 or not args.full:
                print(f"  step {step:3}: accumulated={len(seen):5}  in-DOM={dom}")

            if len(seen) == prev:
                stagnant += 1
            else:
                stagnant = 0
                prev = len(seen)

            if not args.full:
                if step >= args.max:
                    break
                continue

            # --full: on a stall, try a cooldown + jiggle before giving up,
            # in case IG is throttling the paginated loads.
            if stagnant >= 5:
                if recoveries >= MAX_RECOVERIES:
                    print(f"[*] growth stopped at {len(seen)} after {recoveries} cooldown retries")
                    break
                recoveries += 1
                print(f"[~] stalled at {len(seen)}; cooldown #{recoveries} (25s)...")
                dialog.evaluate(jiggle_js)
                time.sleep(25)
                dialog.evaluate(scroll_js)
                time.sleep(2)
                collect(dialog, seen, order, me)
                if len(seen) > prev:
                    print(f"[+] resumed: {len(seen)}")
                    stagnant = 0
                    prev = len(seen)

        print(f"\n[=] total unique collected: {len(seen)}")
        print(f"[=] first 10 in scroll order (newest-followed first on IG):")
        for u in order[:10]:
            print("     ", u)
        print(f"[=] last 10 in scroll order (oldest-followed):")
        for u in order[-10:]:
            print("     ", u)

        if args.save:
            import json
            out = ROOT / "data" / "following.json"
            out.parent.mkdir(exist_ok=True)
            # Bot cache format: scrape order (newest-first); bot reverses on load.
            out.write_text(json.dumps(order, indent=2), encoding="utf-8")
            print(f"\n[+] wrote {len(order)} usernames to {out}")

        pg.keyboard.press("Escape")


if __name__ == "__main__":
    main()
