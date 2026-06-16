# unfollower

Browser-automation tool that unfollows Instagram accounts you followed long ago — oldest-first, with a whitelist and humanized pacing. Comes with a web dashboard for control + live status.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
```

You can configure credentials in the UI (Credentials tab) or by editing `.env`:

```powershell
Copy-Item .env.example .env
# then edit .env
```

## Run with the dashboard

```powershell
python server.py
```

Open <http://127.0.0.1:8000> in your browser.

The dashboard has:

- **Top bar** — current state badge, phase detail, current target, countdown to next action, progress bar.
- **Action row** — Start / Pause / Resume / Stop / Refresh / Clear cached following.
- **Stats** — following total, unfollowed (session + total), failed, whitelisted, daily cap.
- **Following tab** — full list (oldest first) with status badges (`pending`, `current`, `unfollowed`, `failed`, `whitelisted`), text filter + status filter.
- **Unfollowed tab** — live log of completed unfollows.
- **Failed tab** — live log of failures with reason.
- **Config tab** — edit `config.yaml` (pacing, browser, behavior).
- **Whitelist tab** — edit `whitelist.txt`.
- **Credentials tab** — write `.env` from the browser.

State updates over WebSocket — the page reflects bot progress in real time.

## Run headless (no UI)

```powershell
python main.py
```

CLI mode runs the same `Bot` class and prints status changes to the terminal. Ctrl-C stops gracefully.

## Files

| Path | Purpose |
|------|---------|
| `server.py` | FastAPI app — REST + WebSocket + serves dashboard. |
| `bot.py` | `Bot` worker + `StateManager` (shared between CLI and server). |
| `main.py` | CLI runner. |
| `static/index.html` | Single-page dashboard (Alpine.js, no build step). |
| `config.yaml` | Pacing, browser, behavior settings. |
| `whitelist.txt` | Usernames to never unfollow. |
| `.env` | Credentials. **Not committed.** |
| `data/session.json` | Saved login cookies. |
| `data/following.json` | Cached following list. Delete (or click "Clear cached following") to force re-scrape. |
| `data/unfollowed.log` | Append-only log of unfollows. Used to skip already-processed users on rerun. |
| `data/failed.log` | Append-only log of failures. |

## Tuning pacing

`config.yaml > pacing` (also editable in the Config tab):

- `daily_cap` — max unfollows per run.
- `min_delay_seconds` / `max_delay_seconds` — random wait between each unfollow.
- `long_break_every_n` — take a longer pause every N unfollows.
- `long_break_min_seconds` / `long_break_max_seconds` — range for that long pause.
- `distraction_chance` — probability of an extra-long random pause.

Conservative defaults: ~80/day, 45–180s between actions, 5–15 min break every 15 unfollows.

## Notes

- 5+ consecutive failures → the bot bails. Treat that as a soft action-block and stop for the day.
- Re-runs read `data/unfollowed.log` and skip anyone already processed.
- The following list is scraped once and cached. Use "Clear cached following" to refresh it.
- Locale defaults to `en-US` because the button-text matching is English-only — set IG to English in your account settings, or update the regexes in `bot.py` for your language.
