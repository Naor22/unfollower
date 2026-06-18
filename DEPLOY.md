# Deploying to a Raspberry Pi (always-on, headless)

Goal: the Pi runs the bot unattended — a daily batch of unfollows forever — and
exposes the dashboard so you can check on it from anywhere over Tailscale.

This is the **copy-session** model: you stay logged in on your PC, export the
session cookies, and the Pi's headless Chromium reuses them. No login on the Pi.

---

## 0. Prerequisites

- Raspberry Pi running **64-bit Raspberry Pi OS** (Bookworm recommended). 64-bit
  matters — Chromium + Playwright are happiest there. Check with `uname -m`
  (should say `aarch64`).
- Python 3.11+ (`python3 --version`).

## 1. Install system Chromium + libraries

Playwright doesn't ship a Chromium build for the Pi, so we use the OS one.

On current Raspberry Pi OS / Debian (Bookworm+), the package is `chromium`
(not `chromium-browser`) and several libs use the `t64` suffix:

```bash
sudo apt update
sudo apt install -y chromium fonts-liberation libnss3 \
  libatk-bridge2.0-0t64 libgtk-3-0t64 libgbm1 libasound2t64
# Find the binary (usually /usr/bin/chromium):
which chromium chromium-browser
# Sanity check it runs headless:
chromium --version
chromium --headless=new --no-sandbox --dump-dom https://example.com | head -5
```

(On older OS releases the names are `chromium-browser`, `libasound2`,
`libatk-bridge2.0-0`, `libgtk-3-0` without the `t64` suffix.)

## 2. Copy the project to the Pi

From your PC (PowerShell), e.g. with scp (replace pi host/IP):

```powershell
scp -r "C:\Users\Naor\Desktop\code\unfollower" pi@raspberrypi.local:/home/pi/unfollower
```

Don't worry about copying `.venv` — we recreate it on the Pi.

## 3. Python environment on the Pi

```bash
cd /home/pi/unfollower
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# We use system Chromium, so NO `playwright install` needed.
```

## 4. Bring over the following list

Copy the imported list (and optionally your progress logs) to the Pi:

```powershell
scp "C:\Users\Naor\Desktop\code\unfollower\data\following.json" pi@raspberrypi.local:/home/pi/unfollower/data/
# optional, to continue exactly where the PC left off:
scp "C:\Users\Naor\Desktop\code\unfollower\data\unfollowed.log" pi@raspberrypi.local:/home/pi/unfollower/data/
scp "C:\Users\Naor\Desktop\code\unfollower\data\skipped.log"    pi@raspberrypi.local:/home/pi/unfollower/data/
```

## 5. Log in — pick ONE model

Note: an Instagram `sessionid` lasts **months**, so neither option is daily
maintenance.

### Option A — Permanent Pi-native login (recommended)

A persistent Chromium profile on the Pi. Log in **once**, stays logged in across
runs/reboots, self-refreshes, and avoids the "new login from a new device"
challenge you'd get by copying a PC session.

In `config.yaml` set:
```yaml
browser:
  user_data_dir: "/home/pi/unfollower/data/ig-profile"
  executable_path: "/usr/bin/chromium"
```
Ensure `.env` has `IG_USERNAME` + `IG_PASSWORD`, then run the console login
helper (it prompts for your 2FA code in the terminal — no screen needed):
```bash
cd /home/pi/unfollower && source .venv/bin/activate
python pi_login.py
```
On success the profile is logged in and the bot reuses it. (If Instagram shows
an image *captcha* rather than a code, it can't be solved in a terminal — use
Option B.)

### Option B — Copy session from your PC

Leave `browser.user_data_dir` empty. On your **PC** (logged in via the CDP
Chrome) run `python export_session.py` to write `data\session.json`, then:
```powershell
scp "C:\Users\Naor\Desktop\code\unfollower\data\session.json" pi@raspberrypi.local:/home/pi/unfollower/data/
```
Re-run this only if the session ever expires (rare).

## 6. Configure the rest for the Pi

```yaml
browser:
  cdp_endpoint: ""                       # IMPORTANT: empty -> bot launches its own browser
  headless: true                         # no screen on the Pi
  executable_path: "/usr/bin/chromium"   # from step 1
  # user_data_dir set above if you chose Option A

pacing:
  daily_cap: 150                         # your choice

behavior:
  use_following_cache: true              # use the imported following.json
  daily_loop: true                       # run a batch, sleep ~24h, repeat forever

server:
  host: "0.0.0.0"                        # reachable over Tailscale/LAN
  port: 8000
  autostart: true                        # start the bot automatically on boot
```

## 7. Test it once by hand

```bash
cd /home/pi/unfollower && source .venv/bin/activate
python server.py
```

From your PC/phone, open `http://<pi-tailscale-or-lan-ip>:8000`. With
`autostart: true` it should already be running — watch Unfollowed/Skipped climb,
then it enters **sleeping** until the next daily batch. Ctrl-C to stop the test.

## 8. Run it as a service (always-on + auto-restart + boot)

```bash
sudo cp /home/pi/unfollower/deploy/unfollower.service /etc/systemd/system/
# edit the file if your user/path isn't pi:/home/pi/unfollower
sudo systemctl daemon-reload
sudo systemctl enable --now unfollower
sudo systemctl status unfollower         # should be active (running)
journalctl -u unfollower -f              # live logs
```

Now it survives reboots and crashes. With `autostart` + `daily_loop`, the bot
resumes its daily unfollows on its own.

### "Restart service" button + watchdog (passwordless sudo)

The dashboard's **Restart service** button and the **watchdog** (System tab) both
restart this systemd unit. The server runs non-root, so it needs passwordless sudo
for exactly that one command — `systemctl --no-block restart <unit>`. Grant it
(replace `naor223` with your Pi user and `unfollower` with your unit if different):

```bash
echo 'naor223 ALL=(root) NOPASSWD: /usr/bin/systemctl --no-block restart unfollower' \
  | sudo tee /etc/sudoers.d/unfollower-restart
sudo chmod 440 /etc/sudoers.d/unfollower-restart
sudo visudo -c                              # validate syntax
```

Notes:
- The sudoers line must match the command **exactly**. Confirm `systemctl` lives at
  `/usr/bin/systemctl` (`command -v systemctl`); adjust the path if not.
- The user must be the one the service runs as: `systemctl show unfollower -p User`.
- If your unit isn't named `unfollower`, change BOTH `server.service_name` in
  `config.yaml` and the unit name in the sudoers line (they must agree).
- Test it directly — this should restart the service with no password prompt:
  `sudo -n systemctl --no-block restart unfollower`
- Without this entry the button shows "Restart failed: … a password is required"
  (`sudo -n` refuses to prompt) — the bot is otherwise unaffected.
- The **watchdog** also needs `server.autostart: true`, or a restart would leave the
  bot stopped (it warns instead of restarting in that case).
- After a restart the dashboard drops for a few seconds, then reconnects on its own.

### System → Logs viewer (optional: full service journal)

The dashboard's **System → Logs** card works out of the box (it falls back to the
captured lifecycle + bot log events). For the **full systemd journal** (uvicorn,
tracebacks, the complete pre-freeze output), the service user needs journal read access.
Simplest — add the user to the journal group (no sudoers needed), then restart:

```bash
sudo usermod -aG systemd-journal naor223     # or the 'adm' group
sudo systemctl restart unfollower
```

Alternatively, a passwordless-sudo line for journalctl (the viewer tries `sudo -n` too):

```bash
echo 'naor223 ALL=(root) NOPASSWD: /usr/bin/journalctl -u unfollower *' \
  | sudo tee /etc/sudoers.d/unfollower-logs
sudo chmod 440 /etc/sudoers.d/unfollower-logs
```

## 8b. Scraper + filter service (separate process, burner account)

The scraping + candidate-filtering can run as a **separate service** on a **second
Chrome logged into a throwaway "scraper" account**, so it never interferes with the
core bot and your **main account bears zero scraping risk**. It scrapes the
configured sources, browser-navigates each candidate to filter it (account-agnostic
checks only — posts / follower range / private), and publishes a cleaned
`data/follow_candidates.json` the core bot consumes. It **also harvests reach
post-links** into `data/reach_pool.json` (when `follow.engagement.story_reach_enabled`
+ `reach_external_harvest` are on), so the main account never loads the easily-gated
hashtag grids itself — it just likes from the cached pool. Browser navigation only —
no IG API.

The burner fills **both** pools to their high-water marks (`scraper.pool_high_mult` ×
the pool's daily figure) only during the bot's dead time; the bot stays idle whenever
either pool it consumes is below its low-water (follow: a daily cap of candidates;
reach: today's remaining like budget), then runs while the scraper waits. One process
holds the Pi at a time.

It uses the **same persistent-profile model as the main bot** (§5 Option A), just a
separate profile dir and the burner's credentials — no 2nd Chrome to launch by hand.

**1. Create a throwaway IG account** (the "scraper"/burner). It only needs to *view*
profiles. If it gets flagged it's expendable; your main account is untouched.

**2. Add the burner credentials to `.env`** on the Pi (alongside the main ones):
```
SCRAPER_IG_USERNAME=your_burner_handle
SCRAPER_IG_PASSWORD=your_burner_password
```

**3. Add the new config keys + set the scraper options.** Pull the defaults in, then
either edit `config.yaml` or use the dashboard (Config → Scraper & autopilot):
```bash
cd /home/naor223/unfollower && source .venv/bin/activate
python upgrade_config.py        # adds scraper{}, keep_running, new log keys
```
Set: `scraper.enabled: true`, `scraper.cdp_endpoint: ""`,
`scraper.user_data_dir: data/scraper-profile`, `follow.external_scraper: true`, and
(recommended) `behavior.keep_running: true`.

**4. Log the burner in once** (console 2FA, no screen needed — like `pi_login.py`):
```bash
python scraper_login.py
# enter the 2FA code if prompted; on success the burner profile is saved
```

**5. Run it from the dashboard.** The server manages the scraper as a child process,
so there's **nothing to install** — open **System → Scraper service** and click
**Start scraper**. It also **auto-starts with the bot** whenever you start in
follow/churn mode (and on boot if `server.autostart` is on). Use **Stop scraper** to
halt it. Live status (running, pool/ready/checked/rejected) shows on the same card.

The scraper writes `data/filter_checked.log` (kept) and `data/filter_rejected.log`
(filtered out); the latter feeds the core bot's done-set, and with
`follow.external_scraper: true` the bot follows **only** checked accounts. Code ships
via the dashboard's **Deploy latest** button; since the scraper is a child of the
server, restarting the main service (the Deploy button does this) also recycles it.

> **Optional — run it under systemd instead** (if you'd rather it be independent of
> the server process): `sudo cp deploy/unfollower-scraper.service /etc/systemd/system/`,
> then `sudo systemctl enable --now unfollower-scraper`. **Don't do both** — the
> dashboard buttons and a systemd unit would launch two scrapers fighting over the
> burner profile. If you previously enabled the unit and now want the dashboard model,
> disable it first: `sudo systemctl disable --now unfollower-scraper`.

## 9. Remote access with Tailscale

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
tailscale ip -4                          # the Pi's tailnet IP
```

Install Tailscale on your phone/laptop (same account) and open
`http://<pi-tailscale-ip>:8000` from anywhere — encrypted, no open ports.

---

## Upgrading an existing Pi deployment to follow / churn (with data merge)

Use this when the Pi already runs an older (unfollow-only) version **for the same
Instagram account** and you've built up new follow/churn data on your PC. Goal:
the Pi ends up running the new code with the **union** of both machines' history —
nothing lost. Substitute your real values for `PI` (e.g. `pi@raspberrypi.local`)
and `PIDIR` (e.g. `/home/pi/unfollower`).

### 1. Stop the service (don't let it run while we swap things)
```bash
ssh PI "sudo systemctl stop unfollower"
```

### 2. Push the new code (NOT config.yaml, .env, or data/)
From your PC (PowerShell), in the project dir:
```powershell
scp bot.py server.py main.py merge_data.py upgrade_config.py requirements.txt PI:PIDIR/
scp -r static PI:PIDIR/
scp -r deploy PI:PIDIR/
```
This keeps the Pi's own `config.yaml`, `.env`, and `data/` untouched.

### 3. Bring the PC's data over to a staging folder on the Pi
Copy only the real data files (skip screenshots / chrome profiles / session):
```powershell
ssh PI "mkdir -p PIDIR/data_pc"
scp data/unfollowed.log data/skipped.log data/failed.log data/following.json `
    data/followed.log data/follow_skipped.log data/follow_failed.log `
    data/follow_candidates.json PI:PIDIR/data_pc/
scp whitelist.txt PI:PIDIR/data_pc/whitelist.txt
```
(Some files may not exist yet — that's fine, scp just skips missing ones.)

### 4. On the Pi: upgrade the config, then merge the data
```bash
ssh PI
cd PIDIR && source .venv/bin/activate
pip install -r requirements.txt          # no new deps, but safe to run

python upgrade_config.py                  # adds mode + follow/churn keys, keeps your settings
python merge_data.py data_pc --dry-run    # preview the union (base = Pi's data/)
python merge_data.py data_pc              # do it (backs up to data/_backup_<ts>/ first)
rm -rf data_pc                            # staging no longer needed
```
`merge_data.py` unions every log (deduped by username, earliest timestamp kept),
`following.json`, `follow_candidates.json`, and `whitelist.txt`.

### 5. Refresh the login session (copy-session model)
On your **PC** (with the CDP Chrome logged in as the same account):
```powershell
python export_session.py
scp data\session.json PI:PIDIR/data/
```

### 6. Pick the mode and start
Set `mode` for the Pi — either edit `config.yaml` (`mode: follow` or `mode: churn`)
or do it from the dashboard after starting. For unattended growth you want:
```yaml
mode: "churn"            # or "follow"
behavior:
  daily_loop: true       # loop forever (follow cap, then churn review, then sleep)
```
Then:
```bash
ssh PI "sudo systemctl start unfollower && systemctl status unfollower"
```
Open the dashboard (`http://PI:8000`), confirm the merged lists look right
(Following/Unfollowed totals include the Pi's old history + your PC's), set your
sources in the **Sources** tab, and watch it run.

> Sanity check before trusting the merge: the merged `following.json` and
> `unfollowed.log` should be about the **same account** on both sides. If the Pi
> was ever logged into a different account, do NOT merge — deploy fresh instead.

---

## Maintenance

- **Session expired?** The dashboard shows a "session expired" error.
  - Option A (persistent profile): `sudo systemctl stop unfollower`, run
    `python pi_login.py` again, then `sudo systemctl start unfollower`.
  - Option B (copy-session): on your PC run `python export_session.py`, then
    `scp data\session.json pi@...:/home/pi/unfollower/data/` and
    `sudo systemctl restart unfollower`.
- **Instagram "new login" challenge:** mostly avoided with Option A. If it
  appears, approve it from the Instagram phone app, then re-run the login step.
- **Change the daily cap / add exceptions:** use the dashboard (Config + Exclude/
  Whitelist) — changes apply on the next daily batch.
- **Logs:** `data/unfollowed.log`, `data/skipped.log`, `data/failed.log`, plus
  `journalctl -u unfollower`.
