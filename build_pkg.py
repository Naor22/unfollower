"""Build unfollower-pi.zip containing exactly what the Pi needs."""
import shutil, zipfile, yaml
from pathlib import Path

ROOT = Path(__file__).parent
STAGE = ROOT / "_pkg" / "unfollower"
ZIP = ROOT / "unfollower-pi.zip"
PI_HOME = "/home/naor223"
PI_DIR = f"{PI_HOME}/unfollower"

# clean staging
if (ROOT / "_pkg").exists():
    shutil.rmtree(ROOT / "_pkg")
STAGE.mkdir(parents=True)

ROOT_FILES = [
    "bot.py", "server.py", "main.py",
    "import_following.py", "export_session.py", "pi_login.py",
    "inspect_profile.py", "inspect_following.py",
    "requirements.txt", "whitelist.txt", "README.md", "DEPLOY.md", ".env",
]
for f in ROOT_FILES:
    src = ROOT / f
    if src.exists():
        shutil.copy2(src, STAGE / f)

# static/
shutil.copytree(ROOT / "static", STAGE / "static")

# data/ — only the list + progress logs (NOT chrome-profile/session/debug dumps)
(STAGE / "data").mkdir()
for f in ["following.json", "unfollowed.log", "skipped.log"]:
    src = ROOT / "data" / f
    if src.exists():
        shutil.copy2(src, STAGE / "data" / f)

# Pi-tailored config.yaml (start from current, override Pi-specific keys)
cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
cfg["browser"]["cdp_endpoint"] = ""
cfg["browser"]["headless"] = True
cfg["browser"]["executable_path"] = "/usr/bin/chromium"
cfg["browser"]["channel"] = ""
cfg["browser"]["user_data_dir"] = f"{PI_DIR}/data/ig-profile"   # Option A: permanent login
cfg["behavior"]["daily_loop"] = True
cfg["behavior"]["use_following_cache"] = True
cfg["server"]["host"] = "0.0.0.0"
cfg["server"]["autostart"] = True
(STAGE / "config.yaml").write_text(
    yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False), encoding="utf-8")

# systemd unit with the correct user/paths
(STAGE / "deploy").mkdir()
(STAGE / "deploy" / "unfollower.service").write_text(f"""[Unit]
Description=Instagram Unfollower (dashboard + daily bot)
After=network-online.target tailscaled.service
Wants=network-online.target

[Service]
Type=simple
User=naor223
WorkingDirectory={PI_DIR}
ExecStart={PI_DIR}/.venv/bin/python server.py
Environment=PYTHONUNBUFFERED=1
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
""", encoding="utf-8")

# zip it (top-level folder "unfollower/")
with zipfile.ZipFile(ZIP, "w", zipfile.ZIP_DEFLATED) as z:
    for path in STAGE.rglob("*"):
        if path.is_file():
            z.write(path, path.relative_to(ROOT / "_pkg"))

print(f"[+] wrote {ZIP} ({ZIP.stat().st_size // 1024} KB)")
print("[+] contents:")
with zipfile.ZipFile(ZIP) as z:
    for n in z.namelist():
        print("   ", n)
shutil.rmtree(ROOT / "_pkg")
