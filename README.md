# Calendar Sync

Multi-way calendar sync engine in Python for Google + Outlook (ICS/invite model).

## What This Does
- Syncs events across:
  - Google calendars (read/write via Google Calendar API)
  - Outlook calendar (read via public ICS, write via invites)
- Uses source event identity and clone metadata for dedupe and cleanup.
- Supports drift correction (if sync copy is manually edited, it gets corrected).

## How Outlook Works Here
- Read: Outlook public `.ics` URL
- Write: send/maintain invites to Outlook mailbox from Google organizer calendar
- Delete: cancel invite by deleting/updating organizer event

## Project Files
- `calendar_bridge.py`: sync engine
- `config.example.env`: safe template config
- `config.env`: local real config (gitignored)
- `credentials.json`: Google OAuth client secret (gitignored)
- `token_*.json`: Google OAuth tokens per account (gitignored)
- `sync_db.json`: local sync state DB (gitignored)

## 1. Prerequisites
- macOS
- Python 3.11+ (tested with 3.12)
- Google accounts/calendars you want to sync
- Outlook ICS link + auto-accept flow (if using Outlook write path)

## 2. Install Python Environment
```bash
cd /path/to/calendar-sync
/opt/homebrew/bin/python3.12 -m venv .venv
./.venv/bin/pip install --upgrade pip setuptools wheel
./.venv/bin/pip install google-api-python-client google-auth google-auth-oauthlib requests icalevents
```

## 3. Configure Environment
```bash
cp config.example.env config.env
```

Edit `config.env` with your real values:
- Google calendar IDs
- Outlook ICS URL
- Outlook invite email
- mirror/private toggles per endpoint

## 4. Set Up Google API + `credentials.json`
1. Open Google Cloud Console: https://console.cloud.google.com/
2. Create/select project.
3. Enable **Google Calendar API**.
4. Configure OAuth consent screen (External/Internal as needed).
5. Create OAuth Client ID:
   - Type: **Desktop app**
6. Download client secret JSON.
7. Save it in project root as:
   - `credentials.json`

## 5. Authorize Google Accounts
The app supports multiple Google auth accounts (e.g. personal + workspace).

Authorize each account (browser prompt opens):
```bash
./.venv/bin/python calendar_bridge.py --mode auth --auth-account-id personal --reauth
./.venv/bin/python calendar_bridge.py --mode auth --auth-account-id workspace --reauth
```

This creates local token files (gitignored):
- `token_personal.json`
- `token_workspace.json`

## 6. Safe Validation Flow
1. Connection test (no writes):
```bash
./.venv/bin/python calendar_bridge.py --mode test
```

2. Optional cleanup of orphan/duplicate bridge events:
```bash
./.venv/bin/python calendar_bridge.py --mode cleanup --confirm-live
```

3. Safe live watch (ignore existing events at startup, capped writes):
```bash
./.venv/bin/python calendar_bridge.py --mode watch --confirm-live --ignore-existing-on-start --interval-seconds 30 --max-create-clones 5 --max-outlook-invites 2
```

## 7. Common Run Modes
- `auth`: OAuth login for one/all configured Google auth accounts
- `test`: connectivity checks only, no writes
- `cleanup`: remove orphan/duplicate bridge-managed Google sync copies
- `prime`: seed baseline source keys in DB without creating clones
- `sync`: one sync cycle
- `watch`: continuous sync loop

## 8. Privacy / Content Controls
Per endpoint in `config.env`:
- `*_MIRROR_SUMMARY=true|false`
- `*_MIRROR_DESCRIPTION=true|false`
- `*_SET_PRIVATE=true|false` (Google targets)

## 9. Security Notes
Never commit:
- `config.env`
- `credentials.json`
- `token*.json`
- `sync_db.json`

Already handled via `.gitignore`.

## 10. Troubleshooting
- `404 Not Found` on Google calendar: wrong calendar ID or missing permission.
- Duplicate sync copies: run cleanup mode, then restart watch.
- Python warnings: use `.venv` with Python 3.12.
- Outlook Graph API not available: use ICS + invite model (tenant policy may block app registration/consent).

## 11. Run In Background (macOS `launchd`)
Use a LaunchAgent so the sync restarts automatically and survives reboots/login.

### 11.1 Create folders
```bash
mkdir -p ~/.config/calendar-sync
mkdir -p ~/Library/Logs/calendar-sync
```

### 11.2 Create plist
Create file:
- `~/Library/LaunchAgents/com.calendar-sync.watch.plist`

Example plist (update paths if your repo location differs):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.calendar-sync.watch</string>

    <key>ProgramArguments</key>
    <array>
      <string>/path/to/calendar-sync/.venv/bin/python</string>
      <string>/path/to/calendar-sync/calendar_bridge.py</string>
      <string>--mode</string><string>watch</string>
      <string>--confirm-live</string>
      <string>--ignore-existing-on-start</string>
      <string>--interval-seconds</string><string>600</string>
      <string>--max-create-clones</string><string>50</string>
      <string>--max-outlook-invites</string><string>20</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/path/to/calendar-sync</string>

    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>/Users/REPLACE_ME/Library/Logs/calendar-sync/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/REPLACE_ME/Library/Logs/calendar-sync/stderr.log</string>
  </dict>
</plist>
```

### 11.3 Load and start
```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.calendar-sync.watch.plist
launchctl enable gui/$(id -u)/com.calendar-sync.watch
launchctl kickstart -k gui/$(id -u)/com.calendar-sync.watch
```

### 11.4 Check status / logs
```bash
launchctl print gui/$(id -u)/com.calendar-sync.watch | head -n 40
tail -f ~/Library/Logs/calendar-sync/stdout.log
tail -f ~/Library/Logs/calendar-sync/stderr.log
```

### 11.5 Stop / unload
```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.calendar-sync.watch.plist
```

### Notes
- Keep `config.env`, `credentials.json`, and `token*.json` in the project directory.
- Test manually (`--mode test`) before enabling launchd.
- Use `--interval-seconds 600` for 10-minute production cadence.

## 12. Background Run On Linux (`systemd --user`)
### 12.1 Create service file
Create:
- `~/.config/systemd/user/calendar-sync.service`

```ini
[Unit]
Description=Calendar Sync Watcher
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/path/to/calendar-sync
ExecStart=/path/to/calendar-sync/.venv/bin/python /path/to/calendar-sync/calendar_bridge.py --mode watch --confirm-live --ignore-existing-on-start --interval-seconds 600 --max-create-clones 50 --max-outlook-invites 20
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
```

### 12.2 Enable and start
```bash
systemctl --user daemon-reload
systemctl --user enable calendar-sync.service
systemctl --user start calendar-sync.service
```

### 12.3 Check logs
```bash
systemctl --user status calendar-sync.service
journalctl --user -u calendar-sync.service -f
```

### 12.4 Keep running after logout (optional)
```bash
loginctl enable-linger "$USER"
```

## 13. Background Run On Windows (Task Scheduler)
### 13.1 Create task
1. Open **Task Scheduler**.
2. Create Task (not Basic Task).
3. General:
   - Name: `Calendar Sync Watch`
   - Choose “Run whether user is logged on or not” if needed.
4. Triggers:
   - New -> “At log on” (or “At startup”).
5. Actions:
   - Program/script: `C:\path\to\calendar-sync\.venv\Scripts\python.exe`
   - Add arguments:
     - `calendar_bridge.py --mode watch --confirm-live --ignore-existing-on-start --interval-seconds 600 --max-create-clones 50 --max-outlook-invites 20`
   - Start in:
     - `C:\path\to\calendar-sync`
6. Conditions/Settings:
   - Disable “Stop if the computer switches to battery power” if needed.
   - Enable restart on failure.

### 13.2 Optional logging to file
Use a wrapper `.bat` script and point Task Scheduler to it:
```bat
@echo off
cd /d C:\path\to\calendar-sync
C:\path\to\calendar-sync\.venv\Scripts\python.exe calendar_bridge.py --mode watch --confirm-live --ignore-existing-on-start --interval-seconds 600 --max-create-clones 50 --max-outlook-invites 20 >> logs\stdout.log 2>> logs\stderr.log
```
