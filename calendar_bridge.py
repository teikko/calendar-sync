from __future__ import annotations

import hashlib
import json
import os
import argparse
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from icalevents.icalevents import events as parse_ical_events
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if value and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            os.environ.setdefault(key, value)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


load_env_file(os.getenv("CALBRIDGE_CONFIG_FILE", "config.env"))


# ---------- CONFIG (from config.env / env vars) ----------
SCOPES = ["https://www.googleapis.com/auth/calendar"]
UTC = timezone.utc
DB_FILE = os.getenv("DB_FILE", "sync_db.json")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
WINDOW_PAST_DAYS = env_int("WINDOW_PAST_DAYS", 14)
WINDOW_FUTURE_DAYS = env_int("WINDOW_FUTURE_DAYS", 365)
DEFAULT_MAX_CREATE_CLONES_PER_RUN = env_int("DEFAULT_MAX_CREATE_CLONES_PER_RUN", 50)
DEFAULT_MAX_OUTLOOK_INVITES_PER_RUN = env_int("DEFAULT_MAX_OUTLOOK_INVITES_PER_RUN", 10)

# One OAuth token per Google account.
AUTH_ACCOUNTS = {
    "personal": {"token_file": os.getenv("AUTH_PERSONAL_TOKEN_FILE", "token_personal.json")},
    "workspace": {
        "token_file": os.getenv(
            "AUTH_WORKSPACE_TOKEN_FILE",
            os.getenv("AUTH_CHOICEHR_TOKEN_FILE", "token_workspace.json"),
        )
    },
}

# If invite-based targets are used, bridge creates organizer events from this account/calendar.
INVITE_ORGANIZER = {
    "auth_account_id": os.getenv("INVITE_ORGANIZER_AUTH_ACCOUNT_ID", "personal"),
    "calendar_id": os.getenv("INVITE_ORGANIZER_CALENDAR_ID", "primary"),
}

# Busy clone style + loop prevention marker.
CLONE_SUMMARY = os.getenv("CLONE_SUMMARY", "Busy (Synced)")
BRIDGE_MARKER = os.getenv("BRIDGE_MARKER", "CALBRIDGE_MANAGED=1")

# Endpoints:
# - type=google: read/write via Calendar API
# - type=outlook: read via ICS, write via Google invite organizer events
#
# Start small with one personal Google + one org Google + one Outlook org.
# Add more entries later; no code changes needed.
ENDPOINTS = [
    {
        "id": os.getenv("GOOGLE_PERSONAL_ID", "google_personal"),
        "type": "google",
        "name": os.getenv("GOOGLE_PERSONAL_NAME", "Personal Google"),
        "auth_account_id": "personal",
        "calendar_id": os.getenv("GOOGLE_PERSONAL_CALENDAR_ID", "primary"),
        "read_enabled": env_bool("GOOGLE_PERSONAL_READ_ENABLED", True),
        "write_enabled": env_bool("GOOGLE_PERSONAL_WRITE_ENABLED", True),
        "mirror_summary": env_bool("GOOGLE_PERSONAL_MIRROR_SUMMARY", True),
        "mirror_description": env_bool("GOOGLE_PERSONAL_MIRROR_DESCRIPTION", True),
        "set_private": env_bool("GOOGLE_PERSONAL_SET_PRIVATE", False),
    },
    {
        "id": os.getenv("GOOGLE_WORKSPACE_ID", os.getenv("GOOGLE_CHOICEHR_ID", "google_workspace")),
        "type": "google",
        "name": os.getenv("GOOGLE_WORKSPACE_NAME", os.getenv("GOOGLE_CHOICEHR_NAME", "Workspace Google")),
        "auth_account_id": "workspace",
        "calendar_id": os.getenv(
            "GOOGLE_WORKSPACE_CALENDAR_ID",
            os.getenv("GOOGLE_CHOICEHR_CALENDAR_ID", "REPLACE_WITH_WORKSPACE_CALENDAR_ID"),
        ),
        "read_enabled": env_bool("GOOGLE_WORKSPACE_READ_ENABLED", env_bool("GOOGLE_CHOICEHR_READ_ENABLED", True)),
        "write_enabled": env_bool("GOOGLE_WORKSPACE_WRITE_ENABLED", env_bool("GOOGLE_CHOICEHR_WRITE_ENABLED", True)),
        "mirror_summary": env_bool(
            "GOOGLE_WORKSPACE_MIRROR_SUMMARY", env_bool("GOOGLE_CHOICEHR_MIRROR_SUMMARY", True)
        ),
        "mirror_description": env_bool(
            "GOOGLE_WORKSPACE_MIRROR_DESCRIPTION", env_bool("GOOGLE_CHOICEHR_MIRROR_DESCRIPTION", True)
        ),
        "set_private": env_bool("GOOGLE_WORKSPACE_SET_PRIVATE", env_bool("GOOGLE_CHOICEHR_SET_PRIVATE", True)),
    },
    {
        "id": os.getenv("OUTLOOK_ORG_ID", os.getenv("OUTLOOK_WAVECREST_ID", "outlook_org")),
        "type": "outlook",
        "name": os.getenv("OUTLOOK_ORG_NAME", os.getenv("OUTLOOK_WAVECREST_NAME", "Outlook Org")),
        "ics_url": os.getenv("OUTLOOK_ORG_ICS_URL", os.getenv("OUTLOOK_WAVECREST_ICS_URL", "")),
        "invite_email": os.getenv(
            "OUTLOOK_ORG_INVITE_EMAIL", os.getenv("OUTLOOK_WAVECREST_INVITE_EMAIL", "")
        ),
        "read_enabled": env_bool("OUTLOOK_ORG_READ_ENABLED", env_bool("OUTLOOK_WAVECREST_READ_ENABLED", True)),
        "write_enabled": env_bool("OUTLOOK_ORG_WRITE_ENABLED", env_bool("OUTLOOK_WAVECREST_WRITE_ENABLED", True)),
        "mirror_summary": env_bool(
            "OUTLOOK_ORG_MIRROR_SUMMARY", env_bool("OUTLOOK_WAVECREST_MIRROR_SUMMARY", True)
        ),
        "mirror_description": env_bool(
            "OUTLOOK_ORG_MIRROR_DESCRIPTION", env_bool("OUTLOOK_WAVECREST_MIRROR_DESCRIPTION", True)
        ),
        "set_private": env_bool("OUTLOOK_ORG_SET_PRIVATE", env_bool("OUTLOOK_WAVECREST_SET_PRIVATE", False)),
    },
]


def log(status: str, message: str) -> None:
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[{ts}] {status} {message}")


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_to_utc(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return datetime.combine(value, time.min, tzinfo=UTC)


def parse_google_datetime(block: dict[str, str]) -> datetime:
    if "dateTime" in block:
        raw = block["dateTime"]
        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    d = date.fromisoformat(block["date"])
    return datetime.combine(d, time.min, tzinfo=UTC)


def bridge_description(origin_endpoint_id: str, source_uid: str) -> str:
    return (
        f"{BRIDGE_MARKER}\n"
        f"ORIGIN_ENDPOINT={origin_endpoint_id}\n"
        f"ORIGIN_UID={source_uid}\n"
        f"UTC_SYNC=1"
    )


def is_bridge_managed_google_event(item: dict[str, Any]) -> bool:
    private = item.get("extendedProperties", {}).get("private", {})
    if private.get("calbridge_managed") == "1":
        return True
    desc = item.get("description", "") or ""
    return BRIDGE_MARKER in desc


def get_google_service_for_account(auth_account_id: str, force_reauth: bool = False):
    account = AUTH_ACCOUNTS.get(auth_account_id)
    if not account:
        raise ValueError(f"Unknown auth_account_id: {auth_account_id}")

    token_file = account["token_file"]
    creds = None
    if not force_reauth and Path(token_file).exists():
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token and not force_reauth:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def required_auth_accounts() -> set[str]:
    accounts: set[str] = {INVITE_ORGANIZER["auth_account_id"]}
    for endpoint in ENDPOINTS:
        if endpoint["type"] == "google":
            accounts.add(endpoint["auth_account_id"])
    return accounts


def build_google_services(force_reauth: bool = False) -> dict[str, Any]:
    services: dict[str, Any] = {}
    for auth_account_id in sorted(required_auth_accounts()):
        services[auth_account_id] = get_google_service_for_account(
            auth_account_id, force_reauth=force_reauth
        )
    return services


def load_db() -> dict[str, Any]:
    if not os.path.exists(DB_FILE):
        return {"version": 3, "records": {}}

    with open(DB_FILE, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    if isinstance(data, dict) and "records" in data and isinstance(data["records"], dict):
        return {"version": 3, "records": data["records"]}

    # Minimal migration from previous "entries" shape
    if isinstance(data, dict) and "entries" in data and isinstance(data["entries"], dict):
        migrated: dict[str, Any] = {}
        for key, val in data["entries"].items():
            if not isinstance(val, dict):
                continue
            migrated[key] = {
                "source_endpoint_id": val.get("source_id", "legacy"),
                "source_uid": val.get("outlook_uid", "legacy"),
                "start_utc": val.get("start_utc", "unknown"),
                "fingerprint": "",
                "last_seen_run": "",
                "clones": {
                    "legacy_target": {
                        "target_endpoint_id": "legacy_target",
                        "target_type": "google",
                        "calendar_id": val.get("google_calendar_id", "primary"),
                        "event_id": val.get("google_event_id", ""),
                    }
                },
            }
        return {"version": 3, "records": migrated}

    return {"version": 3, "records": {}}


def save_db(db: dict[str, Any]) -> None:
    tmp = f"{DB_FILE}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(db, fh, indent=2, sort_keys=True)
    os.replace(tmp, DB_FILE)


def build_source_key(endpoint_id: str, source_uid: str, start_utc: datetime) -> str:
    # Dedupe contract explicitly includes UID + Start UTC.
    return f"{endpoint_id}|{source_uid}|{utc_iso(start_utc)}"


def event_fingerprint(event: dict[str, Any]) -> str:
    payload = "|".join(
        [
            event["source_endpoint_id"],
            event["source_uid"],
            utc_iso(event["start_utc"]),
            utc_iso(event["end_utc"]),
            event.get("summary", ""),
            event.get("description", ""),
            event.get("location", ""),
            event.get("updated_hint", ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def list_google_source_events(service, endpoint: dict[str, Any]) -> list[dict[str, Any]]:
    start_search = utc_now() - timedelta(days=WINDOW_PAST_DAYS)
    end_search = utc_now() + timedelta(days=WINDOW_FUTURE_DAYS)
    page_token = None
    result: list[dict[str, Any]] = []

    while True:
        response = (
            service.events()
            .list(
                calendarId=endpoint["calendar_id"],
                timeMin=utc_iso(start_search),
                timeMax=utc_iso(end_search),
                singleEvents=True,
                showDeleted=False,
                orderBy="startTime",
                pageToken=page_token,
                maxResults=2500,
            )
            .execute()
        )

        for item in response.get("items", []):
            if item.get("status") == "cancelled":
                continue
            if "start" not in item or "end" not in item:
                continue
            if is_bridge_managed_google_event(item):
                continue

            start_utc = parse_google_datetime(item["start"])
            end_utc = parse_google_datetime(item["end"])
            if end_utc <= start_utc:
                end_utc = start_utc + timedelta(minutes=30)

            source_uid = item.get("iCalUID") or item.get("id")
            if not source_uid:
                continue

            result.append(
                {
                    "source_endpoint_id": endpoint["id"],
                    "source_uid": source_uid,
                    "summary": item.get("summary", ""),
                    "description": item.get("description", ""),
                    "location": item.get("location", ""),
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "updated_hint": item.get("updated", ""),
                }
            )

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return result


def list_outlook_source_events(endpoint: dict[str, Any]) -> list[dict[str, Any]]:
    response = requests.get(endpoint["ics_url"], timeout=30)
    response.raise_for_status()

    start_search = utc_now() - timedelta(days=WINDOW_PAST_DAYS)
    end_search = utc_now() + timedelta(days=WINDOW_FUTURE_DAYS)
    feed_events = parse_ical_events(
        string_content=response.text,
        start=start_search,
        end=end_search,
        tzinfo=UTC,
        sort=True,
    )

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ev in feed_events:
        if not ev.start or not ev.end:
            continue
        if ev.status and str(ev.status).upper() == "CANCELLED":
            continue
        if getattr(ev, "transparent", False):
            continue
        # Invite-based clones that land in Outlook must never re-enter source sync.
        # Be permissive here because Outlook/ICS can alter casing/spacing/formatting.
        summary = (ev.summary or "").strip().casefold()
        description = (ev.description or "")
        if CLONE_SUMMARY.casefold() in summary:
            continue
        if BRIDGE_MARKER in description or "ORIGIN_ENDPOINT=" in description:
            continue

        start_utc = normalize_to_utc(ev.start)
        end_utc = normalize_to_utc(ev.end)
        if end_utc <= start_utc:
            end_utc = start_utc + timedelta(minutes=30)

        source_uid = str(ev.uid or "").strip() or f"no-uid-{endpoint['id']}"
        source_key = build_source_key(endpoint["id"], source_uid, start_utc)
        if source_key in seen:
            continue
        seen.add(source_key)

        result.append(
            {
                "source_endpoint_id": endpoint["id"],
                "source_uid": source_uid,
                "summary": ev.summary or "",
                "description": ev.description or "",
                "location": ev.location or "",
                "start_utc": start_utc,
                "end_utc": end_utc,
                "updated_hint": utc_iso(normalize_to_utc(ev.last_modified))
                if ev.last_modified
                else "",
            }
        )

    return result


def build_clone_summary(source_event: dict[str, Any], target_endpoint: dict[str, Any]) -> str:
    if target_endpoint.get("mirror_summary") and source_event.get("summary"):
        return source_event["summary"]
    return CLONE_SUMMARY


def build_clone_description(source_event: dict[str, Any], target_endpoint: dict[str, Any]) -> str:
    parts: list[str] = []
    if target_endpoint.get("mirror_description") and source_event.get("description"):
        parts.append(str(source_event["description"]).strip())
    parts.append(bridge_description(source_event["source_endpoint_id"], source_event["source_uid"]))
    return "\n\n".join(p for p in parts if p).strip()


def build_clone_body(source_event: dict[str, Any], target_endpoint: dict[str, Any]) -> dict[str, Any]:
    body = {
        "summary": build_clone_summary(source_event, target_endpoint),
        "description": build_clone_description(source_event, target_endpoint),
        "start": {"dateTime": utc_iso(source_event["start_utc"]), "timeZone": "UTC"},
        "end": {"dateTime": utc_iso(source_event["end_utc"]), "timeZone": "UTC"},
        "extendedProperties": {
            "private": {
                "calbridge_managed": "1",
                "calbridge_origin_endpoint": source_event["source_endpoint_id"],
                "calbridge_origin_uid": source_event["source_uid"],
            }
        },
    }
    # Google targets can force synced copies as private visibility.
    if target_endpoint.get("type") == "google" and target_endpoint.get("set_private"):
        body["visibility"] = "private"
    return body


def find_matching_google_clones(
    service, target_endpoint: dict[str, Any], source_event: dict[str, Any]
) -> list[dict[str, Any]]:
    calendar_id = target_endpoint["calendar_id"]
    start_utc = source_event["start_utc"]
    end_utc = source_event["end_utc"]
    window_start = start_utc - timedelta(minutes=1)
    window_end = end_utc + timedelta(minutes=1)

    response = (
        service.events()
        .list(
            calendarId=calendar_id,
            timeMin=utc_iso(window_start),
            timeMax=utc_iso(window_end),
            singleEvents=True,
            showDeleted=False,
            maxResults=250,
        )
        .execute()
    )

    matches: list[dict[str, Any]] = []
    for item in response.get("items", []):
        if not is_bridge_managed_google_event(item):
            continue
        private = item.get("extendedProperties", {}).get("private", {})
        if private.get("calbridge_origin_endpoint") != source_event["source_endpoint_id"]:
            continue
        if private.get("calbridge_origin_uid") != source_event["source_uid"]:
            continue
        if "start" not in item:
            continue
        try:
            item_start = parse_google_datetime(item["start"])
        except Exception:
            continue
        if item_start != start_utc:
            continue
        matches.append(item)

    return matches


def is_invite_organizer_target(endpoint: dict[str, Any]) -> bool:
    if endpoint.get("type") != "google":
        return False
    return (
        endpoint.get("auth_account_id") == INVITE_ORGANIZER["auth_account_id"]
        and endpoint.get("calendar_id") == INVITE_ORGANIZER["calendar_id"]
    )


def resolve_invite_organizer(
    source_endpoint_id: str, writable_endpoints: list[dict[str, Any]]
) -> dict[str, str]:
    default = {
        "auth_account_id": INVITE_ORGANIZER["auth_account_id"],
        "calendar_id": INVITE_ORGANIZER["calendar_id"],
        "endpoint_id": "",
    }

    default_endpoint = next(
        (
            e
            for e in writable_endpoints
            if e.get("type") == "google"
            and e.get("auth_account_id") == default["auth_account_id"]
            and e.get("calendar_id") == default["calendar_id"]
        ),
        None,
    )
    if default_endpoint:
        default["endpoint_id"] = default_endpoint["id"]

    if default_endpoint and default_endpoint["id"] != source_endpoint_id:
        return default

    # Fallback: avoid creating invite carrier in the same endpoint as source.
    alt = next(
        (
            e
            for e in writable_endpoints
            if e.get("type") == "google" and e.get("id") != source_endpoint_id
        ),
        None,
    )
    if alt:
        return {
            "auth_account_id": alt["auth_account_id"],
            "calendar_id": alt["calendar_id"],
            "endpoint_id": alt["id"],
        }

    return default


def is_target_matching_invite_organizer(
    endpoint: dict[str, Any], invite_organizer: dict[str, str]
) -> bool:
    if endpoint.get("type") != "google":
        return False
    return (
        endpoint.get("auth_account_id") == invite_organizer["auth_account_id"]
        and endpoint.get("calendar_id") == invite_organizer["calendar_id"]
    )


def create_or_update_google_clone(
    service, target_endpoint: dict[str, Any], source_event: dict[str, Any], clone_state: dict[str, Any] | None
) -> dict[str, Any]:
    body = build_clone_body(source_event, target_endpoint)
    calendar_id = target_endpoint["calendar_id"]
    auth_account_id = target_endpoint["auth_account_id"]

    if clone_state and clone_state.get("event_id"):
        event_id = clone_state["event_id"]
        try:
            (
                service.events()
                .patch(
                    calendarId=calendar_id,
                    eventId=event_id,
                    body=body,
                    sendUpdates="none",
                )
                .execute()
            )
            return {
                "target_endpoint_id": target_endpoint["id"],
                "target_type": "google",
                "auth_account_id": auth_account_id,
                "calendar_id": calendar_id,
                "event_id": event_id,
            }
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in (404, 410):
                raise

    matches = find_matching_google_clones(service, target_endpoint, source_event)
    if matches:
        primary = matches[0]
        primary_id = primary["id"]
        # If duplicates already exist in the target calendar, keep one and remove extras.
        for dup in matches[1:]:
            try:
                service.events().delete(
                    calendarId=calendar_id,
                    eventId=dup["id"],
                    sendUpdates="none",
                ).execute()
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                if status not in (404, 410):
                    raise
        (
            service.events()
            .patch(
                calendarId=calendar_id,
                eventId=primary_id,
                body=body,
                sendUpdates="none",
            )
            .execute()
        )
        return {
            "target_endpoint_id": target_endpoint["id"],
            "target_type": "google",
            "auth_account_id": auth_account_id,
            "calendar_id": calendar_id,
            "event_id": primary_id,
        }

    created = (
        service.events()
        .insert(
            calendarId=calendar_id,
            body=body,
            sendUpdates="none",
        )
        .execute()
    )
    return {
        "target_endpoint_id": target_endpoint["id"],
        "target_type": "google",
        "auth_account_id": auth_account_id,
        "calendar_id": calendar_id,
        "event_id": created["id"],
    }


def create_or_update_outlook_invite_clone(
    service,
    target_endpoint: dict[str, Any],
    source_event: dict[str, Any],
    clone_state: dict[str, Any] | None,
    invite_organizer: dict[str, str],
) -> dict[str, Any]:
    body = build_clone_body(source_event, target_endpoint)
    body["attendees"] = [{"email": target_endpoint["invite_email"]}]
    organizer_calendar_id = invite_organizer["calendar_id"]
    organizer_auth_account_id = invite_organizer["auth_account_id"]
    organizer_endpoint_id = invite_organizer.get("endpoint_id", "")

    if clone_state and clone_state.get("event_id"):
        event_id = clone_state["event_id"]
        try:
            (
                service.events()
                .patch(
                    calendarId=organizer_calendar_id,
                    eventId=event_id,
                    body=body,
                    sendUpdates="all",
                )
                .execute()
            )
            return {
                "target_endpoint_id": target_endpoint["id"],
            "target_type": "outlook",
            "calendar_id": organizer_calendar_id,
            "auth_account_id": organizer_auth_account_id,
            "organizer_endpoint_id": organizer_endpoint_id,
            "event_id": event_id,
            "invite_email": target_endpoint["invite_email"],
        }
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in (404, 410):
                raise

    matches = find_matching_invite_carriers(
        service=service,
        organizer_calendar_id=organizer_calendar_id,
        source_event=source_event,
        invite_email=target_endpoint["invite_email"],
    )
    if matches:
        primary = matches[0]
        primary_id = primary["id"]
        for dup in matches[1:]:
            try:
                service.events().delete(
                    calendarId=organizer_calendar_id,
                    eventId=dup["id"],
                    sendUpdates="all",
                ).execute()
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                if status not in (404, 410):
                    raise
        (
            service.events()
            .patch(
                calendarId=organizer_calendar_id,
                eventId=primary_id,
                body=body,
                sendUpdates="all",
            )
            .execute()
        )
        return {
            "target_endpoint_id": target_endpoint["id"],
            "target_type": "outlook",
            "calendar_id": organizer_calendar_id,
            "auth_account_id": organizer_auth_account_id,
            "organizer_endpoint_id": organizer_endpoint_id,
            "event_id": primary_id,
            "invite_email": target_endpoint["invite_email"],
        }

    created = (
        service.events()
        .insert(
            calendarId=organizer_calendar_id,
            body=body,
            sendUpdates="all",
        )
        .execute()
    )
    return {
        "target_endpoint_id": target_endpoint["id"],
        "target_type": "outlook",
        "calendar_id": organizer_calendar_id,
        "auth_account_id": organizer_auth_account_id,
        "organizer_endpoint_id": organizer_endpoint_id,
        "event_id": created["id"],
        "invite_email": target_endpoint["invite_email"],
    }


def find_matching_invite_carriers(
    service,
    organizer_calendar_id: str,
    source_event: dict[str, Any],
    invite_email: str,
) -> list[dict[str, Any]]:
    start_utc = source_event["start_utc"]
    end_utc = source_event["end_utc"]
    window_start = start_utc - timedelta(minutes=1)
    window_end = end_utc + timedelta(minutes=1)
    response = (
        service.events()
        .list(
            calendarId=organizer_calendar_id,
            timeMin=utc_iso(window_start),
            timeMax=utc_iso(window_end),
            singleEvents=True,
            showDeleted=False,
            maxResults=250,
        )
        .execute()
    )

    matches: list[dict[str, Any]] = []
    invite_email_norm = invite_email.strip().casefold()
    for item in response.get("items", []):
        if not is_bridge_managed_google_event(item):
            continue
        private = item.get("extendedProperties", {}).get("private", {})
        if private.get("calbridge_origin_endpoint") != source_event["source_endpoint_id"]:
            continue
        if private.get("calbridge_origin_uid") != source_event["source_uid"]:
            continue
        if "start" not in item:
            continue
        try:
            item_start = parse_google_datetime(item["start"])
        except Exception:
            continue
        if item_start != start_utc:
            continue
        attendees = item.get("attendees", []) or []
        attendee_emails = {
            str(a.get("email", "")).strip().casefold()
            for a in attendees
            if a.get("email")
        }
        if invite_email_norm not in attendee_emails:
            continue
        matches.append(item)

    return matches


def delete_clone(services: dict[str, Any], clone: dict[str, Any]) -> None:
    calendar_id = clone.get("calendar_id") or INVITE_ORGANIZER["calendar_id"]
    event_id = clone.get("event_id")
    if not event_id:
        return

    auth_account_id = clone.get("auth_account_id")
    if not auth_account_id and clone.get("target_type") == "google":
        endpoint_id = clone.get("target_endpoint_id")
        endpoint = next((e for e in ENDPOINTS if e.get("id") == endpoint_id), None)
        auth_account_id = endpoint.get("auth_account_id") if endpoint else None
    if not auth_account_id:
        auth_account_id = INVITE_ORGANIZER["auth_account_id"]
    service = services[auth_account_id]
    send_updates = "all" if clone.get("target_type") == "outlook" else "none"
    (
        service.events()
        .delete(
            calendarId=calendar_id,
            eventId=event_id,
            sendUpdates=send_updates,
        )
        .execute()
    )


def clone_exists(services: dict[str, Any], clone: dict[str, Any]) -> bool:
    event_id = clone.get("event_id")
    if not event_id:
        return False

    calendar_id = clone.get("calendar_id") or INVITE_ORGANIZER["calendar_id"]
    auth_account_id = clone.get("auth_account_id")
    if not auth_account_id and clone.get("target_type") == "google":
        endpoint_id = clone.get("target_endpoint_id")
        endpoint = next((e for e in ENDPOINTS if e.get("id") == endpoint_id), None)
        auth_account_id = endpoint.get("auth_account_id") if endpoint else None
    if not auth_account_id:
        auth_account_id = INVITE_ORGANIZER["auth_account_id"]

    service = services[auth_account_id]
    try:
        service.events().get(calendarId=calendar_id, eventId=event_id).execute()
        return True
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        if status in (404, 410):
            return False
        raise


def get_clone_event(services: dict[str, Any], clone: dict[str, Any]) -> dict[str, Any] | None:
    event_id = clone.get("event_id")
    if not event_id:
        return None

    calendar_id = clone.get("calendar_id") or INVITE_ORGANIZER["calendar_id"]
    auth_account_id = clone.get("auth_account_id")
    if not auth_account_id and clone.get("target_type") == "google":
        endpoint_id = clone.get("target_endpoint_id")
        endpoint = next((e for e in ENDPOINTS if e.get("id") == endpoint_id), None)
        auth_account_id = endpoint.get("auth_account_id") if endpoint else None
    if not auth_account_id:
        auth_account_id = INVITE_ORGANIZER["auth_account_id"]

    service = services[auth_account_id]
    try:
        return service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        if status in (404, 410):
            return None
        raise


def clone_is_aligned(
    services: dict[str, Any],
    clone: dict[str, Any],
    source_event: dict[str, Any],
    target_endpoint: dict[str, Any],
) -> bool:
    item = get_clone_event(services, clone)
    if not item:
        return False
    if "start" not in item or "end" not in item:
        return False

    item_start = parse_google_datetime(item["start"])
    item_end = parse_google_datetime(item["end"])
    if item_start != source_event["start_utc"] or item_end != source_event["end_utc"]:
        return False

    expected_summary = build_clone_summary(source_event, target_endpoint)
    if (item.get("summary") or "") != expected_summary:
        return False

    expected_description = build_clone_description(source_event, target_endpoint)
    if (item.get("description") or "").strip() != expected_description.strip():
        return False

    return True


def collect_source_events(
    services: dict[str, Any], readable_endpoints: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], set[str]]:
    source_events: list[dict[str, Any]] = []
    successful_sources: set[str] = set()

    for endpoint in readable_endpoints:
        try:
            if endpoint["type"] == "google":
                service = services[endpoint["auth_account_id"]]
                events_found = list_google_source_events(service, endpoint)
            else:
                events_found = list_outlook_source_events(endpoint)
            successful_sources.add(endpoint["id"])
            log(
                "Syncing...",
                f"Scanned source [{endpoint['id']}] {endpoint['name']}: {len(events_found)} event(s)",
            )
            source_events.extend(events_found)
        except Exception as exc:
            log("Skipping...", f"{endpoint['name']} source fetch failed: {exc}")

    return source_events, successful_sources


def filter_writable_endpoints(
    services: dict[str, Any], writable_endpoints: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    validated: list[dict[str, Any]] = []
    for endpoint in writable_endpoints:
        if endpoint["type"] != "google":
            validated.append(endpoint)
            continue
        calendar_id = endpoint.get("calendar_id")
        service = services[endpoint["auth_account_id"]]
        try:
            service.calendars().get(calendarId=calendar_id).execute()
            validated.append(endpoint)
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status == 404:
                log(
                    "Skipping...",
                    f"{endpoint['name']} target disabled for this run (calendar not found or no access): {calendar_id}",
                )
            else:
                log(
                    "Skipping...",
                    f"{endpoint['name']} target disabled for this run (calendar check failed): {exc}",
                )
    return validated


def run_connection_test(max_preview_events: int = 5) -> None:
    log("Syncing...", "Starting connection test (no writes)")
    services = build_google_services(force_reauth=False)

    has_failures = False
    start_search = utc_now() - timedelta(days=WINDOW_PAST_DAYS)
    end_search = utc_now() + timedelta(days=WINDOW_FUTURE_DAYS)

    for endpoint in ENDPOINTS:
        etype = endpoint["type"]
        name = endpoint["name"]
        log("Syncing...", f"Testing {name} ({etype})")

        if etype == "google":
            calendar_id = endpoint["calendar_id"]
            service = services[endpoint["auth_account_id"]]
            try:
                cal = service.calendars().get(calendarId=calendar_id).execute()
                log("Syncing...", f"{name} calendar access OK: {cal.get('id')}")
            except HttpError as exc:
                has_failures = True
                log("Skipping...", f"{name} calendar access failed: {exc}")
                continue

            if endpoint.get("read_enabled"):
                try:
                    resp = (
                        service.events()
                        .list(
                            calendarId=calendar_id,
                            timeMin=utc_iso(start_search),
                            timeMax=utc_iso(end_search),
                            singleEvents=True,
                            showDeleted=False,
                            orderBy="startTime",
                            maxResults=max_preview_events,
                        )
                        .execute()
                    )
                    log(
                        "Syncing...",
                        f"{name} read test OK ({len(resp.get('items', []))} preview event(s))",
                    )
                except HttpError as exc:
                    has_failures = True
                    log("Skipping...", f"{name} read test failed: {exc}")

        elif etype == "outlook":
            if endpoint.get("read_enabled"):
                try:
                    response = requests.get(endpoint["ics_url"], timeout=30)
                    response.raise_for_status()
                    feed = parse_ical_events(
                        string_content=response.text,
                        start=start_search,
                        end=end_search,
                        tzinfo=UTC,
                        sort=True,
                    )
                    log("Syncing...", f"{name} ICS read OK ({len(feed)} event(s) in window)")
                except Exception as exc:
                    has_failures = True
                    log("Skipping...", f"{name} ICS read failed: {exc}")

            if endpoint.get("write_enabled"):
                try:
                    invite_email = endpoint["invite_email"]
                    if not invite_email:
                        raise ValueError("invite_email is empty")
                    invite_service = services[INVITE_ORGANIZER["auth_account_id"]]
                    invite_calendar_id = INVITE_ORGANIZER["calendar_id"]
                    invite_service.calendars().get(calendarId=invite_calendar_id).execute()
                    log(
                        "Syncing...",
                        f"{name} write path OK (invite via {INVITE_ORGANIZER['auth_account_id']}:{invite_calendar_id} -> {invite_email})",
                    )
                except Exception as exc:
                    has_failures = True
                    log("Skipping...", f"{name} write path check failed: {exc}")

    if has_failures:
        log("Skipping...", "Connection test completed with failures (no writes were made)")
    else:
        log("Syncing...", "Connection test passed (no writes were made)")


def run_auth(auth_account_id: str | None = None, reauth: bool = False) -> None:
    account_ids = [auth_account_id] if auth_account_id else sorted(AUTH_ACCOUNTS.keys())
    for account_id in account_ids:
        if account_id not in AUTH_ACCOUNTS:
            raise ValueError(f"Unknown auth_account_id: {account_id}")
        log("Syncing...", f"Authorizing account {account_id}")
        get_google_service_for_account(account_id, force_reauth=reauth)
        log(
            "Syncing...",
            f"Authorization OK for {account_id} ({AUTH_ACCOUNTS[account_id]['token_file']})",
        )


def run_cleanup(confirm_live: bool = False) -> None:
    log("Syncing...", "Starting cleanup mode for bridge-managed Google clones")
    services = build_google_services(force_reauth=False)
    db = load_db()
    records: dict[str, Any] = db["records"]

    readable_endpoints = [e for e in ENDPOINTS if e.get("read_enabled")]
    source_events, _ = collect_source_events(services, readable_endpoints)
    active_source_keys = {
        build_source_key(ev["source_endpoint_id"], ev["source_uid"], ev["start_utc"])
        for ev in source_events
    }

    tracked_clone_ids: dict[str, str] = {}
    for source_key, record in records.items():
        clones = record.get("clones", {}) or {}
        for target_id, clone in clones.items():
            event_id = clone.get("event_id")
            if event_id:
                tracked_clone_ids[f"{target_id}:{event_id}"] = source_key

    start_search = utc_now() - timedelta(days=WINDOW_PAST_DAYS)
    end_search = utc_now() + timedelta(days=WINDOW_FUTURE_DAYS)
    deletes_planned: list[tuple[str, str, str]] = []

    for endpoint in ENDPOINTS:
        if endpoint["type"] != "google" or not endpoint.get("write_enabled"):
            continue

        service = services[endpoint["auth_account_id"]]
        calendar_id = endpoint["calendar_id"]
        endpoint_id = endpoint["id"]

        page_token = None
        grouped: dict[str, list[dict[str, Any]]] = {}
        while True:
            response = (
                service.events()
                .list(
                    calendarId=calendar_id,
                    timeMin=utc_iso(start_search),
                    timeMax=utc_iso(end_search),
                    singleEvents=True,
                    showDeleted=False,
                    pageToken=page_token,
                    maxResults=2500,
                )
                .execute()
            )
            for item in response.get("items", []):
                if not is_bridge_managed_google_event(item):
                    continue
                private = item.get("extendedProperties", {}).get("private", {})
                origin_endpoint = private.get("calbridge_origin_endpoint")
                origin_uid = private.get("calbridge_origin_uid")
                if not origin_endpoint or not origin_uid or "start" not in item:
                    continue
                try:
                    start_utc = parse_google_datetime(item["start"])
                except Exception:
                    continue
                source_key = build_source_key(origin_endpoint, origin_uid, start_utc)
                grouped.setdefault(source_key, []).append(item)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        for source_key, items in grouped.items():
            keep_event_id: str | None = None
            record = records.get(source_key, {})
            clone_state = (record.get("clones", {}) or {}).get(endpoint_id, {})
            if clone_state.get("event_id"):
                keep_event_id = clone_state["event_id"]
            elif items:
                keep_event_id = items[0]["id"]

            if source_key not in active_source_keys:
                for item in items:
                    deletes_planned.append((endpoint_id, calendar_id, item["id"]))
                continue

            if len(items) > 1:
                for item in items:
                    if item["id"] != keep_event_id:
                        deletes_planned.append((endpoint_id, calendar_id, item["id"]))

    if not deletes_planned:
        log("Syncing...", "Cleanup complete: no orphan/duplicate bridge events found")
        return

    log("Syncing...", f"Cleanup found {len(deletes_planned)} orphan/duplicate bridge event(s)")
    if not confirm_live:
        for endpoint_id, calendar_id, event_id in deletes_planned[:20]:
            log("Skipping...", f"Would delete [{endpoint_id}] {calendar_id}:{event_id}")
        log("Skipping...", "Dry run only. Re-run with --confirm-live to apply cleanup deletes.")
        return

    deleted = 0
    for endpoint_id, calendar_id, event_id in deletes_planned:
        endpoint = next((e for e in ENDPOINTS if e.get("id") == endpoint_id), None)
        if not endpoint:
            continue
        service = services[endpoint["auth_account_id"]]
        try:
            service.events().delete(
                calendarId=calendar_id,
                eventId=event_id,
                sendUpdates="none",
            ).execute()
            deleted += 1
            log("Deleting...", f"Deleted orphan/duplicate bridge event [{endpoint_id}] {event_id}")
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in (404, 410):
                log("Skipping...", f"Cleanup delete failed [{endpoint_id}] {event_id}: {exc}")

    log("Syncing...", f"Cleanup complete: deleted {deleted} bridge event(s)")


def run_prime(max_source_events: int | None = None) -> None:
    log("Syncing...", "Starting prime mode (no writes, baseline existing source events)")
    services = build_google_services(force_reauth=False)
    db = load_db()
    records: dict[str, Any] = db["records"]
    run_id = utc_iso(utc_now())

    readable_endpoints = [e for e in ENDPOINTS if e.get("read_enabled")]
    source_events, _ = collect_source_events(services, readable_endpoints)
    if max_source_events is not None and max_source_events >= 0:
        source_events = source_events[:max_source_events]

    added = 0
    kept = 0
    for ev in source_events:
        source_key = build_source_key(ev["source_endpoint_id"], ev["source_uid"], ev["start_utc"])
        fp = event_fingerprint(ev)
        if source_key in records:
            kept += 1
            continue
        records[source_key] = {
            "source_endpoint_id": ev["source_endpoint_id"],
            "source_uid": ev["source_uid"],
            "start_utc": utc_iso(ev["start_utc"]),
            "end_utc": utc_iso(ev["end_utc"]),
            "fingerprint": fp,
            "last_seen_run": run_id,
            "baseline_seeded": True,
            "clones": {},
        }
        added += 1

    save_db(db)
    log(
        "Syncing...",
        f"Prime complete: baseline added={added}, already_present={kept}, total_sources_seen={len(source_events)}",
    )


def run_sync(
    max_source_events: int | None = None,
    max_create_clones: int = DEFAULT_MAX_CREATE_CLONES_PER_RUN,
    max_outlook_invites: int = DEFAULT_MAX_OUTLOOK_INVITES_PER_RUN,
    ignore_existing_on_start: bool = False,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    log("Syncing...", "Starting sync cycle")
    services = build_google_services(force_reauth=False)
    db = load_db()
    records: dict[str, Any] = db["records"]
    run_id = utc_iso(utc_now())

    readable_endpoints = [e for e in ENDPOINTS if e.get("read_enabled")]
    writable_endpoints = [e for e in ENDPOINTS if e.get("write_enabled")]
    writable_endpoints = filter_writable_endpoints(services, writable_endpoints)
    source_events, successful_sources = collect_source_events(services, readable_endpoints)
    if max_source_events is not None and max_source_events >= 0:
        original_count = len(source_events)
        source_events = source_events[:max_source_events]
        log(
            "Syncing...",
            f"Source event processing limited to {len(source_events)} of {original_count}",
        )

    if runtime_state is None:
        runtime_state = {}
    if ignore_existing_on_start:
        if not runtime_state.get("ignore_seeded"):
            runtime_state["ignore_seeded"] = True
            runtime_state["ignore_source_keys"] = {
                build_source_key(ev["source_endpoint_id"], ev["source_uid"], ev["start_utc"])
                for ev in source_events
            }
            log(
                "Skipping...",
                f"Ignoring {len(runtime_state['ignore_source_keys'])} existing source event(s) at startup",
            )
        ignore_source_keys = runtime_state.get("ignore_source_keys", set())
        source_events = [
            ev
            for ev in source_events
            if build_source_key(ev["source_endpoint_id"], ev["source_uid"], ev["start_utc"])
            not in ignore_source_keys
        ]

    create_count = 0
    outlook_create_count = 0

    active_source_keys: set[str] = set()
    for ev in source_events:
        source_key = build_source_key(ev["source_endpoint_id"], ev["source_uid"], ev["start_utc"])
        active_source_keys.add(source_key)
        fp = event_fingerprint(ev)
        log(
            "Syncing...",
            f"Event detected in [{ev['source_endpoint_id']}] UID={ev['source_uid']} start={utc_iso(ev['start_utc'])}",
        )

        record = records.get(source_key, {})
        old_fp = record.get("fingerprint", "")
        clones = record.get("clones", {})
        if not isinstance(clones, dict):
            clones = {}

        # If a source event was seeded during "prime" mode and has no clones,
        # skip creating clones so only new events after priming are synced.
        if record.get("baseline_seeded") and not clones:
            record["fingerprint"] = fp
            record["last_seen_run"] = run_id
            records[source_key] = record
            log("Skipping...", f"Ignoring baseline event from [{ev['source_endpoint_id']}] UID={ev['source_uid']}")
            continue

        desired_targets = [
            t for t in writable_endpoints if t["id"] != ev["source_endpoint_id"]
        ]
        invite_organizer = resolve_invite_organizer(
            ev["source_endpoint_id"], writable_endpoints
        )
        has_outlook_target = any(t.get("type") == "outlook" for t in desired_targets)
        if has_outlook_target:
            filtered_targets = []
            for t in desired_targets:
                if is_target_matching_invite_organizer(t, invite_organizer):
                    log(
                        "Skipping...",
                        f"Skipping direct clone to [{t['id']}] because Outlook invite organizer creates event there",
                    )
                    continue
                filtered_targets.append(t)
            desired_targets = filtered_targets

        for target in desired_targets:
            clone_state = clones.get(target["id"])
            if old_fp == fp and clone_state:
                try:
                    if clone_is_aligned(services, clone_state, ev, target):
                        log(
                            "Skipping...",
                            f"No changes for source [{ev['source_endpoint_id']}] -> target [{target['id']}]",
                        )
                        continue
                    log(
                        "Syncing...",
                        f"Drift detected on [{target['id']}], reconciling from source [{ev['source_endpoint_id']}]",
                    )
                except HttpError as exc:
                    log(
                        "Skipping...",
                        f"{source_key} -> {target['name']} existence check failed: {exc}",
                    )
                    continue

            is_create = not clone_state
            if is_create and create_count >= max_create_clones:
                log(
                    "Skipping...",
                    f"Create blocked for target [{target['id']}] (max_create_clones={max_create_clones})",
                )
                continue
            if is_create and target["type"] == "outlook" and outlook_create_count >= max_outlook_invites:
                log(
                    "Skipping...",
                    f"Invite send blocked for target [{target['id']}] (max_outlook_invites={max_outlook_invites})",
                )
                continue

            try:
                if target["type"] == "google":
                    target_service = services[target["auth_account_id"]]
                    action = "Creating sync event in" if is_create else "Updating sync event in"
                    log("Syncing...", f"{action} [{target['id']}] from source [{ev['source_endpoint_id']}]")
                    new_clone = create_or_update_google_clone(
                        target_service, target, ev, clone_state
                    )
                else:
                    invite_service = services[invite_organizer["auth_account_id"]]
                    action = "Sending sync invite to" if is_create else "Updating sync invite for"
                    log(
                        "Syncing...",
                        f"{action} [{target['id']}] from source [{ev['source_endpoint_id']}] via organizer [{invite_organizer.get('endpoint_id') or (invite_organizer['auth_account_id'] + ':' + invite_organizer['calendar_id'])}]",
                    )
                    new_clone = create_or_update_outlook_invite_clone(
                        invite_service, target, ev, clone_state, invite_organizer
                    )
                clones[target["id"]] = new_clone
                action = "updated" if clone_state else "created"
                if is_create:
                    create_count += 1
                    if target["type"] == "outlook":
                        outlook_create_count += 1
                log("Syncing...", f"{source_key} -> {target['name']} {action}")
            except HttpError as exc:
                log("Skipping...", f"{source_key} -> {target['name']} failed: {exc}")

        desired_ids = {t["id"] for t in desired_targets}
        for target_id, clone_state in list(clones.items()):
            if target_id in desired_ids:
                continue
            try:
                delete_clone(services, clone_state)
                log("Deleting...", f"Deleting obsolete sync event from [{target_id}] for source [{ev['source_endpoint_id']}]")
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                if status not in (404, 410):
                    log("Skipping...", f"{source_key} obsolete clone delete failed on {target_id}: {exc}")
                    continue
            clones.pop(target_id, None)

        records[source_key] = {
            "source_endpoint_id": ev["source_endpoint_id"],
            "source_uid": ev["source_uid"],
            "start_utc": utc_iso(ev["start_utc"]),
            "end_utc": utc_iso(ev["end_utc"]),
            "fingerprint": fp,
            "last_seen_run": run_id,
            "clones": clones,
        }

    # Source disappeared -> delete all clones and drop record.
    for source_key, record in list(records.items()):
        source_endpoint_id = record.get("source_endpoint_id")
        if source_endpoint_id not in successful_sources:
            continue
        if source_key in active_source_keys:
            continue

        clones = record.get("clones", {})
        if not isinstance(clones, dict):
            clones = {}
        for target_id, clone_state in list(clones.items()):
            try:
                delete_clone(services, clone_state)
                log("Deleting...", f"Deleting sync event from [{target_id}] because source [{source_endpoint_id}] no longer exists")
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                if status not in (404, 410):
                    log("Skipping...", f"{source_key} delete failed on {target_id}: {exc}")
                    continue
            clones.pop(target_id, None)

        records.pop(source_key, None)
        log("Deleting...", f"{source_key} source missing, record removed")

    save_db(db)
    log("Syncing...", "Sync cycle complete")


def run_watch(
    interval_seconds: int,
    max_source_events: int | None = None,
    max_create_clones: int = DEFAULT_MAX_CREATE_CLONES_PER_RUN,
    max_outlook_invites: int = DEFAULT_MAX_OUTLOOK_INVITES_PER_RUN,
    ignore_existing_on_start: bool = False,
) -> None:
    if interval_seconds < 5:
        raise ValueError("interval_seconds must be at least 5")
    log(
        "Syncing...",
        f"Starting watch mode (interval={interval_seconds}s, ignore_existing_on_start={ignore_existing_on_start})",
    )
    runtime_state: dict[str, Any] = {}
    try:
        while True:
            run_sync(
                max_source_events=max_source_events,
                max_create_clones=max_create_clones,
                max_outlook_invites=max_outlook_invites,
                ignore_existing_on_start=ignore_existing_on_start,
                runtime_state=runtime_state,
            )
            time_module.sleep(interval_seconds)
    except KeyboardInterrupt:
        log("Syncing...", "Watch mode stopped")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calendar Bridge")
    parser.add_argument(
        "--mode",
        choices=["auth", "test", "cleanup", "prime", "sync", "watch"],
        default="test",
        help="Use 'auth' to authorize tokens, 'test' for checks, 'cleanup' for orphan/duplicate bridge events, 'prime' to baseline existing events, 'sync' for one live run, or 'watch' for continuous sync.",
    )
    parser.add_argument(
        "--auth-account-id",
        default=None,
        help="In auth mode, authorize only this account id (e.g. personal, workspace). Default: all accounts in AUTH_ACCOUNTS.",
    )
    parser.add_argument(
        "--reauth",
        action="store_true",
        help="In auth mode, force fresh OAuth login even if token file exists.",
    )
    parser.add_argument(
        "--max-source-events",
        type=int,
        default=None,
        help="In sync/watch mode, process at most N source events per cycle (useful for safe rollout).",
    )
    parser.add_argument(
        "--max-create-clones",
        type=int,
        default=DEFAULT_MAX_CREATE_CLONES_PER_RUN,
        help="In sync mode, maximum number of new clones to create in one run.",
    )
    parser.add_argument(
        "--max-outlook-invites",
        type=int,
        default=DEFAULT_MAX_OUTLOOK_INVITES_PER_RUN,
        help="In sync mode, maximum number of new Outlook invite clones to create in one run.",
    )
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Required in sync/watch mode to apply writes/deletes.",
    )
    parser.add_argument(
        "--ignore-existing-on-start",
        action="store_true",
        help="In sync/watch mode, ignore source events already visible at process startup.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=30,
        help="In watch mode, seconds between sync cycles.",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=5,
        help="In test mode, number of events to preview per Google calendar read test.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.mode == "auth":
        run_auth(auth_account_id=args.auth_account_id, reauth=args.reauth)
    elif args.mode == "test":
        run_connection_test(max_preview_events=max(1, args.preview))
    elif args.mode == "cleanup":
        run_cleanup(confirm_live=args.confirm_live)
    elif args.mode == "prime":
        run_prime(max_source_events=args.max_source_events)
    elif args.mode == "sync":
        if not args.confirm_live:
            raise SystemExit("Refusing to run sync without --confirm-live")
        run_sync(
            max_source_events=args.max_source_events,
            max_create_clones=max(0, args.max_create_clones),
            max_outlook_invites=max(0, args.max_outlook_invites),
            ignore_existing_on_start=args.ignore_existing_on_start,
        )
    else:
        if not args.confirm_live:
            raise SystemExit("Refusing to run watch without --confirm-live")
        run_watch(
            interval_seconds=args.interval_seconds,
            max_source_events=args.max_source_events,
            max_create_clones=max(0, args.max_create_clones),
            max_outlook_invites=max(0, args.max_outlook_invites),
            ignore_existing_on_start=args.ignore_existing_on_start,
        )
