from __future__ import annotations

import os
import json
import sqlite3
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler


# -----------------------------
# Environment
# -----------------------------
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
PARKING_TIMEZONE = os.getenv("PARKING_TIMEZONE", "America/Vancouver")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/parking.db")
PARKING_CHANNEL_ID = os.getenv("PARKING_CHANNEL_ID", "")
BOARD_TS_FILE = os.getenv("BOARD_TS_FILE", "/data/board_ts.txt")

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    raise RuntimeError("SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET must be set")


# -----------------------------
# IDs and spot names
# -----------------------------
RANDY_ID = "U1HMCS77V"
KYLIE_ID = "UR0JZ0GR0"
MIKE_ID = "U03EH8HM4G0"
PETER_ID = "U03DVSASKPE"

M1 = "M1"
M2 = "M2"
P1 = "P1"
P2 = "P2"
P3 = "P3"
T1 = "T1"

SPOT_ORDER = [M1, M2, P1, P2, P3, T1]

DISPLAY_SPOT_NAMES = {
    "M1": "M1-#02",
    "M2": "M2-#11",
    "P1": "P1-#08",
    "P2": "P2-#45",
    "P3": "P3-#48",
    "T1": "T1-#43",
}

DISPLAY_NAMES = {
    RANDY_ID: "@Randy",
    KYLIE_ID: "@Kylie",
    MIKE_ID: "@Mike",
    PETER_ID: "@Peter",
}

MANAGEMENT_DEFAULTS = {
    RANDY_ID: M1,
    KYLIE_ID: M2,
}

CINOVA_USER_IDS = {MIKE_ID, PETER_ID}
CINOVA_GROUP_KEY = "cinova"


# -----------------------------
# Slack app
# -----------------------------
slack_app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
handler = SlackRequestHandler(slack_app)
scheduler: Optional[BackgroundScheduler] = None


# -----------------------------
# Models
# -----------------------------
@dataclass
class SpotRecord:
    spot_id: str
    state: str
    reserved_for_user_id: Optional[str]
    held_for_user_id: Optional[str]
    held_for_group: Optional[str]


# -----------------------------
# Helpers
# -----------------------------
def local_now() -> datetime:
    return datetime.now(ZoneInfo(PARKING_TIMEZONE))


def booking_day_text() -> str:
    now = local_now()

    if now.hour < 17:
        return "today"

    if now.weekday() in [4, 5, 6]:
        return "Monday"

    return "tomorrow"


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_db()) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reservations (
                spot_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                reserved_for_user_id TEXT,
                held_for_user_id TEXT,
                held_for_group TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_prefs (
                slack_user_id TEXT PRIMARY KEY,
                notifications_enabled INTEGER NOT NULL DEFAULT 1
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS management_away (
                slack_user_id TEXT PRIMARY KEY,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL
            )
            """
        )

        now = local_now().isoformat()

        defaults = {
            M1: ("reserved", RANDY_ID, None, None),
            M2: ("reserved", KYLIE_ID, None, None),
            P1: ("open", None, None, None),
            P2: ("open", None, None, None),
            P3: ("open", None, None, None),
            T1: ("held_group", None, None, CINOVA_GROUP_KEY),
        }

        for spot_id in SPOT_ORDER:
            row = cur.execute(
                "SELECT spot_id FROM reservations WHERE spot_id = ?",
                (spot_id,),
            ).fetchone()

            if row is None:
                state, reserved_for_user_id, held_for_user_id, held_for_group = defaults[spot_id]

                cur.execute(
                    """
                    INSERT INTO reservations (
                        spot_id, state, reserved_for_user_id,
                        held_for_user_id, held_for_group, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        spot_id,
                        state,
                        reserved_for_user_id,
                        held_for_user_id,
                        held_for_group,
                        now,
                    ),
                )

        conn.commit()


def set_spot_state(
    spot_id: str,
    state: str,
    reserved_for_user_id: Optional[str] = None,
    held_for_user_id: Optional[str] = None,
    held_for_group: Optional[str] = None,
) -> None:
    with closing(get_db()) as conn:
        conn.execute(
            """
            UPDATE reservations
            SET state = ?,
                reserved_for_user_id = ?,
                held_for_user_id = ?,
                held_for_group = ?,
                updated_at = ?
            WHERE spot_id = ?
            """,
            (
                state,
                reserved_for_user_id,
                held_for_user_id,
                held_for_group,
                local_now().isoformat(),
                spot_id,
            ),
        )
        conn.commit()


def get_spot(spot_id: str) -> SpotRecord:
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT spot_id, state, reserved_for_user_id, held_for_user_id, held_for_group
            FROM reservations
            WHERE spot_id = ?
            """,
            (spot_id,),
        ).fetchone()

    return SpotRecord(
        spot_id=row["spot_id"],
        state=row["state"],
        reserved_for_user_id=row["reserved_for_user_id"],
        held_for_user_id=row["held_for_user_id"],
        held_for_group=row["held_for_group"],
    )


def get_all_spots() -> List[SpotRecord]:
    with closing(get_db()) as conn:
        rows = conn.execute(
            """
            SELECT spot_id, state, reserved_for_user_id, held_for_user_id, held_for_group
            FROM reservations
            WHERE spot_id IN ('M1', 'M2', 'P1', 'P2', 'P3', 'T1')
            ORDER BY CASE spot_id
                WHEN 'M1' THEN 1
                WHEN 'M2' THEN 2
                WHEN 'P1' THEN 3
                WHEN 'P2' THEN 4
                WHEN 'P3' THEN 5
                WHEN 'T1' THEN 6
                ELSE 99
            END
            """
        ).fetchall()

    return [
        SpotRecord(
            spot_id=r["spot_id"],
            state=r["state"],
            reserved_for_user_id=r["reserved_for_user_id"],
            held_for_user_id=r["held_for_user_id"],
            held_for_group=r["held_for_group"],
        )
        for r in rows
    ]


def get_user_booked_spot(user_id: str) -> Optional[str]:
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT spot_id
            FROM reservations
            WHERE state = 'reserved' AND reserved_for_user_id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()

    return row["spot_id"] if row else None


def notifications_enabled(user_id: str) -> bool:
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT notifications_enabled
            FROM user_prefs
            WHERE slack_user_id = ?
            """,
            (user_id,),
        ).fetchone()

    if row is None:
        return True

    return bool(row["notifications_enabled"])


def toggle_notifications_for_user(user_id: str) -> bool:
    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT notifications_enabled
            FROM user_prefs
            WHERE slack_user_id = ?
            """,
            (user_id,),
        ).fetchone()

        if row is None:
            new_value = 0
            conn.execute(
                """
                INSERT INTO user_prefs (slack_user_id, notifications_enabled)
                VALUES (?, ?)
                """,
                (user_id, new_value),
            )
        else:
            new_value = 0 if row["notifications_enabled"] else 1
            conn.execute(
                """
                UPDATE user_prefs
                SET notifications_enabled = ?
                WHERE slack_user_id = ?
                """,
                (new_value, user_id),
            )

        conn.commit()

    return bool(new_value)


def parking_date() -> str:
    now = local_now()
    target_date = now.date()

    if now.hour >= 17:
        if now.weekday() == 4:
            target_date = target_date + timedelta(days=3)
        elif now.weekday() == 5:
            target_date = target_date + timedelta(days=2)
        elif now.weekday() == 6:
            target_date = target_date + timedelta(days=1)
        else:
            target_date = target_date + timedelta(days=1)

    return target_date.isoformat()


def user_is_away(user_id: str) -> bool:
    target_date = parking_date()

    with closing(get_db()) as conn:
        row = conn.execute(
            """
            SELECT start_date, end_date
            FROM management_away
            WHERE slack_user_id = ?
            """,
            (user_id,),
        ).fetchone()

    if row is None:
        return False

    return row["start_date"] <= target_date <= row["end_date"]


def set_user_away(user_id: str, start_date: str, end_date: str) -> None:
    with closing(get_db()) as conn:
        conn.execute(
            """
            INSERT INTO management_away (slack_user_id, start_date, end_date)
            VALUES (?, ?, ?)
            ON CONFLICT(slack_user_id)
            DO UPDATE SET start_date = excluded.start_date,
                          end_date = excluded.end_date
            """,
            (user_id, start_date, end_date),
        )
        conn.commit()


def clear_user_away(user_id: str) -> None:
    with closing(get_db()) as conn:
        conn.execute(
            """
            DELETE FROM management_away
            WHERE slack_user_id = ?
            """,
            (user_id,),
        )
        conn.commit()


def maybe_dm(user_id: str, text: str) -> None:
    if not notifications_enabled(user_id):
        return

    try:
        slack_app.client.chat_postMessage(channel=user_id, text=text)
    except Exception:
        pass


# -----------------------------
# Live board message
# -----------------------------
def save_board_ts(ts: str) -> None:
    try:
        with open(BOARD_TS_FILE, "w") as f:
            f.write(ts)
    except Exception as e:
        print(f"Could not save board ts: {e}", flush=True)


def load_board_ts() -> Optional[str]:
    try:
        with open(BOARD_TS_FILE, "r") as f:
            return f.read().strip()
    except Exception:
        return None


def board_line_for_spot(spot: SpotRecord) -> str:
    if spot.state == "open":
        status = "🟢 Open"
    elif spot.state == "held_user":
        name = DISPLAY_NAMES.get(spot.held_for_user_id, f"<@{spot.held_for_user_id}>")
        status = f"🟡 Held for {name}"
    elif spot.state == "held_group":
        status = "🟡 Held for Cinova users"
    elif spot.state == "reserved":
        name = DISPLAY_NAMES.get(spot.reserved_for_user_id, f"<@{spot.reserved_for_user_id}>")
        status = f"🔴 Booked by {name}"
    else:
        status = spot.state

    label = DISPLAY_SPOT_NAMES.get(spot.spot_id, spot.spot_id)
    return f"{label} - {status}"


def build_board_text() -> str:
    lines = ["🚗 *Office Parking Board*\n"]

    for spot in get_all_spots():
        lines.append(board_line_for_spot(spot))

    lines.append(f"\n_Last updated: {local_now().strftime('%-I:%M %p')}_")
    return "\n".join(lines)


def update_parking_board() -> None:
    if not PARKING_CHANNEL_ID:
        print("PARKING_CHANNEL_ID is not set. Board not posted.", flush=True)
        return

    text = build_board_text()
    ts = load_board_ts()

    try:
        if ts:
            slack_app.client.chat_update(
                channel=PARKING_CHANNEL_ID,
                ts=ts,
                text=text,
            )
        else:
            resp = slack_app.client.chat_postMessage(
                channel=PARKING_CHANNEL_ID,
                text=text,
            )
            save_board_ts(resp["ts"])

        print("Parking board updated", flush=True)

    except Exception as e:
        print(f"Board update failed: {e}", flush=True)


# -----------------------------
# Home tab
# -----------------------------
def display_line_for_spot(spot: SpotRecord) -> str:
    if spot.state == "open":
        status = "🟢 Open"

    elif spot.state == "held_user":
        name = DISPLAY_NAMES.get(spot.held_for_user_id, f"<@{spot.held_for_user_id}>")
        status = f"🟡 Held for {name}"

    elif spot.state == "held_group":
        if spot.held_for_group == CINOVA_GROUP_KEY:
            status = "🟡 Held for Cinova users"
        else:
            status = "🟡 Held"

    elif spot.state == "reserved":
        name = DISPLAY_NAMES.get(spot.reserved_for_user_id, f"<@{spot.reserved_for_user_id}>")
        status = f"🔴 Booked by {name}"

    else:
        status = spot.state

    label = DISPLAY_SPOT_NAMES.get(spot.spot_id, spot.spot_id)
    return f"{label} - {status}"


def spot_available_to_user(spot: SpotRecord, user_id: str) -> bool:
    if spot.spot_id == T1:
        return user_id in CINOVA_USER_IDS and spot.state != "reserved"

    if spot.state == "open":
        return True

    return False


def available_spots_for_user(user_id: str) -> List[SpotRecord]:
    return [
        spot
        for spot in get_all_spots()
        if spot_available_to_user(spot, user_id)
    ]


def has_any_available_spot_for_user(user_id: str) -> bool:
    return len(available_spots_for_user(user_id)) > 0


def parking_home_blocks(user_id: str) -> list:
    booked_spot = get_user_booked_spot(user_id)

    if booked_spot:
        label = DISPLAY_SPOT_NAMES.get(booked_spot, booked_spot)
        booking_text = f"You have Spot {label} {booking_day_text()}."
    elif has_any_available_spot_for_user(user_id):
        booking_text = f"You do not have a booking {booking_day_text()}."
    else:
        booking_text = f"Sorry, all spots are reserved for {booking_day_text()}."

    refreshed = local_now().strftime("%-I:%M:%S %p")
    notif_text = "Notifications: On" if notifications_enabled(user_id) else "Notifications: Off"

    available_options = [
        {
            "text": {
                "type": "plain_text",
                "text": DISPLAY_SPOT_NAMES.get(spot.spot_id, spot.spot_id),
            },
            "value": spot.spot_id,
        }
        for spot in available_spots_for_user(user_id)
    ]

    if not available_options:
        available_options = [
            {
                "text": {"type": "plain_text", "text": "No spots available"},
                "value": "none",
            }
        ]

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Office Parking", "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": booking_text},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": notif_text},
                {"type": "mrkdwn", "text": f"Last refreshed: {refreshed}"},
            ],
        },
        {"type": "divider"},
    ]

    for spot in get_all_spots():
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": display_line_for_spot(spot)},
            }
        )

    action_elements = [
        {
            "type": "static_select",
            "placeholder": {"type": "plain_text", "text": "Reserve spot"},
            "action_id": "reserve_spot_select",
            "options": available_options,
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Release"},
            "action_id": "release_today",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Refresh"},
            "action_id": "refresh_home",
        },
    ]

    if user_id in MANAGEMENT_DEFAULTS:
        action_elements.extend(
            [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Set Away Dates"},
                    "action_id": "open_away_modal",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Back / Book My Spot"},
                    "action_id": "clear_away_dates",
                },
            ]
        )

    action_elements.append(
        {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Turn off notifications"
                if notifications_enabled(user_id)
                else "Turn on notifications",
            },
            "action_id": "toggle_notifications",
        }
    )

    blocks.extend(
        [
            {"type": "divider"},
            {
                "type": "actions",
                "elements": action_elements,
            },
        ]
    )

    return blocks


def publish_home(user_id: str) -> None:
    slack_app.client.views_publish(
        user_id=user_id,
        view={"type": "home", "blocks": parking_home_blocks(user_id)},
    )


def all_known_user_ids() -> List[str]:
    users = {RANDY_ID, KYLIE_ID, MIKE_ID, PETER_ID}

    with closing(get_db()) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT reserved_for_user_id
            FROM reservations
            WHERE reserved_for_user_id IS NOT NULL
            """
        ).fetchall()

        for r in rows:
            if r["reserved_for_user_id"]:
                users.add(r["reserved_for_user_id"])

        rows = conn.execute("SELECT slack_user_id FROM user_prefs").fetchall()

        for r in rows:
            if r["slack_user_id"]:
                users.add(r["slack_user_id"])

    return list(users)


def publish_home_all_users() -> None:
    for user_id in all_known_user_ids():
        try:
            publish_home(user_id)
        except Exception:
            pass


# -----------------------------
# Parking logic
# -----------------------------
def reserve_for_user(user_id: str, requested_spot_id: Optional[str] = None) -> str:
    existing = get_user_booked_spot(user_id)

    if existing:
        label = DISPLAY_SPOT_NAMES.get(existing, existing)
        return f"You already have Spot {label} {booking_day_text()}."

    if user_id in CINOVA_USER_IDS and not requested_spot_id:
        requested_spot_id = T1

    available = available_spots_for_user(user_id)

    if requested_spot_id:
        if requested_spot_id == "none":
            return "Sorry, no spots are available."

        matching = [s for s in available if s.spot_id == requested_spot_id]

        if not matching:
            label = DISPLAY_SPOT_NAMES.get(requested_spot_id, requested_spot_id)
            return f"Spot {label} is not available to reserve."

        set_spot_state(requested_spot_id, "reserved", reserved_for_user_id=user_id)
        label = DISPLAY_SPOT_NAMES.get(requested_spot_id, requested_spot_id)
        return f"You have Spot {label} {booking_day_text()}."

    if not available:
        return f"Sorry, all spots are reserved for {booking_day_text()}."

    spot = available[0]
    set_spot_state(spot.spot_id, "reserved", reserved_for_user_id=user_id)
    label = DISPLAY_SPOT_NAMES.get(spot.spot_id, spot.spot_id)
    return f"You have Spot {label} {booking_day_text()}."


def release_for_user(user_id: str) -> str:
    booked_spot = get_user_booked_spot(user_id)

    if booked_spot:
        if booked_spot in [M1, M2] and user_id in MANAGEMENT_DEFAULTS:
            set_spot_state(booked_spot, "open")
        else:
            set_spot_state(booked_spot, "open")

        label = DISPLAY_SPOT_NAMES.get(booked_spot, booked_spot)
        return f"Spot {label} is now open."

    if user_id in CINOVA_USER_IDS:
        t1 = get_spot(T1)

        if t1.state == "held_group" and t1.held_for_group == CINOVA_GROUP_KEY:
            set_spot_state(T1, "open")
            label = DISPLAY_SPOT_NAMES.get(T1, T1)
            return f"Spot {label} is now open."

        if t1.state == "reserved" and t1.reserved_for_user_id in CINOVA_USER_IDS:
            set_spot_state(T1, "open")
            label = DISPLAY_SPOT_NAMES.get(T1, T1)
            return f"Spot {label} is now open."

    return "You do not have a booking to release."


def reset_for_5pm() -> None:
    if user_is_away(RANDY_ID):
        set_spot_state(M1, "open")
    else:
        set_spot_state(M1, "reserved", reserved_for_user_id=RANDY_ID)

    if user_is_away(KYLIE_ID):
        set_spot_state(M2, "open")
    else:
        set_spot_state(M2, "reserved", reserved_for_user_id=KYLIE_ID)

    set_spot_state(P1, "open")
    set_spot_state(P2, "open")
    set_spot_state(P3, "open")
    set_spot_state(T1, "held_group", held_for_group=CINOVA_GROUP_KEY)


# -----------------------------
# Slack handlers
# -----------------------------
@slack_app.event("app_home_opened")
def on_app_home_opened(event, logger):
    publish_home(event["user"])
    logger.info("Published App Home for %s", event["user"])


@slack_app.command("/parking")
def parking_command(ack, body):
    ack()

    user_id = body["user_id"]
    text = body.get("text", "").strip().lower()

    if text == "whoami":
        maybe_dm(user_id, f"Your Slack user ID is: {user_id}")
        return

    if text in ["reserve", "book"]:
        message = reserve_for_user(user_id)
        publish_home_all_users()
        update_parking_board()
        maybe_dm(user_id, f":parking: {message}")
        return

    if text in ["release", "cancel"]:
        message = release_for_user(user_id)
        publish_home_all_users()
        update_parking_board()
        maybe_dm(user_id, f":parking: {message}")
        return

    if text in ["refresh", "status", ""]:
        publish_home(user_id)
        update_parking_board()
        return

    maybe_dm(
        user_id,
        ":parking: Try `/parking reserve`, `/parking release`, `/parking refresh`, or `/parking whoami`."
    )


@slack_app.action("reserve_today")
def reserve_today_action(ack, body):
    ack()

    user_id = body["user"]["id"]
    message = reserve_for_user(user_id)

    publish_home_all_users()
    update_parking_board()
    maybe_dm(user_id, f":parking: {message}")


@slack_app.action("reserve_spot_select")
def reserve_spot_select_action(ack, body):
    ack()

    user_id = body["user"]["id"]
    selected_spot = body["actions"][0]["selected_option"]["value"]

    message = reserve_for_user(user_id, selected_spot)

    publish_home_all_users()
    update_parking_board()
    maybe_dm(user_id, f":parking: {message}")


@slack_app.action("release_today")
def release_today_action(ack, body):
    ack()

    user_id = body["user"]["id"]
    message = release_for_user(user_id)

    publish_home_all_users()
    update_parking_board()
    maybe_dm(user_id, f":parking: {message}")


@slack_app.action("open_away_modal")
def open_away_modal_action(ack, body):
    ack()

    user_id = body["user"]["id"]

    if user_id not in MANAGEMENT_DEFAULTS:
        maybe_dm(user_id, ":parking: Only Randy and Kylie can set away dates.")
        return

    slack_app.client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "away_dates_submit",
            "title": {"type": "plain_text", "text": "Away Dates"},
            "submit": {"type": "plain_text", "text": "Save"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "start_date_block",
                    "label": {"type": "plain_text", "text": "First day away"},
                    "element": {
                        "type": "datepicker",
                        "action_id": "start_date",
                    },
                },
                {
                    "type": "input",
                    "block_id": "end_date_block",
                    "label": {"type": "plain_text", "text": "Last day away"},
                    "element": {
                        "type": "datepicker",
                        "action_id": "end_date",
                    },
                },
            ],
        },
    )


@slack_app.action("clear_away_dates")
def clear_away_dates_action(ack, body):
    ack()

    user_id = body["user"]["id"]

    if user_id not in MANAGEMENT_DEFAULTS:
        maybe_dm(user_id, ":parking: Only Randy and Kylie can use this button.")
        return

    clear_user_away(user_id)

    management_spot = MANAGEMENT_DEFAULTS[user_id]
    set_spot_state(management_spot, "reserved", reserved_for_user_id=user_id)

    label = DISPLAY_SPOT_NAMES.get(management_spot, management_spot)

    maybe_dm(
        user_id,
        f":parking: Welcome back. Spot {label} is booked for you again."
    )

    publish_home_all_users()
    update_parking_board()


@slack_app.action("refresh_home")
def refresh_home_action(ack, body):
    ack()

    user_id = body["user"]["id"]
    publish_home(user_id)
    update_parking_board()


@slack_app.view("away_dates_submit")
def away_dates_submit_view(ack, body, view):
    user_id = body["user"]["id"]

    values = view["state"]["values"]
    start_date = values["start_date_block"]["start_date"]["selected_date"]
    end_date = values["end_date_block"]["end_date"]["selected_date"]

    if start_date > end_date:
        ack(
            {
                "response_action": "errors",
                "errors": {
                    "end_date_block": "End date must be after the start date."
                },
            }
        )
        return

    ack()

    set_user_away(user_id, start_date, end_date)

    management_spot = MANAGEMENT_DEFAULTS[user_id]

    if user_is_away(user_id):
        current_spot = get_spot(management_spot)

        if current_spot.state == "reserved" and current_spot.reserved_for_user_id != user_id:
            pass
        else:
            set_spot_state(management_spot, "open")

    label = DISPLAY_SPOT_NAMES.get(management_spot, management_spot)

    maybe_dm(
        user_id,
        f":parking: Spot {label} will be open from {start_date} to {end_date}."
    )

    publish_home_all_users()
    update_parking_board()

@slack_app.action("toggle_notifications")
def toggle_notifications_action(ack, body):
    ack()

    user_id = body["user"]["id"]
    now_enabled = toggle_notifications_for_user(user_id)

    publish_home(user_id)

    try:
        slack_app.client.chat_postMessage(
            channel=user_id,
            text="Notifications turned on." if now_enabled else "Notifications turned off.",
        )
    except Exception:
        pass


# -----------------------------
# Scheduler
# -----------------------------
def scheduled_5pm_reset() -> None:
    reset_for_5pm()
    publish_home_all_users()
    update_parking_board()
    print("Parking reset completed at 5:00 PM local time", flush=True)


# -----------------------------
# App lifecycle
# -----------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler

    db_dir = os.path.dirname(DATABASE_PATH)

    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    init_db()
    update_parking_board()

    scheduler = BackgroundScheduler(timezone=PARKING_TIMEZONE)
    scheduler.add_job(
        scheduled_5pm_reset,
        "cron",
        day_of_week="mon-fri",
        hour=17,
        minute=0,
    )
    scheduler.start()
    print("Scheduler started", flush=True)

    yield

    if scheduler:
        scheduler.shutdown()

    print("Scheduler stopped", flush=True)


api = FastAPI(title="Parking Bot", lifespan=lifespan)


# -----------------------------
# Routes
# -----------------------------
@api.get("/")
def root():
    return {"status": "Parking bot is running"}


@api.get("/health")
def health():
    return {"ok": True, "time": local_now().isoformat()}


@api.post("/slack/events")
async def slack_events(req: Request):
    body = await req.body()

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return PlainTextResponse("Invalid request", status_code=400)

    if payload.get("type") == "url_verification":
        return PlainTextResponse(payload["challenge"], status_code=200)

    return await handler.handle(req)


@api.post("/slack/interactivity")
async def slack_interactivity(req: Request):
    return await handler.handle(req)


@api.post("/slack/commands")
async def slack_commands(req: Request):
    return await handler.handle(req)
