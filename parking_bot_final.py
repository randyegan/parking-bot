
"""
Parking Bot - final single-file version

Features:
- Slack App Home UI
- Spots: M1, M2, P1, P2, P3, T1
- Default held spots:
    M1 -> Randy (U1HMCS77V)
    M2 -> Kylie (UR0JZ0GR0)
    T1 -> Cinova users (Mike U03EH8HM4G0, Peter U03DVSASKPE)
- Buttons: Reserve, Cancel, Refresh, Toggle notifications
- Manual release only for M1/M2/T1 via Cancel
- Daily 5:00 PM reset:
    P1/P2/P3 -> Open
    M1 -> Held for Randy
    M2 -> Held for Kylie
    T1 -> Held for Cinova users
- No use of respond() in App Home actions
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List

from fastapi import FastAPI, Request
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from apscheduler.schedulers.background import BackgroundScheduler


# -----------------------------
# Environment / config
# -----------------------------
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
PARKING_TIMEZONE = os.getenv("PARKING_TIMEZONE", "America/Vancouver")
DATABASE_PATH = os.getenv("DATABASE_PATH", "parking.db")

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    raise RuntimeError("SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET must be set")

# User IDs
RANDY_ID = "U1HMCS77V"
KYLIE_ID = "UR0JZ0GR0"
MIKE_ID = "U03EH8HM4G0"
PETER_ID = "U03DVSASKPE"

# Spot constants
M1 = "M1"
M2 = "M2"
P1 = "P1"
P2 = "P2"
P3 = "P3"
T1 = "T1"

SPOT_ORDER = [M1, M2, P1, P2, P3, T1]
P_SPOTS = [P1, P2, P3]

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


# -----------------------------
# Slack app / web app
# -----------------------------
slack_app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
handler = SlackRequestHandler(slack_app)
api = FastAPI(title="Parking Bot Final")


# -----------------------------
# Data model notes
# -----------------------------
# reservations table holds one row per spot
# state can be:
#   open
#   held_user       -> held for a specific user_id (M1/M2)
#   held_group      -> held for a group_key (T1 / cinova)
#   reserved        -> booked by reserved_for_user_id
#
# prefs table stores per-user notification preference


@dataclass
class SpotRecord:
    spot_id: str
    state: str
    reserved_for_user_id: Optional[str]
    held_for_user_id: Optional[str]
    held_for_group: Optional[str]


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def local_now() -> datetime:
    return datetime.now(ZoneInfo(PARKING_TIMEZONE))


def init_db() -> None:
    with closing(get_db()) as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS reservations (
            spot_id TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            reserved_for_user_id TEXT,
            held_for_user_id TEXT,
            held_for_group TEXT,
            updated_at TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_prefs (
            slack_user_id TEXT PRIMARY KEY,
            notifications_enabled INTEGER NOT NULL DEFAULT 1
        )
        """)

        # Seed or repair canonical rows spot by spot
        now = local_now().isoformat()
        canonical_defaults = {
            M1: ("held_user", None, RANDY_ID, None),
            M2: ("held_user", None, KYLIE_ID, None),
            P1: ("open", None, None, None),
            P2: ("open", None, None, None),
            P3: ("open", None, None, None),
            T1: ("held_group", None, None, "cinova"),
        }

        for spot_id in SPOT_ORDER:
            row = cur.execute(
                "SELECT spot_id FROM reservations WHERE spot_id = ?",
                (spot_id,),
            ).fetchone()
            if not row:
                state, reserved_for_user_id, held_for_user_id, held_for_group = canonical_defaults[spot_id]
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


def reset_for_new_day() -> None:
    # 5 PM reset
    set_spot_state(M1, "held_user", held_for_user_id=RANDY_ID)
    set_spot_state(M2, "held_user", held_for_user_id=KYLIE_ID)
    set_spot_state(P1, "open")
    set_spot_state(P2, "open")
    set_spot_state(P3, "open")
    set_spot_state(T1, "held_group", held_for_group="cinova")


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
        if not row:
            return True
        return bool(row["notifications_enabled"])


def toggle_notifications(user_id: str) -> bool:
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


def maybe_dm(user_id: str, text: str) -> None:
    if not notifications_enabled(user_id):
        return
    try:
        slack_app.client.chat_postMessage(channel=user_id, text=text)
    except Exception:
        pass


def user_can_claim_group_hold(user_id: str, group_key: Optional[str]) -> bool:
    if group_key == "cinova":
        return user_id in CINOVA_USER_IDS
    return False


def reserve_for_user(user_id: str) -> str:
    existing = get_user_booked_spot(user_id)
    if existing:
        return f"You have Spot {existing} today."

    # 1) Claim user's own management hold
    for held_user_id, held_spot in MANAGEMENT_DEFAULTS.items():
        if user_id == held_user_id:
            spot = get_spot(held_spot)
            if spot.state == "held_user" and spot.held_for_user_id == user_id:
                set_spot_state(held_spot, "reserved", reserved_for_user_id=user_id)
                return f"You have Spot {held_spot} today."

    # 2) Claim T1 if user is Cinova and T1 is group-held
    if user_id in CINOVA_USER_IDS:
        spot = get_spot(T1)
        if spot.state == "held_group" and spot.held_for_group == "cinova":
            set_spot_state(T1, "reserved", reserved_for_user_id=user_id)
            return f"You have Spot {T1} today."

    # 3) Otherwise book first open spot in display order
    for spot_id in SPOT_ORDER:
        spot = get_spot(spot_id)
        if spot.state == "open":
            set_spot_state(spot_id, "reserved", reserved_for_user_id=user_id)
            return f"You have Spot {spot_id} today."

    return "No spots are available right now."


def cancel_for_user(user_id: str) -> str:
    booked_spot = get_user_booked_spot(user_id)
    if not booked_spot:
        return "You do not have a booking to cancel."

    # Cancellation makes the spot open immediately, per your latest UI rule.
    set_spot_state(booked_spot, "open")
    return f"Spot {booked_spot} is now open."


def display_line_for_spot(spot: SpotRecord) -> str:
    if spot.state == "open":
        status = "Open"
    elif spot.state == "held_user":
        if spot.held_for_user_id == RANDY_ID:
            status = "Held for @Randy"
        elif spot.held_for_user_id == KYLIE_ID:
            status = "Held for @Kylie"
        else:
            status = f"Held for <@{spot.held_for_user_id}>"
    elif spot.state == "held_group":
        if spot.held_for_group == "cinova":
            status = "Held for Cinova users"
        else:
            status = "Held"
    elif spot.state == "reserved":
        pretty = DISPLAY_NAMES.get(spot.reserved_for_user_id, f"<@{spot.reserved_for_user_id}>")
        status = f"Booked by {pretty}"
    else:
        status = spot.state

    return f"*{spot.spot_id}*\n{status}"


def parking_home_blocks(user_id: str) -> list:
    booked_spot = get_user_booked_spot(user_id)
    if booked_spot:
        booking_text = f"You have Spot {booked_spot} today."
    else:
        booking_text = "You do not have a booking today."

    notif_text = "Notifications: On" if notifications_enabled(user_id) else "Notifications: Off"
    refreshed = local_now().strftime("%-I:%M:%S %p")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Parking", "emoji": True},
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

    blocks.extend(
        [
            {"type": "divider"},
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Reserve"},
                        "action_id": "reserve_today",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Cancel"},
                        "action_id": "cancel_today",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Refresh"},
                        "action_id": "refresh_home",
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Turn off notifications" if notifications_enabled(user_id) else "Turn on notifications",
                        },
                        "action_id": "toggle_notifications",
                    },
                ],
            },
        ]
    )

    return blocks


def publish_home(user_id: str) -> None:
    slack_app.client.views_publish(
        user_id=user_id,
        view={"type": "home", "blocks": parking_home_blocks(user_id)},
    )


# -----------------------------
# Slack listeners
# -----------------------------
@slack_app.event("app_home_opened")
def on_app_home_opened(event, logger):
    user_id = event["user"]
    publish_home(user_id)
    logger.info("Published App Home for %s", user_id)


@slack_app.command("/parking")
def parking_command(ack, body):
    ack()
    user_id = body["user_id"]
    publish_home(user_id)


@slack_app.action("reserve_today")
def reserve_today_action(ack, body):
    ack()
    user_id = body["user"]["id"]
    message = reserve_for_user(user_id)
    publish_home(user_id)
    maybe_dm(user_id, f":parking: {message}")


@slack_app.action("cancel_today")
def cancel_today_action(ack, body):
    ack()
    user_id = body["user"]["id"]
    message = cancel_for_user(user_id)
    publish_home(user_id)
    maybe_dm(user_id, f":parking: {message}")


@slack_app.action("refresh_home")
def refresh_home_action(ack, body):
    ack()
    user_id = body["user"]["id"]
    publish_home(user_id)
    # No DM for refresh to reduce noise


@slack_app.action("toggle_notifications")
def toggle_notifications_action(ack, body):
    ack()
    user_id = body["user"]["id"]
    new_value = toggle_notifications(user_id)
    publish_home(user_id)
    try:
        slack_app.client.chat_postMessage(
            channel=user_id,
            text="Notifications turned on." if new_value else "Notifications turned off.",
        )
    except Exception:
        pass


# -----------------------------
# FastAPI routes
# -----------------------------
@api.get("/health")
def health():
    return {"ok": True, "time": local_now().isoformat()}


@api.post("/slack/events")
async def slack_events(req: Request):
    return await handler.handle(req)


# -----------------------------
# Daily 5 PM reset
# -----------------------------
def scheduled_5pm_reset():
    reset_for_new_day()
    print("Parking reset completed at 5:00 PM local time")


# -----------------------------
# Startup
# -----------------------------
init_db()

scheduler = BackgroundScheduler(timezone=PARKING_TIMEZONE)
scheduler.add_job(scheduled_5pm_reset, "cron", hour=17, minute=0)
scheduler.start()


# -----------------------------
# Local run reminder
# -----------------------------
# uvicorn parking_bot_final:api --reload --port 3000
