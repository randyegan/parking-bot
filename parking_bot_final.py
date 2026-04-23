from __future__ import annotations

import os
import json
import sqlite3
from contextlib import asynccontextmanager, closing
from datetime import datetime
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from typing import Optional, List

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler


# -----------------------------
# ENV
# -----------------------------
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
PARKING_CHANNEL_ID = os.getenv("PARKING_CHANNEL_ID", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/parking.db")
PARKING_TIMEZONE = os.getenv("PARKING_TIMEZONE", "America/Vancouver")

BOARD_TS_FILE = "/data/board_ts.txt"

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    raise RuntimeError("Missing Slack credentials")


# -----------------------------
# USERS / SPOTS
# -----------------------------
RANDY_ID = "U1HMCS77V"
KYLIE_ID = "UR0JZ0GR0"
MIKE_ID = "U03EH8HM4G0"
PETER_ID = "U03DVSASKPE"

M1, M2, P1, P2, P3, T1 = "M1", "M2", "P1", "P2", "P3", "T1"
SPOT_ORDER = [M1, M2, P1, P2, P3, T1]

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

CINOVA_USERS = {MIKE_ID, PETER_ID}


# -----------------------------
# SLACK
# -----------------------------
slack_app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
handler = SlackRequestHandler(slack_app)
scheduler: Optional[BackgroundScheduler] = None


# -----------------------------
# MODEL
# -----------------------------
@dataclass
class Spot:
    spot_id: str
    state: str
    reserved_for: Optional[str]
    held_for: Optional[str]
    held_group: Optional[str]
    updated_at: Optional[str]


# -----------------------------
# DB
# -----------------------------
def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now():
    return datetime.now(ZoneInfo(PARKING_TIMEZONE)).isoformat()


def init_db():
    with closing(get_db()) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS spots (
            spot_id TEXT PRIMARY KEY,
            state TEXT,
            reserved_for TEXT,
            held_for TEXT,
            held_group TEXT,
            updated_at TEXT
        )
        """)

        defaults = {
            M1: ("held", None, RANDY_ID, None),
            M2: ("held", None, KYLIE_ID, None),
            P1: ("open", None, None, None),
            P2: ("open", None, None, None),
            P3: ("open", None, None, None),
            T1: ("held_group", None, None, "cinova"),
        }

        for spot, vals in defaults.items():
            exists = conn.execute("SELECT 1 FROM spots WHERE spot_id=?", (spot,)).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO spots VALUES (?, ?, ?, ?, ?, ?)",
                    (spot, *vals, now())
                )
        conn.commit()


def get_all():
    with closing(get_db()) as conn:
        rows = conn.execute("SELECT * FROM spots").fetchall()
        return [Spot(*r) for r in rows]


def update_spot(spot, state, r=None, h=None, g=None):
    with closing(get_db()) as conn:
        conn.execute("""
        UPDATE spots
        SET state=?, reserved_for=?, held_for=?, held_group=?, updated_at=?
        WHERE spot_id=?
        """, (state, r, h, g, now(), spot))
        conn.commit()


def get_user_spot(uid):
    with closing(get_db()) as conn:
        row = conn.execute(
            "SELECT spot_id FROM spots WHERE state='reserved' AND reserved_for=?",
            (uid,)
        ).fetchone()
    return row[0] if row else None


# -----------------------------
# BOARD STORAGE
# -----------------------------
def save_ts(ts):
    try:
        with open(BOARD_TS_FILE, "w") as f:
            f.write(ts)
    except:
        pass


def load_ts():
    try:
        return open(BOARD_TS_FILE).read().strip()
    except:
        return None


# -----------------------------
# BOARD VIEW
# -----------------------------
def board_text():
    lines = ["🚗 *Office Parking Board*\n"]

    for s in get_all():
        if s.state == "open":
            status = "🟢 Open"
        elif s.state == "held":
            name = DISPLAY_NAMES.get(s.held_for, f"<@{s.held_for}>")
            status = f"🟡 Held for {name}"
        elif s.state == "held_group":
            status = "🟡 Held for Cinova users"
        else:
            name = DISPLAY_NAMES.get(s.reserved_for, f"<@{s.reserved_for}>")
            status = f"🔴 Booked by {name}"

        lines.append(f"{s.spot_id} - {status}")

    lines.append(f"\n_Last updated: {datetime.now(ZoneInfo(PARKING_TIMEZONE)).strftime('%-I:%M %p')}_")
    return "\n".join(lines)


def update_board():
    if not PARKING_CHANNEL_ID:
        return

    text = board_text()
    ts = load_ts()

    try:
        if ts:
            slack_app.client.chat_update(
                channel=PARKING_CHANNEL_ID,
                ts=ts,
                text=text
            )
        else:
            resp = slack_app.client.chat_postMessage(
                channel=PARKING_CHANNEL_ID,
                text=text
            )
            save_ts(resp["ts"])
    except Exception as e:
        print("Board update failed:", e)


# -----------------------------
# LOGIC
# -----------------------------
def reserve(uid):
    if get_user_spot(uid):
        return

    if uid in MANAGEMENT_DEFAULTS:
        s = next(x for x in get_all() if x.spot_id == MANAGEMENT_DEFAULTS[uid])
        if s.state == "held":
            update_spot(s.spot_id, "reserved", uid)
            return

    if uid in CINOVA_USERS:
        s = next(x for x in get_all() if x.spot_id == T1)
        if s.state == "held_group":
            update_spot(T1, "reserved", uid)
            return

    for s in get_all():
        if s.state == "open":
            update_spot(s.spot_id, "reserved", uid)
            return


def release(uid):
    spot = get_user_spot(uid)
    if not spot:
        return

    update_spot(spot, "open")


def reset():
    update_spot(M1, "held", None, RANDY_ID)
    update_spot(M2, "held", None, KYLIE_ID)
    update_spot(P1, "open")
    update_spot(P2, "open")
    update_spot(P3, "open")
    update_spot(T1, "held_group", None, None, "cinova")


# -----------------------------
# SLACK ACTIONS
# -----------------------------
@slack_app.action("reserve_today")
def reserve_action(ack, body):
    ack()
    reserve(body["user"]["id"])
    update_board()


@slack_app.action("release_today")
def release_action(ack, body):
    ack()
    release(body["user"]["id"])
    update_board()


# -----------------------------
# FASTAPI
# -----------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    global scheduler
    scheduler = BackgroundScheduler(timezone=PARKING_TIMEZONE)
    scheduler.add_job(reset, "cron", hour=17)
    scheduler.start()
    yield
    scheduler.shutdown()


api = FastAPI(lifespan=lifespan)


@api.post("/slack/events")
async def events(req: Request):
    return await handler.handle(req)


@api.post("/slack/interactivity")
async def interactivity(req: Request):
    return await handler.handle(req)
