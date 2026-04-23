# ===== PARKING BOT FINAL (COPY/PASTE THIS WHOLE FILE) =====

import os
import sqlite3
from contextlib import closing
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------- CONFIG ----------------
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
DB_PATH = os.getenv("DATABASE_PATH", "parking.db")
TZ = ZoneInfo("America/Vancouver")

RANDY = "U1HMCS77V"
KYLIE = "UR0JZ0GR0"
MIKE = "U03EH8HM4G0"
PETER = "U03DVSASKPE"

# ---------------- APP ----------------
app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
handler = SlackRequestHandler(app)
api = FastAPI()

# ---------------- DB ----------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(db()) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS spots (
            id TEXT PRIMARY KEY,
            state TEXT,
            user TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS prefs (
            user TEXT PRIMARY KEY,
            notify INTEGER DEFAULT 1
        )
        """)

        defaults = {
            "M1": ("held", RANDY),
            "M2": ("held", KYLIE),
            "P1": ("open", None),
            "P2": ("open", None),
            "P3": ("open", None),
            "T1": ("held_group", "cinova"),
        }

        for k, v in defaults.items():
            if not conn.execute("SELECT 1 FROM spots WHERE id=?", (k,)).fetchone():
                conn.execute("INSERT INTO spots VALUES (?, ?, ?)", (k, v[0], v[1]))

        conn.commit()

init_db()

# ---------------- HELPERS ----------------
def now():
    return datetime.now(TZ)

def get_spots():
    return db().execute("SELECT * FROM spots").fetchall()

def set_spot(id, state, user=None):
    with closing(db()) as conn:
        conn.execute("UPDATE spots SET state=?, user=? WHERE id=?", (state, user, id))
        conn.commit()

def get_user_spot(uid):
    row = db().execute("SELECT id FROM spots WHERE user=? AND state='reserved'", (uid,)).fetchone()
    return row["id"] if row else None

def notify_enabled(uid):
    row = db().execute("SELECT notify FROM prefs WHERE user=?", (uid,)).fetchone()
    return True if not row else bool(row["notify"])

def toggle_notify(uid):
    with closing(db()) as conn:
        row = conn.execute("SELECT notify FROM prefs WHERE user=?", (uid,)).fetchone()
        new = 0 if row and row["notify"] else 1
        conn.execute("INSERT OR REPLACE INTO prefs VALUES (?,?)", (uid, new))
        conn.commit()
    return new

def all_users():
    users = {RANDY, KYLIE, MIKE, PETER}
    rows = db().execute("SELECT user FROM spots WHERE user IS NOT NULL").fetchall()
    for r in rows:
        users.add(r["user"])
    return list(users)

# ---------------- LOGIC ----------------
def reserve(uid):
    if get_user_spot(uid):
        return "You already have a spot."

    # Management claim
    if uid == RANDY:
        set_spot("M1", "reserved", uid)
        return "You have Spot M1 today."
    if uid == KYLIE:
        set_spot("M2", "reserved", uid)
        return "You have Spot M2 today."

    # Cinova
    if uid in [MIKE, PETER]:
        spot = db().execute("SELECT * FROM spots WHERE id='T1'").fetchone()
        if spot["state"] != "reserved":
            set_spot("T1", "reserved", uid)
            return "You have Spot T1 today."

    # Open spot
    for s in get_spots():
        if s["state"] == "open":
            set_spot(s["id"], "reserved", uid)
            return f"You have Spot {s['id']} today."

    return "No spots available."

def release(uid):
    spot = get_user_spot(uid)
    if not spot:
        return "No booking to release."

    set_spot(spot, "open")
    return f"Spot {spot} is now open."

# ---------------- UI ----------------
def render(uid):
    blocks = []

    my_spot = get_user_spot(uid)
    blocks.append({"type":"section","text":{"type":"mrkdwn",
        "text": f"You have Spot {my_spot} today." if my_spot else "No booking today"}})

    for s in get_spots():
        if s["state"] == "open":
            text = "Open"
        elif s["state"] == "reserved":
            text = f"Booked by <@{s['user']}>"
        elif s["state"] == "held":
            text = f"Held for <@{s['user']}>"
        else:
            text = "Held for Cinova users"

        blocks.append({"type":"section","text":{"type":"mrkdwn","text":f"*{s['id']}*\n{text}"}})

    blocks.append({
        "type":"actions",
        "elements":[
            {"type":"button","text":{"type":"plain_text","text":"Reserve"},"action_id":"reserve"},
            {"type":"button","text":{"type":"plain_text","text":"Release"},"action_id":"release"},
            {"type":"button","text":{"type":"plain_text","text":"Refresh"},"action_id":"refresh"},
            {"type":"button","text":{"type":"plain_text","text":"Toggle Notifications"},"action_id":"toggle"}
        ]
    })

    return blocks

def publish(uid):
    app.client.views_publish(user_id=uid, view={"type":"home","blocks":render(uid)})

def publish_all():
    for u in all_users():
        try:
            publish(u)
        except:
            pass

# ---------------- ACTIONS ----------------
@app.event("app_home_opened")
def home(event, logger):
    publish(event["user"])

@app.action("reserve")
def a_reserve(ack, body):
    ack()
    uid = body["user"]["id"]
    msg = reserve(uid)
    publish_all()
    if notify_enabled(uid):
        app.client.chat_postMessage(channel=uid, text=msg)

@app.action("release")
def a_release(ack, body):
    ack()
    uid = body["user"]["id"]
    msg = release(uid)
    publish_all()
    if notify_enabled(uid):
        app.client.chat_postMessage(channel=uid, text=msg)

@app.action("refresh")
def a_refresh(ack, body):
    ack()
    publish(body["user"]["id"])

@app.action("toggle")
def a_toggle(ack, body):
    ack()
    uid = body["user"]["id"]
    new = toggle_notify(uid)
    publish(uid)
    app.client.chat_postMessage(channel=uid, text="Notifications ON" if new else "Notifications OFF")

# ---------------- ROUTES ----------------
@api.post("/slack/events")
async def events(req: Request):
    data = await req.json()
    if data.get("type") == "url_verification":
        return JSONResponse({"challenge": data["challenge"]})
    return await handler.handle(req)

@api.get("/")
def root():
    return {"status":"running"}

# ---------------- SCHEDULER ----------------
def reset():
    set_spot("M1","held",RANDY)
    set_spot("M2","held",KYLIE)
    set_spot("P1","open")
    set_spot("P2","open")
    set_spot("P3","open")
    set_spot("T1","held_group","cinova")
    publish_all()

sched = BackgroundScheduler(timezone=TZ)
sched.add_job(reset,"cron",hour=17,minute=0)
sched.start()
