import os
import sqlite3
from fastapi import FastAPI, Request, Response
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------
# ENV VARIABLES (Railway)
# ---------------------------
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")

# ---------------------------
# SLACK APP
# ---------------------------
slack_app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
handler = SlackRequestHandler(slack_app)

# ---------------------------
# FASTAPI
# ---------------------------
api = FastAPI()

# ---------------------------
# DATABASE
# ---------------------------
DB = "parking.db"

def get_db():
    return sqlite3.connect(DB)

def init_db():
    conn = get_db()
    cur = conn.cursor()

    # spots
    cur.execute("""
    CREATE TABLE IF NOT EXISTS spots (
        name TEXT PRIMARY KEY,
        status TEXT,
        user_id TEXT
    )
    """)

    # users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        name TEXT
    )
    """)

    # settings (prevents your earlier crash)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    conn.commit()

    # Initialize spots
    spots = ["M1", "M2", "P1", "P2", "P3", "T1"]
    for s in spots:
        cur.execute(
            "INSERT OR IGNORE INTO spots (name, status, user_id) VALUES (?, 'Open', NULL)",
            (s,)
        )

    conn.commit()
    conn.close()

# ---------------------------
# USER FRIENDLY NAMES
# ---------------------------
USER_NAMES = {
    "UR0JZ0GR0": "Kylie",
    "U03EH8HM4G0": "Mike",
    "U03DVSASKPE": "Peter",
}

def get_name(user_id):
    return USER_NAMES.get(user_id, user_id)

# ---------------------------
# RESET LOGIC (5PM)
# ---------------------------
def reset_spots():
    conn = get_db()
    cur = conn.cursor()

    print("Running 5PM reset...")

    # P spots open
    for p in ["P1", "P2", "P3"]:
        cur.execute("UPDATE spots SET status='Open', user_id=NULL WHERE name=?", (p,))

    # M spots reserved for management (Kylie example)
    cur.execute("UPDATE spots SET status='Held', user_id=? WHERE name='M1'", ("UR0JZ0GR0",))
    cur.execute("UPDATE spots SET status='Held', user_id=? WHERE name='M2'", ("UR0JZ0GR0",))

    # T1 Cinova (Mike + Peter)
    cur.execute("UPDATE spots SET status='Held', user_id=? WHERE name='T1'", ("U03EH8HM4G0",))

    conn.commit()
    conn.close()

scheduler = BackgroundScheduler()
scheduler.add_job(reset_spots, "cron", hour=17, minute=0)
scheduler.start()

# ---------------------------
# HOME (so Railway doesn't show error page)
# ---------------------------
@api.get("/")
def home():
    return {"status": "Parking bot running"}

# ---------------------------
# SLACK EVENTS (CRITICAL FIX)
# ---------------------------
@api.post("/slack/events")
async def slack_events(req: Request):
    payload = await req.json()

    # Slack verification FIX
    if payload.get("type") == "url_verification":
        return Response(
            content=payload["challenge"],
            media_type="text/plain",
            status_code=200,
        )

    return await handler.handle(req)

# ---------------------------
# HELPER: BUILD UI TEXT
# ---------------------------
def build_home_view(user_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT name, status, user_id FROM spots")
    rows = cur.fetchall()

    text = ""
    user_spot = None

    for name, status, uid in rows:
        if uid == user_id:
            user_spot = name

        if status == "Open":
            text += f"{name} - Open\n"
        else:
            text += f"{name} - Held for {get_name(uid)}\n"

    header = f"You have Spot {user_spot} today\n\n" if user_spot else ""

    conn.close()
    return header + text

# ---------------------------
# SLACK HOME TAB
# ---------------------------
@slack_app.event("app_home_opened")
def update_home(client, event):
    user_id = event["user"]

    client.views_publish(
        user_id=user_id,
        view={
            "type": "home",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": build_home_view(user_id)}},
                {
                    "type": "actions",
                    "elements": [
                        {"type": "button", "text": {"type": "plain_text", "text": "Reserve M1"}, "action_id": "reserve_m1"},
                        {"type": "button", "text": {"type": "plain_text", "text": "Cancel"}, "action_id": "cancel"},
                        {"type": "button", "text": {"type": "plain_text", "text": "Refresh"}, "action_id": "refresh"},
                    ],
                },
            ],
        },
    )

# ---------------------------
# RESERVE
# ---------------------------
@slack_app.action("reserve_m1")
def reserve(ack, body, client):
    ack()
    user_id = body["user"]["id"]

    conn = get_db()
    cur = conn.cursor()

    cur.execute("UPDATE spots SET status='Held', user_id=? WHERE name='M1'", (user_id,))
    conn.commit()
    conn.close()

    update_home(client, {"user": user_id})

# ---------------------------
# CANCEL
# ---------------------------
@slack_app.action("cancel")
def cancel(ack, body, client):
    ack()
    user_id = body["user"]["id"]

    conn = get_db()
    cur = conn.cursor()

    cur.execute("UPDATE spots SET status='Open', user_id=NULL WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

    update_home(client, {"user": user_id})

# ---------------------------
# REFRESH
# ---------------------------
@slack_app.action("refresh")
def refresh(ack, body, client):
    ack()
    update_home(client, {"user": body["user"]["id"]})

# ---------------------------
# INIT DB ON START
# ---------------------------
init_db()
