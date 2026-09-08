import importlib
import os
import sqlite3
import tempfile
import unittest
from datetime import date
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo


os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-test")
os.environ.setdefault("SLACK_SIGNING_SECRET", "test-secret")


class FakeSlackApp:
    def __init__(self, *args, **kwargs):
        self.client = Mock()

    @staticmethod
    def _decorator(*args, **kwargs):
        return lambda function: function

    event = _decorator
    command = _decorator
    action = _decorator
    view = _decorator


with patch("slack_bolt.App", FakeSlackApp):
    parking = importlib.import_module("parking_bot_final")


class ReservationDaysTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        parking.DATABASE_PATH = os.path.join(self.temp_dir.name, "parking.db")
        parking.slack_app.client.reset_mock(return_value=True, side_effect=True)
        parking.USER_NAME_CACHE.clear()
        parking.init_db()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_init_keeps_legacy_table_and_adds_v2_table(self):
        with sqlite3.connect(parking.DATABASE_PATH) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }

        self.assertIn("reservations", tables)
        self.assertIn("reservation_days", tables)
        self.assertNotIn("user_prefs", tables)

    def test_init_removes_existing_notification_preferences(self):
        with sqlite3.connect(parking.DATABASE_PATH) as conn:
            conn.execute(
                "CREATE TABLE user_prefs "
                "(slack_user_id TEXT PRIMARY KEY, notifications_enabled INTEGER)"
            )
            conn.execute("INSERT INTO user_prefs VALUES ('U123', 0)")

        parking.init_db()

        with sqlite3.connect(parking.DATABASE_PATH) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'user_prefs'"
            ).fetchone()

        self.assertIsNone(table)

    def test_new_table_does_not_change_legacy_state(self):
        original = parking.get_spot(parking.M1)

        parking.set_reservation_day(
            "2026-09-08",
            parking.M1,
            "reserved",
            user_id="U-FUTURE",
        )

        current = parking.get_spot(parking.M1)
        self.assertEqual(original, current)

    def test_upsert_and_read_reservation_day(self):
        parking.set_reservation_day(
            date(2026, 9, 8),
            parking.P1,
            "reserved",
            user_id="U123",
        )
        parking.set_reservation_day("2026-09-08", parking.P1, "open")

        record = parking.get_reservation_day("2026-09-08", parking.P1)
        self.assertEqual("open", record.status)
        self.assertIsNone(record.user_id)

    def test_day_list_uses_display_order(self):
        parking.set_reservation_day("2026-09-08", parking.T1, "held_group")
        parking.set_reservation_day("2026-09-08", parking.M1, "open")
        parking.set_reservation_day("2026-09-08", parking.P2, "open")

        records = parking.get_reservation_days("2026-09-08")
        self.assertEqual([parking.M1, parking.P2, parking.T1], [r.spot_id for r in records])

    def test_rejects_invalid_reservation_data(self):
        with self.assertRaises(ValueError):
            parking.set_reservation_day("2026-09-08", "UNKNOWN", "open")

        with self.assertRaises(ValueError):
            parking.set_reservation_day("2026-09-08", parking.P1, "reserved")

        with self.assertRaises(ValueError):
            parking.set_reservation_day(
                "2026-09-08", parking.P1, "open", user_id="U123"
            )

    def test_friday_reset_opens_management_spots(self):
        friday = parking.datetime(2026, 9, 4, 17, 0, tzinfo=ZoneInfo("America/Vancouver"))

        with patch.object(parking, "local_now", return_value=friday):
            parking.reset_for_5pm()

        self.assertEqual("open", parking.get_spot(parking.M1).state)
        self.assertEqual("open", parking.get_spot(parking.M2).state)

    def test_weekend_migration_opens_defaults_only_once(self):
        saturday = parking.datetime(2026, 9, 5, 9, 0, tzinfo=ZoneInfo("America/Vancouver"))

        with patch.object(parking, "local_now", return_value=saturday):
            parking.apply_v2_weekend_migration()

        self.assertEqual("open", parking.get_spot(parking.M1).state)
        self.assertEqual("open", parking.get_spot(parking.M2).state)

        parking.set_spot_state(parking.M1, "reserved", reserved_for_user_id="U-WEEKEND")

        with patch.object(parking, "local_now", return_value=saturday):
            parking.apply_v2_weekend_migration()

        self.assertEqual("U-WEEKEND", parking.get_spot(parking.M1).reserved_for_user_id)

    def test_t1_control_never_assigns_a_user(self):
        parking.set_t1_control("held", "Held for production crew")
        t1 = parking.get_spot(parking.T1)

        self.assertEqual("held_group", t1.state)
        self.assertIsNone(t1.reserved_for_user_id)
        self.assertEqual("Held for production crew", parking.t1_held_message())

        parking.set_t1_control("open")
        self.assertEqual("open", parking.get_spot(parking.T1).state)

    def test_t1_is_not_reservable(self):
        parking.set_t1_control("open")
        self.assertFalse(parking.spot_available_to_user(parking.get_spot(parking.T1), parking.RANDY_ID))

    def test_t1_management_button_is_management_only(self):
        manager_blocks = parking.parking_home_blocks(parking.RANDY_ID)
        staff_blocks = parking.parking_home_blocks("U-STAFF")

        def action_ids(blocks):
            return {
                element.get("action_id")
                for block in blocks
                for element in block.get("elements", [])
            }

        self.assertIn("manage_t1", action_ids(manager_blocks))
        self.assertNotIn("manage_t1", action_ids(staff_blocks))

    def test_button_rows_do_not_overflow(self):
        parking.set_spot_state(parking.M1, "open")
        parking.set_spot_state(parking.M2, "open")
        blocks = parking.parking_home_blocks(parking.RANDY_ID)
        action_blocks = [block for block in blocks if block["type"] == "actions"]

        self.assertTrue(action_blocks)
        self.assertTrue(all(len(block["elements"]) <= 5 for block in action_blocks))

        reserve_rows = [
            block for block in action_blocks
            if any(
                element.get("action_id", "").startswith("reserve_spot_")
                for element in block["elements"]
            )
        ]
        self.assertEqual(1, len(reserve_rows))
        self.assertEqual(5, len(reserve_rows[0]["elements"]))

    def test_reservations_use_hot_buttons_not_dropdown(self):
        blocks = parking.parking_home_blocks("U-STAFF")
        elements = [
            element
            for block in blocks
            for element in block.get("elements", [])
        ]

        self.assertTrue(any(e.get("action_id", "").startswith("reserve_spot_") for e in elements))
        self.assertFalse(any(e.get("type") == "static_select" for e in elements))

        action_ids = [
            e["action_id"] for e in elements if e.get("action_id", "").startswith("reserve_spot_")
        ]
        self.assertEqual(len(action_ids), len(set(action_ids)))

    def test_home_has_no_notification_controls_or_status(self):
        blocks = parking.parking_home_blocks("U-STAFF")
        self.assertNotIn("Notifications:", str(blocks))
        self.assertNotIn("toggle_notifications", str(blocks))
        self.assertNotIn("Turn on notifications", str(blocks))
        self.assertNotIn("Turn off notifications", str(blocks))

    def test_reserve_and_release_workflow(self):
        user_id = "U-STAFF"
        message = parking.reserve_for_user(user_id, parking.P1)
        self.assertIn("You have Spot", message)
        self.assertEqual(parking.P1, parking.get_user_booked_spot(user_id))

        message = parking.release_for_user(user_id)
        self.assertIn("is now open", message)
        self.assertIsNone(parking.get_user_booked_spot(user_id))

    def test_away_dates_affect_home_and_management_spot(self):
        today = date.today().isoformat()
        parking.set_user_away(parking.RANDY_ID, today, today)

        with patch.object(parking, "parking_date", return_value=today):
            self.assertTrue(parking.user_is_away(parking.RANDY_ID))
            blocks = parking.parking_home_blocks(parking.RANDY_ID)

        self.assertIn("Away dates set", str(blocks))

    def test_home_publish_and_live_board_update(self):
        parking.slack_app.client.reset_mock()
        parking.publish_home("U-STAFF")
        parking.slack_app.client.views_publish.assert_called_once()

        parking.slack_app.client.reset_mock()
        with patch.object(parking, "PARKING_CHANNEL_ID", "C-PARKING"), patch.object(
            parking, "load_board_ts", return_value="123.456"
        ):
            parking.update_parking_board()

        parking.slack_app.client.chat_update.assert_called_once()

    def test_send_dm_always_attempts_delivery(self):
        parking.slack_app.client.reset_mock()
        parking.send_dm("U123", "Parking updated")
        parking.slack_app.client.chat_postMessage.assert_called_once_with(
            channel="U123", text="Parking updated"
        )

    def test_status_uses_green_circle_and_plain_names(self):
        open_status = parking.display_status_for_spot(parking.get_spot(parking.P1))
        self.assertEqual("🟢 Open", open_status)

        parking.set_spot_state(
            parking.M1, "reserved", reserved_for_user_id=parking.RANDY_ID
        )
        booked_status = parking.display_status_for_spot(parking.get_spot(parking.M1))
        self.assertEqual("🔴 Booked by Randy", booked_status)
        self.assertNotIn("@", booked_status)

        blocks = parking.parking_home_blocks(parking.RANDY_ID)
        booked_rows = [
            block["text"]
            for block in blocks
            if block.get("type") == "section"
            and "Booked by Randy" in block.get("text", {}).get("text", "")
        ]
        self.assertEqual("plain_text", booked_rows[0]["type"])

    def test_configured_management_names_do_not_call_slack(self):
        parking.slack_app.client.reset_mock()

        self.assertEqual("Randy", parking.display_name_for_user(parking.RANDY_ID))
        self.assertEqual("Kylie", parking.display_name_for_user(parking.KYLIE_ID))
        parking.slack_app.client.users_info.assert_not_called()

    def test_slack_first_name_is_shared_by_board_and_home(self):
        parking.slack_app.client.reset_mock()
        parking.slack_app.client.users_info.return_value = {
            "user": {
                "name": "pat.lee",
                "profile": {
                    "first_name": "Pat",
                    "real_name_normalized": "Pat Lee",
                },
            }
        }
        parking.set_spot_state(parking.P1, "reserved", reserved_for_user_id="U-PAT")
        spot = parking.get_spot(parking.P1)

        self.assertEqual("P1-#08  🔴 Booked by Pat", parking.board_line_for_spot(spot))
        self.assertEqual("🔴 Booked by Pat", parking.display_status_for_spot(spot))
        parking.slack_app.client.users_info.assert_called_once_with(user="U-PAT")

    def test_name_lookup_fallback_uses_slack_rendered_name(self):
        parking.slack_app.client.reset_mock()
        parking.slack_app.client.users_info.side_effect = RuntimeError("Slack unavailable")
        parking.set_spot_state(parking.P1, "reserved", reserved_for_user_id="U-SECRET")

        status = parking.display_status_for_spot(parking.get_spot(parking.P1))

        self.assertEqual("🔴 Booked by <@U-SECRET>", status)
        self.assertNotIn("U-SECRET", parking.USER_NAME_CACHE)

        blocks = parking.parking_home_blocks(parking.RANDY_ID)
        fallback_row = next(
            block for block in blocks
            if block.get("type") == "section"
            and "<@U-SECRET>" in block.get("text", {}).get("text", "")
        )
        self.assertEqual("mrkdwn", fallback_row["text"]["type"])


if __name__ == "__main__":
    unittest.main()
