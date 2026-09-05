import importlib
import os
import sqlite3
import tempfile
import unittest
from datetime import date
from unittest.mock import patch
from zoneinfo import ZoneInfo


os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-test")
os.environ.setdefault("SLACK_SIGNING_SECRET", "test-secret")


class FakeSlackApp:
    def __init__(self, *args, **kwargs):
        self.client = object()

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


if __name__ == "__main__":
    unittest.main()
