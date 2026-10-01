import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402


class IncidentMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "merge.db"
        self.service = MaritimeSARService(self.db_path)
        self.primary = self.service.create_incident(
            "coord-a", "coordinator", "PRI-001", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.duplicate = self.service.create_incident(
            "coord-b", "coordinator", "DUP-001", "海燕号", 35.00, 126.00, 5.0, 2, "南海中心"
        )
        self.good_asset = self.service.add_asset(
            "coord-b", "coordinator", "海巡01", "vessel", ["surface"], 31.01, 122.01, 20, 100, 5
        )
        self.weak_asset = self.service.add_asset(
            "coord-b", "coordinator", "近岸艇", "vessel", ["surface"], 31.01, 122.01, 20, 100, 2
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _state(self):
        return self.service.state()

    def test_merge_continues_compatible_work_review_conflicts_and_preserves_primary(self):
        good_area = self.service.create_search_area(
            "coord-b", "coordinator", self.duplicate["id"], "DUP-A", "surface", 31.02, 122.02, 5, 1
        )
        bad_area = self.service.create_search_area(
            "coord-b", "coordinator", self.duplicate["id"], "DUP-B", "surface", 31.03, 122.03, 5, 2
        )
        good_assigned = self.service.assign_area(
            "coord-b", "coordinator", good_area["id"], self.good_asset["id"], self.good_asset["version"]
        )
        self.service.assign_area(
            "coord-b", "coordinator", bad_area["id"], self.weak_asset["id"], self.weak_asset["version"]
        )
        primary_clue = self.service.record_clue(
            "field1", "field", self.primary["id"], "pri-clue", 31.0, 122.0, 0.8, "radio"
        )
        good_clue = self.service.record_clue(
            "field1", "field", self.duplicate["id"], "good-clue", 31.02, 122.02, 0.7, "visual", good_area["id"]
        )
        held_clue = self.service.record_clue(
            "field2", "field", self.duplicate["id"], "held-clue", 31.03, 122.03, 0.6, "visual", bad_area["id"]
        )
        free_clue = self.service.record_clue(
            "field2", "field", self.duplicate["id"], "free-clue", 31.04, 122.04, 0.6, "radio"
        )

        primary_before = dict(self.primary)
        result = self.service.merge_duplicate_incident(
            "coord-a", "coordinator", self.primary["id"], self.duplicate["id"], "merge-request-1",
            expected_primary_version=self.primary["version"],
            expected_duplicate_version=self.duplicate["version"],
            note="同一艘船重复报警",
        )

        self.assertEqual("merged", result["status"])
        state = self._state()
        primary_after = next(x for x in state["incidents"] if x["id"] == self.primary["id"])
        duplicate_after = next(x for x in state["incidents"] if x["id"] == self.duplicate["id"])
        self.assertEqual(primary_before, {k: primary_after[k] for k in primary_before})
        self.assertEqual("duplicate", duplicate_after["status"])
        self.assertEqual(self.primary["id"], duplicate_after["duplicate_of"])
        self.assertEqual(self.duplicate["version"] + 1, duplicate_after["version"])

        good_area_after = next(x for x in state["search_areas"] if x["id"] == good_area["id"])
        bad_area_after = next(x for x in state["search_areas"] if x["id"] == bad_area["id"])
        self.assertEqual(self.primary["id"], good_area_after["incident_id"])
        self.assertEqual("assigned", good_area_after["status"])
        self.assertEqual(self.good_asset["id"], good_area_after["assigned_asset_id"])
        self.assertEqual(good_assigned["version"] + 1, good_area_after["version"])
        self.assertEqual(self.duplicate["id"], bad_area_after["incident_id"])
        self.assertEqual("review_required", bad_area_after["status"])
        self.assertIsNone(bad_area_after["assigned_asset_id"])
        self.assertEqual("available", next(x for x in state["assets"] if x["id"] == self.weak_asset["id"])["status"])
        self.assertEqual("assigned", next(x for x in state["assets"] if x["id"] == self.good_asset["id"])["status"])

        clues = {x["id"]: x for x in state["clues"]}
        self.assertEqual(self.primary["id"], clues[good_clue["id"]]["incident_id"])
        self.assertEqual(self.primary["id"], clues[free_clue["id"]]["incident_id"])
        self.assertEqual(self.duplicate["id"], clues[held_clue["id"]]["incident_id"])
        self.assertEqual(self.primary["id"], clues[primary_clue["id"]]["incident_id"])
        self.assertIn("incident_merges", state)
        self.assertEqual(1, len(result["conflicts"]))
        self.assertEqual("asset.sea_state", result["conflicts"][0]["code"])

        with self.assertRaises(DomainError) as ctx:
            self.service.merge_duplicate_incident(
                "coord-b", "coordinator", self.primary["id"], self.duplicate["id"], "merge-request-2"
            )
        self.assertEqual(409, ctx.exception.status)

        # After undo, ownership and recorded versions return to their pre-merge snapshot.
        undone = self.service.undo_incident_merge(
            "coord-a", "coordinator", result["id"], reason="确认是两起事件"
        )
        self.assertEqual("undone", undone["status"])
        state = self._state()
        duplicate_after_undo = next(x for x in state["incidents"] if x["id"] == self.duplicate["id"])
        primary_after_undo = next(x for x in state["incidents"] if x["id"] == self.primary["id"])
        self.assertEqual(primary_before, {k: primary_after_undo[k] for k in primary_before})
        self.assertEqual("reported", duplicate_after_undo["status"])
        self.assertIsNone(duplicate_after_undo["duplicate_of"])
        self.assertEqual(self.duplicate["version"], duplicate_after_undo["version"])
        good_area_after_undo = next(x for x in state["search_areas"] if x["id"] == good_area["id"])
        bad_area_after_undo = next(x for x in state["search_areas"] if x["id"] == bad_area["id"])
        self.assertEqual(self.duplicate["id"], good_area_after_undo["incident_id"])
        self.assertEqual(good_assigned["version"], good_area_after_undo["version"])
        self.assertEqual("assigned", bad_area_after_undo["status"])
        self.assertEqual(self.weak_asset["id"], bad_area_after_undo["assigned_asset_id"])
        self.assertEqual("assigned", next(x for x in state["assets"] if x["id"] == self.weak_asset["id"])["status"])
        clues = {x["id"]: x for x in state["clues"]}
        self.assertEqual(self.duplicate["id"], clues[good_clue["id"]]["incident_id"])
        self.assertEqual(self.duplicate["id"], clues[free_clue["id"]]["incident_id"])
        self.assertEqual(bad_area["id"], clues[held_clue["id"]]["area_id"])

    def test_merge_request_is_idempotent_and_does_not_reassign_assets(self):
        area = self.service.create_search_area(
            "coord-b", "coordinator", self.duplicate["id"], "DUP-IDEMP", "surface", 31.02, 122.02, 5
        )
        assigned = self.service.assign_area(
            "coord-b", "coordinator", area["id"], self.good_asset["id"], self.good_asset["version"]
        )
        first = self.service.merge_duplicate_incident(
            "coord-a", "coordinator", self.primary["id"], self.duplicate["id"], "same-request-id"
        )
        second = self.service.merge_duplicate_incident(
            "coord-a", "coordinator", self.primary["id"], self.duplicate["id"], "same-request-id"
        )
        self.assertEqual(first["id"], second["id"])
        state = self._state()
        area_after = next(x for x in state["search_areas"] if x["id"] == area["id"])
        asset_after = next(x for x in state["assets"] if x["id"] == self.good_asset["id"])
        self.assertEqual(assigned["version"] + 1, area_after["version"])
        self.assertEqual(assigned["version"], asset_after["version"])

    def test_concurrent_merge_of_same_duplicate_allows_only_one_commit(self):
        barrier = threading.Barrier(2)

        def merge(merge_id):
            barrier.wait()
            try:
                return ("ok", self.service.merge_duplicate_incident(
                    "coord-a", "coordinator", self.primary["id"], self.duplicate["id"], merge_id
                ))
            except DomainError as exc:
                return ("conflict", exc.status)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(merge, ["race-a", "race-b"]))

        statuses = sorted(status for status, _ in results)
        self.assertEqual(["conflict", "ok"], statuses)
        successful = [payload for status, payload in results if status == "ok"][0]
        self.assertEqual(409, next(payload for status, payload in results if status == "conflict"))
        merges = [m for m in self._state()["incident_merges"] if m["duplicate_incident_id"] == self.duplicate["id"]]
        self.assertEqual([successful["id"]], [m["id"] for m in merges if m["status"] == "merged"])

    def test_undo_refuses_when_released_asset_was_reassigned(self):
        area = self.service.create_search_area(
            "coord-b", "coordinator", self.duplicate["id"], "DUP-CONFLICT", "surface", 31.03, 122.03, 5
        )
        self.service.assign_area(
            "coord-b", "coordinator", area["id"], self.weak_asset["id"], self.weak_asset["version"]
        )
        merged = self.service.merge_duplicate_incident(
            "coord-a", "coordinator", self.primary["id"], self.duplicate["id"], "reassign-case"
        )

        # The conflicting asset is released, then dispatched to another compatible area. Undo must not steal it.
        low_sea_incident = self.service.create_incident(
            "coord-c", "coordinator", "LOW-001", "近岸事件", 31.04, 122.04, 5.0, 2, "近岸中心"
        )
        another_area = self.service.create_search_area(
            "coord-c", "coordinator", low_sea_incident["id"], "LOW-NEW", "surface", 31.04, 122.04, 5
        )
        self.service.assign_area(
            "coord-c", "coordinator", another_area["id"], self.weak_asset["id"], self.weak_asset["version"] + 2
        )
        with self.assertRaises(DomainError) as ctx:
            self.service.undo_incident_merge("coord-a", "coordinator", merged["id"])
        self.assertEqual(409, ctx.exception.status)
        duplicate_after = next(x for x in self._state()["incidents"] if x["id"] == self.duplicate["id"])
        self.assertEqual("duplicate", duplicate_after["status"])

    def test_legacy_duplicate_data_remains_traceable_after_schema_upgrade(self):
        self.tmp.cleanup()
        self.tmp = tempfile.TemporaryDirectory()
        db_path = Path(self.tmp.name) / "legacy.db"
        with sqlite3.connect(db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE incidents(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, vessel_name TEXT, description TEXT DEFAULT '',
                    latitude REAL, longitude REAL, uncertainty_km REAL, drift_direction REAL DEFAULT 0,
                    drift_speed_kn REAL DEFAULT 0, sea_state INTEGER, status TEXT, lead_org TEXT,
                    duplicate_of INTEGER, version INTEGER DEFAULT 1, created_by TEXT, created_at TEXT, updated_at TEXT
                );
                INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,status,lead_org,version,created_by,created_at,updated_at)
                VALUES ('OLD-1','老船',31,122,5,3,'reported','中心',1,'old','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00');
                INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,status,lead_org,duplicate_of,version,created_by,created_at,updated_at)
                VALUES ('OLD-2','老船',31.01,122.01,5,3,'duplicate','中心',1,1,'old','2026-01-01T00:01:00+00:00','2026-01-01T00:01:00+00:00');
                """
            )
        upgraded = MaritimeSARService(db_path)
        state = upgraded.state()
        legacy = next(m for m in state["incident_merges"] if m["duplicate_incident_id"] == 2)
        self.assertEqual("legacy", legacy["status"])
        self.assertFalse(legacy["reversible"])
        self.assertEqual(2, legacy["pre_merge_snapshot"]["incident"]["id"])
        self.assertEqual(1, legacy["primary_incident_id"])
        self.assertEqual("historical", legacy["conflicts"][0]["status"])
        with self.assertRaises(DomainError) as ctx:
            upgraded.undo_incident_merge("coord-a", "coordinator", legacy["id"])
        self.assertEqual(409, ctx.exception.status)


if __name__ == "__main__":
    unittest.main()
