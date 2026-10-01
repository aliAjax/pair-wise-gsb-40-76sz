import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402


class MergeTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "test.db"
        self.svc = MaritimeSARService(self.db)
        self.primary = self.svc.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.secondary = self.svc.create_incident(
            "coord2", "coordinator", "SAR-200", "海燕二号", 31.02, 122.02, 8.0, 1, "东海中心"
        )
        self.good_asset = self.svc.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )
        self.weak_asset = self.svc.add_asset(
            "coord2", "coordinator", "海巡02", "vessel", ["surface"], 31.0, 122.0, 18, 100, 2
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _secondary_setup(self):
        """主事件上布置有效接续；从事件上布置一个待复核区域和两条线索。"""
        # 已指派给海巡01的区域：能力/航程/海况均满足主事件 → 接续后继续占着同一条船
        ok_area = self.svc.create_search_area(
            "coord2", "coordinator", self.secondary["id"], "AREA-OK", "surface", 31.05, 122.05, 5, 1
        )
        self.svc.assign_area("coord2", "coordinator", ok_area["id"], self.good_asset["id"], self.good_asset["version"])
        # 海巡02 只扛得住海况 2，主事件海况 3 → 待复核，且不释放船
        bad_area = self.svc.create_search_area(
            "coord2", "coordinator", self.secondary["id"], "AREA-BAD", "surface", 31.1, 122.1, 5, 2
        )
        self.svc.assign_area("coord2", "coordinator", bad_area["id"], self.weak_asset["id"], self.weak_asset["version"])
        # 未指派区域：直接接续
        self.svc.create_search_area(
            "coord2", "coordinator", self.secondary["id"], "AREA-PLAN", "surface", 31.06, 122.06, 5, 3
        )
        near_clue = self.svc.record_clue(
            "field2", "field", self.secondary["id"], "evt-near", 31.03, 122.03, 0.8, "visual"
        )
        far_clue = self.svc.record_clue(
            "field2", "field", self.secondary["id"], "evt-far", 32.0, 123.0, 0.6, "radio"
        )
        return ok_area, bad_area, near_clue, far_clue


class RevocableMergeTest(MergeTestBase):
    def test_merge_continues_by_capability_and_range_without_changing_primary(self):
        ok_area, bad_area, near_clue, far_clue = self._secondary_setup()
        before = {k: self.primary[k] for k in self.primary.keys()}
        good_version_after_assign = self.svc.list_assets()[0]["version"]

        payload = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            self.secondary["version"], "merge-key-1", "同一船报警",
        )
        merge = payload["merge"]
        self.assertEqual("applied", merge["status"])
        self.assertEqual("MRG-", merge["code"][:4])

        dispositions = {(i["item_type"], i["item_id"]): i["disposition"] for i in payload["items"]}
        self.assertEqual("continued", dispositions[("area", ok_area["id"])])
        self.assertEqual("review", dispositions[("area", bad_area["id"])])
        self.assertEqual("continued", dispositions[("clue", near_clue["id"])])
        self.assertEqual("review", dispositions[("clue", far_clue["id"])])

        state = self.svc.state()
        by_id_area = {a["id"]: a for a in state["search_areas"]}
        self.assertEqual(self.primary["id"], by_id_area[ok_area["id"]]["incident_id"])
        self.assertEqual(self.primary["id"], by_id_area[bad_area["id"]]["incident_id"])
        self.assertEqual("review", by_id_area[bad_area["id"]]["status"])
        # 船仍由原区域占着：接续不重复派船，也不释放冲突船
        self.assertEqual(self.good_asset["id"], by_id_area[ok_area["id"]]["assigned_asset_id"])
        self.assertEqual(self.weak_asset["id"], by_id_area[bad_area["id"]]["assigned_asset_id"])
        assets = {a["id"]: a for a in self.svc.list_assets()}
        self.assertEqual("assigned", assets[self.good_asset["id"]]["status"])
        self.assertEqual("assigned", assets[self.weak_asset["id"]]["status"])
        self.assertEqual(good_version_after_assign, assets[self.good_asset["id"]]["version"])

        by_id_clue = {c["id"]: c for c in state["clues"]}
        self.assertEqual(self.primary["id"], by_id_clue[near_clue["id"]]["incident_id"])
        self.assertEqual(self.primary["id"], by_id_clue[far_clue["id"]]["incident_id"])

        # 从事件冻结，主事件一个字段都没变（含版本）
        secondary_now = next(i for i in state["incidents"] if i["id"] == self.secondary["id"])
        self.assertEqual("duplicate", secondary_now["status"])
        self.assertEqual(self.primary["id"], secondary_now["duplicate_of"])
        primary_now = next(i for i in state["incidents"] if i["id"] == self.primary["id"])
        self.assertEqual(before, {k: primary_now[k] for k in before.keys()})

        # 前后关系在时间线和归并台账中可追溯
        actions = {e["action"] for e in self.svc.incident_timeline(self.secondary["id"])}
        self.assertIn("merge.started", actions)
        self.assertIn("merge.applied", actions)
        self.assertEqual(1, len(state["merges"]))

    def test_concurrent_merge_of_same_secondary_only_one_wins(self):
        self._secondary_setup()
        results: list[object] = []
        errors: list[DomainError] = []

        def run(key: str) -> None:
            try:
                results.append(self.svc.merge_incidents(
                    "coord1", "coordinator", self.primary["id"], self.secondary["id"], 1, key,
                ))
            except DomainError as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        t1 = threading.Thread(target=run, args=("key-conc-1",))
        t2 = threading.Thread(target=run, args=("key-conc-2",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(1, len(results))
        self.assertEqual(1, len(errors))
        self.assertEqual(409, errors[0].status)
        merges = self.svc.list_merges()["merges"]
        self.assertEqual(1, len(merges))

    def test_idempotent_retry_returns_same_merge_without_double_moves(self):
        ok_area, _, _, _ = self._secondary_setup()
        first = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            self.secondary["version"], "idem-key-1",
        )
        area_version_after_merge = next(
            a for a in self.svc.state()["search_areas"] if a["id"] == ok_area["id"]
        )["version"]
        # 过期版本 + 同幂等键：直接返回原归并，不报版本冲突
        again = self.svc.merge_incidents(
            "coord2", "coordinator", self.primary["id"], self.secondary["id"], 1, "idem-key-1",
        )
        self.assertEqual(first["merge"]["id"], again["merge"]["id"])
        self.assertEqual("applied", again["merge"]["status"])
        resumed = self.svc.resume_merge("coord1", "coordinator", first["merge"]["id"], "idem-key-1")
        self.assertEqual("applied", resumed["merge"]["status"])
        area_now = next(a for a in self.svc.state()["search_areas"] if a["id"] == ok_area["id"])
        self.assertEqual(area_version_after_merge, area_now["version"])
        self.assertEqual(1, len(self.svc.list_merges()["merges"]))

    def test_processing_merge_can_resume_without_double_boat_occupation(self):
        ok_area, bad_area, _, _ = self._secondary_setup()
        # 模拟处理步骤在归属迁移前崩溃：归并单 processing、项目 pending、区域还在从事件
        merge = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            self.secondary["version"], "crash-key",
        )
        merge_id = merge["merge"]["id"]
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE incident_merges SET status='processing',applied_at=NULL WHERE id=?", (merge_id,))
        conn.execute("UPDATE merge_items SET disposition='pending' WHERE merge_id=?", (merge_id,))
        conn.execute("UPDATE search_areas SET incident_id=?,version=version-1,status=CASE id WHEN ? THEN 'assigned' ELSE status END WHERE incident_id=?",
                     (self.secondary["id"], bad_area["id"], self.primary["id"]))
        conn.commit(); conn.close()

        weak_before = next(a for a in self.svc.list_assets() if a["id"] == self.weak_asset["id"])
        resumed = self.svc.resume_merge("coord1", "coordinator", merge_id, "crash-key")
        self.assertEqual("applied", resumed["merge"]["status"])
        areas = {a["id"]: a for a in self.svc.state()["search_areas"]}
        self.assertEqual(self.primary["id"], areas[ok_area["id"]]["incident_id"])
        self.assertEqual("review", areas[bad_area["id"]]["status"])
        # 船没有被二次占用：状态/版本与崩溃前一致
        weak_after = next(a for a in self.svc.list_assets() if a["id"] == self.weak_asset["id"])
        self.assertEqual(weak_before["version"], weak_after["version"])
        self.assertEqual("assigned", weak_after["status"])

    def test_revoke_restores_ownership_and_versions(self):
        ok_area, bad_area, near_clue, far_clue = self._secondary_setup()
        sec_version_before = self.secondary["version"]
        area_prev = {a["id"]: dict(a) for a in self.svc.state()["search_areas"]
                     if a["incident_id"] == self.secondary["id"]}
        clue_prev = {c["id"]: dict(c) for c in self.svc.state()["clues"]
                     if c["incident_id"] == self.secondary["id"]}

        merge = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            sec_version_before, "revoke-key",
        )
        # 归并后人为改动过一个接续区域：撤销时保留现状
        plan_area = next(a for a in self.svc.state()["search_areas"] if a["code"] == "AREA-PLAN")
        self.svc.complete_area("coord1", "coordinator", plan_area["id"], "abandoned",
                               next(a for a in self.svc.state()["search_areas"] if a["id"] == plan_area["id"])["version"])

        revoked = self.svc.revoke_merge("coord1", "coordinator", merge["merge"]["id"], "误判重复", "revoke-key")
        self.assertEqual("revoked", revoked["merge"]["status"])
        self.assertEqual("误判重复", revoked["merge"]["revoke_reason"])

        state = self.svc.state()
        areas = {a["id"]: a for a in state["search_areas"]}
        for area_id, prev in area_prev.items():
            cur = areas[area_id]
            if area_id == plan_area["id"]:
                self.assertEqual(self.primary["id"], cur["incident_id"])
                self.assertEqual("abandoned", cur["status"])
                item = next(i for i in revoked["items"] if i["item_type"] == "area" and i["item_id"] == area_id)
                self.assertTrue(item["revoke_outcome"].startswith("kept"))
            else:
                self.assertEqual(prev["incident_id"], cur["incident_id"])
                self.assertEqual(prev["status"], cur["status"])
                self.assertEqual(prev["version"], cur["version"])
                self.assertEqual(prev["assigned_asset_id"], cur["assigned_asset_id"])
        clues = {c["id"]: c for c in state["clues"]}
        for clue_id, prev in clue_prev.items():
            self.assertEqual(prev["incident_id"], clues[clue_id]["incident_id"])
            self.assertEqual(prev["status"], clues[clue_id]["status"])
            self.assertEqual(prev["version"], clues[clue_id]["version"])
            self.assertEqual(prev["area_id"], clues[clue_id]["area_id"])
        secondary_now = next(i for i in state["incidents"] if i["id"] == self.secondary["id"])
        self.assertEqual("reported", secondary_now["status"])
        self.assertIsNone(secondary_now["duplicate_of"])
        self.assertEqual(sec_version_before, secondary_now["version"])

        # 撤销幂等：再撤一次返回同一单
        again = self.svc.revoke_merge("coord1", "coordinator", merge["merge"]["id"], "误判重复")
        self.assertEqual("revoked", again["merge"]["status"])
        # 撤销后允许重新归并
        remerge = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], secondary_now["id"],
            secondary_now["version"], "remerge-key",
        )
        self.assertEqual("applied", remerge["merge"]["status"])
        # 前后关系仍可追溯
        actions = [e["action"] for e in self.svc.incident_timeline(self.secondary["id"])]
        self.assertIn("merge.revoked", actions)

    def test_review_resolve_confirm_and_reject(self):
        _, bad_area, _, far_clue = self._secondary_setup()
        merge = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            self.secondary["version"], "review-key",
        )
        merge_id = merge["merge"]["id"]

        # 冲突仍在时不能直接确认
        with self.assertRaises(DomainError) as ctx:
            self.svc.resolve_merge_item("coord1", "coordinator", merge_id, "area", bad_area["id"], "confirmed")
        self.assertEqual(409, ctx.exception.status)

        # 待复核区域不能绕过复核直接指派或结束
        with self.assertRaises(DomainError) as ctx1:
            self.svc.assign_area("coord1", "coordinator", bad_area["id"], self.good_asset["id"])
        self.assertEqual(409, ctx1.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.svc.complete_area("coord1", "coordinator", bad_area["id"], "abandoned")
        self.assertEqual(409, ctx2.exception.status)

        # 复核退回：释放占用的船，区域废弃
        rejected = self.svc.resolve_merge_item(
            "coord1", "coordinator", merge_id, "area", bad_area["id"], "rejected", "海况不满足",
        )
        item = next(i for i in rejected["items"] if i["item_type"] == "area" and i["item_id"] == bad_area["id"])
        self.assertEqual("review_rejected", item["disposition"])
        area = next(a for a in self.svc.state()["search_areas"] if a["id"] == bad_area["id"])
        self.assertEqual("abandoned", area["status"])
        self.assertIsNone(area["assigned_asset_id"])
        weak = next(a for a in self.svc.list_assets() if a["id"] == self.weak_asset["id"])
        self.assertEqual("available", weak["status"])

        # 复核确认线索
        confirmed = self.svc.resolve_merge_item(
            "coord1", "coordinator", merge_id, "clue", far_clue["id"], "confirmed", "瞭望佐证",
        )
        clue_item = next(i for i in confirmed["items"] if i["item_type"] == "clue" and i["item_id"] == far_clue["id"])
        self.assertEqual("review_confirmed", clue_item["disposition"])
        clue = next(c for c in self.svc.state()["clues"] if c["id"] == far_clue["id"])
        self.assertEqual(self.primary["id"], clue["incident_id"])

        # 已复核的项不能重复处理
        with self.assertRaises(DomainError):
            self.svc.resolve_merge_item("coord1", "coordinator", merge_id, "clue", far_clue["id"], "rejected")

        # 复核清空后允许关闭主事件（其余区域先结束）
        for area in self.svc.state()["search_areas"]:
            if area["incident_id"] == self.primary["id"] and area["status"] in {"planned", "assigned"}:
                fresh = next(a for a in self.svc.state()["search_areas"] if a["id"] == area["id"])
                self.svc.complete_area("coord1", "coordinator", area["id"], "abandoned", fresh["version"])
        primary = next(i for i in self.svc.state()["incidents"] if i["id"] == self.primary["id"])
        closed = self.svc.close_incident("coord1", "coordinator", self.primary["id"], "resolved", primary["version"])
        self.assertEqual("closed", closed["status"])

    def test_review_confirm_succeeds_once_conflict_cleared(self):
        _, bad_area, _, _ = self._secondary_setup()
        merge = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            self.secondary["version"], "fix-key",
        )
        merge_id = merge["merge"]["id"]
        # 海况能力补足后（如增援/换型），复核确认：区域恢复接续，船继续执行
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE assets SET max_sea_state=6,version=version+1 WHERE id=?", (self.weak_asset["id"],))
        conn.commit(); conn.close()

        confirmed = self.svc.resolve_merge_item(
            "coord1", "coordinator", merge_id, "area", bad_area["id"], "confirmed", "资源已升级",
        )
        item = next(i for i in confirmed["items"] if i["item_id"] == bad_area["id"])
        self.assertEqual("review_confirmed", item["disposition"])
        area = next(a for a in self.svc.state()["search_areas"] if a["id"] == bad_area["id"])
        self.assertEqual("assigned", area["status"])
        self.assertEqual(self.weak_asset["id"], area["assigned_asset_id"])
        weak = next(a for a in self.svc.list_assets() if a["id"] == self.weak_asset["id"])
        self.assertEqual("assigned", weak["status"])

    def test_cannot_close_primary_while_review_pending(self):
        self._secondary_setup()
        merge = self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            self.secondary["version"], "close-key",
        )
        primary = next(i for i in self.svc.state()["incidents"] if i["id"] == self.primary["id"])
        with self.assertRaises(DomainError) as ctx:
            self.svc.close_incident("coord1", "coordinator", self.primary["id"], "resolved", primary["version"])
        self.assertEqual(409, ctx.exception.status)
        self.assertIn("待复核", str(ctx.exception))

    def test_frozen_secondary_rejects_new_clues_and_role_guard(self):
        self._secondary_setup()
        self.svc.merge_incidents(
            "coord1", "coordinator", self.primary["id"], self.secondary["id"],
            self.secondary["version"], "freeze-key",
        )
        with self.assertRaises(DomainError) as ctx:
            self.svc.record_clue("field2", "field", self.secondary["id"], "evt-x", 31.1, 122.1, 0.5, "radio")
        self.assertEqual(409, ctx.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.svc.merge_incidents(
                "op1", "operator", self.primary["id"], self.secondary["id"], 1, "role-key",
            )
        self.assertEqual(403, ctx2.exception.status)
        with self.assertRaises(DomainError):
            self.svc.revoke_merge("op1", "operator", 1, "x")


class LegacyDatabaseUpgradeTest(unittest.TestCase):
    LEGACY_SCHEMA = """
        CREATE TABLE incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT NOT NULL UNIQUE, vessel_name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '', latitude REAL NOT NULL, longitude REAL NOT NULL,
            uncertainty_km REAL NOT NULL, drift_direction REAL NOT NULL DEFAULT 0,
            drift_speed_kn REAL NOT NULL DEFAULT 0, sea_state INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'reported', lead_org TEXT NOT NULL,
            duplicate_of INTEGER, version INTEGER NOT NULL DEFAULT 1,
            created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
            capabilities TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'available',
            latitude REAL NOT NULL, longitude REAL NOT NULL, speed_kn REAL NOT NULL,
            range_km REAL NOT NULL, max_sea_state INTEGER NOT NULL,
            version INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL);
        CREATE TABLE search_areas (
            id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id INTEGER NOT NULL, code TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL, center_lat REAL NOT NULL, center_lon REAL NOT NULL, radius_km REAL NOT NULL,
            priority INTEGER NOT NULL DEFAULT 3, status TEXT NOT NULL DEFAULT 'planned',
            assigned_asset_id INTEGER, note TEXT NOT NULL DEFAULT '',
            version INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE clues (
            id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id INTEGER NOT NULL, area_id INTEGER,
            client_event_id TEXT NOT NULL UNIQUE, latitude REAL NOT NULL, longitude REAL NOT NULL,
            confidence REAL NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'unverified',
            distance_from_incident_km REAL NOT NULL, reporter TEXT NOT NULL, details TEXT NOT NULL DEFAULT '',
            recorded_at TEXT NOT NULL, merged_at TEXT);
        CREATE TABLE offline_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT, client_batch_id TEXT NOT NULL UNIQUE, actor TEXT NOT NULL,
            status TEXT NOT NULL, received_at TEXT NOT NULL, merged_at TEXT, summary TEXT NOT NULL);
        CREATE TABLE timeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id INTEGER, actor TEXT NOT NULL,
            action TEXT NOT NULL, details TEXT NOT NULL, created_at TEXT NOT NULL);
    """

    def test_old_database_upgrades_and_stays_traceable(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = Path(tmp.name) / "legacy.db"
        conn = sqlite3.connect(db)
        conn.executescript(self.LEGACY_SCHEMA)
        conn.execute(
            """INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,status,
               lead_org,duplicate_of,version,created_by,created_at,updated_at)
               VALUES('OLD-1','老船',31.0,122.0,10,3,'reported','旧中心',NULL,1,'op','t','t')""",
        )
        conn.execute(
            """INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,status,
               lead_org,duplicate_of,version,created_by,created_at,updated_at)
               VALUES('OLD-2','老船',31.01,122.01,5,3,'reported','旧中心',NULL,1,'op','t','t')""",
        )
        conn.execute(
            """INSERT INTO incidents(code,vessel_name,latitude,longitude,uncertainty_km,sea_state,status,
               lead_org,duplicate_of,version,created_by,created_at,updated_at)
               VALUES('OLD-3','老船',31.01,122.01,5,3,'duplicate','旧中心',1,1,'op','t','t')""",
        )
        conn.execute(
            """INSERT INTO clues(incident_id,client_event_id,latitude,longitude,confidence,source,status,
               distance_from_incident_km,reporter,recorded_at)
               VALUES(1,'legacy-evt',31.02,122.02,0.7,'radio','unverified',2.5,'field','t')""",
        )
        conn.commit(); conn.close()

        svc = MaritimeSARService(db)  # 打开即迁移
        conn = sqlite3.connect(db)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(clues)").fetchall()}
        self.assertIn("version", cols)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertIn("incident_merges", tables)
        self.assertIn("merge_items", tables)
        conn.close()

        state = svc.state()
        legacy_dup = next(i for i in state["incidents"] if i["code"] == "OLD-3")
        self.assertEqual("duplicate", legacy_dup["status"])
        self.assertEqual(1, legacy_dup["duplicate_of"])  # 旧判重关系仍可追溯
        legacy_clue = next(c for c in state["clues"] if c["client_event_id"] == "legacy-evt")
        self.assertEqual(1, legacy_clue["version"])

        # 升级后新归并流程照常工作
        secondary = next(i for i in state["incidents"] if i["code"] == "OLD-2")
        svc.create_search_area("coord1", "coordinator", secondary["id"], "OLD-A", "surface", 31.02, 122.02, 5)
        merge = svc.merge_incidents("coord1", "coordinator", 1, secondary["id"], 1, "legacy-merge-key")
        self.assertEqual("applied", merge["merge"]["status"])
        self.assertIn("merge.started", {e["action"] for e in svc.incident_timeline(secondary["id"])})


if __name__ == "__main__":
    unittest.main()
