"""Maritime search-and-rescue coordination service."""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise DomainError("角色无权执行：%s" % action, 403)


def clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise DomainError("缺少操作人")
    return actor


def validate_position(lat: Any, lon: Any) -> tuple[float, float]:
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError) as exc:
        raise DomainError("经纬度必须是数值") from exc
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise DomainError("经纬度超出有效范围")
    return lat, lon


def json_dump(value: Any, ensure_ascii: bool = False) -> str:
    return json.dumps(value, ensure_ascii=ensure_ascii, sort_keys=True)


class MaritimeSARService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    vessel_name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    uncertainty_km REAL NOT NULL,
                    drift_direction REAL NOT NULL DEFAULT 0,
                    drift_speed_kn REAL NOT NULL DEFAULT 0,
                    sea_state INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'reported',
                    lead_org TEXT NOT NULL,
                    duplicate_of INTEGER REFERENCES incidents(id),
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    capabilities TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    speed_kn REAL NOT NULL,
                    range_km REAL NOT NULL,
                    max_sea_state INTEGER NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS search_areas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    code TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    center_lat REAL NOT NULL,
                    center_lon REAL NOT NULL,
                    radius_km REAL NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 3,
                    status TEXT NOT NULL DEFAULT 'planned',
                    assigned_asset_id INTEGER REFERENCES assets(id),
                    note TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS clues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    area_id INTEGER REFERENCES search_areas(id),
                    client_event_id TEXT NOT NULL UNIQUE,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    confidence REAL NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unverified',
                    distance_from_incident_km REAL NOT NULL,
                    reporter TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    merged_at TEXT,
                    summary TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);

                CREATE TABLE IF NOT EXISTS incident_merges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merge_code TEXT NOT NULL UNIQUE,
                    client_merge_id TEXT UNIQUE,
                    primary_incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    duplicate_incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    status TEXT NOT NULL,
                    expected_primary_version INTEGER,
                    expected_duplicate_version INTEGER NOT NULL,
                    pre_merge_snapshot TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    reversible INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    undone_by TEXT,
                    undone_at TEXT,
                    undo_reason TEXT NOT NULL DEFAULT ''
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_active_merge_duplicate
                    ON incident_merges(duplicate_incident_id) WHERE status='merged';

                CREATE TABLE IF NOT EXISTS incident_merge_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merge_id INTEGER NOT NULL REFERENCES incident_merges(id),
                    record_type TEXT NOT NULL,
                    record_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    before_incident_id INTEGER,
                    after_incident_id INTEGER,
                    before_area_id INTEGER,
                    before_assigned_asset_id INTEGER,
                    before_area_status TEXT,
                    before_version INTEGER,
                    details TEXT NOT NULL DEFAULT '{}',
                    undone INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_merge_changes_merge ON incident_merge_changes(merge_id, id);

                CREATE TABLE IF NOT EXISTS incident_merge_conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merge_id INTEGER NOT NULL REFERENCES incident_merges(id),
                    record_type TEXT NOT NULL,
                    record_id INTEGER,
                    code TEXT NOT NULL,
                    severity TEXT NOT NULL DEFAULT 'review',
                    status TEXT NOT NULL DEFAULT 'pending_review',
                    details TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_merge_conflicts_merge
                    ON incident_merge_conflicts(merge_id, id);
                """
            )
            self._backfill_legacy_merges(conn)

    def _backfill_legacy_merges(self, conn: sqlite3.Connection) -> None:
        """Preserve automatic duplicate markings already present in upgraded databases."""
        now = utcnow()
        rows = conn.execute(
            """
            SELECT child.* FROM incidents child
            WHERE child.duplicate_of IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM incident_merges m
                  WHERE m.duplicate_incident_id=child.id AND m.merge_code LIKE 'legacy-%'
              )
            """
        ).fetchall()
        for child in rows:
            snapshot = {"incident": dict(child)}
            merge_code = "legacy-%s-%s" % (child["id"], uuid.uuid4().hex[:12])
            cur = conn.execute(
                """INSERT INTO incident_merges(merge_code,primary_incident_id,duplicate_incident_id,status,
                   expected_duplicate_version,pre_merge_snapshot,reversible,created_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (merge_code, child["duplicate_of"], child["id"], "legacy", child["version"],
                 json_dump(snapshot), 0, child["created_by"], child["created_at"]),
            )
            conn.execute(
                """INSERT INTO incident_merge_conflicts(merge_id,record_type,record_id,code,severity,status,details,created_at,resolved_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (cur.lastrowid, "incident", child["id"], "legacy.duplicate_marking", "info",
                 "historical", json_dump({"reason": "升级前自动识别的重复报警，仅供追溯"}, ensure_ascii=False),
                 child["created_at"], now),
            )
            conn.execute(
                "INSERT INTO timeline(incident_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
                (child["id"], child["created_by"], "incident.merge_legacy_imported",
                 json_dump({"merge_id": cur.lastrowid, "primary_incident_id": child["duplicate_of"]}, ensure_ascii=False), now),
            )

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), utcnow()),
        )

    def create_incident(self, actor: str, role: str, code: str, vessel_name: str,
                        latitude: float, longitude: float, uncertainty_km: float,
                        sea_state: int, lead_org: str, drift_direction: float = 0,
                        drift_speed_kn: float = 0, description: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "创建遇险事件")
        code, vessel_name, lead_org = code.strip(), vessel_name.strip(), lead_org.strip()
        if not code or not vessel_name or not lead_org:
            raise DomainError("事件编号、船名和负责机构不能为空")
        lat, lon = validate_position(latitude, longitude)
        try:
            uncertainty_km = float(uncertainty_km)
            sea_state = int(sea_state)
            drift_direction = float(drift_direction)
            drift_speed_kn = float(drift_speed_kn)
        except (TypeError, ValueError) as exc:
            raise DomainError("不确定半径、海况和漂移参数必须是数值") from exc
        if uncertainty_km <= 0 or uncertainty_km > 1000:
            raise DomainError("不确定半径应在 0 到 1000 公里之间")
        if not 0 <= sea_state <= 9 or drift_speed_kn < 0:
            raise DomainError("海况或漂移速度无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                "SELECT * FROM incidents WHERE vessel_name=? AND status IN ('reported','coordinating','recovering') ORDER BY id DESC",
                (vessel_name,),
            ).fetchall()
            duplicate_of = None
            for row in duplicate:
                if haversine_km(lat, lon, row["latitude"], row["longitude"]) <= max(20.0, uncertainty_km + row["uncertainty_km"]):
                    duplicate_of = row["id"]
                    break
            status = "duplicate" if duplicate_of else "reported"
            try:
                cur = conn.execute(
                    """INSERT INTO incidents(code,vessel_name,description,latitude,longitude,uncertainty_km,
                       drift_direction,drift_speed_kn,sea_state,status,lead_org,duplicate_of,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, vessel_name, description.strip(), lat, lon, uncertainty_km, drift_direction, drift_speed_kn,
                     sea_state, status, lead_org, duplicate_of, actor, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("事件编号已存在", 409) from exc
            incident_id = int(cur.lastrowid)
            self._audit(conn, incident_id, actor, "incident.reported", {"duplicate_of": duplicate_of})
            if duplicate_of:
                self._audit(conn, duplicate_of, actor, "incident.duplicate_detected", {"duplicate_incident": code})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def list_assets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM assets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def add_asset(self, actor: str, role: str, name: str, kind: str,
                  capabilities: list[str], latitude: float, longitude: float,
                  speed_kn: float, range_km: float, max_sea_state: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "登记搜救资源")
        lat, lon = validate_position(latitude, longitude)
        name, kind = name.strip(), kind.strip()
        caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
        if not name or not kind or not caps:
            raise DomainError("资源名称、类型和能力不能为空")
        try:
            speed_kn, range_km, max_sea_state = float(speed_kn), float(range_km), int(max_sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("速度和航程参数必须是数值") from exc
        if speed_kn <= 0 or range_km <= 0 or not 0 <= max_sea_state <= 9:
            raise DomainError("速度、航程或适用海况无效")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """INSERT INTO assets(name,kind,capabilities,latitude,longitude,speed_kn,range_km,max_sea_state,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (name, kind, json_dump(caps), lat, lon, speed_kn, range_km, max_sea_state, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("资源名称已存在", 409) from exc
            self._audit(conn, None, actor, "asset.registered", {"asset_id": cur.lastrowid, "name": name})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_search_area(self, actor: str, role: str, incident_id: int, code: str,
                           kind: str, center_lat: float, center_lon: float,
                           radius_km: float, priority: int = 3, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "创建搜索区域")
        lat, lon = validate_position(center_lat, center_lon)
        kind, code = kind.strip(), code.strip()
        if not kind or not code:
            raise DomainError("区域类型和编号不能为空")
        try:
            radius_km, priority = float(radius_km), int(priority)
        except (TypeError, ValueError) as exc:
            raise DomainError("半径和优先级必须是数值") from exc
        if radius_km <= 0 or not 1 <= priority <= 5:
            raise DomainError("搜索半径或优先级无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("当前事件不能创建搜索区域", 409)
            try:
                cur = conn.execute(
                    """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,note,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (incident_id, code, kind, lat, lon, radius_km, priority, note.strip(), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("搜索区域编号已存在", 409) from exc
            self._audit(conn, incident_id, actor, "area.created", {"area_id": cur.lastrowid, "code": code})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (cur.lastrowid,)).fetchone())

    def assign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                    expected_asset_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "分配搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not area or not asset:
                raise DomainError("搜索区域或资源不存在", 404)
            if area["assigned_asset_id"] is not None:
                raise DomainError("搜索区域已经分配", 409)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not incident or incident["status"] not in ACTIVE_INCIDENT:
                raise DomainError("事件当前不可分配", 409)
            if expected_asset_version is not None and asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] != "available":
                raise DomainError("资源当前不可用", 409)
            if incident["sea_state"] > asset["max_sea_state"]:
                raise DomainError("海况超出资源能力", 409)
            capabilities = json.loads(asset["capabilities"])
            if area["kind"] not in capabilities:
                raise DomainError("资源不具备该搜索区域能力", 409)
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            if distance > asset["range_km"]:
                raise DomainError("搜索区域超出资源航程", 409)
            now = utcnow()
            changed = conn.execute(
                "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
                (now, asset_id, asset["version"]),
            )
            if changed.rowcount != 1:
                raise DomainError("资源已被其他任务占用", 409)
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
                (asset_id, now, area_id),
            )
            self._audit(conn, area["incident_id"], actor, "area.assigned", {"area_id": area_id, "asset_id": asset_id, "distance_km": round(distance, 2)})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def record_clue(self, actor: str, role: str, incident_id: int, client_event_id: str,
                    latitude: float, longitude: float, confidence: float, source: str,
                    area_id: int | None = None, details: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator", "field"}, "记录搜索线索")
        lat, lon = validate_position(latitude, longitude)
        event_id, source = client_event_id.strip(), source.strip()
        if not event_id or not source:
            raise DomainError("事件幂等编号和线索来源不能为空")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError) as exc:
            raise DomainError("线索置信度必须是数值") from exc
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
            if existing:
                return dict(existing)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能新增线索", 409)
            if area_id is not None:
                area = conn.execute("SELECT * FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)).fetchone()
                if not area:
                    raise DomainError("搜索区域不属于该事件", 409)
            distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
            status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
            cur = conn.execute(
                """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                   distance_from_incident_km,reporter,details,recorded_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (incident_id, area_id, event_id, lat, lon, confidence, source, status, distance, actor, details.strip(), utcnow()),
            )
            self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (cur.lastrowid,)).fetchone())

    def verify_clue(self, actor: str, role: str, clue_id: int, status: str,
                    expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "analyst"}, "核验线索")
        if status not in {"verified", "rejected", "unverified"}:
            raise DomainError("线索状态无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            clue = conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone()
            if not clue:
                raise DomainError("线索不存在", 404)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

    def withdraw_asset(self, actor: str, role: str, asset_id: int, reason: str,
                       expected_asset_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤回资源")
        if not reason.strip():
            raise DomainError("撤回原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] == "available":
                raise DomainError("资源当前未分配", 409)
            now = utcnow()
            areas = conn.execute("SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')", (asset_id,)).fetchall()
            for area in areas:
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?", (now, area["id"]))
                self._audit(conn, area["incident_id"], actor, "area.unassigned", {"area_id": area["id"], "reason": reason.strip()})
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
            self._audit(conn, None, actor, "asset.withdrawn", {"asset_id": asset_id, "reason": reason.strip()})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

    def transfer_incident(self, actor: str, role: str, incident_id: int, new_org: str,
                          expected_version: int, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "移交事件")
        new_org = new_org.strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能移交", 409)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            conn.execute(
                "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (new_org, utcnow(), incident_id, expected_version),
            )
            self._audit(conn, incident_id, actor, "incident.transferred", {"from": incident["lead_org"], "to": new_org, "note": note.strip()})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def complete_area(self, actor: str, role: str, area_id: int, outcome: str,
                      expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束搜索区域")
        if outcome not in {"completed", "abandoned"}:
            raise DomainError("区域结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if expected_version is not None and area["version"] != int(expected_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            if area["assigned_asset_id"] is not None:
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (utcnow(), area["assigned_asset_id"]))
            conn.execute("UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?", (outcome, utcnow(), area_id))
            self._audit(conn, area["incident_id"], actor, "area." + outcome, {"area_id": area_id})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def close_incident(self, actor: str, role: str, incident_id: int, outcome: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束事件")
        if outcome not in {"resolved", "cancelled", "false_alarm"}:
            raise DomainError("结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            active_area = conn.execute(
                "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=? AND status IN ('planned','assigned','active')",
                (incident_id,),
            ).fetchone()["c"]
            if active_area and outcome != "false_alarm":
                raise DomainError("仍有未结束搜索区域，不能关闭事件", 409)
            status = "closed" if outcome == "resolved" else "cancelled"
            conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=? WHERE id=?", (status, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.closed", {"outcome": outcome})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id = client_batch_id.strip()
        if not batch_id or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
            if existing:
                return {"batch_id": batch_id, "idempotent": True, "status": existing["status"], "summary": json.loads(existing["summary"])}
            results = []
            for event in events:
                event_id = str(event.get("client_event_id", "")).strip()
                try:
                    if not event_id:
                        raise DomainError("离线事件缺少 client_event_id")
                    if event.get("type") == "clue":
                        existing_clue = conn.execute("SELECT id FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
                        if existing_clue:
                            results.append({"client_event_id": event_id, "status": "merged", "record_id": existing_clue["id"], "idempotent": True})
                            continue
                        incident_id = int(event["incident_id"])
                        lat, lon = validate_position(event["latitude"], event["longitude"])
                        confidence = float(event["confidence"])
                        if not 0 <= confidence <= 1:
                            raise DomainError("置信度应在 0 到 1 之间")
                        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
                        if not incident:
                            raise DomainError("事件不存在", 404)
                        if incident["status"] in CLOSED_INCIDENT:
                            raise DomainError("已结束事件不能新增线索", 409)
                        area_id = event.get("area_id")
                        if area_id is not None and not conn.execute(
                            "SELECT 1 FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
                        ).fetchone():
                            raise DomainError("搜索区域不属于该事件", 409)
                        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
                        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
                        cur = conn.execute(
                            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                               distance_from_incident_km,reporter,details,recorded_at)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (incident_id, area_id, event_id, lat, lon, confidence,
                             str(event.get("source", "offline")).strip(), status, distance, actor,
                             str(event.get("details", "")).strip(), utcnow()),
                        )
                        self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
                        results.append({"client_event_id": event_id, "status": "merged", "record_id": cur.lastrowid})
                    elif event.get("type") == "timeline":
                        incident_id = int(event["incident_id"])
                        if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
                            raise DomainError("事件不存在", 404)
                        self._audit(conn, incident_id, actor, event.get("action", "offline.note"), event.get("details", {}))
                        results.append({"client_event_id": event_id, "status": "merged", "record_id": None})
                    else:
                        raise DomainError("不支持的离线事件类型")
                except (DomainError, KeyError, TypeError, ValueError) as exc:
                    results.append({"client_event_id": event_id, "status": "rejected", "error": str(exc)})
            summary = {"accepted": sum(1 for item in results if item["status"] == "merged"), "rejected": sum(1 for item in results if item["status"] == "rejected"), "events": results}
            now = utcnow()
            conn.execute(
                "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,merged_at,summary) VALUES(?,?,?,?,?,?)",
                (batch_id, actor, "merged", now, now, json_dump(summary)),
            )
            self._audit(conn, None, actor, "offline.batch_merged", {"batch_id": batch_id, **{k: summary[k] for k in ("accepted", "rejected")}})
            return {"batch_id": batch_id, "idempotent": False, "status": "merged", "summary": summary}

    def _merge_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["reversible"] = bool(row["reversible"])
        result["pre_merge_snapshot"] = json.loads(row["pre_merge_snapshot"])
        return result

    def _get_merge(self, conn: sqlite3.Connection, merge_id: int) -> sqlite3.Row | None:
        return conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()

    def merge_duplicate_incident(self, actor: str, role: str, primary_incident_id: int,
                                 duplicate_incident_id: int,
                                 client_merge_id: str | None = None,
                                 expected_primary_version: int | None = None,
                                 expected_duplicate_version: int | None = None,
                                 note: str = "") -> dict[str, Any]:
        """Merge a duplicate into a primary incident without changing the primary's values."""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "归并重复事件")
        primary_id, duplicate_id = int(primary_incident_id), int(duplicate_incident_id)
        if primary_id == duplicate_id:
            raise DomainError("主事件和从事件不能相同")
        client_id = (client_merge_id or "").strip() or None
        if client_id is not None and len(client_id) > 100:
            raise DomainError("归并幂等编号过长")
        note = note.strip()

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if client_id:
                idempotent = conn.execute(
                    "SELECT * FROM incident_merges WHERE client_merge_id=?", (client_id,)
                ).fetchone()
                if idempotent:
                    if idempotent["duplicate_incident_id"] != duplicate_id or idempotent["primary_incident_id"] != primary_id:
                        raise DomainError("归并幂等编号已用于其他事件", 409)
                    return self._merge_dict(idempotent)

            primary = conn.execute("SELECT * FROM incidents WHERE id=?", (primary_id,)).fetchone()
            duplicate = conn.execute("SELECT * FROM incidents WHERE id=?", (duplicate_id,)).fetchone()
            if not primary or not duplicate:
                raise DomainError("主事件或从事件不存在", 404)
            if primary["status"] not in ACTIVE_INCIDENT:
                raise DomainError("主事件当前不是活动事件，不能接续资源", 409)
            if duplicate["status"] not in ACTIVE_INCIDENT | {"duplicate"}:
                raise DomainError("从事件已关闭，不能归并", 409)
            if duplicate["duplicate_of"] is not None and duplicate["duplicate_of"] != primary_id:
                raise DomainError("该从事件已指向其他主事件", 409)
            if expected_primary_version is not None and primary["version"] != int(expected_primary_version):
                raise DomainError("主事件已变化，请刷新后重试", 409)
            if expected_duplicate_version is not None and duplicate["version"] != int(expected_duplicate_version):
                raise DomainError("从事件已变化，请刷新后重试", 409)
            if conn.execute(
                "SELECT 1 FROM incident_merges WHERE primary_incident_id=? AND status='merged' LIMIT 1",
                (duplicate_id,),
            ).fetchone():
                raise DomainError("从事件本身仍是其他事件的主事件，不能形成归并链", 409)
            if conn.execute(
                "SELECT 1 FROM incident_merges WHERE duplicate_incident_id=? AND status='merged' LIMIT 1",
                (duplicate_id,),
            ).fetchone():
                raise DomainError("从事件已被其他协调员归并", 409)

            now = utcnow()
            merge_code = "merge-%s" % uuid.uuid4().hex
            try:
                merge_cur = conn.execute(
                    """INSERT INTO incident_merges(merge_code,client_merge_id,primary_incident_id,duplicate_incident_id,
                       status,expected_primary_version,expected_duplicate_version,pre_merge_snapshot,note,
                       reversible,created_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,1,?,?)""",
                    (merge_code, client_id, primary_id, duplicate_id, "merged",
                     primary["version"] if expected_primary_version is not None else None,
                     duplicate["version"],
                     json_dump({"incident": dict(duplicate)}, ensure_ascii=False), note, actor, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("从事件已被其他协调员归并", 409) from exc
            merge_id = int(merge_cur.lastrowid)

            held_area_ids: set[int] = set()
            areas = conn.execute(
                "SELECT * FROM search_areas WHERE incident_id=? ORDER BY priority,id",
                (duplicate_id,),
            ).fetchall()
            for area in areas:
                asset = None
                conflict_code = None
                conflict_detail: dict[str, Any] = {}
                if area["assigned_asset_id"] is not None:
                    asset = conn.execute(
                        "SELECT * FROM assets WHERE id=?", (area["assigned_asset_id"],)
                    ).fetchone()
                    if not asset:
                        conflict_code = "asset.missing"
                        conflict_detail = {"reason": "已分配资源不存在"}
                    elif asset["status"] != "assigned":
                        conflict_code = "asset.unavailable"
                        conflict_detail = {"reason": "资源当前不是已分配状态", "asset_status": asset["status"]}
                    elif primary["sea_state"] > asset["max_sea_state"]:
                        conflict_code = "asset.sea_state"
                        conflict_detail = {
                            "reason": "资源不能适应主事件海况",
                            "primary_sea_state": primary["sea_state"],
                            "asset_max_sea_state": asset["max_sea_state"],
                        }
                    else:
                        capabilities = json.loads(asset["capabilities"])
                        distance = haversine_km(
                            asset["latitude"], asset["longitude"],
                            area["center_lat"], area["center_lon"],
                        )
                        if area["kind"] not in capabilities:
                            conflict_code = "asset.capability"
                            conflict_detail = {"reason": "资源不具备区域搜索能力", "required": area["kind"], "capabilities": capabilities}
                        elif distance > asset["range_km"]:
                            conflict_code = "asset.range"
                            conflict_detail = {
                                "reason": "搜索区域超出资源航程",
                                "distance_km": round(distance, 2),
                                "range_km": asset["range_km"],
                            }

                if conflict_code is None:
                    conn.execute(
                        "UPDATE search_areas SET incident_id=?,version=version+1,updated_at=? WHERE id=?",
                        (primary_id, now, area["id"]),
                    )
                    action = "continued" if asset is not None else "transferred"
                    details = {}
                    if asset is not None:
                        distance = haversine_km(
                            asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"]
                        )
                        details = {"asset_id": asset["id"], "distance_km": round(distance, 2)}
                    conn.execute(
                        """INSERT INTO incident_merge_changes(merge_id,record_type,record_id,action,
                           before_incident_id,after_incident_id,before_assigned_asset_id,before_area_status,
                           before_version,details,created_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (merge_id, "search_area", area["id"], action, duplicate_id, primary_id,
                         area["assigned_asset_id"], area["status"], area["version"],
                         json_dump(details, ensure_ascii=False), now),
                    )
                else:
                    held_area_ids.add(area["id"])
                    held_clues = conn.execute(
                        "SELECT id FROM clues WHERE incident_id=? AND area_id=? ORDER BY id",
                        (duplicate_id, area["id"]),
                    ).fetchall()
                    held_clue_ids = [row["id"] for row in held_clues]
                    conflict_detail.update({
                        "area_id": area["id"],
                        "area_code": area["code"],
                        "asset_id": area["assigned_asset_id"],
                        "held_clue_ids": held_clue_ids,
                    })
                    conn.execute(
                        "UPDATE search_areas SET assigned_asset_id=NULL,status='review_required',version=version+1,updated_at=? WHERE id=?",
                        (now, area["id"]),
                    )
                    if asset is not None and asset["status"] == "assigned":
                        conn.execute(
                            "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                            (now, asset["id"]),
                        )
                    conn.execute(
                        """INSERT INTO incident_merge_changes(merge_id,record_type,record_id,action,
                           before_incident_id,after_incident_id,before_assigned_asset_id,before_area_status,
                           before_version,details,created_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (merge_id, "search_area", area["id"], "held_for_review", duplicate_id, duplicate_id,
                         area["assigned_asset_id"], area["status"], area["version"],
                         json_dump({}, ensure_ascii=False), now),
                    )
                    if asset is not None and asset["status"] == "assigned":
                        conn.execute(
                            """INSERT INTO incident_merge_changes(merge_id,record_type,record_id,action,
                               before_incident_id,after_incident_id,before_assigned_asset_id,before_area_status,
                               before_version,details,created_at)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            (merge_id, "asset", asset["id"], "released", duplicate_id, duplicate_id,
                             asset["id"], area["status"], asset["version"],
                             json_dump({"area_id": area["id"]}, ensure_ascii=False), now),
                        )
                    conn.execute(
                        """INSERT INTO incident_merge_conflicts(merge_id,record_type,record_id,code,severity,
                           status,details,created_at) VALUES(?,?,?,?,?,?,?,?)""",
                        (merge_id, "search_area", area["id"], conflict_code, "review", "pending_review",
                         json_dump(conflict_detail, ensure_ascii=False), now),
                    )

            clues = conn.execute("SELECT * FROM clues WHERE incident_id=? ORDER BY id", (duplicate_id,)).fetchall()
            transferred_clues = held_clues = 0
            for clue in clues:
                if clue["area_id"] in held_area_ids:
                    action, target_id, held_clues = "held_for_review", duplicate_id, held_clues + 1
                else:
                    action, target_id, transferred_clues = "transferred", primary_id, transferred_clues + 1
                    conn.execute(
                        "UPDATE clues SET incident_id=?,area_id=? WHERE id=?",
                        (primary_id, clue["area_id"], clue["id"]),
                    )
                conn.execute(
                    """INSERT INTO incident_merge_changes(merge_id,record_type,record_id,action,
                       before_incident_id,after_incident_id,before_area_id,before_version,details,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (merge_id, "clue", clue["id"], action, duplicate_id, target_id,
                     clue["area_id"], 0, json_dump({}, ensure_ascii=False), now),
                )

            conn.execute(
                "UPDATE incidents SET duplicate_of=?,status='duplicate',version=version+1,updated_at=? WHERE id=?",
                (primary_id, now, duplicate_id),
            )
            conflict_count = conn.execute(
                "SELECT COUNT(*) AS c FROM incident_merge_conflicts WHERE merge_id=?", (merge_id,)
            ).fetchone()["c"]
            transferred_area_count = conn.execute(
                "SELECT COUNT(*) AS c FROM incident_merge_changes WHERE merge_id=? AND record_type='search_area' AND action IN ('transferred','continued')",
                (merge_id,),
            ).fetchone()["c"]
            self._audit(conn, primary_id, actor, "incident.merged_duplicate", {
                "merge_id": merge_id,
                "duplicate_incident_id": duplicate_id,
                "transferred_areas": transferred_area_count,
                "transferred_clues": transferred_clues,
                "held_clues": held_clues,
                "conflicts": conflict_count,
                "note": note,
            })
            self._audit(conn, duplicate_id, actor, "incident.merged_into", {
                "merge_id": merge_id,
                "primary_incident_id": primary_id,
                "conflicts": conflict_count,
            })
            result = dict(conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone())
            result["pre_merge_snapshot"] = json.loads(result["pre_merge_snapshot"])
            result["reversible"] = True
            result["changes"] = [dict(r) for r in conn.execute(
                "SELECT * FROM incident_merge_changes WHERE merge_id=? ORDER BY id", (merge_id,)
            ).fetchall()]
            result["conflicts"] = [dict(r) for r in conn.execute(
                "SELECT * FROM incident_merge_conflicts WHERE merge_id=? ORDER BY id", (merge_id,)
            ).fetchall()]
            return result

    def undo_incident_merge(self, actor: str, role: str, merge_id: int | None = None,
                            duplicate_incident_id: int | None = None,
                            reason: str = "") -> dict[str, Any]:
        """Restore ownership and versions captured by a reversible merge."""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤销重复事件归并")
        if merge_id is None and duplicate_incident_id is None:
            raise DomainError("必须提供 merge_id 或 duplicate_incident_id")
        reason = reason.strip()

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if merge_id is not None:
                merge = self._get_merge(conn, int(merge_id))
            else:
                merge = conn.execute(
                    "SELECT * FROM incident_merges WHERE duplicate_incident_id=? ORDER BY id DESC LIMIT 1",
                    (int(duplicate_incident_id),),
                ).fetchone()
            if not merge:
                raise DomainError("归并记录不存在", 404)
            if merge["status"] == "undone":
                return self._merge_dict(merge)
            if merge["status"] != "merged":
                raise DomainError("历史归并记录不能撤销", 409)
            if not merge["reversible"]:
                raise DomainError("升级前的历史归并不能自动撤销", 422)

            merge_id = int(merge["id"])
            duplicate_id = merge["duplicate_incident_id"]
            primary_id = merge["primary_incident_id"]
            snapshot = json.loads(merge["pre_merge_snapshot"])["incident"]
            changes = conn.execute(
                "SELECT * FROM incident_merge_changes WHERE merge_id=? ORDER BY id", (merge_id,)
            ).fetchall()

            for change in changes:
                if change["record_type"] == "clue":
                    clue = conn.execute("SELECT * FROM clues WHERE id=?", (change["record_id"],)).fetchone()
                    if not clue:
                        raise DomainError("线索 %s 已不存在，不能自动撤销" % change["record_id"], 409)
                    if change["action"] == "transferred":
                        if clue["incident_id"] != primary_id or clue["area_id"] != change["before_area_id"]:
                            raise DomainError("线索 %s 在归并后已有后续变化" % clue["id"], 409)
                elif change["record_type"] == "search_area":
                    area = conn.execute("SELECT * FROM search_areas WHERE id=?", (change["record_id"],)).fetchone()
                    if not area:
                        raise DomainError("搜索区域 %s 已不存在，不能自动撤销" % change["record_id"], 409)
                    if area["version"] != change["before_version"] + 1:
                        raise DomainError("搜索区域 %s 在归并后已有后续操作" % area["code"], 409)
                    if change["action"] in {"transferred", "continued"}:
                        if area["incident_id"] != primary_id or area["assigned_asset_id"] != change["before_assigned_asset_id"]:
                            raise DomainError("搜索区域 %s 在归并后已有后续变化" % area["code"], 409)
                    elif change["action"] == "held_for_review":
                        if area["incident_id"] != duplicate_id or area["assigned_asset_id"] is not None:
                            raise DomainError("待复核区域 %s 已被处理，不能自动撤销" % area["code"], 409)
                    if change["action"] in {"transferred", "continued"}:
                        extra_area_clues = conn.execute(
                            """SELECT COUNT(*) AS c FROM clues
                               WHERE incident_id=? AND area_id=? AND id NOT IN (
                                   SELECT record_id FROM incident_merge_changes
                                   WHERE merge_id=? AND record_type='clue'
                               ) LIMIT 1""",
                            (primary_id, area["id"], merge_id),
                        ).fetchone()["c"]
                        if extra_area_clues:
                            raise DomainError("区域 %s 归并后已有新线索，不能自动撤销" % area["code"], 409)
                elif change["record_type"] == "asset" and change["action"] == "released":
                    asset = conn.execute("SELECT * FROM assets WHERE id=?", (change["record_id"],)).fetchone()
                    if not asset or asset["status"] != "available" or asset["version"] != change["before_version"] + 1:
                        raise DomainError("资源 %s 已重新占用，不能自动撤销" % change["record_id"], 409)

            now = utcnow()
            for change in changes:
                if change["record_type"] == "clue" and change["action"] == "transferred":
                    conn.execute(
                        "UPDATE clues SET incident_id=?,area_id=? WHERE id=?",
                        (duplicate_id, change["before_area_id"], change["record_id"]),
                    )
                elif change["record_type"] == "search_area":
                    if change["action"] in {"transferred", "continued"}:
                        conn.execute(
                            "UPDATE search_areas SET incident_id=?,version=? WHERE id=?",
                            (duplicate_id, change["before_version"], change["record_id"]),
                        )
                    elif change["action"] == "held_for_review":
                        conn.execute(
                            "UPDATE search_areas SET incident_id=?,assigned_asset_id=?,status=?,version=? WHERE id=?",
                            (duplicate_id, change["before_assigned_asset_id"], change["before_area_status"],
                             change["before_version"], change["record_id"]),
                        )
                        asset_id = change["before_assigned_asset_id"]
                        if asset_id is not None:
                            conn.execute(
                                "UPDATE assets SET status='assigned',version=? WHERE id=?",
                                (change["before_version"], asset_id),
                            )
                        conn.execute(
                            "UPDATE incident_merge_conflicts SET status='reverted',resolved_at=? WHERE merge_id=? AND record_type='search_area' AND record_id=?",
                            (now, merge_id, change["record_id"]),
                        )
                conn.execute(
                    "UPDATE incident_merge_changes SET undone=1 WHERE id=?", (change["id"],)
                )

            conn.execute(
                """UPDATE incidents SET duplicate_of=?,status=?,version=?,updated_at=? WHERE id=?""",
                (snapshot["duplicate_of"], snapshot["status"], snapshot["version"], now, duplicate_id),
            )
            conn.execute(
                """UPDATE incident_merges SET status='undone',undone_by=?,undone_at=?,undo_reason=?
                   WHERE id=?""",
                (actor, now, reason, merge_id),
            )
            self._audit(conn, primary_id, actor, "incident.merge_undone", {
                "merge_id": merge_id,
                "duplicate_incident_id": duplicate_id,
                "reason": reason,
            })
            self._audit(conn, duplicate_id, actor, "incident.merge_restored", {
                "merge_id": merge_id,
                "primary_incident_id": primary_id,
                "restored_status": snapshot["status"],
                "restored_version": snapshot["version"],
            })
            return self._merge_dict(conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone())

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
            merges = []
            merge_rows = conn.execute("SELECT * FROM incident_merges ORDER BY id DESC").fetchall()
            for row in merge_rows:
                merge = self._merge_dict(row)
                merge["changes"] = [dict(r) for r in conn.execute(
                    "SELECT * FROM incident_merge_changes WHERE merge_id=? ORDER BY id", (row["id"],)
                ).fetchall()]
                merge["conflicts"] = [dict(r) for r in conn.execute(
                    "SELECT * FROM incident_merge_conflicts WHERE merge_id=? ORDER BY id", (row["id"],)
                ).fetchall()]
                merges.append(merge)
        return {
            "incidents": incidents,
            "assets": assets,
            "search_areas": areas,
            "clues": clues,
            "timeline": timeline,
            "incident_merges": merges,
        }

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM timeline WHERE incident_id=? ORDER BY id", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        incident = self.create_incident("coord-demo", "coordinator", "SAR-2026-001", "远星号", 31.2, 122.5, 15.0, 3, "东海搜救中心", description="演示遇险事件")
        self.add_asset("coord-demo", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 22.0, 180.0, 6)
        self.add_asset("coord-demo", "coordinator", "救助直升机", "aircraft", ["air", "night"], 30.8, 122.1, 180.0, 260.0, 5)
        self.create_search_area("coord-demo", "coordinator", incident["id"], "AREA-A", "surface", 31.2, 122.5, 20.0, 1, "首要搜索区")
        return {"seeded": True, "incident_id": incident["id"]}


class ApiHandler(BaseHTTPRequestHandler):
    service: MaritimeSARService

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _actor(self) -> tuple[str, str]:
        return self.headers.get("X-User", ""), self.headers.get("X-Role", "viewer")

    def _json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise DomainError("Content-Length 无效") from exc
        if length > 2_000_000:
            raise DomainError("请求体过大", 413)
        if not length:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DomainError("请求体不是有效 JSON") from exc
        if not isinstance(data, dict):
            raise DomainError("JSON 请求体必须是对象")
        return data

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path in {"/", "/index.html"}:
                body = (ROOT / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/health":
                self._send(200, {"status": "ok", "service": "maritime-sar"})
                return
            if path == "/api/state":
                self._send(200, self.service.state(*self._actor()))
                return
            if path.startswith("/api/incidents/") and path.endswith("/timeline"):
                incident_id = int(path.split("/")[3])
                self._send(200, {"timeline": self.service.incident_timeline(incident_id)})
                return
            self._send(404, {"error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._send(getattr(exc, "status", 400), {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            data, (actor, role) = self._json(), self._actor()
            if path == "/api/incidents":
                result = self.service.create_incident(actor, role, **data)
            elif path == "/api/assets":
                result = self.service.add_asset(actor, role, **data)
            elif path == "/api/areas":
                result = self.service.create_search_area(actor, role, **data)
            elif path == "/api/assignments":
                result = self.service.assign_area(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/incidents/transfer":
                result = self.service.transfer_incident(actor, role, **data)
            elif path == "/api/incidents/close":
                result = self.service.close_incident(actor, role, **data)
            elif path == "/api/offline/batch":
                result = self.service.merge_offline_batch(actor, role, **data)
            elif path == "/api/incidents/merge":
                result = self.service.merge_duplicate_incident(actor, role, **data)
            elif path == "/api/incidents/merge/undo":
                result = self.service.undo_incident_merge(actor, role, **data)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            self._send(exc.status, {"error": str(exc)})
        except (KeyError, TypeError, ValueError) as exc:
            self._send(400, {"error": "请求参数错误: %s" % exc})
        except Exception as exc:
            self._send(500, {"error": "服务器内部错误", "detail": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def serve(service: MaritimeSARService, host: str, port: int) -> None:
    ApiHandler.service = service
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print("Maritime SAR service listening on http://%s:%s" % (host, port))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="海上搜救协调服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8206)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    service = MaritimeSARService(args.db)
    if args.init:
        print(json.dumps(service.seed_demo() if args.seed else {"initialized": True, "db": args.db}, ensure_ascii=False))
        return
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
