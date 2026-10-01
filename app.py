"""Maritime search-and-rescue coordination service."""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate"}
AREA_REVIEW_STATUS = "review"
MERGE_OPEN = ("processing", "applied")


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


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


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
                    merged_at TEXT,
                    version INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS incident_merges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    primary_incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    secondary_incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    status TEXT NOT NULL DEFAULT 'processing',
                    prev_secondary_status TEXT NOT NULL,
                    prev_secondary_duplicate_of INTEGER,
                    prev_secondary_version INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    applied_at TEXT,
                    revoked_by TEXT,
                    revoked_at TEXT,
                    revoke_reason TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS merge_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merge_id INTEGER NOT NULL REFERENCES incident_merges(id),
                    item_type TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    prev_incident_id INTEGER NOT NULL,
                    prev_status TEXT NOT NULL,
                    prev_version INTEGER NOT NULL,
                    prev_area_id INTEGER,
                    asset_id INTEGER,
                    moved_version INTEGER,
                    disposition TEXT NOT NULL DEFAULT 'pending',
                    reason TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    resolution TEXT,
                    resolved_by TEXT,
                    resolved_at TEXT,
                    revoke_outcome TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(merge_id, item_type, item_id)
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
                CREATE INDEX IF NOT EXISTS idx_merges_secondary ON incident_merges(secondary_incident_id, status);
                CREATE INDEX IF NOT EXISTS idx_merge_items_merge ON merge_items(merge_id, id);
                CREATE INDEX IF NOT EXISTS idx_merge_items_disposition ON merge_items(disposition, merge_id);
                """
            )
            self._migrate_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        """Upgrade an older database in place; legacy rows stay traceable."""
        clue_cols = {row["name"] for row in conn.execute("PRAGMA table_info(clues)").fetchall()}
        if "version" not in clue_cols:
            conn.execute("ALTER TABLE clues ADD COLUMN version INTEGER NOT NULL DEFAULT 1")

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
            if area["status"] == AREA_REVIEW_STATUS:
                raise DomainError("区域处于归并待复核，请先复核后再指派", 409)
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
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (clue["incident_id"],)).fetchone()
            if incident and incident["status"] == "duplicate":
                raise DomainError("线索归属事件已归并，请在主事件操作或先撤销归并", 409)
            review_item = conn.execute(
                """SELECT mi.id FROM merge_items mi JOIN incident_merges im ON im.id=mi.merge_id
                   WHERE mi.item_type='clue' AND mi.item_id=? AND mi.disposition='review'
                     AND im.status='applied'""",
                (clue_id,),
            ).fetchone()
            if review_item:
                raise DomainError("线索处于归并待复核，请先在归并单中复核", 409)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=?,version=version+1 WHERE id=?", (status, utcnow(), clue_id))
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
            areas = conn.execute(
                "SELECT id,incident_id,status FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active',?)",
                (asset_id, AREA_REVIEW_STATUS),
            ).fetchall()
            for area in areas:
                next_status = AREA_REVIEW_STATUS if area["status"] == AREA_REVIEW_STATUS else "planned"
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status=?,version=version+1,updated_at=? WHERE id=?", (next_status, now, area["id"]))
                self._audit(conn, area["incident_id"], actor, "area.unassigned", {"area_id": area["id"], "reason": reason.strip(), "was_review": area["status"] == AREA_REVIEW_STATUS})
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
            if area["status"] == AREA_REVIEW_STATUS:
                raise DomainError("区域处于归并待复核，请先在归并单中复核", 409)
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
            active_merge = conn.execute(
                "SELECT id,status FROM incident_merges WHERE secondary_incident_id=? AND status IN ('processing','applied')",
                (incident_id,),
            ).fetchone()
            if active_merge:
                raise DomainError("该事件已归并到主事件（归并单 #%s），请先撤销归并再关闭" % active_merge["id"], 409)
            pending_review = conn.execute(
                """SELECT COUNT(*) AS c FROM merge_items mi JOIN incident_merges im ON im.id=mi.merge_id
                   WHERE im.primary_incident_id=? AND im.status='applied' AND mi.disposition='review'""",
                (incident_id,),
            ).fetchone()["c"]
            if pending_review:
                raise DomainError("仍有 %s 项归并冲突待复核，不能关闭事件" % pending_review, 409)
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

    # ------------------------------------------------------------------
    # 可撤销归并（重复事件接续）
    # ------------------------------------------------------------------

    def _evaluate_area_continuation(self, conn: sqlite3.Connection, primary: sqlite3.Row,
                                    area: sqlite3.Row) -> tuple[bool, str]:
        """按主事件海况、资源能力与航程判断区域能否接续。只读，不改数据。"""
        if area["assigned_asset_id"] is None:
            return True, ""
        asset = conn.execute("SELECT * FROM assets WHERE id=?", (area["assigned_asset_id"],)).fetchone()
        if asset is None:
            return False, "指派资源不存在，待人工核对"
        if asset["status"] == "withdrawn":
            return False, "资源已撤回，待重新安排"
        if primary["sea_state"] > asset["max_sea_state"]:
            return False, "主事件海况 %s 超出资源能力 %s" % (primary["sea_state"], asset["max_sea_state"])
        if area["kind"] not in json.loads(asset["capabilities"]):
            return False, "资源不具备该搜索区域能力（%s）" % area["kind"]
        distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
        if distance > asset["range_km"]:
            return False, "区域距资源 %.1f 公里，超出航程 %.1f 公里" % (distance, asset["range_km"])
        return True, ""

    def merge_incidents(self, actor: str, role: str, primary_incident_id: int,
                        secondary_incident_id: int, expected_secondary_version: int,
                        idempotency_key: str, note: str = "") -> dict[str, Any]:
        """把从事件归并到主事件：资源/区域/线索按能力与航程接续，冲突项进待复核。

        主事件自身字段不被修改。同一从事件同时只允许一个未撤销归并；
        相同幂等键重试返回同一归并，不重复占船。
        """
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "归并重复事件")
        idem = str(idempotency_key or "").strip()
        if not idem:
            raise DomainError("归并幂等编号不能为空")
        primary_incident_id = int(primary_incident_id)
        secondary_incident_id = int(secondary_incident_id)
        if primary_incident_id == secondary_incident_id:
            raise DomainError("主事件与从事件不能相同")

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM incident_merges WHERE idempotency_key=?", (idem,)).fetchone()
            if existing:
                if existing["secondary_incident_id"] != secondary_incident_id or existing["primary_incident_id"] != primary_incident_id:
                    raise DomainError("幂等编号已用于其他归并", 409)
                merge_id = int(existing["id"])
            else:
                primary = conn.execute("SELECT * FROM incidents WHERE id=?", (primary_incident_id,)).fetchone()
                secondary = conn.execute("SELECT * FROM incidents WHERE id=?", (secondary_incident_id,)).fetchone()
                if not primary or not secondary:
                    raise DomainError("主事件或从事件不存在", 404)
                if primary["status"] in CLOSED_INCIDENT:
                    raise DomainError("主事件已结束，不能作为归并目标", 409)
                if secondary["status"] in CLOSED_INCIDENT and secondary["status"] != "duplicate":
                    raise DomainError("从事件已关闭/取消，不能归并", 409)
                if secondary["version"] != int(expected_secondary_version):
                    raise DomainError("从事件已变化，请刷新后重试", 409)
                active_merge = conn.execute(
                    "SELECT id FROM incident_merges WHERE secondary_incident_id=? AND status IN ('processing','applied')",
                    (secondary_incident_id,),
                ).fetchone()
                if active_merge:
                    raise DomainError("该从事件已有未撤销归并 #%s" % active_merge["id"], 409)

                now = utcnow()
                cur = conn.execute(
                    """INSERT INTO incident_merges(idempotency_key,primary_incident_id,secondary_incident_id,
                       prev_secondary_status,prev_secondary_duplicate_of,prev_secondary_version,actor,note,
                       status,created_at,summary)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (idem, primary_incident_id, secondary_incident_id, secondary["status"],
                     secondary["duplicate_of"], secondary["version"], actor, note.strip(),
                     "processing", now, json_dump({})),
                )
                merge_id = int(cur.lastrowid)
                code = "MRG-%s-%03d" % (now[:10].replace("-", ""), merge_id)
                conn.execute("UPDATE incident_merges SET code=? WHERE id=?", (code, merge_id))

                # 先冻结从事件（不碰主事件任何字段），新写入自然被状态挡住
                conn.execute(
                    "UPDATE incidents SET status='duplicate',duplicate_of=?,version=version+1,updated_at=? WHERE id=?",
                    (primary_incident_id, now, secondary_incident_id),
                )

                snapshot_rows = []
                for area in conn.execute("SELECT * FROM search_areas WHERE incident_id=?", (secondary_incident_id,)).fetchall():
                    snapshot_rows.append(("area", area["id"], area["status"], area["version"], None, area["assigned_asset_id"]))
                for clue in conn.execute("SELECT * FROM clues WHERE incident_id=?", (secondary_incident_id,)).fetchall():
                    snapshot_rows.append(("clue", clue["id"], clue["status"], clue["version"], clue["area_id"], None))
                conn.executemany(
                    """INSERT INTO merge_items(merge_id,item_type,item_id,prev_incident_id,prev_status,prev_version,
                       prev_area_id,asset_id,disposition,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    [(merge_id, item_type, item_id, secondary_incident_id, prev_status, prev_version,
                      prev_area_id, asset_id, "pending", now, now)
                     for (item_type, item_id, prev_status, prev_version, prev_area_id, asset_id) in snapshot_rows],
                )
                self._audit(conn, secondary_incident_id, actor, "merge.started",
                            {"merge_id": merge_id, "primary": primary_incident_id, "items": len(snapshot_rows), "note": note.strip()})
                self._audit(conn, primary_incident_id, actor, "merge.target_selected",
                            {"merge_id": merge_id, "secondary": secondary_incident_id})

        return self._process_merge(merge_id, actor)

    def _process_merge(self, merge_id: int, actor: str) -> dict[str, Any]:
        """逐项接续；单项失败不拖垮整单，可重试且重试不重复占船。"""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
            if not merge:
                raise DomainError("归并单不存在", 404)
            if merge["status"] not in MERGE_OPEN:
                return self._merge_payload(conn, merge)

            primary = conn.execute("SELECT * FROM incidents WHERE id=?", (merge["primary_incident_id"],)).fetchone()
            now = utcnow()

            # 区域优先，便于线索判断其关联区域是否落入待复核
            pending = conn.execute(
                "SELECT * FROM merge_items WHERE merge_id=? AND disposition IN ('pending','failed') AND item_type='area' ORDER BY id",
                (merge_id,),
            ).fetchall()
            pending += conn.execute(
                "SELECT * FROM merge_items WHERE merge_id=? AND disposition IN ('pending','failed') AND item_type='clue' ORDER BY id",
                (merge_id,),
            ).fetchall()

            for item in pending:
                try:
                    if item["item_type"] == "area":
                        area = conn.execute("SELECT * FROM search_areas WHERE id=?", (item["item_id"],)).fetchone()
                        if area is None:
                            raise DomainError("搜索区域已不存在")
                        # 重试时以当前归属/版本为基线：已接续的跳过，避免重复操作
                        if area["incident_id"] == merge["secondary_incident_id"]:
                            ok, reason = self._evaluate_area_continuation(conn, primary, area)
                            if ok:
                                conn.execute(
                                    """UPDATE search_areas SET incident_id=?,status=?,version=version+1,updated_at=?
                                       WHERE id=? AND incident_id=? AND version=?""",
                                    (primary["id"], area["status"], now, area["id"],
                                     merge["secondary_incident_id"], area["version"]),
                                )
                                disposition, new_status = "continued", area["status"]
                            else:
                                conn.execute(
                                    """UPDATE search_areas SET incident_id=?,status=?,version=version+1,updated_at=?
                                       WHERE id=? AND incident_id=? AND version=?""",
                                    (primary["id"], AREA_REVIEW_STATUS, now, area["id"],
                                     merge["secondary_incident_id"], area["version"]),
                                )
                                disposition, new_status = "review", AREA_REVIEW_STATUS
                            conn.execute(
                                "UPDATE merge_items SET disposition=?,reason=?,moved_version=prev_version+1,error='',updated_at=? WHERE id=?",
                                (disposition, reason, now, item["id"]),
                            )
                            self._audit(conn, primary["id"], actor, "merge.area_" + disposition,
                                        {"merge_id": merge_id, "area_id": area["id"], "asset_id": area["assigned_asset_id"],
                                         "reason": reason, "new_status": new_status})
                        elif area["incident_id"] == primary["id"]:
                            # 上次处理已完成归属变更（如崩溃发生在提交前）：按当前状态对账，不重复写版本
                            disposition = "review" if area["status"] == AREA_REVIEW_STATUS else "continued"
                            reason = conn.execute("SELECT reason FROM merge_items WHERE id=?", (item["id"],)).fetchone()["reason"]
                            conn.execute(
                                "UPDATE merge_items SET disposition=?,moved_version=?,error='',updated_at=? WHERE id=?",
                                (disposition, area["version"], now, item["id"]),
                            )
                        else:
                            raise DomainError("区域已被移出归并事件，归属事件 #%s" % area["incident_id"])
                    else:
                        clue = conn.execute("SELECT * FROM clues WHERE id=?", (item["item_id"],)).fetchone()
                        if clue is None:
                            raise DomainError("线索已不存在")
                        area_dispositions = {
                            int(r["item_id"]): r["disposition"]
                            for r in conn.execute(
                                "SELECT item_id,disposition FROM merge_items WHERE merge_id=? AND item_type='area'",
                                (merge_id,),
                            ).fetchall()
                        }
                        reasons: list[str] = []
                        if clue["incident_id"] != merge["secondary_incident_id"] and clue["incident_id"] != primary["id"]:
                            raise DomainError("线索已被移出归并事件，归属事件 #%s" % clue["incident_id"])
                        linked_area_id = clue["area_id"] or item["prev_area_id"]
                        linked_disposition = area_dispositions.get(linked_area_id) if linked_area_id else None
                        if linked_disposition == "review":
                            reasons.append("关联搜索区域待复核（#%s）" % linked_area_id)
                        distance = haversine_km(primary["latitude"], primary["longitude"], clue["latitude"], clue["longitude"])
                        if distance > primary["uncertainty_km"] * 3:
                            reasons.append("线索位置相对主事件 %.1f 公里，超出主事件不确定半径的三倍" % distance)
                        disposition = "review" if reasons else "continued"
                        if clue["incident_id"] == merge["secondary_incident_id"]:
                            conn.execute(
                                "UPDATE clues SET incident_id=?,version=version+1,merged_at=? WHERE id=? AND incident_id=? AND version=?",
                                (primary["id"], now, clue["id"], merge["secondary_incident_id"], clue["version"]),
                            )
                            moved_version = item["prev_version"] + 1
                        else:
                            moved_version = clue["version"]
                        conn.execute(
                            "UPDATE merge_items SET disposition=?,reason=?,moved_version=?,error='',updated_at=? WHERE id=?",
                            (disposition, "；".join(reasons), moved_version, now, item["id"]),
                        )
                        self._audit(conn, primary["id"], actor, "merge.clue_" + disposition,
                                    {"merge_id": merge_id, "clue_id": clue["id"], "reason": "；".join(reasons)})
                except DomainError as exc:
                    conn.execute(
                        "UPDATE merge_items SET disposition='failed',error=?,updated_at=? WHERE id=?",
                        (str(exc), now, item["id"]),
                    )
                    self._audit(conn, merge["secondary_incident_id"], actor, "merge.item_failed",
                                {"merge_id": merge_id, "item_type": item["item_type"], "item_id": item["item_id"], "error": str(exc)})

            left = conn.execute(
                "SELECT COUNT(*) AS c FROM merge_items WHERE merge_id=? AND disposition IN ('pending','failed')",
                (merge_id,),
            ).fetchone()["c"]
            if left == 0:
                conn.execute("UPDATE incident_merges SET status='applied',applied_at=?,summary=? WHERE id=?",
                             (now, json_dump(self._merge_summary(conn, merge_id)), merge_id))
                self._audit(conn, merge["secondary_incident_id"], actor, "merge.applied",
                            {"merge_id": merge_id, "primary": merge["primary_incident_id"]})
            else:
                conn.execute("UPDATE incident_merges SET summary=? WHERE id=?",
                             (json_dump(self._merge_summary(conn, merge_id)), merge_id))
            merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
            return self._merge_payload(conn, merge)

    def _merge_summary(self, conn: sqlite3.Connection, merge_id: int) -> dict[str, Any]:
        counts: dict[str, int] = {}
        rows = conn.execute(
            "SELECT item_type,disposition,COUNT(*) AS c FROM merge_items WHERE merge_id=? GROUP BY item_type,disposition",
            (merge_id,),
        ).fetchall()
        for row in rows:
            counts["%s_%s" % (row["item_type"], row["disposition"])] = row["c"]
        return {"counts": counts, "total": len(conn.execute(
            "SELECT 1 FROM merge_items WHERE merge_id=?", (merge_id,)).fetchall())}

    def resume_merge(self, actor: str, role: str, merge_id: int, idempotency_key: str = "") -> dict[str, Any]:
        """继续/重试处理中或有失败项的归并。重试不重复接续、不重复占船。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "继续归并")
        merge_id = int(merge_id)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
            if not merge:
                raise DomainError("归并单不存在", 404)
            if idempotency_key and str(idempotency_key).strip() != merge["idempotency_key"]:
                raise DomainError("幂等编号与该归并不符", 409)
            if merge["status"] == "applied":
                return self._merge_payload(conn, merge)
            if merge["status"] != "processing":
                raise DomainError("归并单当前状态为 %s，不能继续" % merge["status"], 409)
            stale = conn.execute(
                "SELECT COUNT(*) AS c FROM merge_items WHERE merge_id=? AND disposition IN ('pending','failed')",
                (merge_id,),
            ).fetchone()["c"]
            if not stale:
                conn.execute("UPDATE incident_merges SET status='applied',applied_at=? WHERE id=? AND status='processing'",
                             (utcnow(), merge_id))
                self._audit(conn, merge["secondary_incident_id"], actor, "merge.applied",
                            {"merge_id": merge_id, "primary": merge["primary_incident_id"]})
                merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
                return self._merge_payload(conn, merge)
        return self._process_merge(merge_id, actor)

    def revoke_merge(self, actor: str, role: str, merge_id: int, reason: str,
                     idempotency_key: str = "", expected_secondary_version: int | None = None) -> dict[str, Any]:
        """撤销归并：恢复从事件原归属与版本；归并后被改动的项保留现状并记录。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤销归并")
        reason = reason.strip()
        if not reason:
            raise DomainError("撤销原因不能为空")
        merge_id = int(merge_id)

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
            if not merge:
                raise DomainError("归并单不存在", 404)
            if idempotency_key and str(idempotency_key).strip() != merge["idempotency_key"]:
                raise DomainError("幂等编号与该归并不符", 409)
            if merge["status"] == "revoked":
                return self._merge_payload(conn, merge)
            if merge["status"] not in MERGE_OPEN:
                raise DomainError("归并单当前状态为 %s，不能撤销" % merge["status"], 409)
            secondary = conn.execute("SELECT * FROM incidents WHERE id=?", (merge["secondary_incident_id"],)).fetchone()
            if expected_secondary_version is not None and secondary["version"] != int(expected_secondary_version):
                raise DomainError("从事件已变化，请刷新后重试", 409)

            now = utcnow()
            restored = retained = 0
            for item in conn.execute("SELECT * FROM merge_items WHERE merge_id=? ORDER BY item_type,id", (merge_id,)).fetchall():
                outcome: str | None = None
                if item["item_type"] == "area":
                    area = conn.execute("SELECT * FROM search_areas WHERE id=?", (item["item_id"],)).fetchone()
                    if area is None:
                        outcome = "kept:区域记录已不存在"
                    elif area["incident_id"] == merge["primary_incident_id"] and area["version"] == (item["moved_version"] or item["prev_version"] + 1):
                        conn.execute(
                            """UPDATE search_areas SET incident_id=?,status=?,version=?,assigned_asset_id=?,updated_at=?
                               WHERE id=?""",
                            (item["prev_incident_id"], item["prev_status"], item["prev_version"],
                             item["asset_id"], now, item["item_id"]),
                        )
                        outcome = "restored"
                    else:
                        outcome = "kept:归并后已改动，保留现状"
                else:
                    clue = conn.execute("SELECT * FROM clues WHERE id=?", (item["item_id"],)).fetchone()
                    if clue is None:
                        outcome = "kept:线索记录已不存在"
                    elif clue["incident_id"] == merge["primary_incident_id"] and clue["version"] == (item["moved_version"] or item["prev_version"] + 1):
                        conn.execute(
                            "UPDATE clues SET incident_id=?,status=?,version=?,area_id=? WHERE id=?",
                            (item["prev_incident_id"], item["prev_status"], item["prev_version"],
                             item["prev_area_id"], item["item_id"]),
                        )
                        outcome = "restored"
                    else:
                        outcome = "kept:归并后已改动，保留现状"
                conn.execute("UPDATE merge_items SET revoke_outcome=?,updated_at=? WHERE id=?", (outcome, now, item["id"]))
                self._audit(conn, item["prev_incident_id"], actor, "merge.item_" + ("restored" if outcome == "restored" else "retained"),
                            {"merge_id": merge_id, "item_type": item["item_type"], "item_id": item["item_id"], "outcome": outcome})
                restored += outcome == "restored"
                retained += outcome != "restored"

            # 恢复从事件原归属与版本
            conn.execute(
                "UPDATE incidents SET status=?,duplicate_of=?,version=?,updated_at=? WHERE id=?",
                (merge["prev_secondary_status"], merge["prev_secondary_duplicate_of"],
                 merge["prev_secondary_version"], now, merge["secondary_incident_id"]),
            )
            conn.execute(
                "UPDATE incident_merges SET status='revoked',revoked_by=?,revoked_at=?,revoke_reason=? WHERE id=?",
                (actor, now, reason, merge_id),
            )
            self._audit(conn, merge["secondary_incident_id"], actor, "merge.revoked",
                        {"merge_id": merge_id, "reason": reason, "restored": restored, "retained": retained})
            self._audit(conn, merge["primary_incident_id"], actor, "merge.target_released",
                        {"merge_id": merge_id, "secondary": merge["secondary_incident_id"]})
            merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
            return self._merge_payload(conn, merge)

    def resolve_merge_item(self, actor: str, role: str, merge_id: int, item_type: str, item_id: int,
                           resolution: str, note: str = "") -> dict[str, Any]:
        """人工复核冲突项：确认接续（confirmed）或退回（rejected）。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "复核归并冲突项")
        if resolution not in {"confirmed", "rejected"}:
            raise DomainError("复核结论只能是 confirmed 或 rejected")
        if item_type not in {"area", "clue"}:
            raise DomainError("冲突项类型无效")
        merge_id, item_id = int(merge_id), int(item_id)

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
            if not merge:
                raise DomainError("归并单不存在", 404)
            if merge["status"] != "applied":
                raise DomainError("归并单尚未完成接续，不能复核", 409)
            item = conn.execute(
                "SELECT * FROM merge_items WHERE merge_id=? AND item_type=? AND item_id=?",
                (merge_id, item_type, item_id),
            ).fetchone()
            if not item:
                raise DomainError("冲突项不属于该归并单", 404)
            if item["disposition"] != "review":
                raise DomainError("该项目前不是待复核状态（%s）" % item["disposition"], 409)
            now = utcnow()

            if item_type == "area":
                area = conn.execute("SELECT * FROM search_areas WHERE id=?", (item_id,)).fetchone()
                if not area:
                    raise DomainError("搜索区域不存在", 404)
                if resolution == "confirmed":
                    primary = conn.execute("SELECT * FROM incidents WHERE id=?", (merge["primary_incident_id"],)).fetchone()
                    ok, reason = self._evaluate_area_continuation(conn, primary, area)
                    if not ok:
                        raise DomainError("仍不满足接续条件，无法确认：%s" % reason, 409)
                    conn.execute(
                        "UPDATE search_areas SET status=?,version=version+1,updated_at=? WHERE id=?",
                        (item["prev_status"] if item["prev_status"] != "review" else "planned", now, item_id),
                    )
                    self._audit(conn, merge["primary_incident_id"], actor, "merge.review_confirmed",
                                {"merge_id": merge_id, "area_id": item_id, "note": note.strip()})
                else:
                    if area["assigned_asset_id"] is not None:
                        conn.execute(
                            "UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?",
                            (now, area["assigned_asset_id"]),
                        )
                    conn.execute(
                        "UPDATE search_areas SET status='abandoned',assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?",
                        (now, item_id),
                    )
                    self._audit(conn, merge["primary_incident_id"], actor, "merge.review_rejected",
                                {"merge_id": merge_id, "area_id": item_id, "note": note.strip()})
            else:
                clue = conn.execute("SELECT * FROM clues WHERE id=?", (item_id,)).fetchone()
                if not clue:
                    raise DomainError("线索不存在", 404)
                if resolution == "rejected":
                    conn.execute("UPDATE clues SET status='rejected',version=version+1,merged_at=? WHERE id=?", (now, item_id))
                else:
                    conn.execute("UPDATE clues SET version=version+1,merged_at=? WHERE id=?", (now, item_id))
                self._audit(conn, merge["primary_incident_id"], actor, "merge.review_" + resolution,
                            {"merge_id": merge_id, "clue_id": item_id, "note": note.strip()})

            conn.execute(
                "UPDATE merge_items SET disposition=?,resolution=?,resolved_by=?,resolved_at=?,reason=?,updated_at=? WHERE id=?",
                ("review_" + resolution, resolution, actor, now,
                 (item["reason"] + ("；复核说明：" + note.strip() if note.strip() else "")) if item["reason"] else note.strip(),
                 now, item["id"]),
            )
            merge = conn.execute("SELECT * FROM incident_merges WHERE id=?", (merge_id,)).fetchone()
            return self._merge_payload(conn, merge)

    def _merge_payload(self, conn: sqlite3.Connection, merge: sqlite3.Row) -> dict[str, Any]:
        items = conn.execute("SELECT * FROM merge_items WHERE merge_id=? ORDER BY item_type,id", (merge["id"],)).fetchall()
        area_ids = [i["item_id"] for i in items if i["item_type"] == "area"]
        clue_ids = [i["item_id"] for i in items if i["item_type"] == "clue"]
        asset_ids = {i["asset_id"] for i in items if i["asset_id"] is not None}
        areas = {a["id"]: dict(a) for a in conn.execute(
            "SELECT * FROM search_areas WHERE id IN (%s)" % (",".join("?" * len(area_ids)) or "SELECT 0 WHERE 0"),
            area_ids,
        ).fetchall()} if area_ids else {}
        clues = {c["id"]: dict(c) for c in conn.execute(
            "SELECT * FROM clues WHERE id IN (%s)" % (",".join("?" * len(clue_ids)) or "SELECT 0 WHERE 0"),
            clue_ids,
        ).fetchall()} if clue_ids else {}
        assets = {a["id"]: {"id": a["id"], "name": a["name"], "kind": a["kind"]}
                  for a in conn.execute(
                      "SELECT id,name,kind FROM assets WHERE id IN (%s)" % (",".join("?" * len(asset_ids)) or "SELECT 0 WHERE 0"),
                      tuple(asset_ids),
                  ).fetchall()} if asset_ids else {}

        item_payloads = []
        for item in items:
            record = dict(item)
            record["current"] = areas.get(item["item_id"]) if item["item_type"] == "area" else clues.get(item["item_id"])
            if item["asset_id"] is not None:
                record["asset"] = assets.get(item["asset_id"])
            item_payloads.append(record)
        payload = {
            "merge": dict(merge),
            "items": item_payloads,
            "summary": self._merge_summary(conn, merge["id"]),
        }
        return payload

    def list_merges(self) -> dict[str, Any]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM incident_merges ORDER BY id DESC").fetchall()
            merges = [self._merge_payload(conn, row) for row in rows]
        return {"merges": merges}

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
            merge_rows = conn.execute("SELECT * FROM incident_merges ORDER BY id DESC").fetchall()
            merges = [self._merge_payload(conn, row) for row in merge_rows]
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "clues": clues,
                "timeline": timeline, "merges": merges}

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
            if path == "/api/merges":
                self._send(200, self.service.list_merges())
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
                result = self.service.merge_incidents(actor, role, **data)
            elif path == "/api/incidents/merge/resume":
                result = self.service.resume_merge(actor, role, **data)
            elif path == "/api/incidents/merge/revoke":
                result = self.service.revoke_merge(actor, role, **data)
            elif path == "/api/incidents/merge/review":
                result = self.service.resolve_merge_item(actor, role, **data)
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
