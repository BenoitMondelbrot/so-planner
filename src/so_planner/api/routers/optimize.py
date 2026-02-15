# src/so_planner/api/routers/optimize.py
from fastapi import APIRouter, HTTPException, Depends
import logging
import json
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
import time
from datetime import datetime, timedelta
from ...db import get_db
from ...db.models import PlanVersion, ScheduleOp, MachineLoadDaily, BOMLine, BOM
from ...scheduling.utils import compute_daily_loads

# Пытаемся импортировать MILP-решатель.
# Если у вас другой путь/имя — поправьте строку ниже.
try:
    from ...optimize.milp import solve_milp  # MILP-like (routing heuristic)
except Exception as e:  # pylint: disable=broad-except
    solve_milp = None  # type: ignore
try:
    from ...optimize.jobshop import solve_jobshop  # CP-SAT job-shop
except Exception as e:  # pylint: disable=broad-except
    solve_jobshop = None  # type: ignore
try:
    from ...optimize.volume_milp import solve_volume_milp  # Aggregate volume MILP (CP-SAT)
except Exception as e:  # pylint: disable=broad-except
    solve_volume_milp = None  # type: ignore

router = APIRouter(prefix="/optimize", tags=["optimize"])


def _op_key(order_id, item_id, machine_id, op_index, batch_id, duration_sec):
    return (
        str(order_id),
        str(item_id),
        str(machine_id),
        int(op_index or 0),
        str(batch_id or ""),
        int(duration_sec or 0),
    )


class OptimizeRequest(BaseModel):
    weight_setup: float = 1.0
    weight_util: float = 0.0
    weight_makespan: float = 0.0
    weight_smooth: float = 0.0
    time_limit_sec: int = 10
    horizon_start: str | None = None
    horizon_end: str | None = None


class JobshopRequest(BaseModel):
    time_limit_sec: int = 20
    horizon_start: str | None = None
    horizon_end: str | None = None
    include_setup_in_duration: bool = True
    makespan_weight: float = 1.0
    smooth_weight: float = 0.0
    corridor_min_util: float = 0.0
    corridor_max_util: float = 1.0
    gap_penalty_per_hour: float = 0.0
    enforce_daily_cap: bool = False
    daily_cap_hours: float | None = 8.0


class VolumeRequest(BaseModel):
    bucket: str = "week"  # week|month
    time_limit_sec: int = 20
    horizon_start: str | None = None
    horizon_end: str | None = None
    item_ids: list[str] | None = None
    machine_ids: list[str] | None = None
    w_due_qty: float = 1000.0
    w_due_time: float = 50.0
    w_supply: float = 300.0
    w_over: float = 20.0
    w_mix: float = 1.0
    w_stab: float = 0.5


class VolumePreviewSaveRequest(BaseModel):
    volume: dict
    criteria: dict | None = None


def _retry_db(fn, attempts: int = 5, delay: float = 0.2):
    last = None
    for _ in range(attempts):
        try:
            return fn()
        except OperationalError as e:
            last = e
            if "locked" in str(e).lower():
                time.sleep(delay)
                continue
            raise
    if last:
        raise last


def _trace(trace: list[str] | None, msg: str) -> None:
    if trace is not None:
        trace.append(f"{time.strftime('%H:%M:%S')} | {msg}")
    try:
        logging.getLogger("so_planner.optimize").info(msg)
    except Exception:
        pass


def _ensure_plan_order_info_cols(db: Session) -> None:
    try:
        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS plan_order_info (
                    plan_id INTEGER,
                    order_id TEXT,
                    due_date DATE,
                    PRIMARY KEY (plan_id, order_id)
                )
                """
            )
        )
        cols = db.execute(text("PRAGMA table_info(plan_order_info)")).mappings().all()
        names = {str(c.get("name") or "") for c in cols}

        def _add(col: str, ddl: str) -> None:
            if col not in names:
                db.execute(text(f"ALTER TABLE plan_order_info ADD COLUMN {col} {ddl}"))

        _add("start_date", "DATE")
        _add("end_date", "DATE")
        _add("status", "TEXT")
        _add("qty", "REAL")
        _add("workshop", "TEXT")
        _add("fixed_at", "DATETIME")
        _add("updated_at", "DATETIME")
        _add("due_date", "DATE")
        db.commit()
    except Exception:
        db.rollback()


def _load_order_meta(db: Session, plan_id: int) -> dict[str, dict]:
    _ensure_plan_order_info_cols(db)
    rows = db.execute(
        text(
            """
            SELECT order_id, status, qty, due_date
            FROM plan_order_info
            WHERE plan_id=:pid
            """
        ),
        {"pid": plan_id},
    ).mappings().all()
    out: dict[str, dict] = {}
    for r in rows:
        oid = str(r.get("order_id") or "").strip()
        if not oid:
            continue
        out[oid] = {
            "status": str(r.get("status") or "unfixed").lower(),
            "qty": r.get("qty"),
            "due_date": r.get("due_date"),
        }
    return out


def _load_latest_stock(db: Session) -> dict[str, float]:
    try:
        sid = db.execute(text("SELECT id FROM stock_snapshot ORDER BY taken_at DESC LIMIT 1")).scalar()
        if sid is None:
            return {}
        rows = db.execute(
            text(
                """
                SELECT item_id, SUM(stock_qty) AS qty
                FROM stock_line
                WHERE snapshot_id=:sid
                GROUP BY item_id
                """
            ),
            {"sid": int(sid)},
        ).mappings().all()
        out: dict[str, float] = {}
        for r in rows:
            iid = str(r.get("item_id") or "").strip()
            if not iid:
                continue
            try:
                out[iid] = float(r.get("qty") or 0.0)
            except Exception:
                out[iid] = 0.0
        return out
    except Exception:
        return {}


def _load_bom_rows(db: Session, bom_version_id: int | None) -> list[dict]:
    out: list[dict] = []
    try:
        if bom_version_id is not None:
            rows = (
                db.query(BOMLine.item_id, BOMLine.component_id, BOMLine.qty_per, BOMLine.loss)
                .filter(BOMLine.version_id == int(bom_version_id))
                .all()
            )
            for item_id, component_id, qty_per, loss in rows:
                out.append(
                    {
                        "item_id": str(item_id or ""),
                        "component_id": str(component_id or ""),
                        "qty_per": float(qty_per or 0.0),
                        "loss": float(loss or 1.0),
                    }
                )
            if out:
                return out
    except Exception:
        pass

    try:
        rows = db.query(BOM.item_id, BOM.component_id, BOM.qty_per, BOM.loss).all()
        for item_id, component_id, qty_per, loss in rows:
            out.append(
                {
                    "item_id": str(item_id or ""),
                    "component_id": str(component_id or ""),
                    "qty_per": float(qty_per or 0.0),
                    "loss": float(loss or 1.0),
                }
            )
    except Exception:
        pass
    return out


def _copy_plan_order_info(db: Session, src_plan_id: int, dst_plan_id: int, trace: list[str] | None = None) -> int:
    try:
        _ensure_plan_order_info_cols(db)
        cols = db.execute(text("PRAGMA table_info(plan_order_info)")).mappings().all()
        names = [str(c.get("name") or "") for c in cols if str(c.get("name") or "")]
        if not names or "plan_id" not in names:
            _trace(trace, "plan_order_info copy skipped: table/plan_id missing")
            return 0
        copy_cols = [c for c in names if c != "plan_id"]
        if not copy_cols:
            _trace(trace, "plan_order_info copy skipped: no copyable columns")
            return 0

        qcols = ", ".join([f'"{c}"' for c in copy_cols])
        db.execute(text("DELETE FROM plan_order_info WHERE plan_id=:pid"), {"pid": int(dst_plan_id)})
        db.execute(
            text(
                f"""
                INSERT INTO plan_order_info ("plan_id", {qcols})
                SELECT :dst_plan_id AS plan_id, {qcols}
                FROM plan_order_info
                WHERE plan_id=:src_plan_id
                """
            ),
            {"dst_plan_id": int(dst_plan_id), "src_plan_id": int(src_plan_id)},
        )
        copied = int(
            db.execute(text("SELECT COUNT(*) FROM plan_order_info WHERE plan_id=:pid"), {"pid": int(dst_plan_id)}).scalar()
            or 0
        )
        _trace(trace, f"plan_order_info copied: src={src_plan_id} dst={dst_plan_id} rows={copied}")
        return copied
    except Exception as e:
        _trace(trace, f"plan_order_info copy failed: src={src_plan_id} dst={dst_plan_id} err={e}")
        return 0


def _json_dumps(value) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return "null"


def _json_loads(value, default):
    if value is None:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _ensure_volume_result_tables(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS plan_volume_result (
                plan_id INTEGER PRIMARY KEY,
                source_plan_id INTEGER,
                bucket TEXT,
                status TEXT,
                periods_json TEXT,
                horizon_period_idx_json TEXT,
                orders_total INTEGER,
                orders_movable INTEGER,
                kpi_before_json TEXT,
                kpi_after_json TEXT,
                warnings_json TEXT,
                trace_json TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS plan_volume_order_bucket (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER NOT NULL,
                order_id TEXT NOT NULL,
                item_id TEXT,
                demand_qty INTEGER,
                due_date DATE,
                period_idx INTEGER NOT NULL,
                period_start DATE,
                qty_before REAL NOT NULL DEFAULT 0,
                qty_after REAL NOT NULL DEFAULT 0,
                changed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(plan_id, order_id, period_idx)
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_plan_volume_order_bucket_plan ON plan_volume_order_bucket(plan_id)"))
    db.execute(
        text("CREATE INDEX IF NOT EXISTS ix_plan_volume_order_bucket_plan_order ON plan_volume_order_bucket(plan_id, order_id)")
    )
    db.execute(
        text("CREATE INDEX IF NOT EXISTS ix_plan_volume_order_bucket_plan_period ON plan_volume_order_bucket(plan_id, period_idx)")
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS plan_volume_machine_bucket (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER NOT NULL,
                machine_id TEXT NOT NULL,
                period_start DATE NOT NULL,
                load_before_sec INTEGER NOT NULL DEFAULT 0,
                cap_before_sec INTEGER NOT NULL DEFAULT 0,
                util_before REAL NOT NULL DEFAULT 0,
                load_after_sec INTEGER NOT NULL DEFAULT 0,
                cap_after_sec INTEGER NOT NULL DEFAULT 0,
                util_after REAL NOT NULL DEFAULT 0,
                UNIQUE(plan_id, machine_id, period_start)
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_plan_volume_machine_bucket_plan ON plan_volume_machine_bucket(plan_id)"))
    db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_plan_volume_machine_bucket_plan_machine_period "
            "ON plan_volume_machine_bucket(plan_id, machine_id, period_start)"
        )
    )


def _persist_volume_result(
    db: Session,
    src_plan_id: int,
    dst_plan_id: int,
    volume: dict,
    trace: list[str] | None = None,
) -> dict[str, int]:
    _ensure_volume_result_tables(db)

    def _safe_int(v, default: int = 0) -> int:
        try:
            if v is None:
                return int(default)
            return int(float(v))
        except Exception:
            return int(default)

    periods = [str(x) for x in (volume.get("periods") or [])]
    _trace(trace, f"volume_persist: start dst_plan={dst_plan_id} periods={len(periods)}")

    db.execute(text("DELETE FROM plan_volume_result WHERE plan_id=:pid"), {"pid": int(dst_plan_id)})
    db.execute(text("DELETE FROM plan_volume_order_bucket WHERE plan_id=:pid"), {"pid": int(dst_plan_id)})
    db.execute(text("DELETE FROM plan_volume_machine_bucket WHERE plan_id=:pid"), {"pid": int(dst_plan_id)})

    db.execute(
        text(
            """
            INSERT INTO plan_volume_result (
                plan_id, source_plan_id, bucket, status, periods_json, horizon_period_idx_json,
                orders_total, orders_movable, kpi_before_json, kpi_after_json,
                warnings_json, trace_json, created_at, updated_at
            ) VALUES (
                :plan_id, :source_plan_id, :bucket, :status, :periods_json, :horizon_period_idx_json,
                :orders_total, :orders_movable, :kpi_before_json, :kpi_after_json,
                :warnings_json, :trace_json, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """
        ),
        {
            "plan_id": int(dst_plan_id),
            "source_plan_id": int(src_plan_id),
            "bucket": str(volume.get("bucket") or "week"),
            "status": str(volume.get("status") or ""),
            "periods_json": _json_dumps(periods),
            "horizon_period_idx_json": _json_dumps(volume.get("horizon_period_idx") or []),
            "orders_total": int(volume.get("orders_total") or 0),
            "orders_movable": int(volume.get("orders_movable") or 0),
            "kpi_before_json": _json_dumps(volume.get("kpi_before") or {}),
            "kpi_after_json": _json_dumps(volume.get("kpi_after") or {}),
            "warnings_json": _json_dumps(volume.get("warnings") or []),
            "trace_json": _json_dumps(volume.get("trace") or trace or []),
        },
    )

    alloc_rows: list[dict] = []
    alloc_src = volume.get("order_allocations") or []
    if isinstance(alloc_src, list) and alloc_src:
        for r in alloc_src:
            if not isinstance(r, dict):
                continue
            oid = str(r.get("order_id") or "").strip()
            if not oid:
                continue
            try:
                pidx = int(r.get("period_idx") or 0)
            except Exception:
                pidx = 0
            if pidx < 0:
                pidx = 0
            pstart = str(r.get("period_start") or (periods[pidx] if pidx < len(periods) else ""))
            try:
                qb = float(r.get("qty_before") or 0.0)
            except Exception:
                qb = 0.0
            try:
                qa = float(r.get("qty_after") or 0.0)
            except Exception:
                qa = 0.0
            if abs(qb) < 1e-9 and abs(qa) < 1e-9:
                continue
            alloc_rows.append(
                {
                    "plan_id": int(dst_plan_id),
                    "order_id": oid,
                    "item_id": str(r.get("item_id") or ""),
                    "demand_qty": _safe_int(r.get("demand_qty"), 0),
                    "due_date": str(r.get("due_date") or ""),
                    "period_idx": int(pidx),
                    "period_start": pstart,
                    "qty_before": qb,
                    "qty_after": qa,
                    "changed": int(1 if bool(r.get("changed", qb != qa)) else 0),
                }
            )
    else:
        for ch in (volume.get("order_changes") or []):
            if not isinstance(ch, dict):
                continue
            oid = str(ch.get("order_id") or "").strip()
            if not oid:
                continue
            before = ch.get("before") or []
            after = ch.get("after") or []
            span = max(len(before), len(after), len(periods))
            for i in range(span):
                try:
                    qb = float(before[i]) if i < len(before) else 0.0
                except Exception:
                    qb = 0.0
                try:
                    qa = float(after[i]) if i < len(after) else 0.0
                except Exception:
                    qa = 0.0
                if abs(qb) < 1e-9 and abs(qa) < 1e-9:
                    continue
                alloc_rows.append(
                    {
                        "plan_id": int(dst_plan_id),
                        "order_id": oid,
                        "item_id": str(ch.get("item_id") or ""),
                        "demand_qty": _safe_int(ch.get("demand_qty"), 0),
                        "due_date": str(ch.get("due_date") or ""),
                        "period_idx": int(i),
                        "period_start": str(periods[i]) if i < len(periods) else "",
                        "qty_before": qb,
                        "qty_after": qa,
                        "changed": int(1 if qb != qa else 0),
                    }
                )

    if alloc_rows:
        db.execute(
            text(
                """
                INSERT INTO plan_volume_order_bucket (
                    plan_id, order_id, item_id, demand_qty, due_date, period_idx,
                    period_start, qty_before, qty_after, changed
                ) VALUES (
                    :plan_id, :order_id, :item_id, :demand_qty, :due_date, :period_idx,
                    :period_start, :qty_before, :qty_after, :changed
                )
                ON CONFLICT(plan_id, order_id, period_idx) DO UPDATE SET
                    item_id=excluded.item_id,
                    demand_qty=excluded.demand_qty,
                    due_date=excluded.due_date,
                    period_start=excluded.period_start,
                    qty_before=excluded.qty_before,
                    qty_after=excluded.qty_after,
                    changed=excluded.changed
                """
            ),
            alloc_rows,
        )

    before_map: dict[tuple[str, str], dict] = {}
    after_map: dict[tuple[str, str], dict] = {}
    for r in (volume.get("machine_buckets_before") or []):
        if not isinstance(r, dict):
            continue
        mid = str(r.get("machine_id") or "").strip()
        pstart = str(r.get("period_start") or "").strip()
        if mid and pstart:
            before_map[(mid, pstart)] = r
    for r in (volume.get("machine_buckets") or []):
        if not isinstance(r, dict):
            continue
        mid = str(r.get("machine_id") or "").strip()
        pstart = str(r.get("period_start") or "").strip()
        if mid and pstart:
            after_map[(mid, pstart)] = r

    machine_rows: list[dict] = []
    for mid, pstart in sorted(set(before_map.keys()) | set(after_map.keys())):
        b = before_map.get((mid, pstart), {})
        a = after_map.get((mid, pstart), {})
        machine_rows.append(
            {
                "plan_id": int(dst_plan_id),
                "machine_id": str(mid),
                "period_start": str(pstart),
                "load_before_sec": int(b.get("load_sec") or 0),
                "cap_before_sec": int(b.get("cap_sec") or 0),
                "util_before": float(b.get("util") or 0.0),
                "load_after_sec": int(a.get("load_sec") or 0),
                "cap_after_sec": int(a.get("cap_sec") or 0),
                "util_after": float(a.get("util") or 0.0),
            }
        )
    if machine_rows:
        db.execute(
            text(
                """
                INSERT INTO plan_volume_machine_bucket (
                    plan_id, machine_id, period_start,
                    load_before_sec, cap_before_sec, util_before,
                    load_after_sec, cap_after_sec, util_after
                ) VALUES (
                    :plan_id, :machine_id, :period_start,
                    :load_before_sec, :cap_before_sec, :util_before,
                    :load_after_sec, :cap_after_sec, :util_after
                )
                ON CONFLICT(plan_id, machine_id, period_start) DO UPDATE SET
                    load_before_sec=excluded.load_before_sec,
                    cap_before_sec=excluded.cap_before_sec,
                    util_before=excluded.util_before,
                    load_after_sec=excluded.load_after_sec,
                    cap_after_sec=excluded.cap_after_sec,
                    util_after=excluded.util_after
                """
            ),
            machine_rows,
        )

    _trace(
        trace,
        "volume_persist: done "
        f"plan={dst_plan_id} order_rows={len(alloc_rows)} machine_rows={len(machine_rows)}",
    )
    return {
        "order_bucket_rows": int(len(alloc_rows)),
        "machine_bucket_rows": int(len(machine_rows)),
        "periods": int(len(periods)),
    }


def _load_persisted_volume_result(db: Session, plan_id: int) -> dict | None:
    _ensure_volume_result_tables(db)
    head = db.execute(
        text(
            """
            SELECT
                plan_id, source_plan_id, bucket, status, periods_json, horizon_period_idx_json,
                orders_total, orders_movable, kpi_before_json, kpi_after_json,
                warnings_json, trace_json, created_at, updated_at
            FROM plan_volume_result
            WHERE plan_id=:pid
            """
        ),
        {"pid": int(plan_id)},
    ).mappings().first()
    if not head:
        return None
    src_pid = head.get("source_plan_id")

    order_rows = db.execute(
        text(
            """
            SELECT
                order_id, item_id, demand_qty, due_date, period_idx, period_start,
                qty_before, qty_after, changed
            FROM plan_volume_order_bucket
            WHERE plan_id=:pid
            ORDER BY order_id, period_idx
            """
        ),
        {"pid": int(plan_id)},
    ).mappings().all()
    machine_rows = db.execute(
        text(
            """
            SELECT
                machine_id, period_start,
                load_before_sec, cap_before_sec, util_before,
                load_after_sec, cap_after_sec, util_after
            FROM plan_volume_machine_bucket
            WHERE plan_id=:pid
            ORDER BY machine_id, period_start
            """
        ),
        {"pid": int(plan_id)},
    ).mappings().all()

    periods = _json_loads(head.get("periods_json"), [])
    p_len = len(periods) if isinstance(periods, list) else 0
    changes_by_order: dict[str, dict] = {}
    for r in order_rows:
        oid = str(r.get("order_id") or "")
        if not oid:
            continue
        try:
            pidx = int(r.get("period_idx") or 0)
        except Exception:
            pidx = 0
        if pidx < 0:
            pidx = 0
        rec = changes_by_order.get(oid)
        if rec is None:
            rec = {
                "order_id": oid,
                "item_id": str(r.get("item_id") or ""),
                "demand_qty": int(r.get("demand_qty") or 0),
                "due_date": str(r.get("due_date") or ""),
                "before": [0] * p_len,
                "after": [0] * p_len,
            }
            changes_by_order[oid] = rec
        if pidx >= len(rec["before"]):
            delta = pidx + 1 - len(rec["before"])
            rec["before"].extend([0] * delta)
            rec["after"].extend([0] * delta)
        rec["before"][pidx] = float(r.get("qty_before") or 0.0)
        rec["after"][pidx] = float(r.get("qty_after") or 0.0)
    order_changes = [v for v in changes_by_order.values() if v.get("before") != v.get("after")]

    return {
        "plan_id": int(head.get("plan_id") or 0),
        "source_plan_id": (int(src_pid) if src_pid is not None else None),
        "bucket": str(head.get("bucket") or "week"),
        "status": str(head.get("status") or ""),
        "periods": periods,
        "horizon_period_idx": _json_loads(head.get("horizon_period_idx_json"), []),
        "orders_total": int(head.get("orders_total") or 0),
        "orders_movable": int(head.get("orders_movable") or 0),
        "kpi_before": _json_loads(head.get("kpi_before_json"), {}),
        "kpi_after": _json_loads(head.get("kpi_after_json"), {}),
        "warnings": _json_loads(head.get("warnings_json"), []),
        "trace": _json_loads(head.get("trace_json"), []),
        "order_changes": order_changes,
        "order_allocations": [dict(r) for r in order_rows],
        "machine_buckets": [
            {
                "machine_id": str(r.get("machine_id") or ""),
                "period_start": str(r.get("period_start") or ""),
                "load_sec": int(r.get("load_after_sec") or 0),
                "cap_sec": int(r.get("cap_after_sec") or 0),
                "util": float(r.get("util_after") or 0.0),
            }
            for r in machine_rows
        ],
        "machine_buckets_before": [
            {
                "machine_id": str(r.get("machine_id") or ""),
                "period_start": str(r.get("period_start") or ""),
                "load_sec": int(r.get("load_before_sec") or 0),
                "cap_sec": int(r.get("cap_before_sec") or 0),
                "util": float(r.get("util_before") or 0.0),
            }
            for r in machine_rows
        ],
        "saved_at": head.get("updated_at") or head.get("created_at"),
    }


def _bucket_floor_date(ts_value, bucket: str):
    try:
        import pandas as pd

        ts = pd.to_datetime(ts_value, errors="coerce")
        if pd.isna(ts):
            return None
        ts = ts.normalize()
        if str(bucket or "week").lower() == "month":
            return ts.replace(day=1).date()
        return (ts - pd.Timedelta(days=int(ts.weekday()))).date()
    except Exception:
        return None


def _parse_date_any(v):
    try:
        import pandas as pd

        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.normalize().date()
    except Exception:
        return None


def _build_volume_alloc_map(volume: dict) -> dict[str, list[dict]]:
    out: dict[str, dict[int, dict]] = {}
    periods = [str(x) for x in (volume.get("periods") or [])]

    alloc_rows = volume.get("order_allocations") or []
    if isinstance(alloc_rows, list) and alloc_rows:
        for r in alloc_rows:
            if not isinstance(r, dict):
                continue
            oid = str(r.get("order_id") or "").strip()
            if not oid:
                continue
            try:
                pidx = int(r.get("period_idx") or 0)
            except Exception:
                pidx = 0
            if pidx < 0:
                pidx = 0
            pstart = str(r.get("period_start") or (periods[pidx] if pidx < len(periods) else "")).strip()
            if not pstart:
                continue
            try:
                qb = float(r.get("qty_before") or 0.0)
            except Exception:
                qb = 0.0
            try:
                qa = float(r.get("qty_after") or 0.0)
            except Exception:
                qa = 0.0
            if abs(qb) < 1e-9 and abs(qa) < 1e-9:
                continue
            slot = out.setdefault(oid, {})
            prev = slot.get(pidx)
            if prev is None:
                slot[pidx] = {"period_idx": pidx, "period_start": pstart, "qty_before": qb, "qty_after": qa}
            else:
                prev["qty_before"] = float(prev.get("qty_before") or 0.0) + qb
                prev["qty_after"] = float(prev.get("qty_after") or 0.0) + qa
    else:
        for ch in (volume.get("order_changes") or []):
            if not isinstance(ch, dict):
                continue
            oid = str(ch.get("order_id") or "").strip()
            if not oid:
                continue
            before = list(ch.get("before") or [])
            after = list(ch.get("after") or [])
            span = max(len(before), len(after), len(periods))
            if span <= 0:
                continue
            slot = out.setdefault(oid, {})
            for i in range(span):
                try:
                    qb = float(before[i]) if i < len(before) else 0.0
                except Exception:
                    qb = 0.0
                try:
                    qa = float(after[i]) if i < len(after) else 0.0
                except Exception:
                    qa = 0.0
                if abs(qb) < 1e-9 and abs(qa) < 1e-9:
                    continue
                pstart = str(periods[i]) if i < len(periods) else ""
                slot[i] = {"period_idx": int(i), "period_start": pstart, "qty_before": qb, "qty_after": qa}

    out_final: dict[str, list[dict]] = {}
    for oid, mp in out.items():
        rows = [dict(v) for _, v in sorted(mp.items(), key=lambda kv: int(kv[0]))]
        if rows:
            out_final[oid] = rows
    return out_final


def _materialize_volume_schedule_ops(
    *,
    base_ops: list[ScheduleOp],
    dst_plan_id: int,
    volume: dict,
    order_meta: dict[str, dict] | None = None,
    trace: list[str] | None = None,
) -> tuple[list[ScheduleOp], dict[str, str], dict[str, int]]:
    order_meta = order_meta or {}
    bucket = str(volume.get("bucket") or "week").lower()
    now_local = datetime.now()
    today_bucket = _bucket_floor_date(now_local, bucket)
    today_floor_dt = datetime(now_local.year, now_local.month, now_local.day)
    alloc_map = _build_volume_alloc_map(volume)
    _trace(trace, f"volume_materialize: alloc_orders={len(alloc_map)} bucket={bucket}")

    by_order: dict[str, list[ScheduleOp]] = {}
    for op in base_ops:
        oid = str(getattr(op, "order_id", "") or "")
        if not oid:
            continue
        by_order.setdefault(oid, []).append(op)

    used_order_ids = {str(k) for k in by_order.keys()}

    def _uniq(base_oid: str) -> str:
        cand = str(base_oid)
        if cand not in used_order_ids:
            used_order_ids.add(cand)
            return cand
        i = 1
        while True:
            c = f"{base_oid}~v{i}"
            if c not in used_order_ids:
                used_order_ids.add(c)
                return c
            i += 1

    out_ops: list[ScheduleOp] = []
    order_source_map: dict[str, str] = {}
    stat_split_orders = 0
    stat_shifted_orders = 0
    stat_dropped_orders = 0
    stat_scaled_orders = 0
    stat_clamped_past = 0

    for oid, rows in by_order.items():
        rows_sorted = sorted(
            rows,
            key=lambda r: (
                str(getattr(r, "start_ts", "") or ""),
                int(getattr(r, "op_index", 0) or 0),
                str(getattr(r, "machine_id", "") or ""),
                int(getattr(r, "op_id", 0) or 0),
            ),
        )
        m = order_meta.get(oid, {}) if isinstance(order_meta.get(oid, {}), dict) else {}
        is_fixed = str(m.get("status") or "").lower() == "fixed"
        alloc_rows = alloc_map.get(oid) or []

        if is_fixed or not alloc_rows:
            for src in rows_sorted:
                out_ops.append(
                    ScheduleOp(
                        plan_id=int(dst_plan_id),
                        order_id=str(src.order_id or ""),
                        item_id=str(src.item_id or ""),
                        article_name=src.article_name,
                        machine_id=str(src.machine_id or ""),
                        start_ts=src.start_ts,
                        end_ts=src.end_ts,
                        setup_flag=bool(src.setup_flag),
                        lateness_min=float(src.lateness_min or 0.0),
                        qty=float(src.qty or 0.0),
                        duration_sec=int(src.duration_sec or 0),
                        setup_sec=int(src.setup_sec or 0),
                        batch_id=str(src.batch_id or ""),
                        op_index=int(src.op_index or 0),
                    )
                )
            order_source_map[oid] = oid
            continue

        alloc_pos = [r for r in alloc_rows if float(r.get("qty_after") or 0.0) > 0]
        total_after = float(sum(float(r.get("qty_after") or 0.0) for r in alloc_pos))
        if total_after <= 0:
            stat_dropped_orders += 1
            continue

        base_qty = 0.0
        try:
            base_qty = max(float(getattr(r, "qty", 0.0) or 0.0) for r in rows_sorted)
        except Exception:
            base_qty = 0.0
        if base_qty <= 0:
            base_qty = float(sum(float(r.get("qty_before") or 0.0) for r in alloc_rows))
        if base_qty <= 0:
            base_qty = total_after
        if abs(base_qty - total_after) > 1e-6:
            stat_scaled_orders += 1

        base_end = None
        for r in rows_sorted:
            cur = getattr(r, "end_ts", None) or getattr(r, "start_ts", None)
            if cur is None:
                continue
            if base_end is None or cur > base_end:
                base_end = cur
        base_period = _bucket_floor_date(base_end, bucket)
        if base_period is None:
            base_period = _bucket_floor_date(getattr(rows_sorted[0], "start_ts", None), bucket)

        split = len(alloc_pos) > 1
        if split:
            stat_split_orders += 1

        for idx, arow in enumerate(sorted(alloc_pos, key=lambda x: int(x.get("period_idx") or 0))):
            qty_after = float(arow.get("qty_after") or 0.0)
            if qty_after <= 0:
                continue
            target_period = _parse_date_any(arow.get("period_start"))
            clamped_past_segment = False
            if target_period is not None and today_bucket is not None and target_period < today_bucket:
                target_period = today_bucket
                clamped_past_segment = True
            shift_days = 0
            if base_period is not None and target_period is not None:
                shift_days = int((target_period - base_period).days)
            # Guardrail: even with period-level clamp, long routes can still drift before today.
            # Shift the whole order segment forward so earliest operation starts no earlier than today bucket floor.
            if today_floor_dt is not None:
                min_shifted_start = None
                for src in rows_sorted:
                    st = getattr(src, "start_ts", None)
                    if st is None:
                        continue
                    shifted = st + timedelta(days=shift_days)
                    if min_shifted_start is None or shifted < min_shifted_start:
                        min_shifted_start = shifted
                if min_shifted_start is not None and min_shifted_start < today_floor_dt:
                    extra_days = int((today_floor_dt.date() - min_shifted_start.date()).days)
                    if extra_days > 0:
                        shift_days += extra_days
                        clamped_past_segment = True
            if clamped_past_segment:
                stat_clamped_past += 1
            if shift_days != 0:
                stat_shifted_orders += 1

            if split:
                new_oid = _uniq(f"{oid}~v{idx+1}")
            else:
                new_oid = oid
            order_source_map[new_oid] = oid
            unchanged_segment = (not split) and (abs(qty_after - base_qty) <= 1e-6) and (shift_days == 0)

            for src in rows_sorted:
                if unchanged_segment:
                    out_ops.append(
                        ScheduleOp(
                            plan_id=int(dst_plan_id),
                            order_id=str(src.order_id or ""),
                            item_id=str(src.item_id or ""),
                            article_name=src.article_name,
                            machine_id=str(src.machine_id or ""),
                            start_ts=src.start_ts,
                            end_ts=src.end_ts,
                            setup_flag=bool(src.setup_flag),
                            lateness_min=float(src.lateness_min or 0.0),
                            qty=float(src.qty or 0.0),
                            duration_sec=int(src.duration_sec or 0),
                            setup_sec=int(src.setup_sec or 0),
                            batch_id=str(src.batch_id or ""),
                            op_index=int(src.op_index or 0),
                        )
                    )
                    continue

                start_src = getattr(src, "start_ts", None)
                end_src = getattr(src, "end_ts", None)
                start_new = (start_src + timedelta(days=shift_days)) if start_src is not None else None

                dur_src = int(getattr(src, "duration_sec", 0) or 0)
                dur_new = int(round(float(dur_src) * qty_after / base_qty)) if base_qty > 0 else int(dur_src)
                if dur_src > 0 and qty_after > 0 and dur_new <= 0:
                    dur_new = 1
                if dur_new < 0:
                    dur_new = 0

                if start_new is not None and dur_new > 0:
                    end_new = start_new + timedelta(seconds=int(dur_new))
                elif end_src is not None:
                    end_new = end_src + timedelta(days=shift_days)
                else:
                    end_new = start_new

                setup_src = int(getattr(src, "setup_sec", 0) or 0)
                setup_new = int(round(float(setup_src) * qty_after / base_qty)) if base_qty > 0 else int(setup_src)
                if setup_src > 0 and qty_after > 0 and setup_new <= 0:
                    setup_new = 1
                if setup_new < 0:
                    setup_new = 0

                batch_val = str(getattr(src, "batch_id", "") or "")
                if split:
                    batch_val = f"{batch_val}~v{idx+1}" if batch_val else f"v{idx+1}"

                out_ops.append(
                    ScheduleOp(
                        plan_id=int(dst_plan_id),
                        order_id=str(new_oid),
                        item_id=str(getattr(src, "item_id", "") or ""),
                        article_name=getattr(src, "article_name", None),
                        machine_id=str(getattr(src, "machine_id", "") or ""),
                        start_ts=start_new,
                        end_ts=end_new,
                        setup_flag=bool(getattr(src, "setup_flag", False)),
                        lateness_min=float(getattr(src, "lateness_min", 0.0) or 0.0),
                        qty=float(qty_after),
                        duration_sec=int(dur_new),
                        setup_sec=int(setup_new),
                        batch_id=batch_val,
                        op_index=int(getattr(src, "op_index", 0) or 0),
                    )
                )

    stats = {
        "base_ops": int(len(base_ops)),
        "new_ops": int(len(out_ops)),
        "orders_in_base": int(len(by_order)),
        "orders_with_alloc": int(len(alloc_map)),
        "split_orders": int(stat_split_orders),
        "shifted_orders": int(stat_shifted_orders),
        "scaled_orders": int(stat_scaled_orders),
        "dropped_orders": int(stat_dropped_orders),
        "clamped_past_segments": int(stat_clamped_past),
    }
    _trace(trace, f"volume_materialize: stats={stats}")
    return out_ops, order_source_map, stats


def _sync_plan_order_info_for_volume_plan(
    db: Session,
    *,
    plan_id: int,
    source_meta: dict[str, dict],
    order_source_map: dict[str, str],
    trace: list[str] | None = None,
) -> int:
    _ensure_plan_order_info_cols(db)
    rows = db.execute(
        text(
            """
            SELECT
                order_id,
                MIN(start_ts) AS min_start,
                MAX(end_ts) AS max_end,
                MAX(COALESCE(qty, 0)) AS qty
            FROM schedule_ops
            WHERE plan_id=:pid
            GROUP BY order_id
            """
        ),
        {"pid": int(plan_id)},
    ).mappings().all()
    if not rows:
        return 0

    payload: list[dict] = []
    for r in rows:
        oid = str(r.get("order_id") or "").strip()
        if not oid:
            continue
        src_oid = str(order_source_map.get(oid) or oid)
        sm = source_meta.get(src_oid, {}) if isinstance(source_meta.get(src_oid, {}), dict) else {}
        status = str(sm.get("status") or "unfixed").lower()
        start_date = _parse_date_any(r.get("min_start"))
        end_date = _parse_date_any(r.get("max_end"))
        due_date = _parse_date_any(sm.get("due_date"))
        try:
            qty = float(r.get("qty") or 0.0)
        except Exception:
            qty = 0.0
        payload.append(
            {
                "plan_id": int(plan_id),
                "order_id": oid,
                "start_date": (str(start_date) if start_date else None),
                "end_date": (str(end_date) if end_date else None),
                "qty": qty,
                "workshop": (sm.get("workshop") if sm else None),
                "status": status,
                "due_date": (str(due_date) if due_date else None),
            }
        )

    if payload:
        db.execute(
            text(
                """
                INSERT INTO plan_order_info (
                    plan_id, order_id, start_date, end_date, qty, workshop, status, due_date, updated_at
                ) VALUES (
                    :plan_id, :order_id, :start_date, :end_date, :qty, :workshop, :status, :due_date, CURRENT_TIMESTAMP
                )
                ON CONFLICT(plan_id, order_id) DO UPDATE SET
                    start_date=excluded.start_date,
                    end_date=excluded.end_date,
                    qty=excluded.qty,
                    workshop=COALESCE(excluded.workshop, plan_order_info.workshop),
                    status=excluded.status,
                    due_date=COALESCE(excluded.due_date, plan_order_info.due_date),
                    updated_at=CURRENT_TIMESTAMP
                """
            ),
            payload,
        )
    _trace(trace, f"volume_plan_order_info_sync: rows={len(payload)}")
    return int(len(payload))


@router.post("/from-plan/{plan_id}", summary="Run MILP using plan version as warm-start; save new plan")
def optimize_from_plan(plan_id: int, req: OptimizeRequest | None = None, db: Session = Depends(get_db)):
    if solve_milp is None:
        raise HTTPException(status_code=501, detail="MILP solver is not available (missing import).")

    # Retry small helper for SQLite locked situations
    def _retry(fn, attempts: int = 5, delay: float = 0.2):
        last = None
        for _ in range(attempts):
            try:
                return fn()
            except OperationalError as e:  # database is locked
                last = e
                if "locked" in str(e).lower():
                    time.sleep(delay)
                    continue
                raise
        if last:
            raise last

    base = _retry(lambda: db.get(PlanVersion, plan_id))
    if not base:
        raise HTTPException(404, "base plan not found")
    trace: list[str] = []
    _trace(trace, f"milp_from_plan: base_plan={plan_id}")

    # 1) Тянем warm-start из БД
    ops = _retry(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to warm-start from.")
    _trace(trace, f"milp_from_plan: loaded_ops={len(ops)}")

    warm_start = [
        {
            "order_id": o.order_id,
            "item_id": o.item_id,
            "machine_id": o.machine_id,
            "start_ts": o.start_ts,
            "end_ts": o.end_ts,
            "qty": o.qty,
            "duration_sec": o.duration_sec,
            "setup_sec": o.setup_sec,
            "op_index": o.op_index,
            "batch_id": o.batch_id,
        }
        for o in ops
    ]
    name_map = {}
    for o in ops:
        key = _op_key(o.order_id, o.item_id, o.machine_id, o.op_index, o.batch_id, o.duration_sec)
        if key not in name_map:
            name_map[key] = o.article_name

    # 2) Создаём новую версию-потомка
    new_plan = PlanVersion(
        name=f"{base.name} • milp",
        origin="milp",
        status="running",
        parent_plan_id=base.id,
        bom_version_id=base.bom_version_id,
        notes="Optimized from warm-start",
    )
    db.add(new_plan)
    db.commit()
    db.refresh(new_plan)
    _trace(trace, f"milp_from_plan: created_new_plan={new_plan.id}")
    try:
        logging.getLogger("so_planner.optimize").info(
            "MILP from-plan start: base_plan=%s ops=%d params=%s -> new_plan=%s",
            plan_id, len(ops), (req.dict() if req else {}), new_plan.id
        )
        # 3) Запуск MILP; ожидается DataFrame df_ops с колонками как в greedy-сейве
        kwargs = {}
        if req is not None:
            kwargs = {
                "weight_setup": float(req.weight_setup),
                "weight_util": float(req.weight_util),
                "weight_makespan": float(req.weight_makespan),
                "weight_smooth": float(req.weight_smooth),
                "time_limit_sec": int(req.time_limit_sec),
                "horizon_start": req.horizon_start,
                "horizon_end": req.horizon_end,
                "keep_outside_ops": True,
            }
        _trace(trace, f"milp_from_plan: solver_start params={kwargs if kwargs else {}}")
        df_ops = solve_milp(warm_start=warm_start, **kwargs)  # type: ignore[operator]
        if df_ops is None or getattr(df_ops, "empty", True):
            raise RuntimeError("MILP returned empty schedule")
        _trace(trace, f"milp_from_plan: solver_done rows={len(df_ops)}")

        # 4) Сохраняем операции
        bulk_ops = []
        for r in df_ops.itertuples(index=False):
            key = _op_key(
                r.order_id,
                r.item_id,
                r.machine_id,
                getattr(r, "op_index", 0),
                getattr(r, "batch_id", ""),
                getattr(r, "duration_sec", 0),
            )
            article_name = name_map.get(key)
            bulk_ops.append(
                ScheduleOp(
                    plan_id=new_plan.id,
                    order_id=str(r.order_id),
                    item_id=str(r.item_id),
                    article_name=article_name,
                    machine_id=str(r.machine_id),
                    start_ts=r.start_ts,
                    end_ts=r.end_ts,
                    qty=float(getattr(r, "qty", 0) or 0),
                    duration_sec=int(r.duration_sec),
                    setup_sec=int(getattr(r, "setup_sec", 0) or 0),
                    op_index=int(getattr(r, "op_index", 0) or 0),
                    batch_id=str(getattr(r, "batch_id", "") or ""),
                )
            )
        db.bulk_save_objects(bulk_ops)
        db.commit()
        _trace(trace, f"milp_from_plan: saved_ops={len(bulk_ops)}")

        # 5) Агрегаты для heatmap
        loads_df = compute_daily_loads(df_ops)
        bulk_loads = [
            MachineLoadDaily(
                plan_id=new_plan.id,
                machine_id=row.machine_id,
                work_date=row.work_date,
                load_sec=int(row.load_sec),
                cap_sec=int(row.cap_sec),
                util=float(row.util),
            )
            for row in loads_df.itertuples(index=False)
        ]
        db.bulk_save_objects(bulk_loads)
        db.commit()
        _trace(trace, f"milp_from_plan: saved_machine_load_days={int(loads_df['work_date'].nunique())}")

        copied_order_info = _copy_plan_order_info(db, plan_id, int(new_plan.id), trace=trace)

        new_plan.status = "ready"
        db.commit()
        _trace(trace, f"milp_from_plan: ready plan_id={new_plan.id}")
        logging.getLogger("so_planner.optimize").info(
            "MILP from-plan end: base_plan=%s new_plan=%s ops=%d days=%d",
            plan_id, new_plan.id, len(bulk_ops), int(loads_df["work_date"].nunique())
        )
        metrics_before = getattr(df_ops, "attrs", {}).get("metrics_before") if hasattr(df_ops, "attrs") else None
        metrics_after = getattr(df_ops, "attrs", {}).get("metrics_after") if hasattr(df_ops, "attrs") else None
        return {
            "ok": True,
            "plan_id": new_plan.id,
            "parent_plan_id": base.id,
            "ops": len(bulk_ops),
            "days": int(loads_df["work_date"].nunique()),
            "plan_order_info_rows": int(copied_order_info),
            "criteria": (req.dict() if req else {}),
            "report": {"before": metrics_before, "after": metrics_after},
            "trace": trace,
        }
    except Exception as e:  # pylint: disable=broad-except
        _trace(trace, f"milp_from_plan: failed err={e}")
        new_plan.status = "failed"
        db.commit()
        logging.getLogger("so_planner.optimize").exception(
            "MILP from-plan failed: base_plan=%s new_plan=%s", plan_id, new_plan.id
        )
        raise HTTPException(status_code=500, detail=f"MILP failed: {e}")


@router.post("/diff/{plan_id}", summary="Preview optimization diff without saving a new plan")
def optimize_diff(plan_id: int, req: OptimizeRequest | None = None, db: Session = Depends(get_db)):
    if solve_milp is None:
        raise HTTPException(status_code=501, detail="MILP solver is not available (missing import).")

    def _retry(fn, attempts: int = 5, delay: float = 0.2):
        last = None
        for _ in range(attempts):
            try:
                return fn()
            except OperationalError as e:
                last = e
                if "locked" in str(e).lower():
                    time.sleep(delay)
                    continue
                raise
        if last:
            raise last

    base = _retry(lambda: db.get(PlanVersion, plan_id))
    if not base:
        raise HTTPException(404, "base plan not found")

    ops = _retry(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to diff.")

    warm_start = [
        {
            "order_id": o.order_id,
            "item_id": o.item_id,
            "machine_id": o.machine_id,
            "start_ts": o.start_ts,
            "end_ts": o.end_ts,
            "qty": o.qty,
            "duration_sec": o.duration_sec,
            "setup_sec": o.setup_sec,
            "op_index": o.op_index,
            "batch_id": o.batch_id,
        }
        for o in ops
    ]

    try:
        kwargs = {}
        if req is not None:
            kwargs = {
                "weight_setup": float(req.weight_setup),
                "weight_util": float(req.weight_util),
                "weight_makespan": float(req.weight_makespan),
                "weight_smooth": float(req.weight_smooth),
                "time_limit_sec": int(req.time_limit_sec),
                "horizon_start": req.horizon_start,
                "horizon_end": req.horizon_end,
                "keep_outside_ops": True,
            }

        import pandas as pd
        df_before = pd.DataFrame(warm_start)
        # normalize
        df_before["start_ts"] = pd.to_datetime(df_before["start_ts"]) 
        df_before["end_ts"] = pd.to_datetime(df_before["end_ts"]) 

        logging.getLogger("so_planner.optimize").info(
            "MILP diff start: plan=%s ops=%d params=%s",
            plan_id, len(warm_start), (req.dict() if req else {})
        )
        df_after = solve_milp(warm_start=warm_start, **kwargs)  # type: ignore[operator]
        if df_after is None or getattr(df_after, "empty", True):
            raise RuntimeError("Solver returned empty schedule")

        # positions before/after by machine
        def add_pos(df):
            return df.sort_values(["machine_id", "start_ts"]).assign(
                pos=lambda x: x.groupby("machine_id").cumcount()
            )

        b = add_pos(df_before)
        a = add_pos(df_after)
        key_cols = ["order_id","item_id","machine_id","op_index","batch_id","duration_sec"]
        b_keyed = b[key_cols + ["start_ts","end_ts","pos"]].rename(columns={"start_ts":"start_before","end_ts":"end_before","pos":"pos_before"})
        a_keyed = a[key_cols + ["start_ts","end_ts","pos"]].rename(columns={"start_ts":"start_after","end_ts":"end_after","pos":"pos_after"})
        diff = pd.merge(b_keyed, a_keyed, on=key_cols, how="outer")
        diff["delta_start_sec"] = (pd.to_datetime(diff["start_after"]) - pd.to_datetime(diff["start_before"])) .dt.total_seconds()
        diff["delta_end_sec"] = (pd.to_datetime(diff["end_after"]) - pd.to_datetime(diff["end_before"])) .dt.total_seconds()

        # Heatmap before/after
        loads_before = compute_daily_loads(df_before)
        loads_after = compute_daily_loads(df_after)

        def heatmap_payload(loads_df):
            machines = sorted(loads_df["machine_id"].astype(str).unique().tolist())
            dates = sorted(pd.to_datetime(loads_df["work_date"]).dt.date.astype(str).unique().tolist())
            util = {}
            for r in loads_df.itertuples(index=False):
                util[f"{getattr(r,'machine_id')}|{str(getattr(r,'work_date'))[:10]}"] = float(getattr(r, 'util'))
            return {"machines": machines, "dates": dates, "util": util}

        metrics_before = getattr(df_after, "attrs", {}).get("metrics_before") if hasattr(df_after, "attrs") else None
        metrics_after = getattr(df_after, "attrs", {}).get("metrics_after") if hasattr(df_after, "attrs") else None

        def to_records(df):
            recs = df.to_dict(orient="records")
            for r in recs:
                for k,v in list(r.items()):
                    if hasattr(v, "isoformat"):
                        r[k] = str(v)
            return recs

        res = {
            "ok": True,
            "plan_id": plan_id,
            "criteria": (req.dict() if req else {}),
            "metrics": {"before": metrics_before, "after": metrics_after},
            "ops_diff": to_records(diff),
            "heatmap": {"before": heatmap_payload(loads_before), "after": heatmap_payload(loads_after)},
        }
        logging.getLogger("so_planner.optimize").info(
            "MILP diff end: plan=%s ops_diff=%d",
            plan_id, len(res.get("ops_diff", []))
        )
        return res
    except Exception as e:  # pylint: disable=broad-except
        raise HTTPException(status_code=500, detail=f"Diff failed: {e}")


@router.post("/jobshop/from-plan/{plan_id}", summary="Run Job-shop (CP-SAT) from plan; save new plan")
def jobshop_from_plan(plan_id: int, req: JobshopRequest | None = None, db: Session = Depends(get_db)):
    if solve_jobshop is None:
        raise HTTPException(status_code=501, detail="Job-shop solver is not available (missing import).")

    def _retry(fn, attempts: int = 5, delay: float = 0.2):
        last = None
        for _ in range(attempts):
            try:
                return fn()
            except OperationalError as e:
                last = e
                if "locked" in str(e).lower():
                    time.sleep(delay)
                    continue
                raise
        if last:
            raise last

    base = _retry(lambda: db.get(PlanVersion, plan_id))
    if not base:
        raise HTTPException(404, "base plan not found")
    trace: list[str] = []
    _trace(trace, f"jobshop_from_plan: base_plan={plan_id}")

    ops = _retry(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to warm-start from.")
    _trace(trace, f"jobshop_from_plan: loaded_ops={len(ops)}")

    warm_start = [
        {
            "order_id": o.order_id,
            "item_id": o.item_id,
            "machine_id": o.machine_id,
            "start_ts": o.start_ts,
            "end_ts": o.end_ts,
            "qty": o.qty,
            "duration_sec": o.duration_sec,
            "setup_sec": o.setup_sec,
            "op_index": o.op_index,
            "batch_id": o.batch_id,
        }
        for o in ops
    ]
    name_map = {}
    for o in ops:
        key = _op_key(o.order_id, o.item_id, o.machine_id, o.op_index, o.batch_id, o.duration_sec)
        if key not in name_map:
            name_map[key] = o.article_name

    new_plan = PlanVersion(
        name=f"{base.name} • jobshop",
        origin="jobshop",
        status="running",
        parent_plan_id=base.id,
        bom_version_id=base.bom_version_id,
        notes="Job-shop optimized from warm-start",
    )
    db.add(new_plan)
    db.commit()
    db.refresh(new_plan)
    _trace(trace, f"jobshop_from_plan: created_new_plan={new_plan.id}")
    try:
        logging.getLogger("so_planner.optimize").info(
            "Jobshop from-plan start: base_plan=%s ops=%d params=%s -> new_plan=%s",
            plan_id, len(ops), (req.dict() if req else {}), new_plan.id
        )
        kwargs = {}
        if req is not None:
            kwargs = {
                "time_limit_sec": int(req.time_limit_sec),
                "horizon_start": req.horizon_start,
                "horizon_end": req.horizon_end,
                "keep_outside_ops": True,
                "include_setup_in_duration": bool(req.include_setup_in_duration),
                "makespan_weight": float(req.makespan_weight or 1.0),
                "smooth_weight": float(req.smooth_weight or 0.0),
                "corridor_min_util": float(req.corridor_min_util or 0.0),
                "corridor_max_util": float(req.corridor_max_util or 1.0),
                "gap_penalty_per_sec": float((req.gap_penalty_per_hour or 0.0) / 3600.0),
                "enforce_daily_cap": bool(req.enforce_daily_cap),
                "daily_cap_sec": int(round(float((req.daily_cap_hours or 8.0)) * 3600.0)),
            }
        _trace(trace, f"jobshop_from_plan: solver_start params={kwargs if kwargs else {}}")
        df_ops = solve_jobshop(warm_start=warm_start, **kwargs)  # type: ignore[operator]
        if df_ops is None or getattr(df_ops, "empty", True):
            raise RuntimeError("Job-shop returned empty schedule")
        _trace(trace, f"jobshop_from_plan: solver_done rows={len(df_ops)}")

        bulk_ops = []
        for r in df_ops.itertuples(index=False):
            key = _op_key(
                r.order_id,
                r.item_id,
                r.machine_id,
                getattr(r, "op_index", 0),
                getattr(r, "batch_id", ""),
                getattr(r, "duration_sec", 0),
            )
            article_name = name_map.get(key)
            bulk_ops.append(
                ScheduleOp(
                    plan_id=new_plan.id,
                    order_id=str(r.order_id),
                    item_id=str(r.item_id),
                    article_name=article_name,
                    machine_id=str(r.machine_id),
                    start_ts=r.start_ts,
                    end_ts=r.end_ts,
                    qty=float(getattr(r, "qty", 0) or 0),
                    duration_sec=int(r.duration_sec),
                    setup_sec=int(getattr(r, "setup_sec", 0) or 0),
                    op_index=int(getattr(r, "op_index", 0) or 0),
                    batch_id=str(getattr(r, "batch_id", "") or ""),
                )
            )
        db.bulk_save_objects(bulk_ops)
        db.commit()
        _trace(trace, f"jobshop_from_plan: saved_ops={len(bulk_ops)}")

        loads_df = compute_daily_loads(df_ops)
        bulk_loads = [
            MachineLoadDaily(
                plan_id=new_plan.id,
                machine_id=row.machine_id,
                work_date=row.work_date,
                load_sec=int(row.load_sec),
                cap_sec=int(row.cap_sec),
                util=float(row.util),
            )
            for row in loads_df.itertuples(index=False)
        ]
        db.bulk_save_objects(bulk_loads)
        db.commit()
        _trace(trace, f"jobshop_from_plan: saved_machine_load_days={int(loads_df['work_date'].nunique())}")

        copied_order_info = _copy_plan_order_info(db, plan_id, int(new_plan.id), trace=trace)

        new_plan.status = "ready"
        db.commit()
        _trace(trace, f"jobshop_from_plan: ready plan_id={new_plan.id}")
        logging.getLogger("so_planner.optimize").info(
            "Jobshop from-plan end: base_plan=%s new_plan=%s ops=%d days=%d",
            plan_id, new_plan.id, len(bulk_ops), int(loads_df["work_date"].nunique())
        )
        metrics_before = getattr(df_ops, "attrs", {}).get("metrics_before") if hasattr(df_ops, "attrs") else None
        metrics_after = getattr(df_ops, "attrs", {}).get("metrics_after") if hasattr(df_ops, "attrs") else None
        return {
            "ok": True,
            "plan_id": new_plan.id,
            "parent_plan_id": base.id,
            "ops": len(bulk_ops),
            "days": int(loads_df["work_date"].nunique()),
            "plan_order_info_rows": int(copied_order_info),
            "criteria": (req.dict() if req else {}),
            "report": {"before": metrics_before, "after": metrics_after},
            "trace": trace,
        }
    except Exception as e:  # pylint: disable=broad-except
        _trace(trace, f"jobshop_from_plan: failed err={e}")
        new_plan.status = "failed"
        db.commit()
        logging.getLogger("so_planner.optimize").exception(
            "Jobshop from-plan failed: base_plan=%s new_plan=%s", plan_id, new_plan.id
        )
        raise HTTPException(status_code=500, detail=f"Jobshop failed: {e}")


@router.post("/jobshop/diff/{plan_id}", summary="Preview Job-shop optimization diff without saving a new plan")
def jobshop_diff(plan_id: int, req: JobshopRequest | None = None, db: Session = Depends(get_db)):
    if solve_jobshop is None:
        raise HTTPException(status_code=501, detail="Job-shop solver is not available (missing import).")

    def _retry(fn, attempts: int = 5, delay: float = 0.2):
        last = None
        for _ in range(attempts):
            try:
                return fn()
            except OperationalError as e:
                last = e
                if "locked" in str(e).lower():
                    time.sleep(delay)
                    continue
                raise
        if last:
            raise last

    base = _retry(lambda: db.get(PlanVersion, plan_id))
    if not base:
        raise HTTPException(404, "base plan not found")

    ops = _retry(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to diff.")

    warm_start = [
        {
            "order_id": o.order_id,
            "item_id": o.item_id,
            "machine_id": o.machine_id,
            "start_ts": o.start_ts,
            "end_ts": o.end_ts,
            "qty": o.qty,
            "duration_sec": o.duration_sec,
            "setup_sec": o.setup_sec,
            "op_index": o.op_index,
            "batch_id": o.batch_id,
        }
        for o in ops
    ]

    try:
        kwargs = {}
        if req is not None:
            kwargs = {
                "time_limit_sec": int(req.time_limit_sec),
                "horizon_start": req.horizon_start,
                "horizon_end": req.horizon_end,
                "keep_outside_ops": True,
                "include_setup_in_duration": bool(req.include_setup_in_duration),
                "makespan_weight": float(req.makespan_weight or 1.0),
                "smooth_weight": float(req.smooth_weight or 0.0),
                "corridor_min_util": float(req.corridor_min_util or 0.0),
                "corridor_max_util": float(req.corridor_max_util or 1.0),
                "gap_penalty_per_sec": float((req.gap_penalty_per_hour or 0.0) / 3600.0),
                "enforce_daily_cap": bool(req.enforce_daily_cap),
                "daily_cap_sec": int(round(float((req.daily_cap_hours or 8.0)) * 3600.0)),
            }

        import pandas as pd
        df_before = pd.DataFrame(warm_start)
        df_before["start_ts"] = pd.to_datetime(df_before["start_ts"])  # normalize
        df_before["end_ts"] = pd.to_datetime(df_before["end_ts"]) 

        logging.getLogger("so_planner.optimize").info(
            "Jobshop diff start: plan=%s ops=%d params=%s",
            plan_id, len(warm_start), (req.dict() if req else {})
        )
        df_after = solve_jobshop(warm_start=warm_start, **kwargs)  # type: ignore[operator]
        if df_after is None or getattr(df_after, "empty", True):
            raise RuntimeError("Solver returned empty schedule")

        def add_pos(df):
            return df.sort_values(["machine_id", "start_ts"]).assign(
                pos=lambda x: x.groupby("machine_id").cumcount()
            )

        b = add_pos(df_before)
        a = add_pos(df_after)
        key_cols = ["order_id","item_id","machine_id","op_index","batch_id","duration_sec"]
        b_keyed = b[key_cols + ["start_ts","end_ts","pos"]].rename(columns={"start_ts":"start_before","end_ts":"end_before","pos":"pos_before"})
        a_keyed = a[key_cols + ["start_ts","end_ts","pos"]].rename(columns={"start_ts":"start_after","end_ts":"end_after","pos":"pos_after"})
        diff = pd.merge(b_keyed, a_keyed, on=key_cols, how="outer")
        diff["delta_start_sec"] = (pd.to_datetime(diff["start_after"]) - pd.to_datetime(diff["start_before"])) .dt.total_seconds()
        diff["delta_end_sec"] = (pd.to_datetime(diff["end_after"]) - pd.to_datetime(diff["end_before"])) .dt.total_seconds()

        loads_before = compute_daily_loads(df_before)
        loads_after = compute_daily_loads(df_after)

        def heatmap_payload(loads_df):
            machines = sorted(loads_df["machine_id"].astype(str).unique().tolist())
            dates = sorted(pd.to_datetime(loads_df["work_date"]).dt.date.astype(str).unique().tolist())
            util = {}
            for r in loads_df.itertuples(index=False):
                util[f"{getattr(r,'machine_id')}|{str(getattr(r,'work_date'))[:10]}"] = float(getattr(r, 'util'))
            return {"machines": machines, "dates": dates, "util": util}

        metrics_before = getattr(df_after, "attrs", {}).get("metrics_before") if hasattr(df_after, "attrs") else None
        metrics_after = getattr(df_after, "attrs", {}).get("metrics_after") if hasattr(df_after, "attrs") else None

        def to_records(df):
            recs = df.to_dict(orient="records")
            for r in recs:
                for k,v in list(r.items()):
                    if hasattr(v, "isoformat"):
                        r[k] = str(v)
            return recs

        res = {
            "ok": True,
            "plan_id": plan_id,
            "criteria": (req.dict() if req else {}),
            "metrics": {"before": metrics_before, "after": metrics_after},
            "ops_diff": to_records(diff),
            "heatmap": {"before": heatmap_payload(loads_before), "after": heatmap_payload(loads_after)},
        }
        logging.getLogger("so_planner.optimize").info(
            "Jobshop diff end: plan=%s ops_diff=%d",
            plan_id, len(res.get("ops_diff", []))
        )
        return res
    except Exception as e:  # pylint: disable=broad-except
        raise HTTPException(status_code=500, detail=f"Diff failed: {e}")


@router.get("/volume/result/{plan_id}", summary="Get persisted aggregate volume result for a plan")
def volume_result(
    plan_id: int,
    include_order_allocations: bool = True,
    include_machine_buckets: bool = True,
    changed_only: bool = False,
    limit: int = 200000,
    db: Session = Depends(get_db),
):
    if not db.get(PlanVersion, plan_id):
        raise HTTPException(404, "plan not found")

    data = _load_persisted_volume_result(db, int(plan_id))
    if data is None:
        raise HTTPException(404, "volume result not found for plan")

    if not include_order_allocations:
        data.pop("order_allocations", None)
    else:
        alloc = data.get("order_allocations") or []
        if changed_only:
            alloc = [r for r in alloc if int(r.get("changed") or 0) == 1]
        try:
            lim = int(limit or 0)
        except Exception:
            lim = 0
        if lim > 0:
            alloc = alloc[:lim]
        data["order_allocations"] = alloc

    if not include_machine_buckets:
        data.pop("machine_buckets", None)
        data.pop("machine_buckets_before", None)

    return {"ok": True, "plan_id": int(plan_id), "volume": data}


@router.post("/volume/diff/{plan_id}", summary="Preview aggregate volume MILP diff without saving a new plan")
def volume_diff(plan_id: int, req: VolumeRequest | None = None, db: Session = Depends(get_db)):
    if solve_volume_milp is None:
        raise HTTPException(status_code=501, detail="Volume MILP solver is not available (missing import).")

    base = _retry_db(lambda: db.get(PlanVersion, plan_id))
    if not base:
        raise HTTPException(404, "base plan not found")
    trace: list[str] = []
    _trace(trace, f"volume_diff: base_plan={plan_id}")

    ops = _retry_db(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to diff.")
    _trace(trace, f"volume_diff: loaded_ops={len(ops)}")

    warm_start = [
        {
            "order_id": o.order_id,
            "item_id": o.item_id,
            "machine_id": o.machine_id,
            "start_ts": o.start_ts,
            "end_ts": o.end_ts,
            "qty": o.qty,
            "duration_sec": o.duration_sec,
            "setup_sec": o.setup_sec,
            "op_index": o.op_index,
            "batch_id": o.batch_id,
        }
        for o in ops
    ]

    try:
        order_meta = _load_order_meta(db, plan_id)
        stock_by_item = _load_latest_stock(db)
        bom_rows = _load_bom_rows(db, base.bom_version_id)
        _trace(
            trace,
            f"volume_diff: loaded_meta orders={len(order_meta)} stock_items={len(stock_by_item)} bom_rows={len(bom_rows)}",
        )

        kwargs = {}
        if req is not None:
            kwargs = {
                "bucket": str(req.bucket or "week"),
                "time_limit_sec": int(req.time_limit_sec or 0),
                "horizon_start": req.horizon_start,
                "horizon_end": req.horizon_end,
                "item_ids": [str(x) for x in (req.item_ids or []) if str(x)],
                "machine_ids": [str(x) for x in (req.machine_ids or []) if str(x)],
                "w_due_qty": float(req.w_due_qty or 0.0),
                "w_due_time": float(req.w_due_time or 0.0),
                "w_supply": float(req.w_supply or 0.0),
                "w_over": float(req.w_over or 0.0),
                "w_mix": float(req.w_mix or 0.0),
                "w_stab": float(req.w_stab or 0.0),
                "forbid_past": True,
            }
        else:
            kwargs = {
                "bucket": "week",
                "time_limit_sec": 20,
                "w_due_qty": 1000.0,
                "w_due_time": 50.0,
                "w_supply": 300.0,
                "w_over": 20.0,
                "w_mix": 1.0,
                "w_stab": 0.5,
                "forbid_past": True,
            }

        _trace(trace, f"volume_diff: solver_start params={kwargs}")
        logging.getLogger("so_planner.optimize").info(
            "Volume diff start: plan=%s ops=%d params=%s",
            plan_id,
            len(warm_start),
            (req.dict() if req else {}),
        )
        v = solve_volume_milp(
            baseline_ops=warm_start,
            order_meta=order_meta,
            bom_lines=bom_rows,
            stock_by_item=stock_by_item,
            trace=trace,
            **kwargs,
        )
        _trace(
            trace,
            "volume_diff: solver_done "
            f"status={v.get('status')} movable={v.get('orders_movable')} changed={len(v.get('order_changes', []))}",
        )
        res = {
            "ok": True,
            "plan_id": plan_id,
            "criteria": (req.dict() if req else {}),
            "metrics": {"before": v.get("kpi_before"), "after": v.get("kpi_after")},
            "volume": v,
            "trace": trace,
        }
        logging.getLogger("so_planner.optimize").info(
            "Volume diff end: plan=%s status=%s movable=%s changed=%d",
            plan_id,
            str(v.get("status")),
            v.get("orders_movable"),
            len(v.get("order_changes", [])),
        )
        return res
    except Exception as e:  # pylint: disable=broad-except
        _trace(trace, f"volume_diff: failed err={e}")
        logging.getLogger("so_planner.optimize").exception("Volume diff failed: plan=%s", plan_id)
        raise HTTPException(status_code=500, detail=f"Volume diff failed: {e}")


@router.post(
    "/volume/save-preview/{plan_id}",
    summary="Save new volume plan from previously calculated preview result without re-solving",
)
def volume_save_preview(plan_id: int, req: VolumePreviewSaveRequest, db: Session = Depends(get_db)):
    base = _retry_db(lambda: db.get(PlanVersion, plan_id))
    if not base:
        raise HTTPException(404, "base plan not found")

    v = dict(req.volume or {})
    if not isinstance(v.get("order_allocations"), list):
        raise HTTPException(400, "invalid preview volume payload: order_allocations is required")
    if not isinstance(v.get("periods"), list):
        raise HTTPException(400, "invalid preview volume payload: periods is required")

    trace: list[str] = []
    _trace(trace, f"volume_save_preview: base_plan={plan_id}")

    ops = _retry_db(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to materialize from.")
    _trace(trace, f"volume_save_preview: loaded_ops={len(ops)}")

    new_plan = PlanVersion(
        name=f"{base.name} • volume",
        origin="volume_milp",
        status="running",
        parent_plan_id=base.id,
        bom_version_id=base.bom_version_id,
        sales_plan_version_id=getattr(base, "sales_plan_version_id", None),
        notes="Volume MILP plan (saved from preview result)",
    )
    db.add(new_plan)
    db.commit()
    db.refresh(new_plan)
    _trace(trace, f"volume_save_preview: created_new_plan={new_plan.id}")

    try:
        order_meta = _load_order_meta(db, plan_id)
        _trace(trace, f"volume_save_preview: loaded_meta orders={len(order_meta)}")

        # Keep original preview trace and append save/materialization trace for audit.
        preview_trace = list(v.get("trace") or [])
        if preview_trace:
            v["trace"] = [str(x) for x in preview_trace] + list(trace)
        else:
            v["trace"] = list(trace)

        materialized_ops, order_source_map, materialize_stats = _materialize_volume_schedule_ops(
            base_ops=ops,
            dst_plan_id=int(new_plan.id),
            volume=v,
            order_meta=order_meta,
            trace=trace,
        )
        _trace(
            trace,
            f"volume_save_preview: materialized_ops={len(materialized_ops)} base_ops={len(ops)}",
        )
        if materialized_ops:
            db.bulk_save_objects(materialized_ops)

        load_rows = 0
        load_days = 0
        if materialized_ops:
            import pandas as pd

            df_ops_new = pd.DataFrame(
                [
                    {
                        "machine_id": str(getattr(o, "machine_id", "") or ""),
                        "start_ts": getattr(o, "start_ts", None),
                        "end_ts": getattr(o, "end_ts", None),
                        "duration_sec": int(getattr(o, "duration_sec", 0) or 0),
                    }
                    for o in materialized_ops
                ]
            )
            loads_df = compute_daily_loads(df_ops_new)
            if loads_df is not None and not getattr(loads_df, "empty", True):
                load_days = int(loads_df["work_date"].nunique())
                bulk_loads = [
                    MachineLoadDaily(
                        plan_id=int(new_plan.id),
                        machine_id=str(row.machine_id),
                        work_date=row.work_date,
                        load_sec=int(row.load_sec),
                        cap_sec=int(row.cap_sec),
                        util=float(row.util),
                    )
                    for row in loads_df.itertuples(index=False)
                ]
                load_rows = len(bulk_loads)
                if bulk_loads:
                    db.bulk_save_objects(bulk_loads)
        _trace(trace, f"volume_save_preview: materialized_load_rows={load_rows} days={load_days}")

        copied_order_info = _copy_plan_order_info(db, int(plan_id), int(new_plan.id), trace=trace)
        synced_order_info = _sync_plan_order_info_for_volume_plan(
            db,
            plan_id=int(new_plan.id),
            source_meta=order_meta,
            order_source_map=order_source_map,
            trace=trace,
        )
        volume_storage = _persist_volume_result(
            db,
            src_plan_id=int(plan_id),
            dst_plan_id=int(new_plan.id),
            volume=v,
            trace=trace,
        )
        new_plan.status = "ready"
        new_plan.notes = (
            "Volume MILP plan saved from preview result with materialized operation balancing. "
            f"Status={v.get('status')} movable={v.get('orders_movable')} "
            f"alloc_rows={volume_storage.get('order_bucket_rows')} "
            f"ops={len(materialized_ops)}"
        )
        db.commit()
        _trace(trace, f"volume_save_preview: ready plan_id={new_plan.id}")

        logging.getLogger("so_planner.optimize").info(
            "Volume save-preview end: base_plan=%s new_plan=%s status=%s changed=%d",
            plan_id,
            new_plan.id,
            str(v.get("status")),
            len(v.get("order_changes", [])),
        )
        return {
            "ok": True,
            "plan_id": new_plan.id,
            "parent_plan_id": base.id,
            "criteria": dict(req.criteria or {}),
            "report": {"before": v.get("kpi_before"), "after": v.get("kpi_after")},
            "volume": v,
            "volume_only": False,
            "ops": int(len(materialized_ops)),
            "days": int(load_days),
            "materialize": materialize_stats,
            "plan_order_info_rows": int(copied_order_info),
            "plan_order_info_rows_synced": int(synced_order_info),
            "volume_storage": volume_storage,
            "trace": trace,
        }
    except Exception as e:  # pylint: disable=broad-except
        _trace(trace, f"volume_save_preview: failed err={e}")
        new_plan.status = "failed"
        db.commit()
        logging.getLogger("so_planner.optimize").exception(
            "Volume save-preview failed: base_plan=%s new_plan=%s", plan_id, new_plan.id
        )
        raise HTTPException(status_code=500, detail=f"Volume save-preview failed: {e}")


@router.post("/volume/from-plan/{plan_id}", summary="Run aggregate volume MILP from plan and save a new plan copy")
def volume_from_plan(plan_id: int, req: VolumeRequest | None = None, db: Session = Depends(get_db)):
    if solve_volume_milp is None:
        raise HTTPException(status_code=501, detail="Volume MILP solver is not available (missing import).")

    base = _retry_db(lambda: db.get(PlanVersion, plan_id))
    if not base:
        raise HTTPException(404, "base plan not found")
    trace: list[str] = []
    _trace(trace, f"volume_from_plan: base_plan={plan_id}")

    ops = _retry_db(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to warm-start from.")
    _trace(trace, f"volume_from_plan: loaded_ops={len(ops)}")

    warm_start = [
        {
            "order_id": o.order_id,
            "item_id": o.item_id,
            "machine_id": o.machine_id,
            "start_ts": o.start_ts,
            "end_ts": o.end_ts,
            "qty": o.qty,
            "duration_sec": o.duration_sec,
            "setup_sec": o.setup_sec,
            "op_index": o.op_index,
            "batch_id": o.batch_id,
        }
        for o in ops
    ]

    new_plan = PlanVersion(
        name=f"{base.name} • volume",
        origin="volume_milp",
        status="running",
        parent_plan_id=base.id,
        bom_version_id=base.bom_version_id,
        sales_plan_version_id=getattr(base, "sales_plan_version_id", None),
        notes="Volume MILP aggregate plan (operation schedule copied from parent)",
    )
    db.add(new_plan)
    db.commit()
    db.refresh(new_plan)
    _trace(trace, f"volume_from_plan: created_new_plan={new_plan.id}")

    try:
        order_meta = _load_order_meta(db, plan_id)
        stock_by_item = _load_latest_stock(db)
        bom_rows = _load_bom_rows(db, base.bom_version_id)
        _trace(
            trace,
            f"volume_from_plan: loaded_meta orders={len(order_meta)} stock_items={len(stock_by_item)} bom_rows={len(bom_rows)}",
        )

        kwargs = {}
        if req is not None:
            kwargs = {
                "bucket": str(req.bucket or "week"),
                "time_limit_sec": int(req.time_limit_sec or 0),
                "horizon_start": req.horizon_start,
                "horizon_end": req.horizon_end,
                "item_ids": [str(x) for x in (req.item_ids or []) if str(x)],
                "machine_ids": [str(x) for x in (req.machine_ids or []) if str(x)],
                "w_due_qty": float(req.w_due_qty or 0.0),
                "w_due_time": float(req.w_due_time or 0.0),
                "w_supply": float(req.w_supply or 0.0),
                "w_over": float(req.w_over or 0.0),
                "w_mix": float(req.w_mix or 0.0),
                "w_stab": float(req.w_stab or 0.0),
                "forbid_past": True,
            }
        else:
            kwargs = {
                "bucket": "week",
                "time_limit_sec": 20,
                "w_due_qty": 1000.0,
                "w_due_time": 50.0,
                "w_supply": 300.0,
                "w_over": 20.0,
                "w_mix": 1.0,
                "w_stab": 0.5,
                "forbid_past": True,
            }

        _trace(trace, f"volume_from_plan: solver_start params={kwargs}")
        logging.getLogger("so_planner.optimize").info(
            "Volume from-plan start: base_plan=%s ops=%d params=%s -> new_plan=%s",
            plan_id,
            len(warm_start),
            (req.dict() if req else {}),
            new_plan.id,
        )
        v = solve_volume_milp(
            baseline_ops=warm_start,
            order_meta=order_meta,
            bom_lines=bom_rows,
            stock_by_item=stock_by_item,
            trace=trace,
            **kwargs,
        )
        _trace(
            trace,
            "volume_from_plan: solver_done "
            f"status={v.get('status')} movable={v.get('orders_movable')} changed={len(v.get('order_changes', []))}",
        )

        # Materialize balanced operation schedule based on volume allocations.
        materialized_ops, order_source_map, materialize_stats = _materialize_volume_schedule_ops(
            base_ops=ops,
            dst_plan_id=int(new_plan.id),
            volume=v,
            order_meta=order_meta,
            trace=trace,
        )
        _trace(
            trace,
            f"volume_from_plan: materialized_ops={len(materialized_ops)} base_ops={len(ops)}",
        )
        if materialized_ops:
            db.bulk_save_objects(materialized_ops)

        load_rows = 0
        load_days = 0
        if materialized_ops:
            import pandas as pd

            df_ops_new = pd.DataFrame(
                [
                    {
                        "machine_id": str(getattr(o, "machine_id", "") or ""),
                        "start_ts": getattr(o, "start_ts", None),
                        "end_ts": getattr(o, "end_ts", None),
                        "duration_sec": int(getattr(o, "duration_sec", 0) or 0),
                    }
                    for o in materialized_ops
                ]
            )
            loads_df = compute_daily_loads(df_ops_new)
            if loads_df is not None and not getattr(loads_df, "empty", True):
                load_days = int(loads_df["work_date"].nunique())
                bulk_loads = [
                    MachineLoadDaily(
                        plan_id=int(new_plan.id),
                        machine_id=str(row.machine_id),
                        work_date=row.work_date,
                        load_sec=int(row.load_sec),
                        cap_sec=int(row.cap_sec),
                        util=float(row.util),
                    )
                    for row in loads_df.itertuples(index=False)
                ]
                load_rows = len(bulk_loads)
                if bulk_loads:
                    db.bulk_save_objects(bulk_loads)
        _trace(trace, f"volume_from_plan: materialized_load_rows={load_rows} days={load_days}")

        copied_order_info = _copy_plan_order_info(db, int(plan_id), int(new_plan.id), trace=trace)
        synced_order_info = _sync_plan_order_info_for_volume_plan(
            db,
            plan_id=int(new_plan.id),
            source_meta=order_meta,
            order_source_map=order_source_map,
            trace=trace,
        )
        volume_storage = _persist_volume_result(
            db,
            src_plan_id=int(plan_id),
            dst_plan_id=int(new_plan.id),
            volume=v,
            trace=trace,
        )
        new_plan.status = "ready"
        new_plan.notes = (
            "Volume MILP plan with materialized operation balancing and persisted bucket allocation. "
            f"Status={v.get('status')} movable={v.get('orders_movable')} "
            f"alloc_rows={volume_storage.get('order_bucket_rows')} "
            f"ops={len(materialized_ops)}"
        )
        db.commit()
        _trace(trace, f"volume_from_plan: ready plan_id={new_plan.id}")

        logging.getLogger("so_planner.optimize").info(
            "Volume from-plan end: base_plan=%s new_plan=%s status=%s changed=%d",
            plan_id,
            new_plan.id,
            str(v.get("status")),
            len(v.get("order_changes", [])),
        )
        return {
            "ok": True,
            "plan_id": new_plan.id,
            "parent_plan_id": base.id,
            "criteria": (req.dict() if req else {}),
            "report": {"before": v.get("kpi_before"), "after": v.get("kpi_after")},
            "volume": v,
            "volume_only": False,
            "ops": int(len(materialized_ops)),
            "days": int(load_days),
            "materialize": materialize_stats,
            "plan_order_info_rows": int(copied_order_info),
            "plan_order_info_rows_synced": int(synced_order_info),
            "volume_storage": volume_storage,
            "trace": trace,
        }
    except Exception as e:  # pylint: disable=broad-except
        _trace(trace, f"volume_from_plan: failed err={e}")
        new_plan.status = "failed"
        db.commit()
        logging.getLogger("so_planner.optimize").exception(
            "Volume from-plan failed: base_plan=%s new_plan=%s", plan_id, new_plan.id
        )
        raise HTTPException(status_code=500, detail=f"Volume MILP failed: {e}")
