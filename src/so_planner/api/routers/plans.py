# src/so_planner/api/routers/plans.py
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from sqlalchemy import or_, func, text
from sqlalchemy.orm import Session
from ...db import get_db
import os, logging
from ...bom_versioning import (
    article_name_map_from_df,
    bom_df_to_scheduler_df,
    get_resolved_bom_version,
    get_version_rows_df,
)
from ...db.models import PlanVersion, ScheduleOp, MachineLoadDaily, DimMachine
from ...sales_plan_versioning import (
    get_resolved_sales_plan_version,
    get_sales_plan_demand_df,
)
from ...scheduling.greedy_scheduler import (
    run_pipeline,
    _ensure_support_tables,
    load_stock_any,
    build_demand as _build_demand,
    _append_receipts_demand,
)
from ...scheduling.utils import compute_daily_loads
from ...scheduling.greedy.loaders import (
    load_machines as _load_machines,
    load_plan_of_sales,
    load_receipts_any,
)
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from datetime import datetime, timedelta, time

router = APIRouter(prefix="/plans", tags=["plans"])
RECEIPT_ORDER_TOKEN = "-RCPT-"

def _normalize_workshop_tokens(raw: str | None) -> set[str]:
    if not raw:
        return set()
    import re
    parts = re.split(r"[,\s;]+", str(raw))
    tokens = {p.strip().lower() for p in parts if p.strip()}
    digits = {re.sub(r"\D+", "", t) for t in tokens}
    digits = {d for d in digits if d}
    return tokens | digits

def _ensure_plan_order_table(db: Session) -> None:
    """Ensure plan_order_info has columns used by UI (status/start/end/qty)."""
    _ensure_support_tables(db)
    try:
        cols = db.execute(text("PRAGMA table_info(plan_order_info)")).mappings().all()
        def _col_name(row) -> str:
            if isinstance(row, dict):
                return str(row.get("name") or "")
            try:
                return str(row.get("name") or "")
            except Exception:
                pass
            try:
                return str(row[1])
            except Exception:
                return ""
        names = {_col_name(r) for r in cols if _col_name(r)}
        def _add(col: str, ddl: str) -> None:
            if col not in names:
                db.execute(text(f"ALTER TABLE plan_order_info ADD COLUMN {col} {ddl}"))
        _add("start_date", "DATE")
        _add("end_date", "DATE")
        _add("qty", "REAL")
        _add("workshop", "TEXT")
        _add("status", "TEXT")
        _add("fixed_at", "DATETIME")
        _add("updated_at", "DATETIME")
        db.commit()
    except Exception:
        logging.exception("Failed to ensure plan_order_info columns")
        db.rollback()

def _load_order_meta(db: Session, plan_id: int) -> Dict[str, dict]:
    rows = db.execute(
        text("""
            SELECT order_id, status, start_date, end_date, qty, due_date, workshop, fixed_at, updated_at
            FROM plan_order_info WHERE plan_id=:pid
        """),
        {"pid": plan_id},
    ).mappings().all()
    meta: Dict[str, dict] = {}
    for r in rows:
        oid = str(r["order_id"]) if r.get("order_id") is not None else ""
        if not oid:
            continue
        meta[oid] = dict(r)
        meta[oid]["status"] = (r.get("status") or "").lower() or "unfixed"
    return meta


def _machine_base_id(value: str | None) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    return s.split("_", 1)[0]


def _machine_workshop_lookup(db: Session) -> Dict[str, str]:
    rows = db.query(DimMachine.machine_id, DimMachine.name, DimMachine.family).all()
    out: Dict[str, str] = {}
    for mid, name, family in rows:
        wk = str(family or "").strip()
        if not wk:
            continue
        keys = {
            str(mid or "").strip(),
            _machine_base_id(mid),
            str(name or "").strip(),
            _machine_base_id(name),
        }
        for key in keys:
            if key and key not in out:
                out[key] = wk
    return out


def _purge_receipt_orders(db: Session, plan_id: int) -> None:
    """Delete previously generated receipt orders so each run recreates them."""
    like_token = f"%{RECEIPT_ORDER_TOKEN}%"
    db.query(ScheduleOp).filter(
        ScheduleOp.plan_id == plan_id,
        ScheduleOp.order_id.like(like_token),
    ).delete(synchronize_session=False)
    db.execute(
        text("DELETE FROM plan_order_info WHERE plan_id=:pid AND order_id LIKE :pat"),
        {"pid": plan_id, "pat": like_token},
    )
    db.commit()


def _collect_root_orders_from_demand(
    *,
    plan_path: str | None,
    plan_df: pd.DataFrame | None,
    receipts_path: str | None,
    reserved_order_ids: set[str],
    fixed_order_qty: dict[str, float],
    split_child_orders: bool,
) -> Dict[str, dict]:
    """Build root order map (order_id -> qty/due_date) from plan(+receipts) demand."""
    out: Dict[str, dict] = {}
    try:
        source_df = plan_df.copy() if plan_df is not None else load_plan_of_sales(Path(str(plan_path or "")))
        demand = _build_demand(source_df, reserved_order_ids=reserved_order_ids)
        if receipts_path:
            try:
                rp = Path(receipts_path)
                if rp.exists():
                    receipts_df = load_receipts_any(rp)
                    demand, _ = _append_receipts_demand(
                        demand,
                        receipts_df,
                        split_child_orders=split_child_orders,
                        reserved_order_ids=reserved_order_ids,
                        fixed_order_qty=fixed_order_qty,
                    )
            except Exception:
                logging.warning("Failed to load receipts for root demand map: %s", receipts_path)
        if demand is None or demand.empty:
            return out
        for r in demand.itertuples(index=False):
            base_oid = str(getattr(r, "order_id", "") or "")
            item_id = str(getattr(r, "item_id", "") or "")
            if not base_oid or not item_id:
                continue
            root_oid = f"{base_oid}:{item_id}" if split_child_orders else base_oid
            try:
                qty_val = float(getattr(r, "qty", 0) or 0.0)
            except Exception:
                qty_val = 0.0
            due_raw = getattr(r, "due_date", None)
            due_val = None
            try:
                if due_raw is not None and pd.notna(due_raw):
                    due_val = str(pd.to_datetime(due_raw).date())
            except Exception:
                due_val = None
            out[root_oid] = {"qty": qty_val, "due_date": due_val, "base_order_id": base_oid}
    except Exception:
        logging.warning("Failed to build root demand map from source plan")
    return out

def _recompute_plan_loads(db: Session, plan_id: int) -> int:
    ops_rows = (
        db.query(
            ScheduleOp.machine_id,
            ScheduleOp.start_ts,
            ScheduleOp.end_ts,
            ScheduleOp.duration_sec,
        )
        .filter(ScheduleOp.plan_id == plan_id)
        .all()
    )
    db.query(MachineLoadDaily).filter(MachineLoadDaily.plan_id == plan_id).delete(synchronize_session=False)
    if not ops_rows:
        db.commit()
        return 0
    df_ops = pd.DataFrame(ops_rows, columns=["machine_id", "start_ts", "end_ts", "duration_sec"])
    daily = compute_daily_loads(df_ops)
    loads = [
        MachineLoadDaily(
            plan_id=plan_id,
            machine_id=row.machine_id,
            work_date=row.work_date,
            load_sec=int(row.load_sec),
            cap_sec=int(row.cap_sec),
            util=float(row.util),
        )
        for row in daily.itertuples(index=False)
    ]
    db.bulk_save_objects(loads)
    db.commit()
    return len(loads)

def _aggregate_orders_for_plan(
    db: Session,
    plan_id: int,
    item_id: str | None = None,
    workshop: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    order_id: str | None = None,
    limit: int | None = None,
    offset: int = 0,
    with_total: bool = True,
) -> tuple[List[dict], int]:
    import re

    def _parse_date_safe(v):
        if v is None:
            return None
        try:
            return pd.to_datetime(v).date()
        except Exception:
            return None

    def _infer_item_from_order_id(oid: str) -> str:
        s = str(oid or "")
        if ":" in s:
            return s.split(":", 1)[1]
        base = s.split("-", 1)[0]
        return base

    meta = _load_order_meta(db, plan_id)
    wk_tokens = _normalize_workshop_tokens(workshop)
    date_from_dt = None
    date_to_dt = None
    try:
        if date_from:
            date_from_dt = pd.to_datetime(date_from).date()
    except Exception:
        date_from_dt = None
    try:
        if date_to:
            date_to_dt = pd.to_datetime(date_to).date()
    except Exception:
        date_to_dt = None

    q = (
        db.query(
            ScheduleOp.order_id.label("order_id"),
            func.min(ScheduleOp.start_ts).label("start_ts"),
            func.max(ScheduleOp.end_ts).label("end_ts"),
            func.max(ScheduleOp.qty).label("qty"),
            func.min(ScheduleOp.item_id).label("item_id"),
        )
        .filter(ScheduleOp.plan_id == plan_id)
        .group_by(ScheduleOp.order_id)
    )
    if item_id:
        token = f"%{item_id.lower()}%"
        q = q.filter(func.lower(ScheduleOp.item_id).like(token))
    if order_id:
        token = f"%{order_id}%"
        q = q.filter(ScheduleOp.order_id.like(token))
    if date_from_dt:
        q = q.having(func.max(ScheduleOp.end_ts) >= datetime.combine(date_from_dt, datetime.min.time()))
    if date_to_dt:
        q = q.having(func.min(ScheduleOp.start_ts) <= datetime.combine(date_to_dt, datetime.max.time()))

    rows = q.order_by(ScheduleOp.order_id).all()
    out_all: List[dict] = []
    order_ids = [str(r.order_id) for r in rows if r.order_id is not None]
    workshop_by_order: Dict[str, str] = {}
    if order_ids:
        wk_lookup = _machine_workshop_lookup(db)
        wk_rows = (
            db.query(
                ScheduleOp.order_id.label("order_id"),
                ScheduleOp.machine_id.label("machine_id"),
                func.count().label("op_count"),
            )
            .filter(ScheduleOp.plan_id == plan_id)
            .filter(ScheduleOp.order_id.in_(order_ids))
            .group_by(ScheduleOp.order_id, ScheduleOp.machine_id)
            .all()
        )
        counts: Dict[str, Dict[str, int]] = {}
        for wr in wk_rows:
            oid = str(wr.order_id or "")
            mid = str(wr.machine_id or "")
            if not oid or not mid:
                continue
            wk_val = wk_lookup.get(mid) or wk_lookup.get(_machine_base_id(mid))
            if not wk_val:
                continue
            c = counts.setdefault(oid, {})
            c[wk_val] = int(c.get(wk_val, 0)) + int(getattr(wr, "op_count", 0) or 0)
        for oid, c in counts.items():
            if not c:
                continue
            workshop_by_order[oid] = sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    present_order_ids: set[str] = set()
    for r in rows:
        oid = str(r.order_id) if r.order_id is not None else ""
        if not oid:
            continue
        present_order_ids.add(oid)
        start_ts = r.start_ts
        end_ts = r.end_ts
        start_date = start_ts.date() if start_ts else None
        end_date = None
        if end_ts:
            try:
                end_date = (end_ts - timedelta(seconds=1)).date()
            except Exception:
                end_date = end_ts.date()
        m = meta.get(oid, {})
        workshop_val = workshop_by_order.get(oid)
        if not workshop_val and m.get("workshop"):
            workshop_val = str(m.get("workshop"))
        if wk_tokens:
            wk_norm = str(workshop_val or "").strip().lower()
            wk_digits = re.sub(r"\D+", "", wk_norm)
            if wk_norm not in wk_tokens and (not wk_digits or wk_digits not in wk_tokens):
                continue
        qty_val = m.get("qty") if m else None
        if qty_val is None:
            qty_val = float(r.qty) if r.qty is not None else None
        out_all.append(
            {
                "order_id": oid,
                "item_id": str(r.item_id) if r.item_id is not None else "",
                "workshop": workshop_val or "",
                "qty": qty_val,
                "start_date": str(start_date) if start_date else None,
                "end_date": str(end_date) if end_date else None,
                "status": (m.get("status") or "unfixed") if m else "unfixed",
                "due_date": m.get("due_date") if m else None,
            }
        )

    # Include fixed orders without operations (e.g. top-level root orders).
    # They are persisted in plan_order_info but absent in schedule_ops by design.
    for oid, m in meta.items():
        if oid in present_order_ids:
            continue
        status_val = (m.get("status") or "unfixed").lower()
        if status_val != "fixed":
            continue
        if order_id and order_id not in oid:
            continue
        inferred_item = _infer_item_from_order_id(str(oid))
        if item_id and item_id.lower() not in inferred_item.lower():
            continue

        workshop_val = str(m.get("workshop") or "")
        if wk_tokens:
            wk_norm = workshop_val.strip().lower()
            wk_digits = re.sub(r"\D+", "", wk_norm)
            if wk_norm not in wk_tokens and (not wk_digits or wk_digits not in wk_tokens):
                continue

        meta_start = _parse_date_safe(m.get("start_date"))
        meta_end = _parse_date_safe(m.get("end_date"))
        if date_from_dt and meta_end and meta_end < date_from_dt:
            continue
        if date_to_dt and meta_start and meta_start > date_to_dt:
            continue
        if (date_from_dt or date_to_dt) and not meta_start and not meta_end:
            continue

        out_all.append(
            {
                "order_id": str(oid),
                "item_id": inferred_item,
                "workshop": workshop_val,
                "qty": m.get("qty"),
                "start_date": str(meta_start) if meta_start else None,
                "end_date": str(meta_end) if meta_end else None,
                "status": status_val,
                "due_date": m.get("due_date"),
            }
        )

    out_all = sorted(out_all, key=lambda x: str(x.get("order_id") or ""))
    total = len(out_all) if with_total else 0
    start = max(0, int(offset or 0))
    if limit is not None and int(limit) > 0:
        stop = start + int(limit)
        out = out_all[start:stop]
    else:
        out = out_all[start:]
    return out, total

def _load_lag_map(bom_path: str | None) -> dict:
    """Load (parent_item_id, child_item_id) -> lag_days from BOM.xlsx."""
    if not bom_path or not os.path.exists(bom_path):
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(bom_path, sheet_name=0, dtype=object)
        def norm(c: str) -> str:
            return str(c).strip().lower().replace(" ", "").replace("_", "")
        cols = {norm(c): c for c in df.columns}
        item_col = cols.get("itemid") or cols.get("article") or cols.get("item") or list(df.columns)[0]
        parent_col = (
            cols.get("rootitemid")
            or cols.get("rootarticle")
            or cols.get("rootitem")
            or cols.get("parentitemid")
            or cols.get("parentitem")
            or cols.get("parent")
        )
        lag_col = None
        for name in ("lagtime", "lag_time", "lag", "lagdays"):
            if name in cols:
                lag_col = cols[name]; break
        if not lag_col or not parent_col:
            return {}
        df = df[[parent_col, item_col, lag_col]].dropna(subset=[item_col])
        df[parent_col] = df[parent_col].astype(str).str.strip()
        df[lag_col] = pd.to_numeric(df[lag_col], errors="coerce").fillna(0).astype(int)
        df[item_col] = df[item_col].astype(str).str.strip()
        df = df[(df[parent_col] != "") & (df[parent_col] != df[item_col])]
        if df.empty:
            return {}
        out = (
            df.groupby([parent_col, item_col], as_index=False)[lag_col]
              .max()
        )
        return {
            (str(r[parent_col]), str(r[item_col])): int(r[lag_col])
            for _, r in out.iterrows()
        }
    except Exception:
        return {}

def _load_lag_map_from_bom_df(bom_df: pd.DataFrame | None) -> dict:
    """Load (parent_item_id, child_item_id) -> lag_days from normalized BOM df."""
    if bom_df is None or bom_df.empty or "lag_days" not in bom_df.columns:
        return {}
    try:
        tmp = bom_df[["root_item_id", "item_id", "lag_days"]].copy()
        tmp["root_item_id"] = tmp["root_item_id"].astype(str).str.strip()
        tmp["item_id"] = tmp["item_id"].astype(str).str.strip()
        tmp["lag_days"] = pd.to_numeric(tmp["lag_days"], errors="coerce").fillna(0).astype(int)
        tmp = tmp[(tmp["root_item_id"] != "") & (tmp["root_item_id"] != tmp["item_id"])]
        if tmp.empty:
            return {}
        tmp = tmp.groupby(["root_item_id", "item_id"], as_index=False)["lag_days"].max()
        return {
            (str(r.root_item_id), str(r.item_id)): int(r.lag_days)
            for r in tmp.itertuples(index=False)
        }
    except Exception:
        return {}

def _adjust_children_dates(df_ops: pd.DataFrame, fixed_meta: Dict[str, dict], lag_map: dict) -> pd.DataFrame:
    """Clamp children ops within parent window, allowing shift left by lag days from BOM."""
    if df_ops is None or df_ops.empty or not fixed_meta:
        return df_ops
    rows = []
    fixed_parents = {}
    fixed_order_ids: set[str] = set()
    for oid, m in fixed_meta.items():
        if (m.get("status") or "").lower() != "fixed":
            continue
        try:
            ps = pd.to_datetime(m.get("start_date")).to_pydatetime()
        except Exception:
            ps = None
        try:
            pe = pd.to_datetime(m.get("end_date")).to_pydatetime()
        except Exception:
            pe = None
        base = oid.split(":", 1)[0] if ":" in oid else oid
        ent = fixed_parents.get(base)
        if ent is None:
            fixed_parents[base] = {"start": ps, "end": pe}
        else:
            cur_s = ent.get("start")
            cur_e = ent.get("end")
            if ps is not None and (cur_s is None or ps < cur_s):
                ent["start"] = ps
            if pe is not None and (cur_e is None or pe > cur_e):
                ent["end"] = pe
        if ":" in oid:
            fixed_order_ids.add(oid)
    if not fixed_parents:
        return df_ops
    parents_by_child: Dict[str, set[str]] = {}
    for edge, lag in (lag_map or {}).items():
        try:
            p, c = edge
            if int(lag or 0) < 0:
                continue
            parents_by_child.setdefault(str(c), set()).add(str(p))
        except Exception:
            continue
    for r in df_ops.itertuples(index=False):
        oid = str(getattr(r, "order_id", "") or "")
        base = oid.split(":", 1)[0] if ":" in oid else oid
        parent = fixed_parents.get(base)
        if not parent:
            rows.append(r._asdict())
            continue
        item_id = str(getattr(r, "item_id", "") or "")
        if oid == base or oid in fixed_order_ids:
            rows.append(r._asdict())
            continue
        start = getattr(r, "start_ts")
        end = getattr(r, "end_ts")
        dur = int(getattr(r, "duration_sec", 0) or 0)
        if not dur:
            rows.append(r._asdict())
            continue
        lb = parent["start"]
        ub = parent["end"]
        lag_days = 0
        parent_item = None
        candidates = parents_by_child.get(item_id, set())
        if candidates:
            fixed_items = {foid.split(":", 1)[1] for foid in fixed_order_ids if foid.startswith(f"{base}:")}
            by_base = sorted(candidates.intersection(fixed_items))
            if by_base:
                parent_item = by_base[0]
            elif len(candidates) == 1:
                parent_item = next(iter(candidates))
        if parent_item is not None:
            lag_days = int(lag_map.get((str(parent_item), item_id), 0) or 0)
        if lb:
            try:
                lb = lb - timedelta(days=lag_days)
            except Exception:
                pass
        # clamp start/end
        target_start = start
        if lb and target_start < lb:
            target_start = lb
        if ub:
            latest_start = ub - timedelta(seconds=dur)
            if target_start > latest_start:
                target_start = latest_start
        target_end = target_start + timedelta(seconds=dur)
        if ub and target_end > ub:
            target_end = ub
            target_start = target_end - timedelta(seconds=dur)
            if lb and target_start < lb:
                target_start = lb
                target_end = target_start + timedelta(seconds=dur)
        d = r._asdict()
        d["start_ts"] = target_start
        d["end_ts"] = target_end
        rows.append(d)
    return pd.DataFrame(rows)

def _ingest_stock_snapshot(stock_path: str, db: Session) -> Optional[int]:
    """Load stock Excel into stock_snapshot/stock_line; return snapshot id or None."""
    if not stock_path or not os.path.exists(stock_path):
        return None
    try:
        _ensure_support_tables(db)
        df = load_stock_any(Path(stock_path))
        if df is None or df.empty:
            return None
        if "workshop" not in df.columns:
            df["workshop"] = ""
        df = df[["item_id", "stock_qty", "workshop"]].copy()
        df["item_id"] = df["item_id"].astype(str)
        df["workshop"] = df["workshop"].fillna("").astype(str)
        df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)
        df = df.groupby(["item_id", "workshop"], as_index=False)["stock_qty"].sum()

        snap_id = db.execute(
            text("INSERT INTO stock_snapshot (name, taken_at, notes) VALUES (:name, CURRENT_TIMESTAMP, :notes) RETURNING id"),
            {"name": f"Upload {Path(stock_path).name}", "notes": stock_path},
        ).scalar_one()
        rows = [
            {"snapshot_id": int(snap_id), "item_id": r.item_id, "workshop": r.workshop, "stock_qty": int(r.stock_qty)}
            for r in df.itertuples(index=False)
        ]
        if rows:
            db.execute(
                text("INSERT INTO stock_line (snapshot_id,item_id,workshop,stock_qty) VALUES (:snapshot_id,:item_id,:workshop,:stock_qty)"),
                rows,
            )
        db.commit()
        return int(snap_id)
    except Exception as e:
        logging.warning("Stock ingest failed: %s", e)
        return None

class PlanCreate(BaseModel):
    name: str
    notes: str | None = None
    parent_plan_id: int | None = None
    bom_version_id: int | None = None
    sales_plan_version_id: int | None = None

@router.get("", summary="List plan versions")
def list_plans(db: Session = Depends(get_db)):
    plans = db.query(PlanVersion).order_by(PlanVersion.created_at.desc()).all()
    return [
        {
            "id": p.id,
            "name": p.name,
            "origin": p.origin,
            "status": p.status,
            "created_at": p.created_at,
            "parent_plan_id": p.parent_plan_id,
            "bom_version_id": p.bom_version_id,
            "sales_plan_version_id": p.sales_plan_version_id,
        }
        for p in plans
    ]

@router.post("", summary="Create new plan version")
def create_plan(body: PlanCreate, db: Session = Depends(get_db)):
    plan = PlanVersion(
        name=body.name,
        notes=body.notes,
        parent_plan_id=body.parent_plan_id,
        bom_version_id=body.bom_version_id,
        sales_plan_version_id=body.sales_plan_version_id,
        status="draft",
    )
    db.add(plan)
    db.commit()
    db.refresh(plan)
    return {"id": plan.id, "name": plan.name, "status": plan.status}

class RunGreedyRequest(BaseModel):
    plan_path: str | None = None
    bom_path: str | None = None
    machines_path: str | None = None
    stock_path: str | None = None
    receipts_path: str | None = None
    mode: str | None = None
    bom_version_id: int | None = None
    sales_plan_version_id: int | None = None

class OrderUpdate(BaseModel):
    order_id: str
    start_date: str | None = None
    end_date: str | None = None
    qty: float | None = None

class OrdersUpdateRequest(BaseModel):
    orders: List[OrderUpdate]

@router.post("/{plan_id}/approve", summary="Mark plan as approved")
def approve_plan(plan_id: int, db: Session = Depends(get_db)):
    plan = db.get(PlanVersion, plan_id)
    if not plan:
        raise HTTPException(404, "plan not found")
    plan.status = "approved"
    db.commit()
    return {"id": plan.id, "name": plan.name, "status": plan.status}

@router.post("/{plan_id}/run-greedy", summary="Run greedy and save into plan")
def run_greedy_into_plan(plan_id: int, body: RunGreedyRequest | None = None, db: Session = Depends(get_db)):
    plan = db.get(PlanVersion, plan_id)
    if not plan:
        raise HTTPException(404, "plan not found")
    plan.status = "running"
    db.commit()
    _ensure_plan_order_table(db)
    _purge_receipt_orders(db, plan_id)
    meta = _load_order_meta(db, plan_id)
    fixed_orders = {oid for oid, m in meta.items() if (m.get("status") or "").lower() == "fixed"}
    deleted_orders = {oid for oid, m in meta.items() if (m.get("status") or "").lower() == "deleted"}
    fixed_order_qty = {}
    for oid in fixed_orders:
        try:
            qty_val = meta.get(oid, {}).get("qty")
            if qty_val is None:
                continue
            fixed_order_qty[str(oid)] = float(qty_val)
        except Exception:
            continue

    try:
        # 1) Запуск вашего пайплайна. При необходимости пути сделайте настраиваемыми.
        # Use stock if present (default stock.xlsx or env override)
        stock_path_env = os.environ.get("SOPLANNER_STOCK_PATH", "stock.xlsx")
        receipts_path_env = os.environ.get("SOPLANNER_RECEIPTS_PATH", "receipts.xlsx")
        plan_path = (body.plan_path if body else None) or "plan of sales.xlsx"
        machines_path = (body.machines_path if body else None) or "machines.xlsx"
        stock_path = (body.stock_path if body else None) or stock_path_env
        requested_sales_plan_version_id = (body.sales_plan_version_id if body else None) or plan.sales_plan_version_id
        sales_plan_df = None
        resolved_sales_plan_version_id: int | None = None
        if requested_sales_plan_version_id is not None:
            sales_ver = get_resolved_sales_plan_version(db, int(requested_sales_plan_version_id))
            sales_plan_df = get_sales_plan_demand_df(db, int(sales_ver.id))
            if sales_plan_df is None or sales_plan_df.empty:
                raise RuntimeError(f"Sales plan version {sales_ver.id} has no rows")
            resolved_sales_plan_version_id = int(sales_ver.id)
        receipts_field_set = False
        if body is not None:
            fs = getattr(body, "model_fields_set", None)
            if fs is None:
                fs = getattr(body, "__fields_set__", set())
            try:
                receipts_field_set = "receipts_path" in fs
            except Exception:
                receipts_field_set = False
        receipts_path = (body.receipts_path if body is not None else None) if receipts_field_set else receipts_path_env
        mode = (body.mode if body else None) or ""
        root_order_seed_map = _collect_root_orders_from_demand(
            plan_path=plan_path,
            plan_df=sales_plan_df,
            receipts_path=receipts_path if receipts_path else None,
            reserved_order_ids=deleted_orders,
            fixed_order_qty=fixed_order_qty,
            split_child_orders=True,
        )
        requested_bom_version_id = (body.bom_version_id if body else None) or plan.bom_version_id
        bom_version = get_resolved_bom_version(db, requested_bom_version_id)
        bom_rows_df = get_version_rows_df(db, int(bom_version.id))
        scheduler_bom_df = bom_df_to_scheduler_df(bom_rows_df)
        if scheduler_bom_df.empty:
            raise RuntimeError(f"BOM version {bom_version.id} is empty or invalid for scheduling")
        article_name_map = article_name_map_from_df(bom_rows_df)

        # Refresh stock snapshot in DB so reports use the uploaded file
        try:
            _ingest_stock_snapshot(stock_path, db)
        except Exception:
            logging.warning("Stock ingest inside run_greedy_into_plan failed for %s", stock_path)

        out_xlsx, sched_df = run_pipeline(
            plan_path,
            None,
            machines_path,
            "schedule_out.xlsx",
            stock_path=stock_path if stock_path else None,
            receipts_path=receipts_path if receipts_path else None,
            split_child_orders=True,
            align_roots_to_due=True,
            reserved_order_ids=deleted_orders,
            fixed_order_qty=fixed_order_qty,
            mode=mode,
            bom_df=scheduler_bom_df,
            plan_df=sales_plan_df,
            plan_version_id=plan.id,
            db=db,
        )

        # 2) Конверсия дневных слотов расписания -> операции для БД
        # ожидаемые поля в sched_df: order_id,item_id,step,machine_id,date,minutes,qty,...
        import pandas as pd

        if sched_df is None or sched_df.empty:
            raise RuntimeError("Greedy вернул пустое расписание.")

        df_ops = sched_df.copy()
        df_ops["start_ts"] = pd.to_datetime(df_ops["date"])
        df_ops["end_ts"] = pd.to_datetime(df_ops["date"]) + pd.to_timedelta(1, unit="D")
        df_ops["duration_sec"] = (
            pd.to_numeric(df_ops.get("minutes", 0), errors="coerce").fillna(0.0) * 60
        ).astype(int)
        df_ops["setup_sec"] = 0
        df_ops["op_index"] = (
            pd.to_numeric(df_ops.get("step", 1), errors="coerce").fillna(1).astype(int)
        )
        df_ops["batch_id"] = ""
        df_ops["qty"] = pd.to_numeric(df_ops.get("qty", 0), errors="coerce").fillna(0.0)
        # Сдвиг дат детей относительно зафиксированного родителя с учётом лагов из BOM
        lag_map = _load_lag_map_from_bom_df(scheduler_bom_df)
        df_ops = _adjust_children_dates(df_ops, meta, lag_map)
        due_map_all = {}
        if "due_date" in df_ops.columns:
            try:
                due_map_all = (
                    df_ops[["order_id", "due_date"]]
                    .dropna()
                    .drop_duplicates(subset=["order_id"])
                    .set_index("order_id")["due_date"]
                    .to_dict()
                )
                due_map_all = {str(k): v for k, v in due_map_all.items()}
            except Exception:
                due_map_all = {}

        # Убираем ранее рассчитанные операции для нефикисрованных заказов
        q_del = db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan.id)
        if fixed_orders:
            q_del = q_del.filter(~ScheduleOp.order_id.in_(fixed_orders))
        q_del.delete(synchronize_session=False)
        db.query(MachineLoadDaily).filter(MachineLoadDaily.plan_id == plan.id).delete(synchronize_session=False)
        db.commit()

        skip_orders = fixed_orders | deleted_orders
        if skip_orders:
            df_ops = df_ops[~df_ops["order_id"].astype(str).isin(skip_orders)].copy()

        # 3) Массовая вставка операций
        ops = []
        for r in df_ops.itertuples(index=False):
            article_name = article_name_map.get(str(getattr(r, "item_id", "") or "")) or None
            ops.append(
                ScheduleOp(
                    plan_id=plan.id,
                    order_id=str(r.order_id),
                    item_id=str(r.item_id),
                    article_name=article_name,
                    machine_id=str(r.machine_id),
                    start_ts=r.start_ts,
                    end_ts=r.end_ts,
                    qty=float(getattr(r, "qty", 0) or 0.0),
                    duration_sec=int(r.duration_sec),
                    setup_sec=int(getattr(r, "setup_sec", 0) or 0),
                    op_index=int(getattr(r, "op_index", 0) or 0),
                    batch_id=str(getattr(r, "batch_id", "") or ""),
                )
            )
        if ops:
            db.bulk_save_objects(ops)
            db.commit()

        # 4) Обновляем метаданные заказов (только для нефикисрованных)
        agg_rows = []
        if not df_ops.empty:
            df_ops["_start_date"] = pd.to_datetime(df_ops["start_ts"]).dt.date
            df_ops["_end_date"] = (pd.to_datetime(df_ops["end_ts"]) - pd.to_timedelta(1, unit="D")).dt.date
            due_map = {}
            if "due_date" in df_ops.columns:
                try:
                    due_map_raw = (
                        df_ops[["order_id", "due_date"]]
                        .dropna()
                        .drop_duplicates(subset=["order_id"])
                        .set_index("order_id")["due_date"]
                        .to_dict()
                    )
                    due_map = {str(k): v for k, v in due_map_raw.items()}
                except Exception:
                    due_map = {}
            wk_map = {}
            if "workshop" in df_ops.columns:
                try:
                    wk_map = (
                        df_ops[["order_id", "workshop"]]
                        .dropna()
                        .drop_duplicates(subset=["order_id"])
                        .set_index("order_id")["workshop"]
                        .to_dict()
                    )
                except Exception:
                    wk_map = {}
            for oid, grp in df_ops.groupby("order_id"):
                sd = grp["_start_date"].min()
                ed = grp["_end_date"].max()
                qty_val = None
                if "qty" in grp.columns:
                    try:
                        qv = pd.to_numeric(grp["qty"], errors="coerce").max()
                        qty_val = float(qv) if pd.notna(qv) else None
                    except Exception:
                        qty_val = None
                agg_rows.append(
                    {
                        "plan_id": plan.id,
                        "order_id": str(oid),
                        "start_date": str(sd) if sd is not None else None,
                        "end_date": str(ed) if ed is not None else None,
                        "workshop": wk_map.get(str(oid)) if wk_map else "",
                        "qty": qty_val,
                        "status": "unfixed",
                        "due_date": due_map.get(str(oid)) if due_map else None,
                    }
                )
        if agg_rows:
            db.execute(
                text("""
                    INSERT INTO plan_order_info (plan_id, order_id, start_date, end_date, qty, workshop, status, due_date, updated_at)
                    VALUES (:plan_id, :order_id, :start_date, :end_date, :qty, :workshop, :status, :due_date, CURRENT_TIMESTAMP)
                    ON CONFLICT(plan_id, order_id) DO UPDATE SET
                      start_date=excluded.start_date,
                      end_date=excluded.end_date,
                      qty=excluded.qty,
                      workshop=COALESCE(plan_order_info.workshop, excluded.workshop),
                      due_date=CASE
                        WHEN plan_order_info.status='fixed' THEN plan_order_info.due_date
                        ELSE excluded.due_date
                      END,
                      status=CASE WHEN plan_order_info.status='fixed' THEN plan_order_info.status ELSE excluded.status END,
                      updated_at=CURRENT_TIMESTAMP
                """),
                agg_rows,
            )
            db.commit()
        if fixed_orders and due_map_all:
            try:
                upd_rows = []
                for oid in fixed_orders:
                    due_val = due_map_all.get(str(oid))
                    if due_val is None:
                        continue
                    upd_rows.append(
                        {"plan_id": plan.id, "order_id": str(oid), "due_date": str(due_val)}
                    )
                if upd_rows:
                    db.execute(
                        text("""
                            UPDATE plan_order_info
                            SET due_date=:due_date, updated_at=CURRENT_TIMESTAMP
                            WHERE plan_id=:plan_id AND order_id=:order_id
                        """),
                        upd_rows,
                    )
                    db.commit()
            except Exception:
                db.rollback()
        # Автофикс корневых заказов (обеспечивают план продаж)
        try:
            agg_map = {r["order_id"]: r for r in agg_rows}
            base_window_map: Dict[str, dict] = {}
            if not df_ops.empty:
                tmp = df_ops.copy()
                tmp["__base_order_id"] = tmp["order_id"].astype(str).str.split(":", n=1).str[0]
                for base_id, grp in tmp.groupby("__base_order_id"):
                    sd = grp["_start_date"].min() if "_start_date" in grp.columns else None
                    ed = grp["_end_date"].max() if "_end_date" in grp.columns else None
                    base_window_map[str(base_id)] = {
                        "start_date": str(sd) if sd is not None else None,
                        "end_date": str(ed) if ed is not None else None,
                    }
            roots = []
            for root_oid, seed in root_order_seed_map.items():
                base_id = str(seed.get("base_order_id") or str(root_oid).split(":", 1)[0])
                agg_row = agg_map.get(str(root_oid), {})
                win = base_window_map.get(base_id, {})
                qty_val = agg_row.get("qty") if agg_row.get("qty") is not None else seed.get("qty")
                due_val = agg_row.get("due_date") if agg_row.get("due_date") is not None else seed.get("due_date")
                roots.append(
                    {
                        "plan_id": plan.id,
                        "order_id": str(root_oid),
                        "start_date": agg_row.get("start_date") or win.get("start_date"),
                        "end_date": agg_row.get("end_date") or win.get("end_date"),
                        "workshop": agg_row.get("workshop") or "",
                        "qty": qty_val,
                        "status": "fixed",
                        "due_date": due_val,
                    }
                )
            if roots:
                db.execute(
                    text("""
                        INSERT INTO plan_order_info (plan_id, order_id, start_date, end_date, qty, workshop, status, fixed_at, due_date, updated_at)
                        VALUES (:plan_id, :order_id, :start_date, :end_date, :qty, :workshop, 'fixed', CURRENT_TIMESTAMP, :due_date, CURRENT_TIMESTAMP)
                        ON CONFLICT(plan_id, order_id) DO UPDATE SET
                          start_date=excluded.start_date,
                          end_date=excluded.end_date,
                          qty=excluded.qty,
                          workshop=COALESCE(plan_order_info.workshop, excluded.workshop),
                          status='fixed',
                          fixed_at=COALESCE(plan_order_info.fixed_at, excluded.fixed_at),
                          due_date=COALESCE(plan_order_info.due_date, excluded.due_date),
                          updated_at=CURRENT_TIMESTAMP
                    """),
                    roots,
                )
                db.commit()
        except Exception:
            db.rollback()

        # Удаляем устаревшие нефисксированные заказы, которых нет в новом расписании
        try:
            keep_ids = set(df_ops["order_id"].astype(str).unique().tolist()) if not df_ops.empty else set()
            keep_ids |= set(fixed_orders)
            if keep_ids:
                placeholders = ",".join([f":k{i}" for i in range(len(keep_ids))])
                params = {f"k{i}": v for i, v in enumerate(keep_ids)}
                params["pid"] = plan.id
                db.execute(text(f"""
                    DELETE FROM plan_order_info
                    WHERE plan_id=:pid AND status!='fixed' AND order_id NOT IN ({placeholders})
                """), params)
            else:
                db.execute(text("DELETE FROM plan_order_info WHERE plan_id=:pid AND status!='fixed'"), {"pid": plan.id})
            db.commit()
        except Exception:
            db.rollback()

        # 5) Агрегаты для heatmap
        loads_count = _recompute_plan_loads(db, plan.id)
        ops_total = int(db.query(func.count()).select_from(ScheduleOp).filter(ScheduleOp.plan_id == plan.id).scalar() or 0)
        days = int(
            db.query(func.count(func.distinct(MachineLoadDaily.work_date)))
            .filter(MachineLoadDaily.plan_id == plan.id)
            .scalar()
            or 0
        )

        plan.origin = "greedy"
        plan.status = "ready"
        plan.bom_version_id = int(bom_version.id)
        if resolved_sales_plan_version_id is not None:
            plan.sales_plan_version_id = int(resolved_sales_plan_version_id)
        db.commit()
        return {
            "ok": True,
            "plan_id": plan.id,
            "bom_version_id": int(bom_version.id),
            "sales_plan_version_id": int(plan.sales_plan_version_id) if plan.sales_plan_version_id is not None else None,
            "ops": ops_total,
            "days": days,
            "loads": loads_count,
        }
    except Exception:
        plan.status = "failed"
        db.commit()
        raise

@router.get("/{plan_id}/schedule", summary="Get scheduled ops")
def get_schedule(
    plan_id: int,
    limit: int = Query(1000, le=10000),
    offset: int = 0,
    workshop: str | None = Query(
        None,
        description="Optional list of workshops (DimMachine.family), comma/space/semicolon separated",
    ),
    db: Session = Depends(get_db),
):
    wk_tokens = _normalize_workshop_tokens(workshop)
    wk_prefixes = {t for t in wk_tokens if t.isdigit()}
    q = (
        db.query(ScheduleOp)
        .filter(ScheduleOp.plan_id == plan_id)
    )
    if wk_tokens:
        conds = [func.lower(func.trim(DimMachine.family)).in_(wk_tokens)]
        if wk_prefixes:
            conds.append(or_(*[ScheduleOp.machine_id.like(f"{p}%") for p in sorted(wk_prefixes)]))
        q = (
            q.join(DimMachine, ScheduleOp.machine_id == DimMachine.machine_id, isouter=True)
             .filter(or_(*conds))
        )
    q = q.order_by(ScheduleOp.start_ts).offset(offset).limit(limit)
    return [
        {
            "order_id": r.order_id,
            "item_id": r.item_id,
            "article_name": r.article_name,
            "machine_id": r.machine_id,
            "start_ts": r.start_ts,
            "end_ts": r.end_ts,
            "qty": r.qty,
            "duration_sec": r.duration_sec,
            "setup_sec": r.setup_sec,
            "op_index": r.op_index,
            "batch_id": r.batch_id,
        }
        for r in q.all()
    ]

@router.get("/{plan_id}/orders/{order_id}/ops", summary="Get all operations for one order in a plan")
def get_order_ops(plan_id: int, order_id: str, db: Session = Depends(get_db)):
    rows = (
        db.query(ScheduleOp)
        .filter(ScheduleOp.plan_id == plan_id)
        .filter(ScheduleOp.order_id == order_id)
        .order_by(ScheduleOp.start_ts)
        .all()
    )
    return [
        {
            "order_id": r.order_id,
            "item_id": r.item_id,
            "article_name": r.article_name,
            "machine_id": r.machine_id,
            "start_ts": r.start_ts,
            "end_ts": r.end_ts,
            "qty": r.qty,
            "duration_sec": r.duration_sec,
            "setup_sec": r.setup_sec,
            "op_index": r.op_index,
            "batch_id": r.batch_id,
        }
        for r in rows
    ]

@router.get("/{plan_id}/orders", summary="List plan orders (aggregated from schedule_ops)")
def list_plan_orders(
    plan_id: int,
    item_id: str | None = Query(default=None, description="Filter by item_id substring"),
    workshop: str | None = Query(default=None, description="Filter by workshop (family)"),
    date_from: str | None = Query(default=None, description="Start date inclusive"),
    date_to: str | None = Query(default=None, description="End date inclusive"),
    order_id: str | None = Query(default=None, description="Filter by order_id substring"),
    limit: int = Query(default=200, ge=1, le=5000, description="Page size"),
    offset: int = Query(default=0, ge=0, description="Page offset"),
    with_total: bool = Query(default=True, description="Include total count"),
    db: Session = Depends(get_db),
):
    if not db.get(PlanVersion, plan_id):
        raise HTTPException(404, "plan not found")
    _ensure_plan_order_table(db)
    orders, total = _aggregate_orders_for_plan(
        db,
        plan_id,
        item_id=item_id,
        workshop=workshop,
        date_from=date_from,
        date_to=date_to,
        order_id=order_id,
        limit=limit,
        offset=offset,
        with_total=with_total,
    )
    return {
        "status": "ok",
        "count": len(orders),
        "total": int(total) if with_total else None,
        "limit": limit,
        "offset": offset,
        "orders": orders,
    }

@router.patch("/{plan_id}/orders", summary="Update order dates/qty and mark as fixed")
def update_plan_orders(plan_id: int, payload: OrdersUpdateRequest, db: Session = Depends(get_db)):
    if not db.get(PlanVersion, plan_id):
        raise HTTPException(404, "plan not found")
    _ensure_plan_order_table(db)
    if not payload.orders:
        return {"status": "ok", "updated": 0, "skipped": 0}

    updated: List[dict] = []
    skipped = 0
    meta = _load_order_meta(db, plan_id)
    fixed_orders = {oid for oid, m in meta.items() if (m.get("status") or "").lower() == "fixed"}

    for upd in payload.orders:
        oid = (upd.order_id or "").strip()
        if not oid:
            skipped += 1
            continue
        if oid in fixed_orders:
            # already fixed; allow updating children on rerun but keep the fixed ops
            pass
        ops = (
            db.query(ScheduleOp)
            .filter(ScheduleOp.plan_id == plan_id)
            .filter(ScheduleOp.order_id == oid)
            .order_by(ScheduleOp.start_ts)
            .all()
        )
        if not ops:
            skipped += 1
            continue
        old_start = min((o.start_ts for o in ops if o.start_ts), default=None)
        old_end_raw = max((o.end_ts for o in ops if o.end_ts), default=None)
        if old_start is None or old_end_raw is None:
            skipped += 1
            continue
        old_start_date = old_start.date()
        try:
            old_end_date = (old_end_raw - timedelta(seconds=1)).date()
        except Exception:
            old_end_date = old_end_raw.date()
        new_start_date = old_start_date
        new_end_date = old_end_date
        try:
            if upd.start_date:
                new_start_date = pd.to_datetime(upd.start_date).date()
        except Exception:
            pass
        try:
            if upd.end_date:
                new_end_date = pd.to_datetime(upd.end_date).date()
        except Exception:
            pass
        if new_start_date and new_end_date and new_end_date < new_start_date:
            raise HTTPException(status_code=400, detail={"msg": f"end_date < start_date for order {oid}"})
        old_qty = float(ops[0].qty or 0)
        new_qty = float(upd.qty) if upd.qty is not None else old_qty
        ratio = 1.0
        if old_qty:
            try:
                ratio = new_qty / old_qty
            except Exception:
                ratio = 1.0
        span_old = max(1, (new_end_date - new_start_date).days) if new_end_date and new_start_date else 1
        span_src = max(1, (old_end_date - old_start_date).days)

        for op in ops:
            op_date = op.start_ts.date() if op.start_ts else old_start_date
            pos_days = (op_date - old_start_date).days if old_start_date else 0
            rel = pos_days / span_src if span_src else 0
            target_day = new_start_date + timedelta(days=round(rel * span_old)) if new_start_date else op_date
            if new_start_date and target_day < new_start_date:
                target_day = new_start_date
            if new_end_date and target_day > new_end_date:
                target_day = new_end_date
            start_ts = datetime.combine(target_day, op.start_ts.time() if op.start_ts else datetime.min.time())
            dur = op.duration_sec or 0
            if ratio and dur:
                try:
                    dur = int(round(dur * ratio))
                except Exception:
                    pass
            if dur <= 0:
                dur = op.duration_sec or 0 or 1
            end_ts = start_ts + timedelta(seconds=dur)
            op.start_ts = start_ts
            op.end_ts = end_ts
            op.duration_sec = dur
            op.qty = new_qty
        updated.append(
            {
                "order_id": oid,
                "start_date": str(new_start_date) if new_start_date else None,
                "end_date": str(new_end_date) if new_end_date else None,
                "qty": new_qty,
                "workshop": meta.get(oid, {}).get("workshop"),
            }
        )
    db.commit()

    if updated:
        now_rows = []
        for u in updated:
            due = meta.get(u["order_id"], {}).get("due_date")
            wk = meta.get(u["order_id"], {}).get("workshop")
            now_rows.append(
                {
                    "plan_id": plan_id,
                    "order_id": u["order_id"],
                    "start_date": u["start_date"],
                    "end_date": u["end_date"],
                    "qty": u["qty"],
                    "workshop": wk,
                    "status": "fixed",
                    "due_date": due,
                }
            )
        db.execute(
            text("""
                INSERT INTO plan_order_info (plan_id, order_id, start_date, end_date, qty, workshop, status, fixed_at, due_date, updated_at)
                VALUES (:plan_id, :order_id, :start_date, :end_date, :qty, :workshop, :status, CURRENT_TIMESTAMP, :due_date, CURRENT_TIMESTAMP)
                ON CONFLICT(plan_id, order_id) DO UPDATE SET
                  start_date=excluded.start_date,
                  end_date=excluded.end_date,
                  qty=excluded.qty,
                  workshop=COALESCE(plan_order_info.workshop, excluded.workshop),
                  status='fixed',
                  fixed_at=COALESCE(plan_order_info.fixed_at, excluded.fixed_at),
                  due_date=COALESCE(plan_order_info.due_date, excluded.due_date),
                  updated_at=CURRENT_TIMESTAMP
            """),
            now_rows,
        )
        db.commit()

    loads = _recompute_plan_loads(db, plan_id)
    total_ops = int(db.query(func.count()).select_from(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).scalar() or 0)
    return {"status": "ok", "updated": len(updated), "skipped": skipped, "loads": loads, "ops": total_ops}

@router.delete("/{plan_id}/orders/{order_id}", summary="Delete order from schedule and mark as deleted")
def delete_plan_order(plan_id: int, order_id: str, db: Session = Depends(get_db)):
    if not db.get(PlanVersion, plan_id):
        raise HTTPException(404, "plan not found")
    _ensure_plan_order_table(db)
    oid = (order_id or "").strip()
    if not oid:
        raise HTTPException(400, "order_id is required")
    removed_ops = (
        db.query(ScheduleOp)
        .filter(ScheduleOp.plan_id == plan_id)
        .filter(ScheduleOp.order_id == oid)
        .delete(synchronize_session=False)
    )
    db.execute(
        text("""
            INSERT INTO plan_order_info (plan_id, order_id, status, updated_at)
            VALUES (:plan_id, :order_id, 'deleted', CURRENT_TIMESTAMP)
            ON CONFLICT(plan_id, order_id) DO UPDATE SET
              status='deleted',
              updated_at=CURRENT_TIMESTAMP
        """),
        {"plan_id": plan_id, "order_id": oid},
    )
    db.commit()
    loads = _recompute_plan_loads(db, plan_id)
    total_ops = int(db.query(func.count()).select_from(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).scalar() or 0)
    return {"status": "ok", "removed_ops": int(removed_ops), "loads": loads, "ops": total_ops}

@router.get("/{plan_id}/heatmap", summary="Matrix for UI heatmap")
def get_heatmap(
    plan_id: int,
    period: str = Query("day", description="Aggregation period: day|week|month"),
    workshop: str | None = Query(
        None,
        description="Optional list of workshops (DimMachine.family), comma/space/semicolon separated",
    ),
    db: Session = Depends(get_db),
):
    period = (period or "day").lower()
    if period not in ("day", "week", "month"):
        period = "day"

    wk_tokens = _normalize_workshop_tokens(workshop)
    wk_prefixes = {t for t in wk_tokens if t.isdigit()}

    # 1) Load schedule ops for the plan
    ops_q = db.query(
        ScheduleOp.machine_id,
        ScheduleOp.start_ts,
        ScheduleOp.end_ts,
        ScheduleOp.duration_sec,
    ).filter(ScheduleOp.plan_id == plan_id)
    if wk_tokens:
        conds = [func.lower(func.trim(DimMachine.family)).in_(wk_tokens)]
        if wk_prefixes:
            conds.append(or_(*[ScheduleOp.machine_id.like(f"{p}%") for p in sorted(wk_prefixes)]))
        ops_q = (
            ops_q.join(DimMachine, ScheduleOp.machine_id == DimMachine.machine_id, isouter=True)
                 .filter(or_(*conds))
        )
    ops_rows = ops_q.all()

    if not ops_rows:
        # Fallback: return whatever is in machine_load_daily (possibly empty)
        loads_q = db.query(MachineLoadDaily).filter(MachineLoadDaily.plan_id == plan_id)
        if wk_tokens:
            conds = [func.lower(func.trim(DimMachine.family)).in_(wk_tokens)]
            if wk_prefixes:
                conds.append(or_(*[MachineLoadDaily.machine_id.like(f"{p}%") for p in sorted(wk_prefixes)]))
            loads_q = (
                loads_q.join(DimMachine, MachineLoadDaily.machine_id == DimMachine.machine_id, isouter=True)
                       .filter(or_(*conds))
            )
        rows = loads_q.all()
        machine_ids = sorted({str(r.machine_id) for r in rows})
        dates = sorted({r.work_date.date() for r in rows})
        util = {(str(r.machine_id), r.work_date.date()): r.util for r in rows}
    else:
        # 2) Compute per-day loads by splitting on midnight boundaries
        df_ops = pd.DataFrame(ops_rows, columns=["machine_id", "start_ts", "end_ts", "duration_sec"])

        # Try to load per-machine capacity_per_day from machines.xlsx using greedy loader
        cap_map_sec: dict[str, int] | None = None
        try:
            mdf = _load_machines(Path("machines.xlsx"))  # capacity_per_day may be in hours/minutes/seconds
            if not mdf.empty and "machine_id" in mdf.columns and "capacity_per_day" in mdf.columns:
                grp = mdf.groupby("machine_id", as_index=True)["capacity_per_day"].max().to_dict()
                def to_sec(x: float) -> int:
                    try:
                        v = float(x)
                    except Exception:
                        return 0
                    # Heuristic: if >= 24*60 -> already seconds; if >= 24 -> minutes; else -> hours
                    if v >= 24 * 60:
                        return int(round(v))
                    if v >= 24:
                        return int(round(v * 60.0))
                    return int(round(v * 3600.0))
                cap_map_sec = {str(k): max(0, to_sec(v)) for k, v in grp.items()}
        except Exception:
            cap_map_sec = None

        daily = compute_daily_loads(df_ops, cap_map_sec)

        # 3) Aggregate to selected period
        if period == "day":
            agg = daily.copy()
            agg["period_start"] = agg["work_date"].dt.date
            cap_col = "cap_sec"
        else:
            def week_start(d: datetime) -> datetime:
                base = d.replace(hour=0, minute=0, second=0, microsecond=0)
                return base - timedelta(days=base.weekday())

            def month_start(d: datetime) -> datetime:
                return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            agg = daily.copy()
            if period == "week":
                agg["period_start"] = agg["work_date"].apply(lambda x: week_start(x))
            else:
                agg["period_start"] = agg["work_date"].apply(lambda x: month_start(x))
            agg = (
                agg.groupby(["machine_id", "period_start"], as_index=False)[["load_sec", "cap_sec"]]
                .sum()
            )
            cap_col = "cap_sec"

        machine_ids = sorted({str(m) for m in agg["machine_id"].unique().tolist()})
        dates = sorted({(pd.Timestamp(d).to_pydatetime().date() if isinstance(d, pd.Timestamp) else (d.date() if isinstance(d, datetime) else d)) for d in agg["period_start"].tolist()})
        # Compute utilization per machine/period
        util_map = {}
        for row in agg.itertuples(index=False):
            mid = str(getattr(row, "machine_id"))
            pstart = getattr(row, "period_start")
            if isinstance(pstart, pd.Timestamp):
                pstart = pstart.to_pydatetime()
            dkey = pstart.date() if isinstance(pstart, datetime) else pstart
            load_sec = float(getattr(row, "load_sec", 0) or 0)
            cap_sec = float(getattr(row, cap_col, 0) or 0)
            u = (load_sec / cap_sec) if cap_sec > 0 else 0.0
            util_map[(mid, dkey)] = min(max(u, 0.0), 10.0)
        util = util_map

    machine_ids = sorted({str(m) for m in machine_ids})
    base_ids = {mid.split("_")[0] for mid in machine_ids}

    # 4) Machine labels
    machine_ids = sorted({str(m) for m in machine_ids})
    base_ids = {mid.split("_")[0] for mid in machine_ids}

    name_rows = (
        db.query(DimMachine.machine_id, DimMachine.name, DimMachine.family)
        .filter(
            or_(
                DimMachine.machine_id.in_(machine_ids),
                DimMachine.name.in_(base_ids),
            )
        )
        .all()
        if machine_ids
        else []
    )
    info_by_id: dict[str, dict[str, str | None]] = {}
    info_by_base: dict[str, dict[str, str | None]] = {}
    for mid, mname, family in name_rows:
        mid_str = str(mid)
        name_val = (mname or mid_str)
        ws_val = (family or None)
        info_by_id[mid_str] = {"name": name_val, "workshop": ws_val}
        base_key = mid_str.split("_")[0]
        if base_key not in info_by_base:
            info_by_base[base_key] = {"name": name_val, "workshop": ws_val}
        name_base = str(name_val).split("_")[0]
        if name_base not in info_by_base:
            info_by_base[name_base] = {"name": name_val, "workshop": ws_val}

    machines = []
    for mid in machine_ids:
        meta = info_by_id.get(mid) or info_by_base.get(mid) or info_by_base.get(mid.split("_")[0]) or {}
        ws = meta.get("workshop")
        if wk_tokens:
            ws_norm = str(ws or "").strip().lower()
            if ws_norm not in wk_tokens:
                if not any(str(mid).startswith(p) for p in wk_prefixes):
                    continue
        machines.append(
            {
                "id": mid,
                "name": meta.get("name", mid),
                "workshop": ws or (next((p for p in sorted(wk_prefixes) if str(mid).startswith(p)), None) if wk_prefixes else None),
            }
        )

    dates_sorted = sorted(dates)
    return {
        "period": period,
        "machines": machines,
        "dates": [str(d) for d in dates_sorted],
        "util": {f"{m['id']}|{d}": util.get((m["id"], d), 0.0) for m in machines for d in dates_sorted},
    }


def _period_bounds(period: str, anchor_date) -> tuple[datetime, datetime]:
    p = (period or "day").lower()
    if p == "week":
        start_date = anchor_date - timedelta(days=anchor_date.weekday())
        end_date = start_date + timedelta(days=7)
    elif p == "month":
        start_date = anchor_date.replace(day=1)
        if start_date.month == 12:
            end_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            end_date = start_date.replace(month=start_date.month + 1)
    else:
        start_date = anchor_date
        end_date = start_date + timedelta(days=1)
    return (
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time()),
    )


def _overlap_seconds(start_ts: datetime | None, end_ts: datetime | None, left: datetime, right: datetime) -> int:
    if start_ts is None or end_ts is None:
        return 0
    lo = max(start_ts, left)
    hi = min(end_ts, right)
    if hi <= lo:
        return 0
    return int((hi - lo).total_seconds())


def _effective_op_interval(
    start_ts: datetime | None,
    end_ts: datetime | None,
    duration_sec: int | float | None,
) -> tuple[datetime | None, datetime | None]:
    if start_ts is None:
        return None, None
    end_eff = end_ts
    try:
        dur = float(duration_sec or 0)
    except Exception:
        dur = 0.0
    if dur > 0:
        if end_eff is None or end_eff <= start_ts:
            end_eff = start_ts + timedelta(seconds=dur)
        else:
            # Keep parity with compute_daily_loads: day-marker timestamps use duration_sec.
            if start_ts.time() == time.min and end_eff.time() == time.min:
                end_eff = start_ts + timedelta(seconds=dur)
    return start_ts, end_eff


@router.get("/{plan_id}/heatmap/cell", summary="Cell details for UI heatmap")
def get_heatmap_cell_details(
    plan_id: int,
    machine_id: str = Query(..., description="Machine ID (must match schedule machine_id)"),
    date: str = Query(..., description="Cell date in YYYY-MM-DD"),
    period: str = Query("day", description="Aggregation period: day|week|month"),
    limit_ops: int = Query(1000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    plan = db.get(PlanVersion, plan_id)
    if plan is None:
        raise HTTPException(404, "plan not found")

    machine_id = str(machine_id or "").strip()
    if not machine_id:
        raise HTTPException(400, "machine_id is required")

    try:
        anchor_date = pd.to_datetime(date).date()
    except Exception:
        raise HTTPException(400, "date must be in YYYY-MM-DD format")

    period = (period or "day").lower()
    if period not in ("day", "week", "month"):
        period = "day"

    start_ts, end_ts = _period_bounds(period, anchor_date)

    machine_filter = (ScheduleOp.machine_id == machine_id)
    if "_" not in machine_id:
        machine_filter = or_(
            machine_filter,
            ScheduleOp.machine_id.like(f"{machine_id}_%"),
        )

    candidate_q = (
        db.query(ScheduleOp)
        .filter(ScheduleOp.plan_id == plan_id)
        .filter(machine_filter)
        .filter(ScheduleOp.start_ts < end_ts)
        .filter(
            or_(
                ScheduleOp.end_ts > start_ts,
                func.coalesce(ScheduleOp.duration_sec, 0) > 0,
            )
        )
    )

    candidates = candidate_q.order_by(ScheduleOp.start_ts.asc(), ScheduleOp.op_id.asc()).all()
    total_ops = 0
    load_sec_by_ops = 0
    rows: List[tuple[Any, datetime | None, datetime | None, int]] = []
    for r in candidates:
        start_eff, end_eff = _effective_op_interval(r.start_ts, r.end_ts, r.duration_sec)
        overlap_sec = _overlap_seconds(start_eff, end_eff, start_ts, end_ts)
        if overlap_sec <= 0:
            continue
        total_ops += 1
        load_sec_by_ops += int(overlap_sec)
        if len(rows) < limit_ops:
            rows.append((r, start_eff, end_eff, int(overlap_sec)))

    orders_map: Dict[str, dict] = {}
    operations: List[dict] = []
    for r, start_eff, end_eff, overlap_sec in rows:

        oid = str(r.order_id or "")
        if oid:
            if oid not in orders_map:
                orders_map[oid] = {
                    "order_id": oid,
                    "item_id": str(r.item_id or ""),
                    "article_name": str(r.article_name or ""),
                    "qty": float(r.qty or 0.0),
                    "ops_count": 0,
                    "duration_sec": 0,
                    "setup_sec": 0,
                    "window_load_sec": 0,
                    "first_start": start_eff,
                    "last_end": end_eff,
                }
            rec = orders_map[oid]
            rec["ops_count"] = int(rec["ops_count"]) + 1
            rec["duration_sec"] = int(rec["duration_sec"]) + int(r.duration_sec or 0)
            rec["setup_sec"] = int(rec["setup_sec"]) + int(r.setup_sec or 0)
            rec["window_load_sec"] = int(rec["window_load_sec"]) + int(overlap_sec)
            if not rec.get("item_id"):
                rec["item_id"] = str(r.item_id or "")
            if not rec.get("article_name"):
                rec["article_name"] = str(r.article_name or "")
            if r.qty is not None:
                rec["qty"] = float(r.qty)
            if rec.get("first_start") is None or (start_eff and start_eff < rec["first_start"]):
                rec["first_start"] = start_eff
            if rec.get("last_end") is None or (end_eff and end_eff > rec["last_end"]):
                rec["last_end"] = end_eff

        operations.append(
            {
                "order_id": oid,
                "item_id": str(r.item_id or ""),
                "article_name": str(r.article_name or ""),
                "qty": float(r.qty or 0.0),
                "start_ts": start_eff,
                "end_ts": end_eff,
                "duration_sec": int(r.duration_sec or 0),
                "setup_sec": int(r.setup_sec or 0),
                "window_load_sec": int(overlap_sec),
                "op_index": int(r.op_index) if r.op_index is not None else None,
                "batch_id": str(r.batch_id) if r.batch_id is not None else None,
            }
        )

    load_row = (
        db.query(
            func.sum(MachineLoadDaily.load_sec).label("load_sec"),
            func.sum(MachineLoadDaily.cap_sec).label("cap_sec"),
        )
        .filter(MachineLoadDaily.plan_id == plan_id)
        .filter(MachineLoadDaily.machine_id == machine_id)
        .filter(MachineLoadDaily.work_date >= start_ts)
        .filter(MachineLoadDaily.work_date < end_ts)
        .one()
    )
    load_sec = int(load_row.load_sec or 0)
    cap_sec = int(load_row.cap_sec or 0)
    util = (float(load_sec) / float(cap_sec)) if cap_sec > 0 else 0.0

    base_mid = machine_id.split("_", 1)[0]
    machine_meta_row = (
        db.query(DimMachine.machine_id, DimMachine.name, DimMachine.family)
        .filter(
            or_(
                DimMachine.machine_id == machine_id,
                DimMachine.machine_id == base_mid,
                DimMachine.name == machine_id,
                DimMachine.name == base_mid,
            )
        )
        .first()
    )
    machine_name = machine_id
    machine_workshop = None
    if machine_meta_row:
        machine_name = str(machine_meta_row[1] or machine_id)
        machine_workshop = machine_meta_row[2]

    orders = sorted(
        orders_map.values(),
        key=lambda x: (
            x.get("first_start") or datetime.max,
            x.get("order_id") or "",
        ),
    )
    for rec in orders:
        if rec.get("first_start") is None:
            rec["first_start"] = None
        if rec.get("last_end") is None:
            rec["last_end"] = None

    return {
        "plan_id": int(plan_id),
        "period": period,
        "anchor_date": str(anchor_date),
        "window_start": start_ts,
        "window_end": end_ts,
        "machine": {
            "id": machine_id,
            "name": machine_name,
            "workshop": machine_workshop,
        },
        "summary": {
            "orders": len(orders),
            "operations": total_ops,
            "returned_operations": len(rows),
            "truncated": bool(total_ops > len(rows)),
            "load_sec": int(load_sec),
            "cap_sec": int(cap_sec),
            "util": float(util),
            "window_load_sec_from_ops": int(load_sec_by_ops),
        },
        "orders": orders,
        "operations": operations,
    }
