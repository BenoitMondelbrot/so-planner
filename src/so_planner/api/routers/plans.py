# src/so_planner/api/routers/plans.py
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from sqlalchemy import or_, func, text
from sqlalchemy.orm import Session
from ...db import get_db
import os, logging
from pydantic import BaseModel
from ...db.models import PlanVersion, ScheduleOp, MachineLoadDaily, DimMachine
from ...scheduling.greedy_scheduler import run_pipeline, _ensure_netting_tables, load_stock_any
from ...scheduling.utils import compute_daily_loads
from ...scheduling.greedy.loaders import load_machines as _load_machines, load_bom_article_name_map
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from datetime import datetime, timedelta

router = APIRouter(prefix="/plans", tags=["plans"])

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
    _ensure_netting_tables(db)
    try:
        cols = db.execute(text("PRAGMA table_info(plan_order_info)")).mappings().all()
        names = {str(getattr(r, "name", r[1])) for r in cols}
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
) -> List[dict]:
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
            func.max(DimMachine.family).label("workshop"),
        )
        .join(DimMachine, ScheduleOp.machine_id == DimMachine.machine_id, isouter=True)
        .filter(ScheduleOp.plan_id == plan_id)
        .group_by(ScheduleOp.order_id)
    )
    if item_id:
        token = f"%{item_id.lower()}%"
        q = q.filter(func.lower(ScheduleOp.item_id).like(token))
    if order_id:
        token = f"%{order_id}%"
        q = q.filter(ScheduleOp.order_id.like(token))
    if wk_tokens:
        q = q.filter(func.lower(func.trim(DimMachine.family)).in_(wk_tokens))

    rows = q.all()
    out: List[dict] = []
    for r in rows:
        oid = str(r.order_id) if r.order_id is not None else ""
        if not oid:
            continue
        start_ts = r.start_ts
        end_ts = r.end_ts
        start_date = start_ts.date() if start_ts else None
        end_date = None
        if end_ts:
            try:
                end_date = (end_ts - timedelta(seconds=1)).date()
            except Exception:
                end_date = end_ts.date()
        # intersect filter
        if date_from_dt and end_date and end_date < date_from_dt:
            continue
        if date_to_dt and start_date and start_date > date_to_dt:
            continue
        m = meta.get(oid, {})
        workshop_val = str(r.workshop) if r.workshop is not None else None
        if not workshop_val and m.get("workshop"):
            workshop_val = str(m.get("workshop"))
        qty_val = m.get("qty") if m else None
        if qty_val is None:
            qty_val = float(r.qty) if r.qty is not None else None
        out.append(
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
    return sorted(out, key=lambda x: x["order_id"])

def _load_lag_map(bom_path: str | None) -> dict:
    """Load item_id -> lag_days from BOM.xlsx column 'lag time' (in days)."""
    if not bom_path or not os.path.exists(bom_path):
        return {}
    try:
        import pandas as pd
        df = pd.read_excel(bom_path, sheet_name=0, dtype=object)
        def norm(c: str) -> str:
            return str(c).strip().lower().replace(" ", "").replace("_", "")
        cols = {norm(c): c for c in df.columns}
        item_col = cols.get("item_id") or cols.get("article") or cols.get("item") or list(df.columns)[0]
        lag_col = None
        for name in ("lagtime", "lag_time", "lag", "lagdays"):
            if name in cols:
                lag_col = cols[name]; break
        if not lag_col:
            return {}
        df = df[[item_col, lag_col]].dropna()
        df[lag_col] = pd.to_numeric(df[lag_col], errors="coerce").fillna(0).astype(int)
        df[item_col] = df[item_col].astype(str).str.strip()
        return {str(r[item_col]): int(r[lag_col]) for _, r in df.iterrows() if str(r[item_col])}
    except Exception:
        return {}

def _adjust_children_dates(df_ops: pd.DataFrame, fixed_meta: Dict[str, dict], lag_map: dict) -> pd.DataFrame:
    """Clamp children ops within parent window, allowing shift left by lag days from BOM."""
    if df_ops is None or df_ops.empty or not fixed_meta:
        return df_ops
    rows = []
    fixed_parents = {}
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
        fixed_parents[base] = {"start": ps, "end": pe}
    if not fixed_parents:
        return df_ops
    for r in df_ops.itertuples(index=False):
        oid = str(getattr(r, "order_id", "") or "")
        base = oid.split(":", 1)[0] if ":" in oid else oid
        parent = fixed_parents.get(base)
        if not parent or oid == base or oid == (base + ":" + base.split("-", 1)[0] if ":" in oid else base):
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
        lag_days = int(lag_map.get(str(getattr(r, "item_id", "") or ""), 0))
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
        _ensure_netting_tables(db)
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
        }
        for p in plans
    ]

@router.post("", summary="Create new plan version")
def create_plan(body: PlanCreate, db: Session = Depends(get_db)):
    plan = PlanVersion(
        name=body.name,
        notes=body.notes,
        parent_plan_id=body.parent_plan_id,
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
    mode: str | None = None

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
    meta = _load_order_meta(db, plan_id)
    fixed_orders = {oid for oid, m in meta.items() if (m.get("status") or "").lower() == "fixed"}
    deleted_orders = {oid for oid, m in meta.items() if (m.get("status") or "").lower() == "deleted"}

    try:
        # 1) Запуск вашего пайплайна. При необходимости пути сделайте настраиваемыми.
        # Use stock if present (default stock.xlsx or env override)
        stock_path_env = os.environ.get("SOPLANNER_STOCK_PATH", "stock.xlsx")
        plan_path = (body.plan_path if body else None) or "plan of sales.xlsx"
        bom_path = (body.bom_path if body else None) or "BOM.xlsx"
        machines_path = (body.machines_path if body else None) or "machines.xlsx"
        stock_path = (body.stock_path if body else None) or stock_path_env
        mode = (body.mode if body else None) or ""

        # Refresh stock snapshot in DB so reports use the uploaded file
        try:
            _ingest_stock_snapshot(stock_path, db)
        except Exception:
            logging.warning("Stock ingest inside run_greedy_into_plan failed for %s", stock_path)

        out_xlsx, sched_df = run_pipeline(
            plan_path,
            bom_path,
            machines_path,
            "schedule_out.xlsx",
            stock_path=stock_path if stock_path else None,
            split_child_orders=True,
            align_roots_to_due=True,
            mode=mode,
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
        lag_map = _load_lag_map(bom_path)
        df_ops = _adjust_children_dates(df_ops, meta, lag_map)

        article_name_map = {}
        try:
            if bom_path:
                article_name_map = load_bom_article_name_map(Path(bom_path))
        except Exception:
            article_name_map = {}


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
                agg_rows.append(
                    {
                        "plan_id": plan.id,
                        "order_id": str(oid),
                        "start_date": str(sd) if sd is not None else None,
                        "end_date": str(ed) if ed is not None else None,
                        "workshop": wk_map.get(str(oid)) if wk_map else "",
                        "qty": float(grp["qty"].iloc[0]) if "qty" in grp.columns else None,
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
                      due_date=COALESCE(plan_order_info.due_date, excluded.due_date),
                      status=CASE WHEN plan_order_info.status='fixed' THEN plan_order_info.status ELSE excluded.status END,
                      updated_at=CURRENT_TIMESTAMP
                """),
                agg_rows,
            )
            db.commit()
        # Автофикс корневых заказов (обеспечивают план продаж)
        try:
            agg_map = {r["order_id"]: r for r in agg_rows}
            roots = []
            for oid in list(agg_map.keys()):
                base = str(oid).split(":", 1)[0]
                base_item = base.split("-", 1)[0] if "-" in base else base
                if str(oid) == f"{base}:{base_item}":
                    roots.append(agg_map[oid])
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
        db.commit()
        return {"ok": True, "plan_id": plan.id, "ops": ops_total, "days": days, "loads": loads_count}
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
    db: Session = Depends(get_db),
):
    if not db.get(PlanVersion, plan_id):
        raise HTTPException(404, "plan not found")
    _ensure_plan_order_table(db)
    orders = _aggregate_orders_for_plan(db, plan_id, item_id=item_id, workshop=workshop, date_from=date_from, date_to=date_to, order_id=order_id)
    return {"status": "ok", "count": len(orders), "orders": orders}

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
