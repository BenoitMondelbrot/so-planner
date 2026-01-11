# src/so_planner/api/routers/plans.py
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from sqlalchemy import or_, func
from sqlalchemy.orm import Session
from ...db import get_db
import os
from pydantic import BaseModel
from ...db.models import PlanVersion, ScheduleOp, MachineLoadDaily, DimMachine
from ...scheduling.greedy_scheduler import run_pipeline
from ...scheduling.utils import compute_daily_loads
from ...scheduling.greedy.loaders import load_machines as _load_machines, load_bom_article_name_map
from pathlib import Path
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

    try:
        # 1) Запуск вашего пайплайна. При необходимости пути сделайте настраиваемыми.
        # Use stock if present (default stock.xlsx or env override)
        stock_path_env = os.environ.get("SOPLANNER_STOCK_PATH", "stock.xlsx")
        plan_path = (body.plan_path if body else None) or "plan of sales.xlsx"
        bom_path = (body.bom_path if body else None) or "BOM.xlsx"
        machines_path = (body.machines_path if body else None) or "machines.xlsx"
        stock_path = (body.stock_path if body else None) or stock_path_env
        mode = (body.mode if body else None) or ""
        out_xlsx, sched_df = run_pipeline(
            plan_path,
            bom_path,
            machines_path,
            "schedule_out.xlsx",
            stock_path=stock_path if stock_path else None,
            split_child_orders=True,
            align_roots_to_due=True,
            mode=mode,
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

        article_name_map = {}
        try:
            if bom_path:
                article_name_map = load_bom_article_name_map(Path(bom_path))
        except Exception:
            article_name_map = {}


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
        db.bulk_save_objects(ops)
        db.commit()

        # 4) Агрегаты для heatmap
        loads_df = compute_daily_loads(df_ops)  # machine_id, work_date, load_sec, cap_sec, util
        loads = [
            MachineLoadDaily(
                plan_id=plan.id,
                machine_id=row.machine_id,
                work_date=row.work_date,
                load_sec=int(row.load_sec),
                cap_sec=int(row.cap_sec),
                util=float(row.util),
            )
            for row in loads_df.itertuples(index=False)
        ]
        db.bulk_save_objects(loads)
        db.commit()

        plan.origin = "greedy"
        plan.status = "ready"
        db.commit()
        return {"ok": True, "plan_id": plan.id, "ops": len(ops), "days": int(loads_df["work_date"].nunique())}
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
