# src/so_planner/api/routers/optimize.py
from fastapi import APIRouter, HTTPException, Depends
import logging
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
import time
from ...db import get_db
from ...db.models import PlanVersion, ScheduleOp, MachineLoadDaily
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

    # 1) Тянем warm-start из БД
    ops = _retry(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to warm-start from.")

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
        notes="Optimized from warm-start",
    )
    db.add(new_plan)
    db.commit()
    db.refresh(new_plan)
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
        df_ops = solve_milp(warm_start=warm_start, **kwargs)  # type: ignore[operator]
        if df_ops is None or getattr(df_ops, "empty", True):
            raise RuntimeError("MILP returned empty schedule")

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

        new_plan.status = "ready"
        db.commit()
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
            "criteria": (req.dict() if req else {}),
            "report": {"before": metrics_before, "after": metrics_after},
        }
    except Exception as e:  # pylint: disable=broad-except
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

    ops = _retry(lambda: db.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all())
    if not ops:
        raise HTTPException(400, "Selected plan has no schedule to warm-start from.")

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
        notes="Job-shop optimized from warm-start",
    )
    db.add(new_plan)
    db.commit()
    db.refresh(new_plan)
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
        df_ops = solve_jobshop(warm_start=warm_start, **kwargs)  # type: ignore[operator]
        if df_ops is None or getattr(df_ops, "empty", True):
            raise RuntimeError("Job-shop returned empty schedule")

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

        new_plan.status = "ready"
        db.commit()
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
            "criteria": (req.dict() if req else {}),
            "report": {"before": metrics_before, "after": metrics_after},
        }
    except Exception as e:  # pylint: disable=broad-except
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
