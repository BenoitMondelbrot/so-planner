from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import os
import re
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

try:
    from ortools.sat.python import cp_model  # type: ignore

    _HAVE_CP_SAT = True
except Exception:  # pragma: no cover
    _HAVE_CP_SAT = False


def _safe_name(value: str, max_len: int = 48) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or ""))
    if not s:
        s = "x"
    return s[:max_len]


def _to_ts(v: Any) -> pd.Timestamp | None:
    if v is None:
        return None
    try:
        ts = pd.to_datetime(v)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def _bucket_floor(ts: pd.Timestamp, bucket: str) -> pd.Timestamp:
    t = pd.Timestamp(ts).normalize()
    if bucket == "month":
        return t.replace(day=1)
    # week (ISO Monday)
    return t - pd.Timedelta(days=int(t.weekday()))


def _bucket_next(ts: pd.Timestamp, bucket: str) -> pd.Timestamp:
    if bucket == "month":
        return (pd.Timestamp(ts) + pd.offsets.MonthBegin(1)).normalize()
    return (pd.Timestamp(ts) + pd.Timedelta(days=7)).normalize()


def _bucket_range(start: pd.Timestamp, end: pd.Timestamp, bucket: str) -> List[pd.Timestamp]:
    out: List[pd.Timestamp] = []
    cur = pd.Timestamp(start).normalize()
    right = pd.Timestamp(end).normalize()
    while cur <= right:
        out.append(cur)
        cur = _bucket_next(cur, bucket)
    return out


def _infer_item_from_order_id(order_id: str) -> str:
    oid = str(order_id or "").strip()
    if ":" in oid:
        return oid.split(":", 1)[1].strip()
    parts = [p for p in re.split(r"[-_]", oid) if p]
    if len(parts) >= 2:
        return parts[0]
    return oid


@dataclass
class _OrderData:
    order_id: str
    item_id: str
    demand_qty: int
    due_date: date
    due_period: pd.Timestamp
    completion_period: pd.Timestamp
    is_fixed: bool
    route_machines: set[str]


def solve_volume_milp(
    baseline_ops: Iterable[Dict[str, Any]],
    *,
    order_meta: Dict[str, Dict[str, Any]] | None = None,
    bom_lines: Iterable[Dict[str, Any]] | None = None,
    stock_by_item: Dict[str, float] | None = None,
    bucket: str = "week",
    horizon_start: datetime | str | None = None,
    horizon_end: datetime | str | None = None,
    item_ids: Iterable[str] | None = None,
    machine_ids: Iterable[str] | None = None,
    time_limit_sec: int = 20,
    parallel_workers: int | None = None,
    w_due_qty: float = 1000.0,
    w_due_time: float = 50.0,
    w_supply: float = 300.0,
    w_over: float = 20.0,
    w_mix: float = 1.0,
    w_stab: float = 0.5,
    forbid_past: bool = True,
    guard_no_due_penalty: bool = True,
    trace: List[str] | None = None,
) -> Dict[str, Any]:
    trace_out: List[str] = trace if trace is not None else []

    def _tr(msg: str) -> None:
        trace_out.append(str(msg))

    if not _HAVE_CP_SAT:
        raise RuntimeError("ortools CP-SAT is not installed")

    if str(bucket or "").lower() not in {"week", "month"}:
        bucket = "week"
    bucket = str(bucket).lower()
    _tr(f"init: bucket={bucket}")

    df = pd.DataFrame(list(baseline_ops))
    _tr(f"input: baseline_rows={len(df)}")
    if df.empty:
        return {
            "ok": True,
            "status": "EMPTY",
            "bucket": bucket,
            "periods": [],
            "orders_total": 0,
            "orders_movable": 0,
            "kpi_before": {},
            "kpi_after": {},
            "machine_buckets": [],
            "order_changes": [],
            "order_allocations": [],
            "forbid_past": bool(forbid_past),
            "warnings": ["no baseline operations"],
            "trace": trace_out,
        }

    # Normalize required columns.
    for col, default in (
        ("order_id", ""),
        ("item_id", ""),
        ("machine_id", ""),
        ("qty", 0),
        ("duration_sec", 0),
        ("setup_sec", 0),
        ("start_ts", None),
        ("end_ts", None),
    ):
        if col not in df.columns:
            df[col] = default

    df["order_id"] = df["order_id"].astype(str)
    df["item_id"] = df["item_id"].astype(str)
    df["machine_id"] = df["machine_id"].astype(str)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce").fillna(0).astype(int)
    df["start_ts"] = pd.to_datetime(df["start_ts"], errors="coerce")
    df["end_ts"] = pd.to_datetime(df["end_ts"], errors="coerce")

    no_end = df["end_ts"].isna() | (df["end_ts"] <= df["start_ts"])
    df.loc[no_end, "end_ts"] = df.loc[no_end, "start_ts"] + pd.to_timedelta(
        df.loc[no_end, "duration_sec"].clip(lower=0), unit="s"
    )
    df = df[df["order_id"] != ""].copy()
    _tr(f"normalize: rows_nonempty_order={len(df)}")
    if df.empty:
        return {
            "ok": True,
            "status": "EMPTY_ORDERS",
            "bucket": bucket,
            "periods": [],
            "orders_total": 0,
            "orders_movable": 0,
            "kpi_before": {},
            "kpi_after": {},
            "machine_buckets": [],
            "order_changes": [],
            "order_allocations": [],
            "forbid_past": bool(forbid_past),
            "warnings": ["no non-empty order_id in baseline"],
            "trace": trace_out,
        }

    order_meta = order_meta or {}
    stock_by_item = {str(k): float(v or 0.0) for k, v in (stock_by_item or {}).items()}

    # Order-level baseline quantity and route shape.
    qty_by_order = (
        df.groupby("order_id", as_index=False)["qty"]
        .max()
        .set_index("order_id")["qty"]
        .astype(float)
        .to_dict()
    )
    item_by_order_ops = (
        df.sort_values(["order_id", "start_ts"])
        .groupby("order_id", as_index=False)["item_id"]
        .first()
        .set_index("order_id")["item_id"]
        .astype(str)
        .to_dict()
    )
    finish_by_order = (
        df.groupby("order_id", as_index=False)["end_ts"]
        .max()
        .set_index("order_id")["end_ts"]
        .to_dict()
    )

    # Unit machine time per order-unit (seconds per piece), derived from baseline route.
    unit_sec: Dict[Tuple[str, str], float] = {}
    route_machines: Dict[str, set[str]] = {}
    for r in (
        df.groupby(["order_id", "machine_id"], as_index=False)["duration_sec"]
        .sum()
        .itertuples(index=False)
    ):
        oid = str(getattr(r, "order_id"))
        mid = str(getattr(r, "machine_id"))
        dur = float(getattr(r, "duration_sec") or 0.0)
        denom = max(float(qty_by_order.get(oid, 0.0) or 0.0), 1.0)
        coef = max(0.0, dur / denom)
        if coef <= 0:
            continue
        unit_sec[(oid, mid)] = coef
        route_machines.setdefault(oid, set()).add(mid)
    _tr(f"route: orders_with_routes={len(route_machines)} unit_pairs={len(unit_sec)}")

    # Build order records (union baseline + known metadata).
    all_orders = set(df["order_id"].astype(str).unique().tolist()) | {
        str(k) for k in order_meta.keys()
    }
    hs = _to_ts(horizon_start)
    he = _to_ts(horizon_end)
    if hs is not None and he is not None and he < hs:
        he = hs

    if len(df):
        first_ts = pd.to_datetime(df["start_ts"]).min()
    else:
        first_ts = pd.Timestamp.utcnow()

    orders: Dict[str, _OrderData] = {}
    for oid in sorted(all_orders):
        meta = order_meta.get(oid, {}) if isinstance(order_meta.get(oid, {}), dict) else {}
        status = str(meta.get("status") or "unfixed").lower()
        is_fixed = status == "fixed"

        qty_meta = meta.get("qty")
        try:
            qty_meta_f = float(qty_meta) if qty_meta is not None else None
        except Exception:
            qty_meta_f = None
        qty0 = int(round(max(0.0, float(qty_by_order.get(oid, 0.0) or 0.0))))
        demand = int(round(max(0.0, qty_meta_f if qty_meta_f is not None else float(qty0))))
        if demand < qty0:
            demand = qty0
        if is_fixed:
            demand = qty0

        item = str(meta.get("item_id") or item_by_order_ops.get(oid) or _infer_item_from_order_id(oid))

        due_raw = meta.get("due_date")
        due_ts = _to_ts(due_raw)
        if due_ts is None:
            due_ts = _to_ts(finish_by_order.get(oid))
        if due_ts is None:
            due_ts = hs or first_ts or pd.Timestamp.utcnow()
        due_ts = due_ts.normalize()

        comp_ts = _to_ts(finish_by_order.get(oid))
        if comp_ts is None:
            comp_ts = due_ts
        comp_ts = comp_ts.normalize()

        orders[oid] = _OrderData(
            order_id=oid,
            item_id=item,
            demand_qty=demand,
            due_date=due_ts.date(),
            due_period=_bucket_floor(due_ts, bucket),
            completion_period=_bucket_floor(comp_ts, bucket),
            is_fixed=is_fixed,
            route_machines=route_machines.get(oid, set()),
        )
    _tr(f"orders: total={len(orders)} fixed={sum(1 for o in orders.values() if o.is_fixed)}")

    if not orders:
        return {
            "ok": True,
            "status": "EMPTY_ORDERS",
            "bucket": bucket,
            "periods": [],
            "orders_total": 0,
            "orders_movable": 0,
            "kpi_before": {},
            "kpi_after": {},
            "machine_buckets": [],
            "order_changes": [],
            "order_allocations": [],
            "forbid_past": bool(forbid_past),
            "warnings": ["no orders after normalization"],
            "trace": trace_out,
        }

    # Build planning periods.
    cand_periods = [o.completion_period for o in orders.values()] + [o.due_period for o in orders.values()]
    if hs is not None:
        cand_periods.append(_bucket_floor(hs, bucket))
    if he is not None:
        cand_periods.append(_bucket_floor(he, bucket))
    p_min = min(cand_periods)
    p_max = max(cand_periods)
    periods = _bucket_range(p_min, p_max, bucket)
    p_index = {p: i for i, p in enumerate(periods)}
    if not periods:
        periods = [p_min]
        p_index = {p_min: 0}
    _tr(f"periods: count={len(periods)} start={str(pd.Timestamp(periods[0]).date())} end={str(pd.Timestamp(periods[-1]).date())}")

    q0: Dict[Tuple[str, int], int] = {}
    for oid, od in orders.items():
        vec = [0] * len(periods)
        idx = p_index.get(od.completion_period)
        if idx is None:
            idx = min(range(len(periods)), key=lambda x: abs((periods[x] - od.completion_period).days))
        vec[idx] = int(max(0, od.demand_qty))
        for t in range(len(periods)):
            q0[(oid, t)] = int(vec[t])

    # Machine capacities by bucket from baseline, with per-machine fallback.
    df["bucket_start"] = df["start_ts"].apply(
        lambda x: _bucket_floor(pd.Timestamp(x), bucket) if pd.notna(x) else pd.NaT
    )
    load_base = (
        df.dropna(subset=["bucket_start"])
        .groupby(["machine_id", "bucket_start"], as_index=False)["duration_sec"]
        .sum()
    )
    cap_map: Dict[Tuple[str, int], int] = {}
    cap_default = 5 * 8 * 3600 if bucket == "week" else 22 * 8 * 3600
    machine_ids_all = sorted({m for od in orders.values() for m in od.route_machines})
    med_by_machine: Dict[str, int] = {}
    if not load_base.empty:
        for mid, g in load_base.groupby("machine_id"):
            med = int(round(float(pd.to_numeric(g["duration_sec"], errors="coerce").fillna(0).median())))
            med_by_machine[str(mid)] = max(1, med)
    for mid in machine_ids_all:
        dcap = med_by_machine.get(mid, cap_default)
        for t, p in enumerate(periods):
            row = load_base[(load_base["machine_id"].astype(str) == str(mid)) & (load_base["bucket_start"] == p)]
            if len(row):
                val = int(float(row["duration_sec"].iloc[0]))
            else:
                val = int(dcap)
            cap_map[(mid, t)] = max(1, val)
    _tr(f"capacity: machines={len(machine_ids_all)}")

    # Selection scope.
    item_filter = {str(x) for x in (item_ids or []) if str(x)}
    machine_filter = {str(x) for x in (machine_ids or []) if str(x)}

    horizon_idx: set[int] = set(range(len(periods)))
    if hs is not None or he is not None:
        hs_floor = _bucket_floor(hs if hs is not None else periods[0], bucket)
        he_floor = _bucket_floor(he if he is not None else periods[-1], bucket)
        horizon_idx = {i for i, p in enumerate(periods) if hs_floor <= p <= he_floor}
        if not horizon_idx:
            horizon_idx = set(range(len(periods)))
    _tr(f"horizon: selected_periods={len(horizon_idx)}")

    horizon_opt_idx: set[int] = set(horizon_idx)
    if bool(forbid_past):
        today_floor = _bucket_floor(pd.Timestamp.now().normalize(), bucket)
        horizon_opt_idx = {i for i in horizon_idx if periods[i] >= today_floor}
        _tr(
            "horizon_no_past: "
            f"forbid_past=True selected={len(horizon_opt_idx)} "
            f"today_floor={str(pd.Timestamp(today_floor).date())}"
        )
    else:
        _tr("horizon_no_past: forbid_past=False")

    movable: Dict[str, bool] = {}
    for oid, od in orders.items():
        ok = True
        if od.is_fixed:
            ok = False
        if ok and item_filter and od.item_id not in item_filter:
            ok = False
        if ok and machine_filter:
            if not od.route_machines:
                ok = False
            elif not od.route_machines.issubset(machine_filter):
                ok = False
        cidx = p_index.get(od.completion_period, 0)
        if ok and cidx not in horizon_idx:
            ok = False
        if ok and od.demand_qty <= 0:
            ok = False
        movable[oid] = ok
    _tr(f"scope: movable_orders={sum(1 for v in movable.values() if v)}")

    if not any(movable.values()):
        # No movable orders in scope: return baseline snapshot.
        baseline = _build_volume_report(
            orders=orders,
            periods=periods,
            q_map=q0,
            unit_sec=unit_sec,
            cap_map=cap_map,
            bom_lines=bom_lines,
            stock_by_item=stock_by_item,
        )
        baseline["ok"] = True
        baseline["status"] = "NO_SCOPE"
        baseline["orders_total"] = len(orders)
        baseline["orders_movable"] = 0
        baseline["order_changes"] = []
        baseline["order_allocations"] = []
        baseline["machine_buckets_before"] = baseline.get("machine_buckets", [])
        baseline["forbid_past"] = bool(forbid_past)
        baseline["warnings"] = ["no movable orders in selected scope"]
        baseline["trace"] = trace_out
        return baseline

    # Guardrail for accidental objective settings:
    # if both due-related penalties are zero, forbid dropping movable demand via unmet.
    service_guard_active = bool(guard_no_due_penalty) and float(w_due_qty or 0.0) <= 0.0 and float(w_due_time or 0.0) <= 0.0
    warnings_out: List[str] = []
    if service_guard_active:
        msg = "guardrail: due penalties are zero, unmet disabled for movable orders"
        warnings_out.append(msg)
        _tr(msg)

    model = cp_model.CpModel()
    q_var: Dict[Tuple[str, int], Any] = {}
    unmet_var: Dict[str, cp_model.IntVar] = {}
    delta_vars: List[cp_model.IntVar] = []

    for oid, od in orders.items():
        d = int(max(0, od.demand_qty))
        safe_oid = _safe_name(oid)
        q_sum_terms: List[Any] = []
        for t in range(len(periods)):
            q0v = int(q0.get((oid, t), 0))
            if movable.get(oid, False) and t in horizon_opt_idx:
                q = model.NewIntVar(0, d, f"q_{safe_oid}_{t}")
                if w_stab > 0:
                    dv = model.NewIntVar(0, d, f"dv_{safe_oid}_{t}")
                    model.Add(dv >= q - q0v)
                    model.Add(dv >= q0v - q)
                    delta_vars.append(dv)
            else:
                # Keep frozen cells as constants, but block movable allocations in past buckets.
                if movable.get(oid, False) and t in horizon_idx and t not in horizon_opt_idx:
                    q = 0
                else:
                    q = int(q0v)
            q_var[(oid, t)] = q
            q_sum_terms.append(q)
        u = model.NewIntVar(0, d, f"unmet_{safe_oid}")
        unmet_var[oid] = u
        model.Add(sum(q_sum_terms) + u == d)
        if service_guard_active and movable.get(oid, False):
            model.Add(u == 0)
    _tr(f"model: order_vars={len(q_var)} unmet_vars={len(unmet_var)}")

    # Assortment activation per item-machine-period.
    z_var: Dict[Tuple[str, str, int], cp_model.IntVar] = {}
    orders_by_item_machine: Dict[Tuple[str, str], List[str]] = {}
    for oid, od in orders.items():
        for m in od.route_machines:
            if (oid, m) in unit_sec:
                orders_by_item_machine.setdefault((od.item_id, m), []).append(oid)
    for (item, mid), olist in orders_by_item_machine.items():
        if not olist:
            continue
        if not any(movable.get(o, False) for o in olist):
            continue
        m_bound = sum(max(0, orders[o].demand_qty) for o in olist)
        if m_bound <= 0:
            continue
        safe_item = _safe_name(item)
        safe_mid = _safe_name(mid)
        for t in sorted(horizon_opt_idx):
            z = model.NewBoolVar(f"z_{safe_item}_{safe_mid}_{t}")
            model.Add(sum(q_var[(o, t)] for o in olist) <= int(m_bound) * z)
            z_var[(item, mid, t)] = z
    _tr(f"model: assortment_vars={len(z_var)}")

    # Machine capacities with soft overload.
    A_SCALE = 100
    over_var: Dict[Tuple[str, int], cp_model.IntVar] = {}
    impacted_machines = sorted(
        {
            m
            for o, od in orders.items()
            if movable.get(o, False)
            for m in od.route_machines
            if (o, m) in unit_sec
        }
    )
    for mid in impacted_machines:
        safe_mid = _safe_name(mid)
        rel_orders = [o for o, od in orders.items() if mid in od.route_machines and (o, mid) in unit_sec]
        if not rel_orders:
            continue
        for t in sorted(horizon_opt_idx):
            terms: List[cp_model.LinearExpr] = []
            const_load = 0
            for oid in rel_orders:
                c = int(round(float(unit_sec.get((oid, mid), 0.0)) * A_SCALE))
                if c <= 0:
                    continue
                qv = q_var[(oid, t)]
                if isinstance(qv, int):
                    const_load += c * int(qv)
                else:
                    terms.append(c * qv)
            if not terms:
                continue
            cap_scaled = int(round(float(cap_map.get((mid, t), cap_default)) * A_SCALE))
            ov = model.NewIntVar(0, max(1, cap_scaled * 10), f"ov_{safe_mid}_{t}")
            over_var[(mid, t)] = ov
            model.Add(sum(terms) + int(const_load) <= cap_scaled + ov * A_SCALE)
    _tr(f"model: overload_vars={len(over_var)} impacted_machines={len(impacted_machines)}")

    # Supplier shortage proxy (no receipts yet): inventory balance per component.
    B_SCALE = 1000
    short_vars: List[cp_model.IntVar] = []
    bom_map: Dict[str, Dict[str, float]] = {}
    for r in (bom_lines or []):
        try:
            item = str(r.get("item_id") or "")
            comp = str(r.get("component_id") or "")
            qty_per = float(r.get("qty_per") or 0.0)
            loss = float(r.get("loss") or 1.0)
        except Exception:
            continue
        if not item or not comp:
            continue
        coef = max(0.0, qty_per * max(loss, 0.0))
        if coef <= 0:
            continue
        bom_map.setdefault(item, {})
        bom_map[item][comp] = bom_map[item].get(comp, 0.0) + coef

    components = sorted({comp for mp in bom_map.values() for comp in mp.keys()})
    # Keep model bounded on very large datasets; shortage is still reported from simulation.
    complexity = len(components) * max(1, len(horizon_opt_idx)) * max(1, len(orders))
    enable_supply_model = complexity <= 250_000
    _tr(f"supply: components={len(components)} enable_model={bool(enable_supply_model)} complexity={int(complexity)}")
    if components:
        for comp in components:
            if not enable_supply_model:
                break
            safe_comp = _safe_name(comp)
            prev_inv: cp_model.IntVar | None = None
            init_stock = max(0.0, float(stock_by_item.get(comp, 0.0) or 0.0))
            init_scaled = int(round(init_stock * B_SCALE))
            for t in sorted(horizon_opt_idx):
                cons_terms: List[cp_model.LinearExpr] = []
                const_cons = 0
                for oid, od in orders.items():
                    coef = float(bom_map.get(od.item_id, {}).get(comp, 0.0) or 0.0)
                    if coef <= 0:
                        continue
                    c = int(round(coef * B_SCALE))
                    if c > 0:
                        qv = q_var[(oid, t)]
                        if isinstance(qv, int):
                            const_cons += c * int(qv)
                        else:
                            cons_terms.append(c * qv)
                if not cons_terms and const_cons <= 0:
                    continue
                sh = model.NewIntVar(0, 10**12, f"sh_{safe_comp}_{t}")
                inv = model.NewIntVar(0, 10**12, f"inv_{safe_comp}_{t}")
                if prev_inv is None:
                    model.Add(inv == init_scaled - int(const_cons) - sum(cons_terms) + sh)
                else:
                    model.Add(inv == prev_inv - int(const_cons) - sum(cons_terms) + sh)
                prev_inv = inv
                short_vars.append(sh)
    _tr(f"model: supply_shortage_vars={len(short_vars)}")

    # Due-time expressions.
    late_qty_terms: List[cp_model.LinearExpr] = []
    late_time_terms: List[cp_model.LinearExpr] = []
    for oid, od in orders.items():
        due_idx = p_index.get(od.due_period, 0)
        for t, p in enumerate(periods):
            q = q_var[(oid, t)]
            if t > due_idx:
                qv = q_var[(oid, t)]
                late_qty_terms.append(qv)
                delay_days = max(1, int((pd.Timestamp(p).date() - od.due_date).days))
                late_time_terms.append(delay_days * qv)

    # Objective with integer scaling.
    W_SCALE = 1000

    def _w(v: float, div: float = 1.0) -> int:
        return int(round(float(v or 0.0) * W_SCALE / float(div)))

    w_due_qty_i = _w(w_due_qty)
    w_due_time_i = _w(w_due_time)
    w_mix_i = _w(w_mix)
    w_over_i = _w(w_over, div=3600.0)
    w_stab_i = _w(w_stab)
    w_supply_i = _w(w_supply, div=B_SCALE)
    if w_supply > 0 and w_supply_i == 0:
        w_supply_i = 1
    if w_over > 0 and w_over_i == 0:
        w_over_i = 1

    objective_terms: List[cp_model.LinearExpr] = []
    if w_due_qty_i > 0 and unmet_var:
        objective_terms.append(w_due_qty_i * sum(unmet_var.values()))
    if w_due_time_i > 0 and late_time_terms:
        objective_terms.append(w_due_time_i * sum(late_time_terms))
    if w_mix_i > 0 and z_var:
        objective_terms.append(w_mix_i * sum(z_var.values()))
    if w_over_i > 0 and over_var:
        objective_terms.append(w_over_i * sum(over_var.values()))
    if w_stab_i > 0 and delta_vars:
        objective_terms.append(w_stab_i * sum(delta_vars))
    if w_supply_i > 0 and short_vars:
        objective_terms.append(w_supply_i * sum(short_vars))
    if not objective_terms:
        objective_terms = [sum(unmet_var.values())]
    model.Minimize(sum(objective_terms))
    _tr(
        "objective: "
        f"w_due_qty={w_due_qty} w_due_time={w_due_time} w_supply={w_supply} "
        f"w_over={w_over} w_mix={w_mix} w_stab={w_stab}"
    )

    solver = cp_model.CpSolver()
    if time_limit_sec and time_limit_sec > 0:
        solver.parameters.max_time_in_seconds = float(time_limit_sec)
    try:
        env_workers = int(os.environ.get("SOPLANNER_MAX_WORKERS", "0") or 0)
    except Exception:
        env_workers = 0
    workers = parallel_workers or env_workers or (os.cpu_count() or 1)
    solver.parameters.num_search_workers = int(max(1, workers))
    solver.parameters.log_search_progress = False
    _tr(f"solve: time_limit_sec={int(time_limit_sec or 0)} workers={int(max(1, workers))}")

    st = solver.Solve(model)
    feasible = st in (cp_model.OPTIMAL, cp_model.FEASIBLE)  # type: ignore[attr-defined]

    def _status_name(code: int) -> str:
        try:
            if code == cp_model.OPTIMAL:
                return "OPTIMAL"
            if code == cp_model.FEASIBLE:
                return "FEASIBLE"
            if code == cp_model.INFEASIBLE:
                return "INFEASIBLE"
            if code == cp_model.MODEL_INVALID:
                return "MODEL_INVALID"
            if code == cp_model.UNKNOWN:
                return "UNKNOWN"
        except Exception:
            pass
        return str(code)

    q_after: Dict[Tuple[str, int], int] = {}
    if feasible:
        for k, v in q_var.items():
            if isinstance(v, int):
                q_after[k] = int(v)
            else:
                q_after[k] = int(solver.Value(v))
    else:
        q_after = dict(q0)
    _tr(f"solve_done: status={_status_name(int(st))} feasible={bool(feasible)}")

    before = _build_volume_report(
        orders=orders,
        periods=periods,
        q_map=q0,
        unit_sec=unit_sec,
        cap_map=cap_map,
        bom_lines=bom_lines,
        stock_by_item=stock_by_item,
    )
    after = _build_volume_report(
        orders=orders,
        periods=periods,
        q_map=q_after,
        unit_sec=unit_sec,
        cap_map=cap_map,
        bom_lines=bom_lines,
        stock_by_item=stock_by_item,
    )

    changed_orders: List[Dict[str, Any]] = []
    allocations: List[Dict[str, Any]] = []
    period_labels = [str(pd.Timestamp(p).date()) for p in periods]
    for oid, od in orders.items():
        b = [int(q0.get((oid, t), 0)) for t in range(len(periods))]
        a = [int(q_after.get((oid, t), 0)) for t in range(len(periods))]
        for t in range(len(periods)):
            qb = int(b[t])
            qa = int(a[t])
            if qb == 0 and qa == 0:
                continue
            allocations.append(
                {
                    "order_id": oid,
                    "item_id": od.item_id,
                    "demand_qty": int(od.demand_qty),
                    "due_date": str(od.due_date),
                    "period_idx": int(t),
                    "period_start": period_labels[t],
                    "qty_before": qb,
                    "qty_after": qa,
                    "changed": bool(qb != qa),
                }
            )
        if b != a:
            changed_orders.append(
                {
                    "order_id": oid,
                    "item_id": od.item_id,
                    "demand_qty": int(od.demand_qty),
                    "due_date": str(od.due_date),
                    "before": b,
                    "after": a,
                }
            )
    _tr(f"result: changed_orders={len(changed_orders)}")
    _tr(f"result: allocation_rows={len(allocations)}")

    if not feasible:
        warnings_out.append(f"solver status: {_status_name(int(st))}")

    return {
        "ok": bool(feasible),
        "status": _status_name(int(st)),
        "bucket": bucket,
        "periods": period_labels,
        "horizon_period_idx": sorted(horizon_opt_idx),
        "orders_total": int(len(orders)),
        "orders_movable": int(sum(1 for v in movable.values() if v)),
        "kpi_before": before["kpi"],
        "kpi_after": after["kpi"],
        "machine_buckets": after["machine_buckets"],
        "machine_buckets_before": before["machine_buckets"],
        "order_changes": changed_orders,
        "order_allocations": allocations,
        "forbid_past": bool(forbid_past),
        "warnings": warnings_out,
        "trace": trace_out,
    }


def _build_volume_report(
    *,
    orders: Dict[str, _OrderData],
    periods: List[pd.Timestamp],
    q_map: Dict[Tuple[str, int], int],
    unit_sec: Dict[Tuple[str, str], float],
    cap_map: Dict[Tuple[str, int], int],
    bom_lines: Iterable[Dict[str, Any]] | None,
    stock_by_item: Dict[str, float] | None,
) -> Dict[str, Any]:
    stock_by_item = stock_by_item or {}

    # Due/early KPIs.
    unmet_qty = 0
    late_qty = 0
    late_weighted_days = 0
    early_qty = 0
    early_weighted_days = 0

    for oid, od in orders.items():
        due_idx = min(range(len(periods)), key=lambda i: abs((periods[i] - od.due_period).days))
        total = 0
        for t, p in enumerate(periods):
            q = int(q_map.get((oid, t), 0))
            total += q
            if q <= 0:
                continue
            if t > due_idx:
                delay = max(1, int((pd.Timestamp(p).date() - od.due_date).days))
                late_qty += q
                late_weighted_days += q * delay
            elif t < due_idx:
                ahead = max(1, int((od.due_date - pd.Timestamp(p).date()).days))
                early_qty += q
                early_weighted_days += q * ahead
        unmet_qty += max(0, int(od.demand_qty) - int(total))

    # Precompute positive production points once.
    positive_points: List[Tuple[str, int, int]] = []
    for (oid, t), q in q_map.items():
        qq = int(q or 0)
        if qq > 0 and oid in orders:
            positive_points.append((oid, int(t), qq))

    # Capacity KPIs.
    machine_set = sorted({m for _, m in unit_sec.keys()})
    load_acc: Dict[Tuple[str, int], float] = {}
    for oid, t, q in positive_points:
        od = orders.get(oid)
        if od is None:
            continue
        for m in od.route_machines:
            coef = float(unit_sec.get((oid, m), 0.0) or 0.0)
            if coef <= 0:
                continue
            load_acc[(m, t)] = load_acc.get((m, t), 0.0) + coef * float(q)

    load_rows: List[Dict[str, Any]] = []
    overload_sec = 0.0
    util_vals: List[float] = []
    for m in machine_set:
        for t, p in enumerate(periods):
            load = float(load_acc.get((m, t), 0.0))
            cap = float(cap_map.get((m, t), 0) or 0.0)
            over = max(0.0, load - cap)
            util = (load / cap) if cap > 0 else 0.0
            overload_sec += over
            util_vals.append(util)
            load_rows.append(
                {
                    "machine_id": str(m),
                    "period_start": str(pd.Timestamp(p).date()),
                    "load_sec": int(round(load)),
                    "cap_sec": int(round(cap)),
                    "util": float(round(util, 4)),
                }
            )

    # Assortment KPI: number of (item,machine,period) with positive qty.
    mix_cells: set[Tuple[str, str, int]] = set()
    for oid, t, _q in positive_points:
        od = orders.get(oid)
        if od is None:
            continue
        item = str(od.item_id)
        for m in od.route_machines:
            if (oid, m) in unit_sec:
                mix_cells.add((item, str(m), int(t)))
    mix_count = int(len(mix_cells))

    # Supplier shortage simulation (no receipts yet).
    bom_map: Dict[str, Dict[str, float]] = {}
    for r in (bom_lines or []):
        try:
            item = str(r.get("item_id") or "")
            comp = str(r.get("component_id") or "")
            qty_per = float(r.get("qty_per") or 0.0)
            loss = float(r.get("loss") or 1.0)
        except Exception:
            continue
        if not item or not comp:
            continue
        coef = max(0.0, qty_per * max(loss, 0.0))
        if coef <= 0:
            continue
        bom_map.setdefault(item, {})
        bom_map[item][comp] = bom_map[item].get(comp, 0.0) + coef

    shortage_qty = 0.0
    if bom_map:
        comp_set = sorted({c for mp in bom_map.values() for c in mp})
        inv = {c: max(0.0, float(stock_by_item.get(c, 0.0) or 0.0)) for c in comp_set}
        # Aggregate produced qty by item/period.
        item_qty: Dict[Tuple[str, int], float] = {}
        for oid, t, q in positive_points:
            it = str(orders[oid].item_id)
            item_qty[(it, t)] = item_qty.get((it, t), 0.0) + float(q)
        # Simulate inventory day-by-day with zero receipts.
        items = sorted({it for it, _ in item_qty.keys()})
        for t in range(len(periods)):
            for c in comp_set:
                cons = 0.0
                for it in items:
                    coef = float(bom_map.get(it, {}).get(c, 0.0) or 0.0)
                    if coef <= 0:
                        continue
                    cons += coef * float(item_qty.get((it, t), 0.0))
                if cons <= 0:
                    continue
                have = float(inv.get(c, 0.0) or 0.0)
                if cons <= have:
                    inv[c] = have - cons
                else:
                    shortage_qty += cons - have
                    inv[c] = 0.0

    kpi = {
        "unmet_qty": int(unmet_qty),
        "late_qty": int(late_qty),
        "late_weighted_days": int(late_weighted_days),
        "early_qty": int(early_qty),
        "early_weighted_days": int(early_weighted_days),
        "overload_sec": int(round(overload_sec)),
        "overload_hours": float(round(overload_sec / 3600.0, 3)),
        "mix_count": int(mix_count),
        "shortage_qty": float(round(shortage_qty, 3)),
        "avg_util": float(round((sum(util_vals) / len(util_vals)) if util_vals else 0.0, 4)),
    }
    return {"kpi": kpi, "machine_buckets": load_rows}
