from __future__ import annotations

import math
from dataclasses import dataclass
import time
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

# We rely on OR-Tools routing solver to build per-machine sequences that minimize
# changeovers (setups). We then build a continuous-time schedule by concatenation.
try:
    from ortools.constraint_solver import pywrapcp, routing_enums_pb2  # type: ignore
    _HAVE_OR_TOOLS = True
except Exception:  # pragma: no cover
    _HAVE_OR_TOOLS = False


@dataclass
class OptimizeConfig:
    # Relative weights for objective components
    weight_setup: float = 1.0      # penalize changeovers/setup seconds
    weight_util: float = 0.0       # tie-breaker by duration
    weight_makespan: float = 0.0   # minimize route span (per machine)
    weight_smooth: float = 0.0     # daily load smoothing heuristic (post-processing)
    time_limit_sec: int = 10       # per-machine search time (assigned by caller)
    horizon_start: datetime | None = None
    horizon_end: datetime | None = None
    keep_outside_ops: bool = True


def _sequence_min_changeovers(items: List[Dict[str, Any]], cfg: OptimizeConfig) -> List[int]:
    """Return visit order indices for items on one machine using OR-Tools Routing.

    Nodes: depot (0) + N items (1..N). Costs between items encode setup seconds when item_id switches.
    """
    n = len(items)
    if n <= 1 or not _HAVE_OR_TOOLS:
        return list(range(n))

    # Build cost matrix (N+1 with depot at 0)
    # cost[i->j] = weight_setup * setup_sec(j) if item_i != item_j else 0
    # Add tiny tie-breaker by duration (favor longer tasks earlier) via weight_util
    depot = 0
    size = n + 1
    def item_idx(k: int) -> int:
        return k + 1

    cost: List[List[int]] = [[0] * size for _ in range(size)]
    # Helper accessors
    def item_field(idx: int, field: str, default: Any = None):
        return items[idx].get(field, default)

    for i in range(size):
        for j in range(size):
            if i == j:
                c = 0
            elif i == depot or j == depot:
                # depot to task / task to depot has zero cost
                c = 0
            else:
                ii = i - 1
                jj = j - 1
                same = (str(item_field(ii, "item_id")) == str(item_field(jj, "item_id")))
                setup_j = int(item_field(jj, "setup_sec", 0) or 0)
                dur_j = int(item_field(jj, "duration_sec", 0) or 0)
                c = 0 if same else setup_j
                # subtle tie-breaker: prefer putting longer jobs earlier (reduces makespan in practice)
                if cfg.weight_util > 0:
                    c = int(cfg.weight_setup * c + cfg.weight_util * (max(1, dur_j) // 60))
                else:
                    c = int(cfg.weight_setup * c)
            cost[i][j] = max(0, int(c))

    manager = pywrapcp.RoutingIndexManager(size, 1, depot)
    routing = pywrapcp.RoutingModel(manager)

    def transit_callback(from_index: int, to_index: int) -> int:
        i = manager.IndexToNode(from_index)
        j = manager.IndexToNode(to_index)
        return int(cost[i][j])

    transit_cb_idx = routing.RegisterTransitCallback(transit_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_cb_idx)

    # Add time dimension to allow minimizing makespan (span of route)
    # Transit time here equals setup(changeover) + duration of next job
    def time_callback(from_index: int, to_index: int) -> int:
        i = manager.IndexToNode(from_index)
        j = manager.IndexToNode(to_index)
        if i == 0 or j == 0:
            return 0
        ii = i - 1
        jj = j - 1
        same = (str(items[ii].get("item_id")) == str(items[jj].get("item_id")))
        setup_j = int(items[jj].get("setup_sec", 0) or 0)
        duration_j = int(items[jj].get("duration_sec", 0) or 0)
        setup_time = 0 if same else setup_j
        return int(setup_time + duration_j)

    time_cb_idx = routing.RegisterTransitCallback(time_callback)
    routing.AddDimension(
        time_cb_idx,
        0,             # slack
        10**9,         # large horizon
        True,          # start cumul at 0
        "time",
    )
    time_dim = routing.GetDimensionOrDie("time")
    if cfg.weight_makespan and cfg.weight_makespan > 0:
        # Penalize route span (end - start) to reduce makespan
        # Coefficient expects integer; scale modestly
        coef = max(1, int(cfg.weight_makespan * 1))
        time_dim.SetGlobalSpanCostCoefficient(coef)

    # Force end at depot as well
    routing.SetFixedCostOfAllVehicles(0)

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.log_search = False
    if cfg.time_limit_sec and cfg.time_limit_sec > 0:
        search_parameters.time_limit.FromSeconds(int(cfg.time_limit_sec))

    solution = routing.SolveWithParameters(search_parameters)
    if solution is None:
        # fallback: identity order
        return list(range(n))

    # Extract order from depot -> ... -> depot, map to 0..n-1 indices
    order: List[int] = []
    index = routing.Start(0)
    while not routing.IsEnd(index):
        node = manager.IndexToNode(index)
        if node != depot:
            order.append(node - 1)
        index = solution.Value(routing.NextVar(index))
    return order


def _build_schedule_for_machine(items: List[Dict[str, Any]], base_start: datetime, cfg: OptimizeConfig) -> List[Dict[str, Any]]:
    order = _sequence_min_changeovers(items, cfg)
    out: List[Dict[str, Any]] = []
    t = base_start
    prev_item: str | None = None
    for k in order:
        it = items[k]
        setup_sec = int(it.get("setup_sec", 0) or 0)
        duration_sec = int(it.get("duration_sec", 0) or 0)
        if prev_item is not None and str(prev_item) != str(it.get("item_id")):
            t = t + timedelta(seconds=setup_sec)
        start = t
        end = start + timedelta(seconds=duration_sec)
        row = dict(it)
        row["start_ts"] = start
        row["end_ts"] = end
        out.append(row)
        t = end
        prev_item = str(it.get("item_id"))
    return out


def _smooth_sequence_local(rows: List[Dict[str, Any]], base_start: datetime, cfg: OptimizeConfig) -> List[Dict[str, Any]]:
    """Lightweight local search: swap adjacent tasks if daily load variance reduces.
    Only applied when cfg.weight_smooth > 0.
    """
    if not rows or not (cfg.weight_smooth and cfg.weight_smooth > 0):
        return rows
    rows = list(rows)

    def schedule(rows_local: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, datetime], int]]:
        t = base_start
        prev_item = None
        sched = []
        loads: Dict[Tuple[str, datetime], int] = {}
        for r in rows_local:
            setup = int(r.get("setup_sec", 0) or 0)
            dur = int(r.get("duration_sec", 0) or 0)
            if prev_item is not None and str(prev_item) != str(r.get("item_id")):
                t = t + timedelta(seconds=setup)
            start = t
            end = start + timedelta(seconds=dur)
            rr = dict(r)
            rr["start_ts"] = start
            rr["end_ts"] = end
            sched.append(rr)
            # accumulate daily load in seconds
            d0 = datetime(start.year, start.month, start.day)
            loads[("day", d0)] = loads.get(("day", d0), 0) + dur
            prev_item = str(r.get("item_id"))
            t = end
        return sched, loads

    def smooth_score(loads: Dict[Tuple[str, datetime], int]) -> float:
        # target cap 8h per day per machine
        cap = 8 * 3600
        return sum((v - cap) ** 2 for (k, _), v in loads.items() if k == "day")

    sched0, loads0 = schedule(rows)
    best_score = smooth_score(loads0)
    improved = True
    tries = 0
    while improved and tries < 200:
        improved = False
        tries += 1
        for i in range(len(rows) - 1):
            cand = rows[:]
            cand[i], cand[i + 1] = cand[i + 1], cand[i]
            _, loads_c = schedule(cand)
            sc = smooth_score(loads_c)
            if sc + 1e-6 < best_score:
                rows = cand
                best_score = sc
                improved = True
                break
    # return final scheduled rows (with times)
    sched_final, _ = schedule(rows)
    return sched_final


def _metrics(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"ops": 0, "setups": 0, "setup_sec": 0, "makespan_sec": 0, "avg_util": 0.0}
    # sort by machine/start
    df2 = df.sort_values(["machine_id", "start_ts"]).reset_index(drop=True)
    setups = 0
    setup_sec = 0
    for _, g in df2.groupby("machine_id"):
        prev_item = None
        for r in g.itertuples(index=False):
            cur_item = str(getattr(r, "item_id"))
            if prev_item is not None and prev_item != cur_item:
                setups += 1
                setup_sec += int(getattr(r, "setup_sec", 0) or 0)
            prev_item = cur_item
    makespan_sec = int((df2["end_ts"].max() - df2["start_ts"].min()).total_seconds())
    try:
        from ..scheduling.utils import compute_daily_loads
        loads = compute_daily_loads(df2)
        avg_util = float(loads["util"].mean()) if len(loads) else 0.0
    except Exception:
        avg_util = 0.0
    return {
        "ops": int(len(df2)),
        "setups": int(setups),
        "setup_sec": int(setup_sec),
        "makespan_sec": int(makespan_sec),
        "avg_util": float(round(avg_util, 4)),
    }


def solve_milp(
    warm_start: Iterable[Dict[str, Any]],
    *,
    weight_setup: float = 1.0,
    weight_util: float = 0.0,
    weight_makespan: float = 0.0,
    weight_smooth: float = 0.0,
    time_limit_sec: int = 10,
    horizon_start: datetime | str | None = None,
    horizon_end: datetime | str | None = None,
    keep_outside_ops: bool = True,
    parallel_workers: int | None = None,
) -> pd.DataFrame:
    """Optimize schedule by resequencing operations on each machine.

    Input warm_start: list of dicts with keys at least
      [order_id, item_id, machine_id, start_ts, end_ts, duration_sec, setup_sec, op_index, batch_id, qty]

    Runtime semantics: `time_limit_sec` (aka runtime) — это лимит времени поиска решения
    для одного станка (Routing/OR‑Tools). Все станки в пределах горизонта оптимизируются,
    для каждого используется один и тот же лимит; расход времени на одном станке не уменьшает
    лимит на других.

    Returns a pandas DataFrame with updated start_ts/end_ts and the same columns.
    """
    if not _HAVE_OR_TOOLS:
        raise RuntimeError("ortools is not installed")

    df0 = pd.DataFrame(list(warm_start))
    if df0.empty:
        return df0

    # Normalize timestamps
    df0["start_ts"] = pd.to_datetime(df0["start_ts"])
    df0["end_ts"] = pd.to_datetime(df0["end_ts"])
    base_start_all = pd.to_datetime(df0["start_ts"].min()).to_pydatetime()

    hs = pd.to_datetime(horizon_start) if horizon_start is not None else None
    he = pd.to_datetime(horizon_end) if horizon_end is not None else None
    if hs is not None and he is not None and he < hs:
        he = hs

    cfg = OptimizeConfig(
        weight_setup=float(weight_setup or 0.0),
        weight_util=float(weight_util or 0.0),
        weight_makespan=float(weight_makespan or 0.0),
        weight_smooth=float(weight_smooth or 0.0),
        time_limit_sec=int(time_limit_sec or 0),
        horizon_start=hs.to_pydatetime() if hs is not None else None,
        horizon_end=he.to_pydatetime() if he is not None else None,
        keep_outside_ops=bool(keep_outside_ops),
    )

    def row_to_item(r) -> Dict[str, Any]:
        return {
            "order_id": str(r.order_id),
            "item_id": str(r.item_id),
            "machine_id": str(r.machine_id),
            "qty": float(getattr(r, "qty", 0) or 0),
            "duration_sec": int(getattr(r, "duration_sec", 0) or 0),
            "setup_sec": int(getattr(r, "setup_sec", 0) or 0),
            "op_index": int(getattr(r, "op_index", 0) or 0),
            "batch_id": str(getattr(r, "batch_id", "") or ""),
            "_start_ts0": pd.to_datetime(r.start_ts),
            "_end_ts0": pd.to_datetime(r.end_ts),
        }

    # Mask for horizon
    if hs is not None and he is not None:
        opt_mask = (df0["start_ts"] < he) & (df0["end_ts"] > hs)
    elif hs is not None:
        opt_mask = df0["start_ts"] >= hs
    elif he is not None:
        opt_mask = df0["end_ts"] <= he
    else:
        opt_mask = pd.Series([True] * len(df0), index=df0.index)

    df_opt = df0[opt_mask].copy()
    df_keep = df0[~opt_mask].copy() if cfg.keep_outside_ops else pd.DataFrame(columns=df0.columns)

    # logger (optional)
    try:
        import logging
        _log = logging.getLogger("so_planner.optimize")
        _log.info(
            "MILP start: ops=%d machines=%d runtime=%ss weights={setup:%.2f,util:%.2f,makespan:%.2f,smooth:%.2f}",
            len(df0), int(df0["machine_id"].nunique()), int(time_limit_sec or 0),
            float(weight_setup or 0.0), float(weight_util or 0.0), float(weight_makespan or 0.0), float(weight_smooth or 0.0)
        )
    except Exception:
        _log = None  # type: ignore

    out_rows: List[Dict[str, Any]] = []
    # For elapsed time logging only
    t0 = time.perf_counter()

    # Normalize duration for load-based sorting and compute loads per machine
    try:
        df_opt["duration_sec"] = pd.to_numeric(df_opt["duration_sec"], errors="coerce").fillna(0).astype(int)
    except Exception:
        pass
    groups = {m: g.copy() for m, g in df_opt.groupby("machine_id")}
    loads_by_machine: Dict[str, int] = {
        str(m): int(pd.to_numeric(g.get("duration_sec", 0), errors="coerce").fillna(0).sum()) for m, g in groups.items()
    }
    # Sort machines by total load descending (heavier first)
    sorted_machines = sorted(groups.keys(), key=lambda m: loads_by_machine.get(str(m), 0), reverse=True)

    # Worker function for one machine
    def _solve_one(machine_id: str) -> List[Dict[str, Any]]:
        g = groups[machine_id]
        items = [row_to_item(r) for r in g.itertuples(index=False)]
        base_start = pd.to_datetime(g["start_ts"].min()).to_pydatetime() if len(g) else base_start_all
        cfg_local = OptimizeConfig(
            weight_setup=cfg.weight_setup,
            weight_util=cfg.weight_util,
            weight_makespan=cfg.weight_makespan,
            weight_smooth=cfg.weight_smooth,
            time_limit_sec=cfg.time_limit_sec,
            horizon_start=cfg.horizon_start,
            horizon_end=cfg.horizon_end,
            keep_outside_ops=cfg.keep_outside_ops,
        )
        if _log:
            _log.info(
                "MILP machine start: machine=%s items=%d load_sec=%d runtime=%ss",
                machine_id,
                len(items),
                int(loads_by_machine.get(str(machine_id), 0)),
                int(cfg_local.time_limit_sec or 0),
            )
        t1 = time.perf_counter()
        sched = _build_schedule_for_machine(items, base_start, cfg_local)
        if cfg.weight_smooth and cfg.weight_smooth > 0:
            sched = _smooth_sequence_local(sched, base_start, cfg)
        if _log:
            _log.info(
                "MILP machine end: machine=%s elapsed=%.2fs ops=%d",
                machine_id,
                time.perf_counter() - t1,
                len(sched),
            )
        return sched

    if len(sorted_machines) > 0:
        # Determine parallelism
        max_workers_env = int(os.environ.get("SOPLANNER_MAX_WORKERS", "0") or 0)
        if parallel_workers is not None and parallel_workers > 0:
            workers = min(parallel_workers, len(sorted_machines))
        elif max_workers_env > 0:
            workers = min(max_workers_env, len(sorted_machines))
        else:
            workers = min((os.cpu_count() or 1), len(sorted_machines))

        if workers <= 1:
            for m in sorted_machines:
                out_rows.extend(_solve_one(str(m)))
        else:
            if _log:
                _log.info("MILP parallel: workers=%d machines=%d", workers, len(sorted_machines))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="milp") as ex:
                results = list(ex.map(lambda mid: _solve_one(str(mid)), sorted_machines))
            for sched in results:
                out_rows.extend(sched)

    # Normalize result frame; ensure expected columns even if empty
    expected_cols = [
        "order_id","item_id","machine_id","start_ts","end_ts",
        "qty","duration_sec","setup_sec","op_index","batch_id",
    ]
    df = pd.DataFrame(out_rows, columns=expected_cols)
    # Order by machine/start for stability (only if any rows)
    if not df.empty:
        df = df.sort_values(["machine_id", "start_ts"]).reset_index(drop=True)
    # Merge with outside ops if needed
    if len(df_keep):
        df_full = pd.concat([
            df_keep[[
                "order_id","item_id","machine_id","start_ts","end_ts","qty","duration_sec","setup_sec","op_index","batch_id"
            ]].copy(),
            df[[
                "order_id","item_id","machine_id","start_ts","end_ts","qty","duration_sec","setup_sec","op_index","batch_id"
            ]].copy(),
        ], ignore_index=True)
        df_full = df_full.sort_values(["machine_id","start_ts"]).reset_index(drop=True)
    else:
        df_full = df
    # Attach simple metrics for external consumption
    # Ensure proper dtypes for downstream consumers
    try:
        df_full["start_ts"] = pd.to_datetime(df_full["start_ts"])
        df_full["end_ts"] = pd.to_datetime(df_full["end_ts"])
    except Exception:
        pass

    mb = _metrics(df0)
    ma = _metrics(df_full)
    df_full.attrs["metrics_before"] = mb
    df_full.attrs["metrics_after"] = ma
    try:
        if _log:
            _log.info(
                "MILP end: elapsed=%.2fs ops_before=%d ops_after=%d setups_before=%d setups_after=%d makespan_before=%d makespan_after=%d",
                time.perf_counter() - t0,
                int(mb.get("ops", 0)), int(ma.get("ops", 0)),
                int(mb.get("setups", 0)), int(ma.get("setups", 0)),
                int(mb.get("makespan_sec", 0)), int(ma.get("makespan_sec", 0)),
            )
    except Exception:
        pass
    return df_full
