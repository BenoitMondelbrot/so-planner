from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
from datetime import datetime
import time
import os

import pandas as pd
import logging

try:
    # OR-Tools CP-SAT job-shop modeling
    from ortools.sat.python import cp_model  # type: ignore
    _HAVE_CP_SAT = True
except Exception:  # pragma: no cover
    _HAVE_CP_SAT = False


def _to_sec(dt: datetime) -> int:
    return int(dt.timestamp())


def solve_jobshop(
    warm_start: Iterable[Dict[str, Any]],
    *,
    time_limit_sec: int = 20,
    horizon_start: datetime | str | None = None,
    horizon_end: datetime | str | None = None,
    keep_outside_ops: bool = True,
    include_setup_in_duration: bool = True,
    parallel_workers: int | None = None,
    # Objective weights (weighted sum)
    makespan_weight: float = 1.0,
    smooth_weight: float = 0.0,
    corridor_min_util: float = 0.0,
    corridor_max_util: float = 1.0,
    gap_penalty_per_sec: float = 0.0,
    # Daily capacity hard limit (per machine, per day)
    enforce_daily_cap: bool = False,
    daily_cap_sec: int | None = None,
) -> pd.DataFrame:
    """Job-shop optimization with precedence and machine capacity (NoOverlap).

    Inputs follow the same schema as `solve_milp` warm start:
      [order_id, item_id, machine_id, start_ts, end_ts, duration_sec, setup_sec, op_index, batch_id, qty]

    - Enforces intra-order precedences by op_index: all ops with index k
      must finish before any op with index k+1 start (per order_id).
    - Respects machine capacity across all orders on the machine (NoOverlap).
    - Optionally includes setup_sec into processing time (sequence-independent approx).
    - Operations outside the optimization horizon (if provided) are kept fixed and
      blocked on their machines to prevent overlap.

    Returns DataFrame with updated start_ts/end_ts for optimized ops, unchanged for kept ops.
    """
    if not _HAVE_CP_SAT:
        raise RuntimeError("ortools CP-SAT is not installed")

    df0 = pd.DataFrame(list(warm_start))
    if df0.empty:
        return df0

    # Normalize timestamps
    df0["start_ts"] = pd.to_datetime(df0["start_ts"])  # type: ignore
    df0["end_ts"] = pd.to_datetime(df0["end_ts"])      # type: ignore

    hs = pd.to_datetime(horizon_start) if horizon_start is not None else None
    he = pd.to_datetime(horizon_end) if horizon_end is not None else None
    if hs is not None and he is not None and he < hs:
        he = hs

    # Horizon mask (what to optimize)
    if hs is not None and he is not None:
        opt_mask = (df0["start_ts"] < he) & (df0["end_ts"] > hs)
    elif hs is not None:
        opt_mask = df0["start_ts"] >= hs
    elif he is not None:
        opt_mask = df0["end_ts"] <= he
    else:
        opt_mask = pd.Series([True] * len(df0), index=df0.index)

    df_opt = df0[opt_mask].copy()
    df_keep = df0[~opt_mask].copy() if keep_outside_ops else pd.DataFrame(columns=df0.columns)
    try:
        _log.info(
            "Jobshop mask: ops_total=%d opt=%d keep=%d keep_outside_ops=%s hs=%s he=%s",
            len(df0), len(df_opt), len(df_keep), bool(keep_outside_ops), str(hs), str(he)
        )
    except Exception:
        pass

    # Convert numeric fields
    for col in ("duration_sec", "setup_sec", "qty", "op_index"):
        if col in df_opt.columns:
            df_opt[col] = pd.to_numeric(df_opt[col], errors="coerce").fillna(0)
    for col in ("duration_sec", "setup_sec"):
        if col in df_opt.columns:
            df_opt[col] = df_opt[col].astype(int)
    if len(df_keep):
        for col in ("duration_sec", "setup_sec"):
            if col in df_keep.columns:
                df_keep[col] = pd.to_numeric(df_keep[col], errors="coerce").fillna(0).astype(int)

    # Base time origin for integer model (seconds since base)
    base_start = min(df0["start_ts"]) if len(df0) else pd.Timestamp.utcnow()
    if hs is not None:
        base_start = min(base_start, hs)
    base_start = pd.to_datetime(base_start).to_pydatetime()

    # Upper bound for time domain (generous but finite)
    sum_dur_opt = int(pd.to_numeric(df_opt.get("duration_sec", 0), errors="coerce").fillna(0).sum())
    if include_setup_in_duration and "setup_sec" in df_opt.columns:
        sum_dur_opt += int(pd.to_numeric(df_opt.get("setup_sec", 0), errors="coerce").fillna(0).sum())
    keep_end_max = 0
    if len(df_keep):
        keep_end_max = int((pd.to_datetime(df_keep["end_ts"]).max().to_pydatetime() - base_start).total_seconds())
    H = max(keep_end_max, 0) + sum_dur_opt + 24 * 3600  # add 1 day slack
    H = max(H, 1)

    model = cp_model.CpModel()
    _log = logging.getLogger("so_planner.optimize")

    # Quick diagnostics: precedence violations in input schedule
    try:
        def _count_prec_viol(df_in: pd.DataFrame) -> Tuple[int, int]:
            if df_in is None or df_in.empty:
                return 0, 0
            tmp = df_in[["order_id", "op_index", "start_ts", "end_ts"]].copy()
            tmp["op_index"] = pd.to_numeric(tmp["op_index"], errors="coerce").fillna(0).astype(int)
            agg = tmp.groupby([tmp["order_id"].astype(str), tmp["op_index"]]).agg(
                e_max=("end_ts", lambda x: pd.to_datetime(x).max()),
                s_min=("start_ts", lambda x: pd.to_datetime(x).min()),
            ).reset_index().rename(columns={"order_id": "order_id", "op_index": "lvl"})
            nxt = agg[["order_id", "lvl", "s_min"]].copy()
            nxt["lvl"] = nxt["lvl"] - 1
            nxt = nxt.rename(columns={"s_min": "s_min_next"})
            merged = agg.merge(nxt, on=["order_id", "lvl"], how="left")
            merged = merged.dropna(subset=["s_min_next"]).copy()
            merged["viol"] = (pd.to_datetime(merged["e_max"]) > pd.to_datetime(merged["s_min_next"]))
            n_pairs_viol = int(merged["viol"].sum())
            n_orders_viol = int(merged.loc[merged["viol"], "order_id"].nunique()) if n_pairs_viol > 0 else 0
            return n_pairs_viol, n_orders_viol

        n_pairs_all, n_orders_all = _count_prec_viol(df0)
        if n_pairs_all > 0:
            _log.warning(
                "Jobshop input precedence violations: pairs=%d orders=%d (full set)",
                n_pairs_all, n_orders_all,
            )
        if len(df_keep):
            n_pairs_keep, n_orders_keep = _count_prec_viol(df_keep)
            if n_pairs_keep > 0:
                _log.warning(
                    "Jobshop input precedence violations: pairs=%d orders=%d (kept/outside horizon)",
                    n_pairs_keep, n_orders_keep,
                )
    except Exception:
        # never fail due to diagnostics
        pass

    # Resolve parallelism for CP-SAT search
    workers = None
    try:
        env_workers = int(os.environ.get("SOPLANNER_MAX_WORKERS", "0") or 0)
    except Exception:
        env_workers = 0
    if parallel_workers is not None and parallel_workers > 0:
        workers = parallel_workers
    elif env_workers > 0:
        workers = env_workers
    else:
        workers = max(1, (os.cpu_count() or 1))

    # LB for optimized ops: not earlier than horizon_start (if provided)
    lb_opt = 0
    if hs is not None:
        lb_opt = max(0, int((hs.to_pydatetime() - base_start).total_seconds()))

    # Variables per operation
    # We create records for both kept (fixed) and optimized ops so we can enforce precedences.
    class Rec:
        __slots__ = ("key", "order_id", "op_index", "machine_id", "start", "end", "interval", "is_fixed")

    def make_key(r) -> Tuple[str, str, str, int, str, int]:
        return (
            str(getattr(r, "order_id")),
            str(getattr(r, "item_id")),
            str(getattr(r, "machine_id")),
            int(getattr(r, "op_index", 0) or 0),
            str(getattr(r, "batch_id", "") or ""),
            int(getattr(r, "duration_sec", 0) or 0),
        )

    by_machine: Dict[str, List[cp_model.IntervalVar]] = {}
    recs: List[Rec] = []

    # Fixed ops (outside horizon) block machines
    for i, r in enumerate(df_keep.itertuples(index=False)):
        mid = str(getattr(r, "machine_id"))
        s0 = int((pd.to_datetime(getattr(r, "start_ts")).to_pydatetime() - base_start).total_seconds())
        e0 = int((pd.to_datetime(getattr(r, "end_ts")).to_pydatetime() - base_start).total_seconds())
        d0 = max(0, e0 - s0)
        if d0 <= 0:
            continue
        s = model.NewIntVar(s0, s0, f"fix_s_{mid}_{i}")
        e = model.NewIntVar(e0, e0, f"fix_e_{mid}_{i}")
        interval = model.NewIntervalVar(s, d0, e, f"fix_int_{mid}_{i}")
        by_machine.setdefault(mid, []).append(interval)
        rr = Rec()
        rr.key = make_key(r)
        rr.order_id = str(getattr(r, "order_id"))
        rr.op_index = int(getattr(r, "op_index", 0) or 0)
        rr.machine_id = mid
        rr.start = s
        rr.end = e
        rr.interval = interval
        rr.is_fixed = True
        recs.append(rr)

    # Optimized ops (inside horizon)
    for i, r in enumerate(df_opt.itertuples(index=False)):
        mid = str(getattr(r, "machine_id"))
        dur = int(getattr(r, "duration_sec", 0) or 0)
        setup = int(getattr(r, "setup_sec", 0) or 0)
        p = max(1, int(dur + (setup if include_setup_in_duration else 0)))
        s = model.NewIntVar(lb_opt, H, f"s_{mid}_{i}")
        e = model.NewIntVar(lb_opt, H, f"e_{mid}_{i}")
        interval = model.NewIntervalVar(s, p, e, f"int_{mid}_{i}")
        model.Add(e == s + p)
        by_machine.setdefault(mid, []).append(interval)
        rr = Rec()
        rr.key = make_key(r)
        rr.order_id = str(getattr(r, "order_id"))
        rr.op_index = int(getattr(r, "op_index", 0) or 0)
        rr.machine_id = mid
        rr.start = s
        rr.end = e
        rr.interval = interval
        rr.is_fixed = False
        recs.append(rr)

    # Machine capacity: disjunctive per machine
    for mid, intervals in by_machine.items():
        if intervals:
            model.AddNoOverlap(intervals)
            try:
                # Log per-machine summary before solve
                n_ops = sum(1 for rr in recs if rr.machine_id == str(mid) and not rr.is_fixed)
                _log.info("Jobshop machine queued: machine=%s ops=%d (fixed=%d)", str(mid), n_ops, len(intervals)-n_ops)
            except Exception:
                pass

    # Precedence per order_id by op_index: all ops of index k must finish before any op of k+1
    from collections import defaultdict
    order_buckets: Dict[str, Dict[int, List[Rec]]] = defaultdict(lambda: defaultdict(list))
    for rr in recs:
        order_buckets[rr.order_id][rr.op_index].append(rr)
    for order_id, levels in order_buckets.items():
        idxs = sorted(levels.keys())
        for a, b in zip(idxs, idxs[1:]):
            for r_prev in levels[a]:
                for r_next in levels[b]:
                    model.Add(r_prev.end <= r_next.start)

    # Objective: weighted sum of components
    # - Makespan over all ops
    # - Sum of per-machine spans (proxy for smoothing)
    # - Sum of inter-level gaps per order (inventory gaps)
    ends = [rr.end for rr in recs]
    if not ends:
        return df0
    makespan = model.NewIntVar(0, H, "makespan")
    model.AddMaxEquality(makespan, ends)

    # Per-machine span terms
    sum_machine_span_terms: List[cp_model.IntVar] = []
    for mid, intervals in by_machine.items():
        vars_start = [rr.start for rr in recs if rr.machine_id == str(mid)]
        vars_end = [rr.end for rr in recs if rr.machine_id == str(mid)]
        if not vars_start or not vars_end:
            continue
        mn = model.NewIntVar(0, H, f"m_{mid}_min_start")
        mx = model.NewIntVar(0, H, f"m_{mid}_max_end")
        model.AddMinEquality(mn, vars_start)
        model.AddMaxEquality(mx, vars_end)
        span = model.NewIntVar(0, H, f"m_{mid}_span")
        model.Add(span == mx - mn)
        sum_machine_span_terms.append(span)

    # Inter-level gap terms per order
    from collections import defaultdict as _dd
    buckets: Dict[str, Dict[int, List[Any]]] = _dd(lambda: _dd(list))
    for rr in recs:
        buckets[str(rr.order_id)][int(rr.op_index)].append(rr)
    gap_terms: List[cp_model.IntVar] = []
    for ord_id, levels in buckets.items():
        idxs = sorted(levels.keys())
        for a, b in zip(idxs, idxs[1:]):
            next_starts = [r.start for r in levels[b]]
            prev_ends = [r.end for r in levels[a]]
            if not next_starts or not prev_ends:
                continue
            smin = model.NewIntVar(0, H, f"{ord_id}_lvl{b}_smin")
            emin = model.NewIntVar(0, H, f"{ord_id}_lvl{a}_emax")
            model.AddMinEquality(smin, next_starts)
            model.AddMaxEquality(emin, prev_ends)
            gap = model.NewIntVar(0, H, f"{ord_id}_{a}_{b}_gap")
            # With precedence constraints, smin >= emin always; enforce exact gap equality
            model.Add(gap == smin - emin)
            gap_terms.append(gap)

    # Build weighted linear objective (use coefficient scaling to support fractional weights)
    coef_scale = 1000  # preserve up to 3 decimals
    w_ms = int(round((makespan_weight or 0.0) * coef_scale))
    w_span = int(round((smooth_weight or 0.0) * coef_scale))
    w_gap = int(round((gap_penalty_per_sec or 0.0) * coef_scale))

    objective_terms: List[cp_model.LinearExpr] = []
    if w_ms > 0:
        objective_terms.append(w_ms * makespan)
    if w_span > 0 and sum_machine_span_terms:
        objective_terms.append(w_span * sum(sum_machine_span_terms))
    if w_gap > 0 and gap_terms:
        objective_terms.append(w_gap * sum(gap_terms))
    # Fallback to makespan if all weights are zero
    if not objective_terms:
        objective_terms = [makespan]
    model.Minimize(sum(objective_terms))

    solver = cp_model.CpSolver()
    if time_limit_sec and time_limit_sec > 0:
        solver.parameters.max_time_in_seconds = float(time_limit_sec)
    if workers and workers > 0:
        solver.parameters.num_search_workers = int(workers)
    solver.parameters.log_search_progress = False

    try:
        _log.info(
            "Jobshop start: ops=%d machines=%d runtime=%ss weights={makespan:%.3f,smooth_span:%.3f,gap:%.6f}",
            len(df0), int(df0["machine_id"].nunique()), int(time_limit_sec or 0),
            float(makespan_weight or 0.0), float(smooth_weight or 0.0), float(gap_penalty_per_sec or 0.0)
        )
    except Exception:
        pass

    def _status_name(st: int) -> str:
        try:
            if st == cp_model.OPTIMAL: return "OPTIMAL"
            if st == cp_model.FEASIBLE: return "FEASIBLE"
            if st == cp_model.INFEASIBLE: return "INFEASIBLE"
            if st == cp_model.MODEL_INVALID: return "MODEL_INVALID"
            if st == cp_model.UNKNOWN: return "UNKNOWN"
        except Exception:
            pass
        return str(st)

    t_solve0 = time.perf_counter()
    status = solver.Solve(model)
    t_solve = time.perf_counter() - t_solve0
    try:
        _log.info("Jobshop status: %s (%s) elapsed=%.2fs", str(status), _status_name(status), t_solve)
    except Exception:
        pass
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):  # type: ignore[attr-defined]
        # Fallback: fast feasible rebuild preserving per-machine sequences,
        # enforcing precedences and optional daily cap without local swap search.
        try:
            try:
                _log.warning("Jobshop solve failed (status=%s:%s). Using greedy fallback.", str(status), _status_name(status))
            except Exception:
                pass
            df_fb = _fast_rebuild_feasible(
                df0,
                base_start,
                include_setup_in_duration=bool(include_setup_in_duration),
                enforce_daily_cap=bool(enforce_daily_cap),
                daily_cap_sec=int(daily_cap_sec or (8 * 3600)) if enforce_daily_cap else None,
            )
            try:
                _log.info("Jobshop greedy fallback result rows: %d", len(df_fb) if df_fb is not None else 0)
            except Exception:
                pass
            return df_fb
        except Exception:
            return df0

    # Per-machine result logs similar to MILP
    try:
        for mid in by_machine.keys():
            mid = str(mid)
            rr_m = [rr for rr in recs if rr.machine_id == mid]
            if not rr_m:
                continue
            starts = [int(solver.Value(rr.start)) for rr in rr_m]
            ends_v = [int(solver.Value(rr.end)) for rr in rr_m]
            span_m = (max(ends_v) - min(starts)) if starts and ends_v else 0
            n_nonfixed = sum(1 for rr in rr_m if not rr.is_fixed)
            n_fixed = sum(1 for rr in rr_m if rr.is_fixed)
            _log.info(
                "Jobshop machine result: machine=%s elapsed=%.2fs ops=%d fixed=%d span_sec=%d",
                mid, t_solve, n_nonfixed, n_fixed, int(span_m)
            )
    except Exception:
        pass

    # Build results for optimized ops
    def rec_to_dict(r_src, start_sec: int, end_sec: int) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "order_id": str(getattr(r_src, "order_id")),
            "item_id": str(getattr(r_src, "item_id")),
            "machine_id": str(getattr(r_src, "machine_id")),
            "qty": float(getattr(r_src, "qty", 0) or 0),
            "duration_sec": int(getattr(r_src, "duration_sec", 0) or 0),
            "setup_sec": int(getattr(r_src, "setup_sec", 0) or 0),
            "op_index": int(getattr(r_src, "op_index", 0) or 0),
            "batch_id": str(getattr(r_src, "batch_id", "") or ""),
            "start_ts": (base_start + pd.Timedelta(seconds=int(start_sec))),
            "end_ts": (base_start + pd.Timedelta(seconds=int(end_sec))),
        }
        return d

    out_rows: List[Dict[str, Any]] = []
    # Map back: iterate df_opt rows in their order and use solver values from corresponding recs (match by composite key)
    # Build quick index: (order,item,machine,op_index,batch,duration) -> Rec (first non-fixed match)
    from collections import defaultdict as _dd
    idx_nonfixed: Dict[Tuple[str, str, str, int, str, int], List[Rec]] = _dd(list)
    for rr in recs:
        if not rr.is_fixed:
            idx_nonfixed[rr.key].append(rr)

    for r in df_opt.itertuples(index=False):
        key = (
            str(getattr(r, "order_id")),
            str(getattr(r, "item_id")),
            str(getattr(r, "machine_id")),
            int(getattr(r, "op_index", 0) or 0),
            str(getattr(r, "batch_id", "") or ""),
            int(getattr(r, "duration_sec", 0) or 0),
        )
        lst = idx_nonfixed.get(key, [])
        if not lst:
            # Fallback to time-preserving copy (shouldn't happen)
            out_rows.append(rec_to_dict(r, 0, int(getattr(r, "duration_sec", 0) or 0)))
            continue
        rr = lst.pop(0)
        s_val = int(solver.Value(rr.start))
        e_val = int(solver.Value(rr.end))
        out_rows.append(rec_to_dict(r, s_val, e_val))

    # Merge with kept operations
    if len(df_keep):
        for r in df_keep.itertuples(index=False):
            out_rows.append({
                "order_id": str(getattr(r, "order_id")),
                "item_id": str(getattr(r, "item_id")),
                "machine_id": str(getattr(r, "machine_id")),
                "qty": float(getattr(r, "qty", 0) or 0),
                "duration_sec": int(getattr(r, "duration_sec", 0) or 0),
                "setup_sec": int(getattr(r, "setup_sec", 0) or 0),
                "op_index": int(getattr(r, "op_index", 0) or 0),
                "batch_id": str(getattr(r, "batch_id", "") or ""),
                "start_ts": pd.to_datetime(getattr(r, "start_ts")),
                "end_ts": pd.to_datetime(getattr(r, "end_ts")),
            })

    df = pd.DataFrame(out_rows, columns=[
        "order_id","item_id","machine_id","start_ts","end_ts",
        "qty","duration_sec","setup_sec","op_index","batch_id",
    ])
    try:
        _log.info("Jobshop result rows: %d (opt + kept)", len(df))
    except Exception:
        pass
    if not df.empty:
        df = df.sort_values(["machine_id", "start_ts"]).reset_index(drop=True)

    # Optional: enforce daily capacity limit before smoothing (hard cap)
    try:
        if enforce_daily_cap:
            cap = int(daily_cap_sec or (8 * 3600))
            try:
                _log.info("Jobshop enforce daily cap: cap_sec=%d", cap)
            except Exception:
                pass
            df = _enforce_daily_cap(df, base_start, include_setup_in_duration=include_setup_in_duration, daily_cap_sec=cap)
    except Exception:
        pass

    # Post-processing heuristic disabled: objectives are integrated into the solver's weighted sum now.

    # Attach simple metrics similar to MILP solver
    def _metrics(df: pd.DataFrame) -> Dict[str, Any]:
        if df is None or df.empty:
            return {"ops": 0, "setups": 0, "setup_sec": 0, "makespan_sec": 0, "avg_util": 0.0}
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
        makespan_sec = int((pd.to_datetime(df2["end_ts"]).max() - pd.to_datetime(df2["start_ts"]).min()).total_seconds())
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

    try:
        df["start_ts"] = pd.to_datetime(df["start_ts"])  # type: ignore
        df["end_ts"] = pd.to_datetime(df["end_ts"])      # type: ignore
    except Exception:
        pass
    df.attrs["metrics_before"] = _metrics(df0)
    df.attrs["metrics_after"] = _metrics(df)
    return df


def _post_smooth_with_precedence(
    df_in: pd.DataFrame,
    base_start: datetime,
    *,
    include_setup_in_duration: bool,
    smooth_weight: float,
    corridor_min_util: float,
    corridor_max_util: float,
    gap_penalty_per_sec: float,
    enforce_daily_cap: bool = False,
    daily_cap_sec: int | None = None,
) -> pd.DataFrame:
    """Heuristic: improve schedule by adjacent swaps per machine.

    - Keeps machine sequences mostly intact; tries local swaps.
    - Rebuilds full feasible schedule after each accepted swap respecting precedences and machine capacity.
    - Objective combines corridor smoothing penalty and inventory gap penalty.
    """
    if df_in is None or df_in.empty:
        return df_in

    df = df_in.copy().reset_index(drop=True)

    # Extract sequences per machine from current order by start_ts
    machines = sorted(df["machine_id"].astype(str).unique().tolist())
    seqs = {m: df[df["machine_id"].astype(str) == str(m)]
                .sort_values(["start_ts","end_ts"]) 
                .index.tolist() for m in machines}

    # Helper: rebuild feasible schedule given sequences
    def rebuild_from_seqs(seqs_local: Dict[str, List[int]]) -> pd.DataFrame:
        # Initialize availability per machine and result starts/ends
        t_machine: Dict[str, datetime] = {str(m): base_start for m in machines}
        start_ts = [None] * len(df)
        end_ts = [None] * len(df)
        # Track per order per level the max end time achieved so far
        from collections import defaultdict
        end_by_order_level: Dict[str, Dict[int, datetime]] = defaultdict(dict)
        # Track used seconds per machine-day
        used_by_day: Dict[Tuple[str, datetime], int] = {}

        # Prepare remaining counts per machine
        pointers: Dict[str, int] = {str(m): 0 for m in machines}
        counts: Dict[str, int] = {str(m): len(seqs_local.get(str(m), [])) for m in machines}

        # Precompute processing times
        proc: List[int] = []
        for r in df.itertuples(index=True, name=None):
            idx = r[0]
            dur = int(getattr(r, "duration_sec", 0) or 0)
            setup = int(getattr(r, "setup_sec", 0) or 0)
            p = max(1, int(dur + (setup if include_setup_in_duration else 0)))
            proc.append(p)

        # Global loop: schedule until all pointers reach counts
        scheduled = 0
        total = sum(counts.values())
        # For quick index to row attrs
        order_ids = df["order_id"].astype(str).tolist()
        op_idx = pd.to_numeric(df["op_index"], errors="coerce").fillna(0).astype(int).tolist()
        machine_ids = df["machine_id"].astype(str).tolist()

        # Build map from machine to sequence list for quick picks
        while scheduled < total:
            # Find next schedulable operation: the machine whose next op has predecessors completed
            best_choice = None
            best_ready_time = None
            for m in machines:
                m = str(m)
                p = pointers[m]
                if p >= counts[m]:
                    continue
                idx = seqs_local[m][p]
                ord_id = order_ids[idx]
                level = op_idx[idx]
                # release time = max end over all previous levels for this order
                rel = base_start
                if level > 0:
                    # consider max over [0..level-1]
                    have = [end_by_order_level.get(ord_id, {}).get(k) for k in range(level)]
                    have = [x for x in have if x is not None]
                    if have:
                        rel = max(have)
                ready = max(t_machine[m], rel)
                if (best_ready_time is None) or (ready < best_ready_time):
                    best_ready_time = ready
                    best_choice = (m, idx, ready)

            if best_choice is None:
                # fallback: pick any pending and push by machine availability
                for m in machines:
                    m = str(m)
                    p = pointers[m]
                    if p < counts[m]:
                        idx = seqs_local[m][p]
                        best_choice = (m, idx, t_machine[m])
                        break

            m, idx, st = best_choice  # type: ignore
            ptime = int(proc[idx])
            # Enforce daily cap: push start to a day with enough remaining capacity (if enabled and ptime<=cap)
            if enforce_daily_cap and (daily_cap_sec is not None) and daily_cap_sec > 0 and ptime <= daily_cap_sec:
                while True:
                    day0 = datetime(st.year, st.month, st.day)
                    used = used_by_day.get((m, day0), 0)
                    if used + ptime <= int(daily_cap_sec):
                        break
                    # push to next day start
                    next_day = day0 + pd.Timedelta(days=1)
                    st = max(next_day, st)  # ensure not moving backwards
            en = st + pd.Timedelta(seconds=ptime)
            start_ts[idx] = st
            end_ts[idx] = en
            # update machine pointer and availability
            pointers[m] += 1
            t_machine[m] = en
            # update order-level completion
            ord_id = order_ids[idx]
            level = op_idx[idx]
            prev = end_by_order_level.get(ord_id, {})
            prev[level] = max(prev.get(level, base_start), en)
            end_by_order_level[ord_id] = prev
            # update used_by_day with split across days
            if enforce_daily_cap and (daily_cap_sec is not None) and daily_cap_sec > 0:
                cur = datetime(st.year, st.month, st.day)
                while cur < en:
                    cur_end = cur + pd.Timedelta(days=1)
                    seg_start = st if st > cur else cur
                    seg_end = en if en < cur_end else cur_end
                    sec = int((seg_end - seg_start).total_seconds())
                    if sec > 0:
                        used_by_day[(m, cur)] = used_by_day.get((m, cur), 0) + sec
                    cur = cur_end
            scheduled += 1

        out = df.copy()
        out["start_ts"] = pd.to_datetime(start_ts)
        out["end_ts"] = pd.to_datetime(end_ts)
        return out.sort_values(["machine_id","start_ts"]).reset_index(drop=True)

    # Penalty evaluation
    def eval_penalty(df_sched: pd.DataFrame) -> Tuple[float, int]:
        # corridor smoothing
        from ..scheduling.utils import compute_daily_loads
        loads = compute_daily_loads(df_sched)
        smooth_pen = 0.0
        if len(loads):
            for r in loads.itertuples(index=False):
                cap = int(getattr(r, "cap_sec", 8*3600) or 8*3600)
                min_sec = max(0, int(cap * float(corridor_min_util)))
                max_sec = max(0, int(cap * float(corridor_max_util)))
                val = int(getattr(r, "load_sec", 0) or 0)
                if val < min_sec:
                    diff = float(min_sec - val)
                    smooth_pen += diff * diff
                elif val > max_sec:
                    diff = float(val - max_sec)
                    smooth_pen += diff * diff
        # inventory gap: per order between consecutive levels
        gap_pen = 0.0
        from collections import defaultdict
        by_order = defaultdict(list)
        for r in df_sched.itertuples(index=False):
            by_order[str(getattr(r, "order_id"))].append((int(getattr(r, "op_index", 0) or 0), pd.to_datetime(getattr(r, "start_ts")), pd.to_datetime(getattr(r, "end_ts"))))
        for ord_id, ops in by_order.items():
            # group by level
            levels: Dict[int, Tuple[datetime, datetime]] = {}
            from collections import defaultdict as _dd
            st_min = _dd(lambda: None)
            en_max = _dd(lambda: None)
            for lvl, st, en in ops:
                if st_min[lvl] is None or st < st_min[lvl]:
                    st_min[lvl] = st
                if en_max[lvl] is None or en > en_max[lvl]:
                    en_max[lvl] = en
            if len(st_min) == 0:
                continue
            idxs = sorted(en_max.keys())
            for a, b in zip(idxs, idxs[1:]):
                end_prev = en_max[a]
                start_next = st_min[b]
                if end_prev is None or start_next is None:
                    continue
                gap = (start_next - end_prev).total_seconds()
                if gap > 0:
                    gap_pen += float(gap)
        total_pen = float(smooth_weight) * smooth_pen + float(gap_penalty_per_sec) * gap_pen
        # Return makespan (seconds) for tie-breakers
        makespan_sec = int((pd.to_datetime(df_sched["end_ts"]).max() - pd.to_datetime(df_sched["start_ts"]).min()).total_seconds()) if len(df_sched) else 0
        return total_pen, makespan_sec

    # Hill-climb: adjacent swaps per machine
    best = df
    best_pen, best_ms = eval_penalty(best)
    improved = True
    tries = 0
    max_tries = 200
    while improved and tries < max_tries:
        improved = False
        tries += 1
        for m in machines:
            seq = list(seqs[str(m)])
            n = len(seq)
            for i in range(n - 1):
                cand_seq = dict(seqs)
                s2 = list(seq)
                s2[i], s2[i+1] = s2[i+1], s2[i]
                cand_seq[str(m)] = s2
                cand_df = rebuild_from_seqs(cand_seq)
                pen, ms = eval_penalty(cand_df)
                if (pen + 1e-6 < best_pen) or (abs(pen - best_pen) <= 1e-6 and ms < best_ms):
                    best = cand_df
                    best_pen, best_ms = pen, ms
                    seqs[str(m)] = s2
                    improved = True
                    break
            if improved:
                break
    return best


def _enforce_daily_cap(
    df_in: pd.DataFrame,
    base_start: datetime,
    *,
    include_setup_in_duration: bool,
    daily_cap_sec: int,
) -> pd.DataFrame:
    """Rebuild schedule to enforce hard daily capacity per machine.

    Takes the order of operations per machine from df_in and re-schedules with:
      - machine no-overlap (sequential)
      - order precedence by op_index
      - daily cap: if op fits in remaining day capacity, schedule it; otherwise move to next day start
      - long ops (ptime > cap) are scheduled as-is (may span multiple days)
    """
    if df_in is None or df_in.empty:
        return df_in
    df = df_in.copy().reset_index(drop=True)
    machines = sorted(df["machine_id"].astype(str).unique().tolist())
    # sequences by current start order
    seqs = {m: df[df["machine_id"].astype(str) == str(m)].sort_values(["start_ts","end_ts"]).index.tolist() for m in machines}

    #re-use smoother's rebuild with zero weights and cap enabled
    return _post_smooth_with_precedence(
        df,
        base_start,
        include_setup_in_duration=include_setup_in_duration,
        smooth_weight=0.0,
        corridor_min_util=0.0,
        corridor_max_util=1.0,
        gap_penalty_per_sec=0.0,
        enforce_daily_cap=True,
        daily_cap_sec=int(daily_cap_sec),
    )


def _greedy_levelwise_schedule(
    df_in: pd.DataFrame,
    base_start: datetime,
    *,
    include_setup_in_duration: bool,
    enforce_daily_cap: bool = False,
    daily_cap_sec: int | None = None,
) -> pd.DataFrame:
    """Fast fallback: schedule per precedence level, preserving per-machine order.

    - Orders are scheduled level-by-level (op_index ascending),
      so all ops of level k finish before any of level k+1.
    - Within each level, per machine we follow the original order by start_ts.
    - Enforces machine no-overlap and optional daily cap (simple push-to-next-day).
    """
    if df_in is None or df_in.empty:
        return df_in

    df = df_in.copy().reset_index(drop=True)
    df["op_index"] = pd.to_numeric(df["op_index"], errors="coerce").fillna(0).astype(int)
    machines = sorted(df["machine_id"].astype(str).unique().tolist())
    levels = sorted(df["op_index"].astype(int).unique().tolist())

    # Stable order by machine and original start/end
    df = df.sort_values(["machine_id", "start_ts", "end_ts"]).reset_index(drop=True)

    # Precompute lists per (level, machine)
    from collections import defaultdict
    seqs: Dict[tuple, list] = defaultdict(list)
    order_ids = df["order_id"].astype(str).tolist()
    machine_ids = df["machine_id"].astype(str).tolist()
    op_idx = df["op_index"].astype(int).tolist()
    durations = pd.to_numeric(df["duration_sec"], errors="coerce").fillna(0).astype(int).tolist()
    setups = pd.to_numeric(df.get("setup_sec", 0), errors="coerce").fillna(0).astype(int).tolist()
    for i in range(len(df)):
        seqs[(op_idx[i], machine_ids[i])].append(i)

    # Outputs
    start_ts = [None] * len(df)
    end_ts = [None] * len(df)
    t_machine: Dict[str, datetime] = {m: base_start for m in machines}
    end_by_order_level: Dict[str, Dict[int, datetime]] = defaultdict(dict)
    used_by_day: Dict[tuple, int] = {}

    def ptime(i: int) -> int:
        dur = int(durations[i] or 0)
        stp = int(setups[i] or 0)
        return max(1, int(dur + (stp if include_setup_in_duration else 0)))

    for lvl in levels:
        for m in machines:
            key = (lvl, str(m))
            seq = seqs.get(key, [])
            for i in seq:
                ord_id = order_ids[i]
                rel = base_start
                if lvl > 0:
                    prev_levels = end_by_order_level.get(ord_id, {})
                    have = [prev_levels.get(k) for k in range(lvl)]
                    have = [x for x in have if x is not None]
                    if have:
                        rel = max(have)
                st = t_machine[str(m)]
                if st < rel:
                    st = rel
                p = ptime(i)
                # enforce daily cap by pushing to next day start if no room
                if enforce_daily_cap and (daily_cap_sec is not None) and daily_cap_sec > 0 and p <= daily_cap_sec:
                    while True:
                        day0 = datetime(st.year, st.month, st.day)
                        used = used_by_day.get((str(m), day0), 0)
                        if used + p <= int(daily_cap_sec):
                            break
                        next_day = day0 + pd.Timedelta(days=1)
                        st = max(next_day, st)
                en = st + pd.Timedelta(seconds=p)
                start_ts[i] = st
                end_ts[i] = en
                t_machine[str(m)] = en
                prev = end_by_order_level.get(ord_id, {})
                prev[lvl] = max(prev.get(lvl, base_start), en)
                end_by_order_level[ord_id] = prev
                if enforce_daily_cap and (daily_cap_sec is not None) and daily_cap_sec > 0:
                    cur = datetime(st.year, st.month, st.day)
                    while cur < en:
                        cur_end = cur + pd.Timedelta(days=1)
                        seg_start = st if st > cur else cur
                        seg_end = en if en < cur_end else cur_end
                        sec = int((seg_end - seg_start).total_seconds())
                        if sec > 0:
                            used_by_day[(str(m), cur)] = used_by_day.get((str(m), cur), 0) + sec
                        cur = cur_end

    out = df.copy()
    out["start_ts"] = pd.to_datetime(start_ts)
    out["end_ts"] = pd.to_datetime(end_ts)
    return out.sort_values(["machine_id", "start_ts"]).reset_index(drop=True)


def _fast_rebuild_feasible(
    df_in: pd.DataFrame,
    base_start: datetime,
    *,
    include_setup_in_duration: bool,
    enforce_daily_cap: bool = False,
    daily_cap_sec: int | None = None,
) -> pd.DataFrame:
    """Fast feasible rebuild without swaps using earliest-ready selection.

    - Preserves the original order of operations per machine (by start_ts, end_ts).
    - At each step, picks the next machine whose next operation is ready earliest
      (max of machine availability and order precedence release), schedules it.
    - Enforces NoOverlap and optional daily cap with simple push-to-next-day.
    """
    if df_in is None or df_in.empty:
        return df_in

    df = df_in.copy().reset_index(drop=True)
    # Normalize dtypes
    df["op_index"] = pd.to_numeric(df["op_index"], errors="coerce").fillna(0).astype(int)
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce").fillna(0).astype(int)
    if "setup_sec" in df.columns:
        df["setup_sec"] = pd.to_numeric(df["setup_sec"], errors="coerce").fillna(0).astype(int)
    else:
        df["setup_sec"] = 0

    # Extract sequences per machine preserving input order
    machines = sorted(df["machine_id"].astype(str).unique().tolist())
    seqs = {m: df[df["machine_id"].astype(str) == str(m)]
                .sort_values(["start_ts","end_ts"]) 
                .index.tolist() for m in machines}

    # State
    from collections import defaultdict
    t_machine: Dict[str, datetime] = {str(m): base_start for m in machines}
    pointers: Dict[str, int] = {str(m): 0 for m in machines}
    counts: Dict[str, int] = {str(m): len(seqs.get(str(m), [])) for m in machines}
    start_ts: List[datetime | None] = [None] * len(df)
    end_ts: List[datetime | None] = [None] * len(df)
    end_by_order_level: Dict[str, Dict[int, datetime]] = defaultdict(dict)
    used_by_day: Dict[Tuple[str, datetime], int] = {}

    # Aliases to arrays for speed
    order_ids = df["order_id"].astype(str).tolist()
    op_idx = df["op_index"].astype(int).tolist()
    duration = df["duration_sec"].astype(int).tolist()
    setup = df["setup_sec"].astype(int).tolist()

    def ptime(i: int) -> int:
        d = int(duration[i] or 0)
        s = int(setup[i] or 0)
        return max(1, int(d + (s if include_setup_in_duration else 0)))

    total = sum(counts.values())
    scheduled = 0
    while scheduled < total:
        best = None  # (machine_id, idx, ready_time)
        best_ready = None
        # find earliest ready across machines
        for m in machines:
            m = str(m)
            p = pointers[m]
            if p >= counts[m]:
                continue
            i = seqs[m][p]
            rel = base_start
            lvl = op_idx[i]
            if lvl > 0:
                prev = end_by_order_level.get(order_ids[i], {})
                have = [prev.get(k) for k in range(lvl)]
                have = [x for x in have if x is not None]
                if have:
                    rel = max(have)
            ready = max(t_machine[m], rel)
            if (best_ready is None) or (ready < best_ready):
                best_ready = ready
                best = (m, i, ready)

        if best is None:
            # No machine has pending ops: break
            break
        m, i, st = best
        p = ptime(i)
        # enforce daily cap
        if enforce_daily_cap and (daily_cap_sec is not None) and daily_cap_sec > 0 and p <= daily_cap_sec:
            while True:
                day0 = datetime(st.year, st.month, st.day)
                used = used_by_day.get((m, day0), 0)
                if used + p <= int(daily_cap_sec):
                    break
                st = max(day0 + pd.Timedelta(days=1), st)
        en = st + pd.Timedelta(seconds=p)
        start_ts[i] = st
        end_ts[i] = en
        pointers[m] += 1
        t_machine[m] = en
        # update order-level completion
        ord_id = order_ids[i]
        lvl = op_idx[i]
        prev = end_by_order_level.get(ord_id, {})
        prev[lvl] = max(prev.get(lvl, base_start), en)
        end_by_order_level[ord_id] = prev
        if enforce_daily_cap and (daily_cap_sec is not None) and daily_cap_sec > 0:
            cur = datetime(st.year, st.month, st.day)
            while cur < en:
                cur_end = cur + pd.Timedelta(days=1)
                seg_start = st if st > cur else cur
                seg_end = en if en < cur_end else cur_end
                sec = int((seg_end - seg_start).total_seconds())
                if sec > 0:
                    used_by_day[(m, cur)] = used_by_day.get((m, cur), 0) + sec
                cur = cur_end
        scheduled += 1

    out = df.copy()
    out["start_ts"] = pd.to_datetime(start_ts)
    out["end_ts"] = pd.to_datetime(end_ts)
    return out.sort_values(["machine_id", "start_ts"]).reset_index(drop=True)
    _log = logging.getLogger("so_planner.optimize")
    try:
        _log.info(
            "Jobshop start: ops=%d machines=%d runtime=%ss enforce_cap=%s cap_sec=%s smooth_w=%.2f corridor=[%.2f,%.2f] gap_w=%s",
            len(df0), int(df0["machine_id"].nunique()), int(time_limit_sec or 0),
            bool(enforce_daily_cap), (int(daily_cap_sec or 0) if enforce_daily_cap else None),
            float(smooth_weight or 0.0), float(corridor_min_util or 0.0), float(corridor_max_util or 1.0),
            float(gap_penalty_per_sec or 0.0)
        )
    except Exception:
        pass
