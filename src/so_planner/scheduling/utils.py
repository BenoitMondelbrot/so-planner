# src/so_planner/scheduling/utils.py
import pandas as pd
from datetime import datetime, timedelta, time

def compute_daily_loads(
    df_ops: pd.DataFrame,
    cap_per_machine_day_sec: dict[str, int] | None = None,
) -> pd.DataFrame:
    """
    На вход df с колонками: machine_id,start_ts,end_ts,duration_sec
    Режет каждую операцию по границам дней и суммирует по дням.
    Возвращает df: machine_id, work_date (datetime на 00:00), load_sec, cap_sec, util
    """
    if df_ops is None or df_ops.empty:
        return pd.DataFrame(columns=["machine_id", "work_date", "load_sec", "cap_sec", "util"])

    req_cols = {"machine_id", "start_ts", "end_ts"}
    missing = req_cols - set(df_ops.columns)
    if missing:
        raise ValueError(f"compute_daily_loads: missing columns: {sorted(missing)}")

    records = []
    for r in df_ops.itertuples(index=False):
        mid = getattr(r, "machine_id")
        start = getattr(r, "start_ts")
        end = getattr(r, "end_ts")
        if pd.isna(start):
            continue
        # Нормализуем к Python datetime
        if isinstance(start, pd.Timestamp):
            start = start.to_pydatetime()
        if isinstance(end, pd.Timestamp):
            end = end.to_pydatetime()
        # Prefer duration_sec when timestamps are day markers or end is missing.
        dur = getattr(r, "duration_sec", None)
        try:
            dur = float(dur)
        except Exception:
            dur = 0.0
        if dur and dur > 0:
            if end is None or pd.isna(end) or end <= start:
                end = start + timedelta(seconds=dur)
            else:
                try:
                    if start.time() == time.min and end.time() == time.min:
                        end = start + timedelta(seconds=dur)
                except Exception:
                    pass
        if end is None or pd.isna(end) or end <= start:
            continue

        # Идем от полуночи к полуночи, распределяя длительность по дням
        day_cursor = datetime.combine(start.date(), time.min)
        while day_cursor < end:
            day_start = day_cursor
            day_end = day_start + timedelta(days=1)
            seg_start = start if start > day_start else day_start
            seg_end = end if end < day_end else day_end
            sec = (seg_end - seg_start).total_seconds()
            if sec > 0:
                records.append((mid, day_start, int(round(sec))))
            day_cursor = day_end

    if not records:
        return pd.DataFrame(columns=["machine_id", "work_date", "load_sec", "cap_sec", "util"])

    g = (
        pd.DataFrame(records, columns=["machine_id", "work_date", "load_sec"])
        .groupby(["machine_id", "work_date"], as_index=False)["load_sec"].sum()
    )

    if cap_per_machine_day_sec:
        g["cap_sec"] = g["machine_id"].map(cap_per_machine_day_sec).fillna(8 * 3600).astype(int)
    else:
        g["cap_sec"] = 8 * 3600

    g["util"] = (g["load_sec"] / g["cap_sec"]).clip(0, 10.0)
    return g[["machine_id", "work_date", "load_sec", "cap_sec", "util"]]
