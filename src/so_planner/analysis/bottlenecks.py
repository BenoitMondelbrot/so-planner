from sqlalchemy.orm import Session
from sqlalchemy import select, func
from ..models import Loads

def scan_bottlenecks(session: Session, util_threshold: float = 0.9):
    rows = session.execute(select(Loads)).scalars().all()
    by_machine = {}
    hot_days = []
    for r in rows:
        cap = r.minutes_used + r.minutes_free
        util = (r.minutes_used / cap) if cap > 0 else 0.0
        rec = by_machine.setdefault(r.machine_id, {"max_util":0.0, "avg_util_sum":0.0, "n":0})
        rec["max_util"] = max(rec["max_util"], util)
        rec["avg_util_sum"] += util
        rec["n"] += 1
        if util >= util_threshold:
            hot_days.append((r.machine_id, r.date, round(util, 3)))
    summary = {m: {"max_util": round(v["max_util"],3), "avg_util": round(v["avg_util_sum"]/max(1,v["n"]),3)} 
               for m, v in by_machine.items()}
    return summary, hot_days
