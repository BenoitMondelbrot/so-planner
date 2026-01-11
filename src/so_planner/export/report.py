import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..models import ScheduleOp, Loads, DimMachine, Demand
from ..db.models import MachineLoadDaily  # prefer plan-aware loads when available
import matplotlib.pyplot as plt


def export_excel(session: Session, out_path: str, plan_id: int | None = None):
    """Export report to Excel.

    When plan_id is provided, exports only data for that plan (schedule_ops and machine_load_daily).
    Falls back to legacy Loads table when machine_load_daily is empty.
    """
    # Plan-aware ops
    if plan_id is not None:
        ops = session.query(ScheduleOp).filter(ScheduleOp.plan_id == plan_id).order_by(ScheduleOp.start_ts).all()
        loads_daily = session.query(MachineLoadDaily).filter(MachineLoadDaily.plan_id == plan_id).all()
    else:
        # legacy: all ops/loads
        ops = session.execute(select(ScheduleOp)).scalars().all()
        loads_daily = []

    # Fallback to legacy Loads sheet if no plan-aware loads
    if loads_daily:
        df_loads = pd.DataFrame([
            {
                "machine_id": l.machine_id,
                "date": pd.to_datetime(getattr(l, "work_date")),
                "minutes_used": int(getattr(l, "load_sec", 0)) / 60.0,
                "minutes_free": max(0.0, (int(getattr(l, "cap_sec", 0)) - int(getattr(l, "load_sec", 0))) / 60.0),
                "queue_len": None,
            }
            for l in loads_daily
        ])
    else:
        legacy_loads = session.execute(select(Loads)).scalars().all()
        df_loads = pd.DataFrame([
            {
                "machine_id": l.machine_id,
                "date": l.date,
                "minutes_used": l.minutes_used,
                "minutes_free": l.minutes_free,
                "queue_len": l.queue_len,
            }
            for l in legacy_loads
        ])

    machines = session.execute(select(DimMachine)).scalars().all()
    demand = session.execute(select(Demand)).scalars().all()

    df_ops = pd.DataFrame([
        {
            "op_id": o.op_id,
            "plan_id": o.plan_id,
            "order_id": o.order_id,
            "item_id": o.item_id,
            "article_name": getattr(o, "article_name", None),
            "machine_id": o.machine_id,
            "start_ts": o.start_ts,
            "end_ts": o.end_ts,
            "lateness_min": getattr(o, "lateness_min", 0.0),
        }
        for o in ops
    ])
    df_m = pd.DataFrame([
        {
            "machine_id": m.machine_id,
            "name": m.name,
            "family": m.family,
            "capacity_per_shift": m.capacity_per_shift,
        }
        for m in machines
    ])
    df_d = pd.DataFrame([
        {
            "order_id": getattr(d, "order_id", None),
            "item_id": d.item_id,
            "due_date": d.due_date,
            "qty": d.qty,
            "priority": d.priority,
        }
        for d in demand
    ])

    # Simple Gantt per first 200 ops
    if not df_ops.empty:
        gantt_png = out_path.replace(".xlsx", "_gantt.png")
        fig, ax = plt.subplots(figsize=(10, 6))
        head = df_ops.sort_values("start_ts").head(200)
        base = head["start_ts"].min()
        for i, (_, r) in enumerate(head.iterrows()):
            start_delta = (pd.to_datetime(r["start_ts"]) - pd.to_datetime(base)).total_seconds() / 3600.0
            dur_h = (pd.to_datetime(r["end_ts"]) - pd.to_datetime(r["start_ts"])) .total_seconds() / 3600.0
            ax.broken_barh([(start_delta, dur_h)], (i * 0.9, 0.8))
        ax.set_xlabel("Hours from first start")
        ax.set_ylabel("Operations (first 200)")
        ax.set_title("Gantt (simplified)")
        plt.tight_layout()
        fig.savefig(gantt_png, dpi=150)
        plt.close(fig)
    else:
        gantt_png = None

    with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm") as writer:
        df_ops.to_excel(writer, sheet_name="Schedule", index=False)
        df_loads.to_excel(writer, sheet_name="Loads", index=False)
        df_m.to_excel(writer, sheet_name="Machines", index=False)
        df_d.to_excel(writer, sheet_name="Demand", index=False)

        # KPI sheet
        workbook = writer.book
        kpi_sheet = workbook.add_worksheet("KPI")
        writer.sheets["KPI"] = kpi_sheet

        total_late = df_ops["lateness_min"].sum() if not df_ops.empty else 0.0
        kpi_sheet.write(0, 0, "Total lateness (min)")
        kpi_sheet.write(0, 1, float(total_late))
        kpi_sheet.write(1, 0, "# operations")
        kpi_sheet.write(1, 1, int(len(df_ops)))
        kpi_sheet.write(2, 0, "# machines")
        kpi_sheet.write(2, 1, int(len(df_m)))

        # Insert Gantt image if available
        if gantt_png:
            kpi_sheet.insert_image(4, 0, gantt_png, {"x_scale": 0.8, "y_scale": 0.8})

        # If plan filter was used, annotate
        if plan_id is not None:
            kpi_sheet.write(3, 0, "Plan ID")
            kpi_sheet.write(3, 1, int(plan_id))

    return out_path, gantt_png
