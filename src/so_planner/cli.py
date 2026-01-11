import argparse
from .db import init_db, SessionLocal
from .ingest.loader import load_excels
from .scheduling.greedy import run_greedy
from .analysis.bottlenecks import scan_bottlenecks
from .export.report import export_excel
from .optimize.milp import run_milp

def main():
    parser = argparse.ArgumentParser(description="S&O Planner CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="Ingest -> Greedy schedule -> Export")
    run_p.add_argument("--machines", required=True)
    run_p.add_argument("--bom", required=True)
    run_p.add_argument("--plan", required=True)
    run_p.add_argument("--out", default="out/report.xlsx")

    args = parser.parse_args()

    init_db()
    with SessionLocal() as s:
        m,b,d = load_excels(s, args.machines, args.bom, args.plan)
        print(f"Ingested: machines={m}, bom={b}, demand={d}")
        ops = run_greedy(s)
        print(f"Greedy scheduled ops: {ops}")
        summary, hot = scan_bottlenecks(s)
        print("Bottlenecks summary:", summary)
        milp = run_milp(s)
        print("MILP (mock) KPIs:", milp)
        out_xlsx, gantt_png = export_excel(s, args.out)
        print("Exported:", out_xlsx, "Gantt:", gantt_png)

if __name__ == "__main__":
    main()
