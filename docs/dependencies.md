# Project Dependencies

This list is derived from third-party imports found in `src/` and `tools/`.
Stdlib modules are excluded. `pyproject.toml` lists the same set; install via
`pip install -e .[runtime]` for API + Excel support out of the box.

## Runtime (imports in src/)
- fastapi: API entrypoints in `src/so_planner/api/*.py`.
- pydantic: request/response schemas in `src/so_planner/schemas.py` and API models.
- pydantic-settings: config loader in `src/so_planner/config.py`.
- sqlalchemy: ORM/data access in `src/so_planner/db/*`, `src/so_planner/api/*`,
  and `src/so_planner/analysis/*`.
- pandas: dataframes & Excel I/O across `src/so_planner/*` and `tools/*.py`.
- numpy: numeric ops in `src/so_planner/ingest/*`, `src/so_planner/scheduling/*`,
  and `src/so_planner/api/*`.
- openpyxl: Excel read/write & charts in `src/so_planner/ingest/loader.py` and
  `src/so_planner/scheduling/greedy_scheduler.py`.
- matplotlib: report plotting in `src/so_planner/export/report.py`.
- ortools: MILP/CP-SAT optimizers in `src/so_planner/optimize/milp.py` and
  `src/so_planner/optimize/jobshop.py` (raises at runtime if missing).

## Optional / runtime helpers
- uvicorn: run the FastAPI app.
- python-dotenv: required by `pydantic-settings` to load `.env` files referenced
  in `src/so_planner/config.py`.
- xlsxwriter: optional Excel writer fallback used via `pandas.ExcelWriter` in
  `tools/export_order_ops.py`.
- psycopg2-binary: PostgreSQL driver if you switch from SQLite to Postgres
  (set `DATABASE_URL` accordingly).

## Install examples
- Minimal runtime: `pip install -e .`
- With API/Excel helpers: `pip install -e .[runtime]`
- With Postgres: add `psycopg2-binary` to one of the commands above.
