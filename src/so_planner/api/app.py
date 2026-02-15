# === imports (чистим) ===
from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
import logging
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os, shutil, uuid, traceback
import time
import pandas as pd
import numpy as np
import datetime as dt

from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import text, func, or_
from collections import defaultdict
from threading import Lock, Thread
from copy import deepcopy


# ВАЖНО: не тянем Base из .models, а берём только SessionLocal/init_db из пакета db
from ..db import SessionLocal, init_db

from pathlib import Path

from ..ingest.loader import load_excels, validate_files
from ..bom_versioning import bom_df_to_scheduler_df, get_resolved_bom_version, get_version_rows_df
from ..scheduling.greedy_scheduler import run_greedy, compute_orders_timeline, load_stock_any
from ..export.report import export_excel
from ..analysis.bottlenecks import scan_bottlenecks
from .routers import plans, optimize, bom, sales_plans

# ================== App ==================
app = FastAPI(title="S&O Planner API")

STATIC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ui"))
if os.path.isdir(STATIC_DIR):  # монтируем только если каталог существует
    app.mount("/ui", StaticFiles(directory=STATIC_DIR, html=True), name="ui")

app.include_router(plans.router)
app.include_router(optimize.router)
app.include_router(bom.router)
app.include_router(sales_plans.router)



# Attach per-request id for DB logs
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    try:
        from ..db import set_request_id
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:8]
        set_request_id(rid)
        response = await call_next(request)
        try:
            response.headers["X-Request-ID"] = rid
        except Exception:
            pass
        return response
    except Exception:
        return await call_next(request)

# Храним последние «активные» пути входных файлов
LAST_PATHS = {
    "machines": "machines.xlsx",
    "bom": "BOM.xlsx",
    "plan": "plan of sales.xlsx",
    "stock": "stock.xlsx",  # optional: stock Excel used by greedy-by-files
    "receipts": "receipts.xlsx",  # optional: outgoing receipts used as fixed orders
    "out": "schedule_out.xlsx",
}

# Transfer-deficit in-memory cache (per-process)
_TRANSFER_DEFICIT_CACHE_TTL_SEC = 600
_TRANSFER_DEFICIT_CACHE_MAX = 24
_TRANSFER_DEFICIT_CACHE: dict[tuple, dict] = {}
_TRANSFER_DEFICIT_CACHE_LOCK = Lock()
_TRANSFER_DEFICIT_JOBS_TTL_SEC = 1800
_TRANSFER_DEFICIT_JOBS_MAX = 32
_TRANSFER_DEFICIT_JOBS: dict[str, dict] = {}
_TRANSFER_DEFICIT_JOBS_LOCK = Lock()

class StockLineIn(BaseModel):
    item_id: str
    stock_qty: int
    workshop: Optional[str] = ""

class TransferDeficitExport(BaseModel):
    out_path: Optional[str] = None
    items: Optional[List[str]] = None
    workshops: Optional[List[str]] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    period: Optional[str] = "day"


class TransferDeficitJobIn(BaseModel):
    items: Optional[List[str]] = None
    workshops: Optional[List[str]] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    period: Optional[str] = "day"

# ================== Helpers (JSON-safe) ==================
def _to_py_scalar(x):
    """Convert numpy/pandas scalars & dates to JSON-friendly Python types."""
    if x is None:
        return None
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        v = float(x)
        return 0.0 if abs(v) < 1e-12 else v
    if isinstance(x, (pd.Timestamp, dt.datetime, dt.date)):
        return str(x)  # ISO
    return x

def df_to_records_py(df: pd.DataFrame):
    records = df.to_dict(orient="records")
    return [{k: _to_py_scalar(v) for k, v in rec.items()} for rec in records]

def any_to_jsonable(obj):
    """Best-effort conversion of arbitrary objects (DataFrame/Series/numpy/maps/lists) to JSONable."""
    if obj is None:
        return None
    if isinstance(obj, pd.DataFrame):
        return df_to_records_py(obj)
    if isinstance(obj, pd.Series):
        return [_to_py_scalar(v) for v in obj.to_list()]
    if isinstance(obj, (np.integer, np.floating, pd.Timestamp, dt.datetime, dt.date)):
        return _to_py_scalar(obj)
    if isinstance(obj, (list, tuple)):
        return [any_to_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {str(k): any_to_jsonable(v) for k, v in obj.items()}
    return obj


def _normalize_workshop_tokens_list(values: Optional[List[str]]) -> tuple[list[str], list[str]]:
    if not values:
        return [], []
    import re
    parts: list[str] = []
    for v in values:
        parts.extend(re.split(r"[,\s;]+", str(v)))
    tokens = {p.strip().lower() for p in parts if p and p.strip()}
    digits = {re.sub(r"\D+", "", t) for t in tokens}
    digits = {d for d in digits if d}
    all_tokens = sorted(tokens | digits)
    prefixes = sorted({t for t in all_tokens if t.isdigit()})
    return all_tokens, prefixes

def _parse_token_list(values: Optional[List[str]]) -> list[str]:
    """Split comma/space/newline separated values into a unique ordered list."""
    if not values:
        return []
    import re
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        parts = re.split(r"[,\s;]+", str(v))
        for p in parts:
            t = p.strip()
            if not t:
                continue
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(t)
    return out


def _clean_item_id(value) -> str:
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _resolve_plan_bom_version_id(s: Session, plan_id: int) -> int | None:
    plan_bom_version_id = None
    try:
        plan_bom_version_id = s.execute(
            text("SELECT bom_version_id FROM plan_versions WHERE id=:pid"),
            {"pid": plan_id},
        ).scalar_one_or_none()
    except Exception:
        plan_bom_version_id = None
    try:
        resolved = get_resolved_bom_version(s, int(plan_bom_version_id) if plan_bom_version_id is not None else None)
        return int(resolved.id)
    except Exception:
        return None


def _load_plan_bom_df(s: Session, plan_id: int) -> tuple[pd.DataFrame, int | None]:
    cols = ["item_id", "component_id", "qty_per", "loss", "workshop"]
    bom_version_id = _resolve_plan_bom_version_id(s, plan_id)
    if bom_version_id is not None:
        try:
            df = get_version_rows_df(s, int(bom_version_id))
            if df is None:
                return pd.DataFrame(columns=cols), int(bom_version_id)
            out = df.copy()
            for col in cols:
                if col not in out.columns:
                    out[col] = 1.0 if col == "loss" else None
            return out[cols], int(bom_version_id)
        except Exception:
            pass
    try:
        legacy = s.execute(
            text(
                "SELECT item_id, component_id, qty_per, COALESCE(loss, 1.0) AS loss, COALESCE(workshop, '') AS workshop FROM bom"
            )
        ).mappings().all()
    except Exception:
        try:
            legacy = s.execute(
                text("SELECT item_id, component_id, qty_per, 1.0 AS loss, '' AS workshop FROM bom")
            ).mappings().all()
        except Exception:
            legacy = []
    if not legacy:
        return pd.DataFrame(columns=cols), bom_version_id
    return pd.DataFrame(
        [
            {
                "item_id": str(r.get("item_id") or ""),
                "component_id": str(r.get("component_id") or ""),
                "qty_per": float(r.get("qty_per") or 0.0),
                "loss": float(r.get("loss") or 1.0),
                "workshop": str(r.get("workshop") or ""),
            }
            for r in legacy
        ],
        columns=cols,
    ), bom_version_id


def _transfer_cache_get(key: tuple) -> Optional[dict]:
    now = time.time()
    with _TRANSFER_DEFICIT_CACHE_LOCK:
        rec = _TRANSFER_DEFICIT_CACHE.get(key)
        if not rec:
            return None
        ts = float(rec.get("ts") or 0.0)
        if (now - ts) > _TRANSFER_DEFICIT_CACHE_TTL_SEC:
            _TRANSFER_DEFICIT_CACHE.pop(key, None)
            return None
        rec["last"] = now
        return deepcopy(rec.get("value"))


def _transfer_cache_set(key: tuple, value: dict) -> None:
    now = time.time()
    with _TRANSFER_DEFICIT_CACHE_LOCK:
        _TRANSFER_DEFICIT_CACHE[key] = {"ts": now, "last": now, "value": value}
        if len(_TRANSFER_DEFICIT_CACHE) > _TRANSFER_DEFICIT_CACHE_MAX:
            drop_n = len(_TRANSFER_DEFICIT_CACHE) - _TRANSFER_DEFICIT_CACHE_MAX
            victims = sorted(
                _TRANSFER_DEFICIT_CACHE.items(),
                key=lambda kv: float(kv[1].get("last") or kv[1].get("ts") or 0.0),
            )
            for old_key, _ in victims[:drop_n]:
                _TRANSFER_DEFICIT_CACHE.pop(old_key, None)


def _transfer_data_version(s: Session, plan_id: int) -> tuple:
    op_row = s.execute(
        text("SELECT COALESCE(MAX(op_id),0) AS max_op_id, COUNT(*) AS cnt FROM schedule_ops WHERE plan_id=:pid"),
        {"pid": plan_id},
    ).mappings().first() or {}
    stock_row = s.execute(
        text("SELECT COALESCE(MAX(id),0) AS max_snapshot_id, COALESCE(MAX(taken_at),'') AS max_taken_at FROM stock_snapshot")
    ).mappings().first() or {}
    bom_version_id = _resolve_plan_bom_version_id(s, plan_id)
    bom_row = {"max_bom_line_id": 0, "bom_line_count": 0}
    try:
        if bom_version_id is not None:
            bom_row = s.execute(
                text(
                    """
                    SELECT COALESCE(MAX(id),0) AS max_bom_line_id, COUNT(*) AS bom_line_count
                    FROM bom_lines
                    WHERE version_id = :vid
                    """
                ),
                {"vid": int(bom_version_id)},
            ).mappings().first() or bom_row
        else:
            bom_row = s.execute(
                text("SELECT COALESCE(MAX(id),0) AS max_bom_line_id, COUNT(*) AS bom_line_count FROM bom")
            ).mappings().first() or bom_row
    except Exception:
        bom_row = {"max_bom_line_id": 0, "bom_line_count": 0}
    return (
        int(op_row.get("max_op_id") or 0),
        int(op_row.get("cnt") or 0),
        int(stock_row.get("max_snapshot_id") or 0),
        str(stock_row.get("max_taken_at") or ""),
        int(bom_version_id or 0),
        int(bom_row.get("max_bom_line_id") or 0),
        int(bom_row.get("bom_line_count") or 0),
    )


def _transfer_job_cleanup_locked() -> None:
    now = time.time()
    stale_ids = [
        jid for jid, job in _TRANSFER_DEFICIT_JOBS.items()
        if (now - float(job.get("updated_ts") or job.get("created_ts") or now)) > _TRANSFER_DEFICIT_JOBS_TTL_SEC
    ]
    for jid in stale_ids:
        _TRANSFER_DEFICIT_JOBS.pop(jid, None)
    if len(_TRANSFER_DEFICIT_JOBS) <= _TRANSFER_DEFICIT_JOBS_MAX:
        return
    victims = sorted(
        _TRANSFER_DEFICIT_JOBS.items(),
        key=lambda kv: float(kv[1].get("updated_ts") or kv[1].get("created_ts") or 0.0),
    )
    drop_n = len(_TRANSFER_DEFICIT_JOBS) - _TRANSFER_DEFICIT_JOBS_MAX
    for jid, _ in victims[:drop_n]:
        _TRANSFER_DEFICIT_JOBS.pop(jid, None)


def _transfer_job_payload(job: dict, include_result: bool = False) -> dict:
    payload = {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "plan_id": job.get("plan_id"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
    }
    if job.get("status") == "error":
        payload["error"] = job.get("error")
    if include_result and job.get("status") == "done":
        payload["result"] = job.get("result")
    return payload

def _ingest_stock_snapshot(stock_path: str, db: Session) -> Optional[int]:
    """Load stock Excel into stock_snapshot/stock_line; return new snapshot id."""
    if not stock_path or not os.path.exists(stock_path):
        return None
    try:
        ensure_tables(db)
        df = load_stock_any(Path(stock_path))
        if df is None or df.empty:
            return None
        if "workshop" not in df.columns:
            df["workshop"] = ""
        df = df[["item_id", "stock_qty", "workshop"]].copy()
        df["item_id"] = df["item_id"].astype(str)
        df["workshop"] = df["workshop"].fillna("").astype(str)
        df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)
        df = df.groupby(["item_id", "workshop"], as_index=False)["stock_qty"].sum()

        snap_name = f"Upload {Path(stock_path).name}"
        snap_id = db.execute(
            text("INSERT INTO stock_snapshot (name, taken_at, notes) VALUES (:name, CURRENT_TIMESTAMP, :notes) RETURNING id"),
            {"name": snap_name, "notes": stock_path},
        ).scalar_one()
        rows = [
            {"snapshot_id": int(snap_id), "item_id": r.item_id, "workshop": r.workshop, "stock_qty": int(r.stock_qty)}
            for r in df.itertuples(index=False)
        ]
        if rows:
            db.execute(
                text("INSERT INTO stock_line (snapshot_id,item_id,workshop,stock_qty) VALUES (:snapshot_id,:item_id,:workshop,:stock_qty)"),
                rows,
            )
        db.commit()
        return int(snap_id)
    except Exception as e:
        logging.warning("Stock ingest failed: %s", e)
        return None

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ================== Schemas ==================
class IngestRequest(BaseModel):
    machines: str
    bom: str
    plan: str
    stock: Optional[str] = None
    receipts: Optional[str] = None

class ExportRequest(BaseModel):
    out_path: str
    plan_id: Optional[int] = None

# ================== Lifespan ==================
@app.on_event("startup")
def _startup():
    # Basic logging setup (ensure our optimization logs are visible)
    try:
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        logging.getLogger("so_planner.optimize").setLevel(logging.INFO)
    except Exception:
        pass
    init_db()
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("out", exist_ok=True)

# ================== UI ==================
@app.get("/", response_class=HTMLResponse)
def index():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        return HTMLResponse("<h3>UI not found. Build or place index.html to /ui</h3>", status_code=200, media_type="text/html; charset=utf-8")
    # Robust decoding: prefer UTF-8, fallback to Windows-1251, then ignore errors
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            html = f.read()
            return HTMLResponse(html, media_type="text/html; charset=utf-8")
    except UnicodeDecodeError:
        try:
            with open(index_path, "r", encoding="cp1251") as f:
                html = f.read()
                return HTMLResponse(html, media_type="text/html; charset=utf-8")
        except Exception:
            with open(index_path, "rb") as f:
                data = f.read()
            try:
                return HTMLResponse(data.decode("utf-8", errors="ignore"), media_type="text/html; charset=utf-8")
            except Exception:
                # As a last resort, return bytes as latin-1 to preserve content
                return HTMLResponse(data.decode("latin-1", errors="ignore"), media_type="text/html; charset=utf-8")

# ================== Ingest & Validate ==================
@app.post("/ingest/validate")
def ingest_validate(req: IngestRequest):
    try:
        result = validate_files(req.machines, req.bom, req.plan)
        # Optional stock path validation (from request or previously set path)
        stock_path = req.stock or LAST_PATHS.get("stock")
        receipts_path = req.receipts or LAST_PATHS.get("receipts")
        if stock_path:
            from ..scheduling.greedy.loaders import load_stock_any
            stock_info = {}
            try:
                sdf = load_stock_any(stock_path)
                stock_info = {"stock_path": stock_path, "stock_rows": int(len(sdf))}
            except Exception as e:
                stock_info = {"stock_path": stock_path, "stock_error": str(e)}
            if isinstance(result, dict):
                result.update(stock_info)
        if receipts_path:
            from ..scheduling.greedy.loaders import load_receipts_any
            receipts_info = {}
            try:
                rdf = load_receipts_any(Path(receipts_path))
                receipts_info = {"receipts_path": receipts_path, "receipts_rows": int(len(rdf))}
            except Exception as e:
                receipts_info = {"receipts_path": receipts_path, "receipts_error": str(e)}
            if isinstance(result, dict):
                result.update(receipts_info)
        return JSONResponse(content=any_to_jsonable(result))
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})

@app.post("/ingest")
def ingest(req: IngestRequest):
    try:
        with SessionLocal() as s:
            cnts = load_excels(s, req.machines, req.bom, req.plan)
        # обновим активные пути
        LAST_PATHS.update({"machines": req.machines, "bom": req.bom, "plan": req.plan})
        if req.stock:
            LAST_PATHS.update({"stock": req.stock})
        if req.receipts:
            LAST_PATHS.update({"receipts": req.receipts})
        payload = {
            "status": "ok",
            "counts": {
                "machines": int(cnts[0]) if len(cnts) > 0 else None,
                "bom": int(cnts[1]) if len(cnts) > 1 else None,
                "demand": int(cnts[2]) if len(cnts) > 2 else None,
            },
            "active_paths": LAST_PATHS,
        }
        return JSONResponse(content=payload)
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})

@app.post("/upload/ingest")
async def upload_and_ingest(machines: UploadFile = File(...),
                            bom: UploadFile = File(...),
                            plan: UploadFile = File(...),
                            stock: UploadFile | None = File(default=None),
                            receipts: UploadFile | None = File(default=None)):
    try:
        def _save(uf: UploadFile) -> str:
            ext = os.path.splitext(uf.filename or "")[1] or ".xlsx"
            path = os.path.join("uploads", f"{uuid.uuid4().hex}{ext}")
            with open(path, "wb") as out:
                shutil.copyfileobj(uf.file, out)
            return path

        mpath = _save(machines)
        bpath = _save(bom)
        ppath = _save(plan)
        spath = None
        rpath = None
        if stock is not None:
            spath = _save(stock)
        if receipts is not None:
            rpath = _save(receipts)

        with SessionLocal() as s:
            cnts = load_excels(s, mpath, bpath, ppath)

        # обновим активные пути
        LAST_PATHS.update({"machines": mpath, "bom": bpath, "plan": ppath})
        if spath:
            LAST_PATHS.update({"stock": spath})
        if rpath:
            LAST_PATHS.update({"receipts": rpath})
        else:
            LAST_PATHS.update({"receipts": ""})

        payload = {
            "status": "ok",
            "stored_paths": {"machines": mpath, "bom": bpath, "plan": ppath, "stock": spath, "receipts": rpath},
            "counts": {
                "machines": int(cnts[0]) if len(cnts) > 0 else None,
                "bom": int(cnts[1]) if len(cnts) > 1 else None,
                "demand": int(cnts[2]) if len(cnts) > 2 else None,
            },
            "active_paths": LAST_PATHS,
        }
        return JSONResponse(content=payload)
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        return JSONResponse(status_code=400, content={"status": "error", "error": str(e), "trace": tb})

def ensure_tables(db: Session):
    from so_planner.scheduling.greedy_scheduler import _ensure_support_tables
    _ensure_support_tables(db)

def _ensure_plan_order_info_cols(db: Session) -> None:
    """Ensure plan_order_info has columns used by UI/reporting."""
    ensure_tables(db)
    try:
        cols = db.execute(text("PRAGMA table_info(plan_order_info)")).mappings().all()
        def _col_name(row) -> str:
            if isinstance(row, dict):
                return str(row.get("name") or "")
            try:
                return str(row.get("name") or "")
            except Exception:
                pass
            try:
                return str(row[1])
            except Exception:
                return ""
        names = {_col_name(r) for r in cols if _col_name(r)}
        def _add(col: str, ddl: str) -> None:
            if col not in names:
                db.execute(text(f"ALTER TABLE plan_order_info ADD COLUMN {col} {ddl}"))
        _add("start_date", "DATE")
        _add("end_date", "DATE")
        _add("qty", "REAL")
        _add("workshop", "TEXT")
        _add("status", "TEXT")
        _add("fixed_at", "DATETIME")
        _add("updated_at", "DATETIME")
        db.commit()
    except Exception:
        logging.exception("Failed to ensure plan_order_info columns")
        db.rollback()



# ================== Scheduling (Greedy) ==================
@app.post("/schedule/greedy")
def schedule_greedy(mode: str = "", bom_version_id: int | None = None):
    """
    Запускает greedy-планировщик по LAST_PATHS и возвращает сводку:
      - out (xlsx), rows, min/max даты, preview,
      - bottlenecks / hot_days,
      - active_paths,
      - warnings.
    """
    try:
        with SessionLocal() as s:
            bom_ver = get_resolved_bom_version(s, bom_version_id)
            bom_df = bom_df_to_scheduler_df(get_version_rows_df(s, int(bom_ver.id)))
            if bom_df.empty:
                raise ValueError(f"BOM version {bom_ver.id} is empty or invalid for scheduling")
            out_file, sched = run_greedy(
                s,
                LAST_PATHS["plan"],
                LAST_PATHS["bom"],
                LAST_PATHS["machines"],
                LAST_PATHS["out"],
                split_child_orders=True,      # отдельный order на article
                align_roots_to_due=True,      # JIT: корень "в due", дети назад
                guard_limit_days=200 * 365,   # большой лимит (≈200 лет)
                mode=mode,
                stock_path=LAST_PATHS.get("stock"),
                receipts_path=LAST_PATHS.get("receipts"),
                bom_df=bom_df,
            )

            summary, hot = scan_bottlenecks(s)

        # сводная информация по расписанию
        rows_cnt = int(len(sched))
        min_date = str(pd.to_datetime(sched["date"]).min().date()) if rows_cnt else None
        max_date = str(pd.to_datetime(sched["date"]).max().date()) if rows_cnt else None

        preview_cols = ["order_id", "item_id", "step", "machine_id", "date", "minutes", "qty", "due_date", "lag_days", "base_order_id"]
        preview_cols = [c for c in preview_cols if c in sched.columns]
        preview_df = sched[preview_cols].head(50).copy() if rows_cnt else pd.DataFrame(columns=preview_cols)
        if "date" in preview_df.columns:
            preview_df["date"] = pd.to_datetime(preview_df["date"]).dt.date.astype(str)
        if "due_date" in preview_df.columns:
            preview_df["due_date"] = pd.to_datetime(preview_df["due_date"]).dt.date.astype(str)
        preview = df_to_records_py(preview_df) if rows_cnt else []

        try:
            warnings = list(sched.attrs.get("warnings") or [])
        except Exception:
            warnings = []

        payload = {
            "status": "ok",
            "out": str(out_file),
            "bom_version_id": int(bom_ver.id),
            "rows": rows_cnt,
            "min_date": min_date,
            "max_date": max_date,
            "preview": preview,
            "bottlenecks": any_to_jsonable(summary),
            "hot_days": any_to_jsonable(hot),
            "active_paths": LAST_PATHS,
            "warnings": warnings,
        }
        return JSONResponse(content=payload)

    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


# ================== Gantt (orders) ==================
@app.get("/gantt/orders")
def gantt_orders():
    """
    Возвращает таймлайн заказов:
      order_id, item_id, start_date, finish_date, duration_days, due_date, finish_lag,
      + base_order_id для группировки (часть до двоеточия в order_id).
    Берём из последнего расчёта (LAST_PATHS["out"]) — лист 'schedule'.
    """
    try:
        out_path = LAST_PATHS.get("out") or "schedule_out.xlsx"
        if not os.path.exists(out_path):
            raise FileNotFoundError(f"Не найден файл расписания: {out_path}")

        sched = pd.read_excel(out_path, sheet_name="schedule", dtype=object)
        sched["date"] = pd.to_datetime(sched["date"]).dt.date
        sched["due_date"] = pd.to_datetime(sched["due_date"]).dt.date

        orders = compute_orders_timeline(sched)

        def _base(oid: str) -> str:
            return oid.split(":", 1)[0] if ":" in oid else oid

        records = []
        for _, r in orders.iterrows():
            oid = str(r["order_id"])
            records.append({
                "base_order_id": _base(oid),
                "order_id": oid,
                "item_id": str(r["item_id"]),
                "start_date": str(r["start_date"]),
                "finish_date": str(r["finish_date"]),
                "duration_days": int(r["duration_days"]),
                "due_date": str(r["due_date"]),
                "finish_lag": int(r["finish_lag"]),
            })

        try:
            from ..db.models import BOM  # импорт рядом с остальными моделями
            item_ids = {r["item_id"] for r in records}
            if item_ids:
                with SessionLocal() as s:
                    bom_rows = (
                        s.query(BOM.item_id, BOM.article_name)
                        .filter(BOM.item_id.in_(item_ids))
                        .all()
                    )
                name_map = {str(iid): (name or str(iid)) for iid, name in bom_rows}
                for r in records:
                    r["item_name"] = name_map.get(str(r["item_id"]))
        except Exception:
            # тихо пропускаем — если нет справочника, просто без имени
            pass 
        return JSONResponse(content={"status": "ok", "count": len(records), "orders": records})
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})

@app.get("/gantt/edges")
def gantt_edges():
    """
    Возвращает список рёбер наследования для визуализации:
    [{base_order_id, parent_item, child_item}]
    """
    try:
        out_path = LAST_PATHS.get("out") or "schedule_out.xlsx"
        if not os.path.exists(out_path):
            raise FileNotFoundError(f"Не найден файл расписания: {out_path}")

        sched = pd.read_excel(out_path, sheet_name="schedule", dtype=object)
        present = set(str(x) for x in sched["item_id"].unique())

        # грузим BOM, находим пары root_item_id -> item_id
        bom = pd.read_excel(LAST_PATHS["bom"], sheet_name=0, dtype=object)
        def nc(s): return str(s).strip().lower().replace(" ","").replace("_","")
        cols = {nc(c): c for c in bom.columns}
        art = cols.get("article") or cols.get("item") or cols.get("item_id")
        root = cols.get("rootarticle") or cols.get("root article")
        if not art or not root:
            return JSONResponse(content={"status":"ok", "edges":[]})

        pairs = bom[[art, root]].dropna().astype(str)
        pairs = pairs[(pairs[art] != "") & (pairs[root] != "")]
        pairs = pairs[(pairs[art].isin(present)) & (pairs[root].isin(present))]

        # базовый order_id берём из order_id "<base>:<item>"
        def base_from_item(it: str) -> list[str]:
            # найдём все базовые ордера, где встречается этот item
            sub = sched[sched["item_id"].astype(str)==it]
            bases = set()
            for oid in sub["order_id"].astype(str):
                bases.add(oid.split(":",1)[0] if ":" in oid else oid)
            return list(bases)

        edges = []
        for _, r in pairs.iterrows():
            child = str(r[art]); parent = str(r[root])
            # рёбра строим для всех base_order_id, где присутствуют обе позиции
            bases = set(base_from_item(child)) & set(base_from_item(parent))
            for b in bases:
                edges.append({"base_order_id": b, "parent_item": parent, "child_item": child})

        return JSONResponse(content={"status":"ok", "edges": edges})
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


# ================== Export ==================
@app.post("/export")
def export(req: ExportRequest):
    try:
        with SessionLocal() as s:
            out_xlsx, gantt_png = export_excel(s, req.out_path, req.plan_id)
        payload = {"status": "ok", "xlsx": str(out_xlsx), "gantt_png": str(gantt_png) if gantt_png else None}
        return JSONResponse(content=payload)
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})

@app.post("/stock/snapshots")
def create_stock_snapshot(name: str = "snapshot", notes: Optional[str] = None, db: Session = Depends(get_db)):
    ensure_tables(db)
    q = text("""
      INSERT INTO stock_snapshot (name, taken_at, notes)
      VALUES (:name, CURRENT_TIMESTAMP, :notes) RETURNING id
    """)
    sid = db.execute(q, {"name": name, "notes": notes}).scalar_one()
    db.commit()
    return {"id": int(sid)}

@app.post("/stock/snapshots/{snapshot_id}/lines:bulk")
def bulk_stock_lines(snapshot_id: int, lines: List[StockLineIn], db: Session = Depends(get_db)):
    ensure_tables(db)
    rows = [dict(l.dict(), snapshot_id=snapshot_id) for l in lines]
    db.execute(text("""
      INSERT INTO stock_line (snapshot_id,item_id,workshop,stock_qty)
      VALUES (:snapshot_id,:item_id,:workshop,:stock_qty)
    """), rows)
    db.commit()
    return {"inserted": len(rows)}

# ================== Reports (DB-based) ==================
@app.get("/reports/plans/{plan_id}/items")
def report_plan_items(plan_id: int):
    try:
        from ..db.models import ScheduleOp
        with SessionLocal() as s:
            rows = (
                s.query(
                    ScheduleOp.item_id.label("item_id"),
                    func.max(ScheduleOp.article_name).label("item_name"),
                )
                .filter(ScheduleOp.plan_id == plan_id)
                .group_by(ScheduleOp.item_id)
                .all()
            )
        items = []
        for r in rows:
            if r.item_id is None:
                continue
            items.append({"item_id": str(r.item_id), "item_name": str(r.item_name) if r.item_name is not None else None})
        items = sorted(items, key=lambda x: x["item_id"])
        return {"status": "ok", "count": len(items), "items": items}
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})

@app.get("/reports/plans/{plan_id}/product_summary")
def report_product_summary(plan_id: int, item_id: str = Query(..., description="Артикул (item_id)")):
    try:
        item_id = str(item_id or "").strip()
        if not item_id:
            raise HTTPException(status_code=400, detail={"msg": "item_id is required"})

        plan_meta = None
        try:
            from ..db.models import PlanVersion
            with SessionLocal() as s:
                plan = s.get(PlanVersion, plan_id)
                if plan:
                    plan_meta = {
                        "id": plan.id,
                        "name": plan.name,
                        "origin": plan.origin,
                        "status": plan.status,
                        "created_at": str(plan.created_at) if plan.created_at is not None else None,
                        "parent_plan_id": plan.parent_plan_id,
                        "bom_version_id": plan.bom_version_id,
                    }
        except Exception:
            plan_meta = None

        def _to_date(x):
            if x is None or x == "":
                return None
            try:
                return pd.to_datetime(x).date()
            except Exception:
                return None

        def _date_str(x):
            d = _to_date(x)
            return str(d) if d is not None else None

        def _end_date_from_ts(ts):
            if ts is None:
                return None
            try:
                return (ts - dt.timedelta(seconds=1)).date()
            except Exception:
                try:
                    return ts.date()
                except Exception:
                    return None

        def _to_float(x):
            try:
                return float(x)
            except Exception:
                return 0.0
        def _norm_item(x):
            s = str(x).strip()
            if s.endswith(".0"):
                s = s[:-2]
            return s

        with SessionLocal() as s:
            ensure_tables(s)
            _ensure_plan_order_info_cols(s)

            # order meta (status/due/start/end/qty)
            meta_rows = s.execute(
                text(
                    """
                    SELECT order_id, status, due_date, start_date, end_date, qty
                    FROM plan_order_info WHERE plan_id=:pid
                    """
                ),
                {"pid": plan_id},
            ).mappings().all()
            meta = {}
            for r in meta_rows:
                oid = str(r.get("order_id") or "")
                if not oid:
                    continue
                meta[oid] = {
                    "status": (r.get("status") or "unfixed").lower(),
                    "due_date": r.get("due_date"),
                    "start_date": r.get("start_date"),
                    "end_date": r.get("end_date"),
                    "qty": r.get("qty"),
                }

            ops = s.execute(
                text(
                    """
                    SELECT order_id, item_id, article_name, start_ts, end_ts, qty
                    FROM schedule_ops
                    WHERE plan_id = :pid AND item_id = :item
                    """
                ),
                {"pid": plan_id, "item": item_id},
            ).mappings().all()

            orders = {}
            item_name = None
            for r in ops:
                oid = str(r.get("order_id") or "")
                if not oid:
                    continue
                if item_name is None and r.get("article_name"):
                    item_name = str(r.get("article_name"))
                entry = orders.get(oid)
                start_ts = r.get("start_ts")
                end_ts = r.get("end_ts")
                qty = r.get("qty")
                if entry is None:
                    orders[oid] = {
                        "order_id": oid,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "qty": _to_float(qty) if qty is not None else 0.0,
                    }
                else:
                    if start_ts and (entry["start_ts"] is None or start_ts < entry["start_ts"]):
                        entry["start_ts"] = start_ts
                    if end_ts and (entry["end_ts"] is None or end_ts > entry["end_ts"]):
                        entry["end_ts"] = end_ts
                    if qty is not None:
                        entry["qty"] = max(_to_float(qty), entry.get("qty") or 0.0)

            order_rows = []
            for oid, ent in orders.items():
                m = meta.get(oid, {})
                status = (m.get("status") or "unfixed").lower()
                if status == "deleted":
                    continue
                qty_val = m.get("qty")
                if qty_val is None:
                    qty_val = ent.get("qty")
                start_date = m.get("start_date") or (ent.get("start_ts").date() if ent.get("start_ts") else None)
                end_date = m.get("end_date") or _end_date_from_ts(ent.get("end_ts"))
                order_rows.append(
                    {
                        "order_id": oid,
                        "status": status,
                        "qty": _to_float(qty_val),
                        "start_date": _date_str(start_date),
                        "end_date": _date_str(end_date),
                        "due_date": _date_str(m.get("due_date")),
                    }
                )

            def _summarize(rows, status_name: str):
                subset = [r for r in rows if (r.get("status") or "unfixed") == status_name]
                if not subset:
                    return {"count": 0, "qty": 0.0, "start_date": None, "end_date": None, "due_date_min": None, "due_date_max": None}
                qty_sum = sum(_to_float(r.get("qty")) for r in subset)
                start_dates = [d for d in (_to_date(r.get("start_date")) for r in subset) if d]
                end_dates = [d for d in (_to_date(r.get("end_date")) for r in subset) if d]
                due_dates = [d for d in (_to_date(r.get("due_date")) for r in subset) if d]
                return {
                    "count": len(subset),
                    "qty": float(qty_sum),
                    "start_date": str(min(start_dates)) if start_dates else None,
                    "end_date": str(max(end_dates)) if end_dates else None,
                    "due_date_min": str(min(due_dates)) if due_dates else None,
                    "due_date_max": str(max(due_dates)) if due_dates else None,
                }

            order_rows = sorted(
                order_rows,
                key=lambda r: (
                    0 if (r.get("status") or "") == "fixed" else 1,
                    r.get("due_date") or "9999-12-31",
                    r.get("start_date") or "9999-12-31",
                    r.get("order_id") or "",
                ),
            )
            fixed_sum = _summarize(order_rows, "fixed")
            unfixed_sum = _summarize(order_rows, "unfixed")

            # Stock snapshot
            snap_row = s.execute(text("SELECT id, taken_at FROM stock_snapshot ORDER BY taken_at DESC LIMIT 1")).fetchone()
            stock_snapshot_id = snap_row[0] if snap_row else None
            stock_snapshot_taken_at = str(snap_row[1]) if snap_row and snap_row[1] is not None else None
            stock_qty = 0.0
            stock_by_workshop = []
            if stock_snapshot_id is not None:
                stock_qty = _to_float(
                    s.execute(
                        text("SELECT COALESCE(SUM(stock_qty),0) FROM stock_line WHERE snapshot_id=:sid AND item_id=:item"),
                        {"sid": stock_snapshot_id, "item": item_id},
                    ).scalar_one_or_none()
                )
                stock_rows = s.execute(
                    text(
                        """
                        SELECT COALESCE(workshop,'') AS workshop, SUM(stock_qty) AS qty
                        FROM stock_line
                        WHERE snapshot_id=:sid AND item_id=:item
                        GROUP BY COALESCE(workshop,'')
                        """
                    ),
                    {"sid": stock_snapshot_id, "item": item_id},
                ).mappings().all()
                stock_by_workshop = [
                    {"workshop": str(r.get("workshop") or ""), "qty": _to_float(r.get("qty"))} for r in stock_rows
                ]

            # Demand sources (parents): BOM from plan version in DB (with legacy DB fallback only).
            bom_df, _ = _load_plan_bom_df(s, plan_id)
            bom_rows = []
            if bom_df is not None and not bom_df.empty:
                bom_rows = bom_df[["item_id", "component_id", "qty_per", "loss"]].to_dict("records")
            pair_map = {}
            def _skip_parent(parent: str, child: str) -> bool:
                if not parent or not child:
                    return True
                if parent == child:
                    return True
                if parent in {"0", "0.0"}:
                    return True
                return False
            for br in bom_rows:
                child = _norm_item(br.get("item_id") or "")
                parent = _norm_item(br.get("component_id") or "")
                if _skip_parent(parent, child):
                    continue
                qty_per = _to_float(br.get("qty_per"))
                if qty_per <= 0:
                    qty_per = 1.0
                loss = _to_float(br.get("loss"))
                if loss <= 0:
                    loss = 1.0
                mult = float(qty_per) * float(loss)
                key = (parent, child)
                prev = pair_map.get(key)
                if prev is None or mult > prev:
                    pair_map[key] = mult
            parents_by_child = defaultdict(list)
            for (parent, child), qty in pair_map.items():
                parents_by_child[child].append((parent, qty))

            multipliers = defaultdict(float)
            item_key = _norm_item(item_id)
            for parent, qty in parents_by_child.get(item_key, []):
                multipliers[parent] += float(qty or 1.0)

            demand_rows = []
            if multipliers:
                parent_ids = list(multipliers.keys())
                placeholders = ",".join([f":p{i}" for i in range(len(parent_ids))])
                params = {"pid": plan_id}
                for i, pid in enumerate(parent_ids):
                    params[f"p{i}"] = pid
                parent_ops = s.execute(
                    text(
                        f"""
                        SELECT order_id, item_id, article_name, start_ts, end_ts, qty
                        FROM schedule_ops
                        WHERE plan_id = :pid AND item_id IN ({placeholders})
                        """
                    ),
                    params,
                ).mappings().all()
                parent_orders = {}
                for r in parent_ops:
                    oid = str(r.get("order_id") or "")
                    pid = str(r.get("item_id") or "")
                    if not oid or not pid:
                        continue
                    key = (oid, pid)
                    entry = parent_orders.get(key)
                    start_ts = r.get("start_ts")
                    end_ts = r.get("end_ts")
                    qty = r.get("qty")
                    if entry is None:
                        parent_orders[key] = {
                            "order_id": oid,
                            "item_id": pid,
                            "article_name": r.get("article_name"),
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "qty": _to_float(qty) if qty is not None else 0.0,
                        }
                    else:
                        if start_ts and (entry["start_ts"] is None or start_ts < entry["start_ts"]):
                            entry["start_ts"] = start_ts
                        if end_ts and (entry["end_ts"] is None or end_ts > entry["end_ts"]):
                            entry["end_ts"] = end_ts
                        if qty is not None:
                            entry["qty"] = max(_to_float(qty), entry.get("qty") or 0.0)
                        if entry.get("article_name") in (None, "") and r.get("article_name"):
                            entry["article_name"] = r.get("article_name")

                for (oid, pid), ent in parent_orders.items():
                    m = meta.get(oid, {})
                    status = (m.get("status") or "unfixed").lower()
                    if status == "deleted":
                        continue
                    mult = float(multipliers.get(pid) or 0.0)
                    if mult <= 0:
                        continue
                    parent_qty = m.get("qty")
                    if parent_qty is None:
                        parent_qty = ent.get("qty")
                    parent_qty = _to_float(parent_qty)
                    demand_rows.append(
                        {
                            "parent_item_id": pid,
                            "parent_name": str(ent.get("article_name")) if ent.get("article_name") is not None else None,
                            "order_id": oid,
                            "base_order_id": oid.split(":", 1)[0] if ":" in oid else oid,
                            "multiplier": mult,
                            "parent_qty": parent_qty,
                            "required_qty": float(np.ceil((parent_qty * mult) - 1e-9)),
                            "start_date": _date_str(m.get("start_date") or (ent.get("start_ts").date() if ent.get("start_ts") else None)),
                            "end_date": _date_str(m.get("end_date") or _end_date_from_ts(ent.get("end_ts"))),
                            "due_date": _date_str(m.get("due_date")),
                            "status": status,
                        }
                    )

            demand_rows = sorted(
                demand_rows,
                key=lambda r: (r.get("due_date") or "9999-12-31", r.get("start_date") or "9999-12-31", r.get("order_id") or ""),
            )
            total_demand = sum(_to_float(r.get("required_qty")) for r in demand_rows)

            return {
                "status": "ok",
                "plan": plan_meta,
                "item": {"item_id": item_id, "item_name": item_name},
                "receipts": {
                    "stock_qty": float(stock_qty),
                    "stock_snapshot_id": stock_snapshot_id,
                    "stock_snapshot_taken_at": stock_snapshot_taken_at,
                    "fixed": fixed_sum,
                    "unfixed": unfixed_sum,
                    "orders": order_rows,
                },
                "demand": {
                    "total_qty": float(total_demand),
                    "rows": demand_rows,
                },
            }
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})

@app.get("/reports/plans/{plan_id}/orders_timeline")
def report_orders_timeline(
    plan_id: int,
    workshops: Optional[List[str]] = Query(default=None),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    order_id: Optional[str] = None,
    limit: Optional[int] = Query(default=None, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
    with_total: bool = Query(default=True),
):
    try:
        plan_meta = None
        try:
            from ..db.models import PlanVersion
            with SessionLocal() as s:
                plan = s.get(PlanVersion, plan_id)
                if plan:
                    plan_meta = {
                        "id": plan.id,
                        "name": plan.name,
                        "origin": plan.origin,
                        "status": plan.status,
                        "created_at": str(plan.created_at) if plan.created_at is not None else None,
                        "parent_plan_id": plan.parent_plan_id,
                    }
        except Exception:
            plan_meta = None
        from ..db.models import ScheduleOp, DimMachine
        with SessionLocal() as s:
            tokens, prefixes = _normalize_workshop_tokens_list(workshops)
            q = (
                s.query(
                    ScheduleOp.order_id.label("order_id"),
                    func.min(ScheduleOp.start_ts).label("start_ts"),
                    func.max(ScheduleOp.end_ts).label("end_ts"),
                    func.min(ScheduleOp.item_id).label("item_id"),
                    func.max(ScheduleOp.article_name).label("article_name"),
                )
                .filter(ScheduleOp.plan_id == plan_id)
            )
            if tokens:
                conds = [func.lower(func.trim(DimMachine.family)).in_(tokens)]
                if prefixes:
                    conds.append(or_(*[ScheduleOp.machine_id.like(f"{p}%") for p in prefixes]))
                q = (
                    q.join(DimMachine, ScheduleOp.machine_id == DimMachine.machine_id, isouter=True)
                     .filter(or_(*conds))
                )
            if order_id:
                q = q.filter(ScheduleOp.order_id.like(f"%{order_id}%"))
            q = q.group_by(ScheduleOp.order_id)
            if date_from:
                try:
                    d_from = pd.to_datetime(date_from).to_pydatetime()
                    q = q.having(func.max(ScheduleOp.end_ts) >= d_from)
                except Exception:
                    pass
            if date_to:
                try:
                    d_to = pd.to_datetime(date_to).to_pydatetime() + dt.timedelta(days=1) - dt.timedelta(seconds=1)
                    q = q.having(func.min(ScheduleOp.start_ts) <= d_to)
                except Exception:
                    pass

            query_total = int(q.count()) if with_total else None
            q = q.order_by(func.max(ScheduleOp.end_ts).desc(), ScheduleOp.order_id.asc()).offset(offset)
            if limit is not None:
                q = q.limit(int(limit))
            rows = q.all()
        if not rows:
            summary = {
                "total": 0,
                "with_due_date": 0,
                "on_time": 0,
                "late": 0,
                "no_due_date": 0,
                "avg_lag": 0.0,
                "max_lag": 0,
            }
            return {
                "status": "ok",
                "count": 0,
                "total": int(query_total or 0) if with_total else None,
                "limit": limit,
                "offset": offset,
                "orders": [],
                "plan": plan_meta,
                "summary": summary,
                "summary_scope": "page",
            }
        # due_date from plan_order_info (if present)
        due_map = {}
        status_map = {}
        try:
            with SessionLocal() as s:
                order_ids = [str(r.order_id) for r in rows if r.order_id is not None]
                due_rows = []
                if order_ids:
                    ph = ",".join([f":o{i}" for i in range(len(order_ids))])
                    params = {"pid": plan_id}
                    for i, oid in enumerate(order_ids):
                        params[f"o{i}"] = oid
                    due_rows = s.execute(
                        text(f"SELECT order_id,due_date,status FROM plan_order_info WHERE plan_id=:pid AND order_id IN ({ph})"),
                        params,
                    ).mappings().all()
                for r in due_rows:
                    if r["order_id"]:
                        due_map[str(r["order_id"])]= str(r["due_date"]) if r["due_date"] is not None else None
                        status_map[str(r["order_id"])] = (r.get("status") or "").lower()
        except Exception:
            due_map = {}
            status_map = {}

        out = []
        for rr in rows:
            oid = str(rr.order_id) if rr.order_id is not None else ""
            if (status_map.get(oid) or "").lower() == "deleted":
                continue
            due = due_map.get(oid)
            lag = 0
            try:
                if due:
                    fd = pd.to_datetime(rr.end_ts).normalize()
                    dd = pd.to_datetime(due).normalize()
                    lag = int((fd - dd).days)
            except Exception:
                lag = 0
            start_ts = pd.to_datetime(rr.start_ts)
            end_ts = pd.to_datetime(rr.end_ts)
            duration_days = int((end_ts.normalize() - start_ts.normalize()).days + 1)
            out.append({
                "base_order_id": oid.split(":", 1)[0] if ":" in oid else oid,
                "order_id": oid,
                "item_id": str(rr.item_id) if rr.item_id is not None else "",
                "article_name": str(rr.article_name) if rr.article_name is not None else None,
                "item_name": str(rr.article_name) if rr.article_name is not None else None,
                "start_date": str(start_ts.date()),
                "finish_date": str(end_ts.date()),
                "duration_days": duration_days,
                "due_date": due,
                "finish_lag": lag,
                "status": status_map.get(oid),
            })
        page_total = len(out)
        with_due = 0
        on_time = 0
        late = 0
        no_due = 0
        lag_sum = 0
        max_lag = None
        for row in out:
            due = row.get("due_date")
            if not due:
                no_due += 1
                continue
            with_due += 1
            lag = int(row.get("finish_lag") or 0)
            lag_sum += lag
            if max_lag is None or lag > max_lag:
                max_lag = lag
            if lag <= 0:
                on_time += 1
            else:
                late += 1
        avg_lag = round(lag_sum / with_due, 2) if with_due else 0.0
        summary = {
            "total": page_total,
            "with_due_date": with_due,
            "on_time": on_time,
            "late": late,
            "no_due_date": no_due,
            "avg_lag": avg_lag,
            "max_lag": int(max_lag or 0),
        }
        return {
            "status": "ok",
            "count": page_total,
            "total": int(query_total) if with_total and query_total is not None else None,
            "limit": limit,
            "offset": offset,
            "orders": out,
            "plan": plan_meta,
            "summary": summary,
            "summary_scope": "page",
        }
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


@app.get("/reports/plans/{plan_id}/edges")
def report_edges(plan_id: int, workshops: Optional[List[str]] = Query(default=None)):
    try:
        # Items present in schedule and map base_order_id -> items
        from ..db.models import ScheduleOp, DimMachine
        tokens, prefixes = _normalize_workshop_tokens_list(workshops)
        with SessionLocal() as s:
            q = (
                s.query(ScheduleOp.order_id, ScheduleOp.item_id)
                 .filter(ScheduleOp.plan_id == plan_id)
                 .distinct()
            )
            if tokens:
                conds = [func.lower(func.trim(DimMachine.family)).in_(tokens)]
                if prefixes:
                    conds.append(or_(*[ScheduleOp.machine_id.like(f"{p}%") for p in prefixes]))
                q = (
                    q.join(DimMachine, ScheduleOp.machine_id == DimMachine.machine_id, isouter=True)
                     .filter(or_(*conds))
                )
            rows = q.all()
        present = set()
        base_map = {}
        for oid, item in rows:
            if item is None:
                continue
            iid = str(item)
            present.add(iid)
            bo = str(oid or "")
            base = bo.split(":", 1)[0] if ":" in bo else bo
            base_map.setdefault(base, set()).add(iid)
        if not present:
            return {"status": "ok", "edges": []}

        with SessionLocal() as s:
            bom_df, _ = _load_plan_bom_df(s, plan_id)
        if bom_df is None or bom_df.empty:
            return {"status": "ok", "edges": []}
        pairs = set()
        for r in bom_df[["item_id", "component_id"]].to_dict("records"):
            child = _clean_item_id(r.get("item_id"))
            parent = _clean_item_id(r.get("component_id"))
            if not child or not parent or child == parent:
                continue
            if child not in present or parent not in present:
                continue
            pairs.add((parent, child))
        if not pairs:
            return {"status": "ok", "edges": []}
        edges = []
        for base, items_in_base in base_map.items():
            # for each parent-child pair that both present in this base
            for parent, child in pairs:
                if parent in items_in_base and child in items_in_base:
                    edges.append({
                        "base_order_id": base,
                        "parent_item": parent,
                        "child_item": child,
                    })
        return {"status": "ok", "edges": edges}
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


@app.get("/reports/plans/{plan_id}/orders_by_workshop")
def report_orders_by_workshop(plan_id: int, workshops: Optional[List[str]] = Query(default=None)):
    """
    Возвращает список order_id, которые имеют хотя бы одну операцию на станке,
    относящемся к одному из выбранных цехов (DimMachine.family).
    Параметр `workshops` может повторяться, допускается произвольный регистр.
    """
    try:
        if not workshops:
            return {"status": "ok", "count": 0, "order_ids": []}
        # normalize tokens to lowercase trimmed
        tokens, prefixes = _normalize_workshop_tokens_list(workshops)
        if not tokens:
            return {"status": "ok", "count": 0, "order_ids": []}

        from ..db.models import ScheduleOp, DimMachine
        from sqlalchemy import func
        with SessionLocal() as s:
            conds = [func.lower(func.trim(DimMachine.family)).in_(tokens)]
            if prefixes:
                conds.append(or_(*[ScheduleOp.machine_id.like(f"{p}%") for p in prefixes]))
            rows = (
                s.query(ScheduleOp.order_id)
                 .join(DimMachine, ScheduleOp.machine_id == DimMachine.machine_id, isouter=True)
                 .filter(ScheduleOp.plan_id == plan_id)
                 .filter(or_(*conds))
                 .distinct()
                 .all()
            )
        order_ids = [str(r[0]) for r in rows if r and r[0] is not None]
        return {"status": "ok", "count": len(order_ids), "order_ids": order_ids}
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


@app.get("/reports/plans/{plan_id}/ops_for_orders")
def report_ops_for_orders(
    plan_id: int,
    orders: Optional[List[str]] = Query(default=None),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    workshops: Optional[List[str]] = Query(default=None),
    limit: int = Query(default=5000, ge=1, le=50000),
    offset: int = Query(default=0, ge=0),
    with_total: bool = Query(default=False),
):
    """
    Возвращает операции для выбранных заказов (order_id) с доп. полями станка.
    Параметры:
      - orders: повторяющийся параметр для списка order_id (обязателен)
      - date_from/date_to: ограничение по датам (пересечение по интервалу start_ts..end_ts)
      - workshops: список цехов (DimMachine.family), если указан — возвращаем только операции на этих цехах
    """
    try:
        if not orders:
            return {"status": "ok", "ops": []}
        tokens, prefixes = _normalize_workshop_tokens_list(workshops)
        if not tokens:
            tokens = []
        from ..db.models import ScheduleOp, DimMachine
        from sqlalchemy import and_, func
        with SessionLocal() as s:
            q = (
                s.query(
                    ScheduleOp.order_id,
                    ScheduleOp.item_id,
                    ScheduleOp.article_name,
                    ScheduleOp.machine_id,
                    ScheduleOp.start_ts,
                    ScheduleOp.end_ts,
                    ScheduleOp.duration_sec,
                    ScheduleOp.setup_sec,
                    ScheduleOp.qty,
                    ScheduleOp.op_index,
                    ScheduleOp.batch_id,
                    DimMachine.name.label("machine_name"),
                    DimMachine.family.label("workshop"),
                )
                .join(DimMachine, ScheduleOp.machine_id == DimMachine.machine_id, isouter=True)
                .filter(ScheduleOp.plan_id == plan_id)
                .filter(ScheduleOp.order_id.in_(orders))
            )
            if date_from:
                q = q.filter(func.date(ScheduleOp.end_ts) >= date_from)
            if date_to:
                q = q.filter(func.date(ScheduleOp.start_ts) <= date_to)
            if tokens:
                conds = [func.lower(func.trim(DimMachine.family)).in_(tokens)]
                if prefixes:
                    conds.append(or_(*[ScheduleOp.machine_id.like(f"{p}%") for p in prefixes]))
                q = q.filter(or_(*conds))
            total = int(q.count()) if with_total else None
            q = q.order_by(ScheduleOp.start_ts.asc(), ScheduleOp.order_id.asc()).offset(offset).limit(limit)
            rows = q.all()
        out = [
            {
                "order_id": r.order_id,
                "item_id": r.item_id,
                "article_name": r.article_name,
                "machine_id": r.machine_id,
                "start_ts": str(r.start_ts),
                "end_ts": str(r.end_ts),
                "duration_sec": int(r.duration_sec or 0),
                "setup_sec": int(r.setup_sec or 0) if r.setup_sec is not None else 0,
                "qty": float(r.qty or 0),
                "op_index": int(r.op_index or 0) if r.op_index is not None else 0,
                "batch_id": r.batch_id,
                "machine_name": r.machine_name,
                "workshop": r.workshop,
            }
            for r in rows
        ]
        return {
            "status": "ok",
            "count": len(out),
            "total": int(total) if with_total and total is not None else None,
            "limit": limit,
            "offset": offset,
            "ops": out,
        }
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


# ================== Transfer deficit report ==================
def _compute_transfer_deficit(
    plan_id: int,
    items: Optional[List[str]] = None,
    workshops: Optional[List[str]] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    period: str = "day",
):
    period = (period or "day").lower()
    if period not in ("day", "week", "month"):
        period = "day"

    item_tokens = _parse_token_list(items)
    item_filter = {t.lower() for t in item_tokens}
    wk_tokens, wk_prefixes = _normalize_workshop_tokens_list(workshops)

    def _match_workshop(val: str | None) -> bool:
        if not wk_tokens:
            return True
        if not val:
            return False
        w = val.strip().lower()
        if not w:
            return False
        if w in wk_tokens:
            return True
        return any(w.startswith(p) for p in wk_prefixes)

    try:
        with SessionLocal() as s:
            cache_key = (
                int(plan_id),
                tuple(sorted(item_filter)),
                tuple(sorted(wk_tokens)),
                str(date_from or ""),
                str(date_to or ""),
                str(period or "day"),
                _transfer_data_version(s, plan_id),
            )
            cached = _transfer_cache_get(cache_key)
            if cached is not None:
                return cached

            def _cache_return(payload: dict) -> dict:
                _transfer_cache_set(cache_key, payload)
                return deepcopy(payload)

            sql = """
                SELECT order_id, item_id, article_name, machine_id, start_ts, end_ts, qty
                FROM schedule_ops
                WHERE plan_id = :pid
            """
            sql_params = {"pid": plan_id}
            if date_from:
                sql += " AND date(end_ts) >= :date_from"
                sql_params["date_from"] = str(date_from)
            if date_to:
                sql += " AND date(start_ts) <= :date_to"
                sql_params["date_to"] = str(date_to)

            ops = s.execute(text(sql), sql_params).mappings().all()
            if not ops:
                return _cache_return({"period": period, "dates": [], "rows": [], "stock_snapshot_id": None, "stock_snapshot_taken_at": None})

            machines = s.execute(text("SELECT machine_id, name, family FROM dim_machine")).mappings().all()
            family_by_id = {str(r["machine_id"]): r.get("family") for r in machines}
            family_by_name = {}
            for r in machines:
                nm = str(r["name"])
                fam = r.get("family")
                if fam is not None and nm not in family_by_name:
                    family_by_name[nm] = fam

            def map_workshop(mid: str | None) -> str | None:
                if mid is None:
                    return None
                mid_str = str(mid)
                if mid_str in family_by_id and family_by_id[mid_str] is not None:
                    return str(family_by_id[mid_str])
                base = mid_str.split("_")[0]
                fam = family_by_id.get(base) or family_by_name.get(mid_str) or family_by_name.get(base)
                return str(fam) if fam is not None else None

            df = pd.DataFrame(ops)
            if df.empty:
                return _cache_return({"period": period, "dates": [], "rows": [], "stock_snapshot_id": None, "stock_snapshot_taken_at": None})
            df["start_ts"] = pd.to_datetime(df["start_ts"])
            df["end_ts"] = pd.to_datetime(df["end_ts"])
            df["base_order_id"] = df["order_id"].astype(str).str.split(":", n=1).str[0]
            df["workshop"] = df["machine_id"].apply(map_workshop)

            if item_filter:
                bases = set(
                    df[df["item_id"].astype(str).str.lower().isin(item_filter)]["base_order_id"].unique().tolist()
                )
                if not bases:
                    return _cache_return({"period": period, "dates": [], "rows": [], "stock_snapshot_id": None, "stock_snapshot_taken_at": None})
                df = df[df["base_order_id"].isin(bases)]

            orders = df.groupby(["order_id", "item_id", "base_order_id"], as_index=False).agg(
                start_ts=("start_ts", "min"),
                end_ts=("end_ts", "max"),
                qty=("qty", lambda s2: float(pd.to_numeric(s2, errors="coerce").max() or 0.0)),
                article_name=("article_name", "first"),
                workshop=("workshop", lambda s2: s2.dropna().mode().iat[0] if not s2.dropna().empty else None),
            )
            orders["start_date"] = orders["start_ts"].dt.date
            orders["end_date"] = orders["end_ts"].dt.date
            orders["duration_days"] = (pd.to_datetime(orders["end_date"]) - pd.to_datetime(orders["start_date"])).dt.days + 1
            orders.loc[orders["duration_days"] < 1, "duration_days"] = 1
            orders["base_item"] = (
                orders["base_order_id"].fillna("").astype(str).str.split(":", n=1).str[0].str.split("-", n=1).str[0]
            )

            item_workshop = {}
            for item_id, grp in orders.groupby("item_id"):
                w = grp["workshop"].dropna()
                if not w.empty:
                    item_workshop[str(item_id)] = str(w.mode().iat[0])

            item_name = {}
            for _, r in orders.iterrows():
                nm = str(r.get("article_name") or "").strip()
                iid = str(r.get("item_id"))
                if nm and iid not in item_name:
                    item_name[iid] = nm

            parent_by_base = {}
            for _, r in orders.iterrows():
                base = str(r["base_order_id"])
                base_item = str(r["base_item"])
                if str(r["item_id"]) == base_item and base not in parent_by_base:
                    parent_by_base[base] = r
            for base, grp in orders.groupby("base_order_id"):
                bkey = str(base)
                if bkey not in parent_by_base and not grp.empty:
                    parent_by_base[bkey] = grp.iloc[0]

            bom_df, _ = _load_plan_bom_df(s, plan_id)
            bom_map = {}
            bom_workshop_by_item: dict[str, str] = {}
            if bom_df is not None and not bom_df.empty:
                for br in bom_df[["item_id", "component_id", "qty_per", "loss", "workshop"]].to_dict("records"):
                    child = _clean_item_id(br.get("item_id"))
                    parent = _clean_item_id(br.get("component_id"))
                    if not child or not parent or child == parent:
                        continue
                    q = float(br.get("qty_per") or 0.0)
                    if q <= 0:
                        q = 1.0
                    loss = float(br.get("loss") or 1.0)
                    if loss <= 0:
                        loss = 1.0
                    q *= loss
                    key = (parent, child)
                    prev = bom_map.get(key)
                    if prev is None or q > prev:
                        bom_map[key] = q
                    wk = str(br.get("workshop") or "").strip()
                    if wk and wk.lower() not in ("nan", "none", "null"):
                        if wk.endswith(".0"):
                            wk = wk[:-2]
                        if wk and child not in bom_workshop_by_item:
                            bom_workshop_by_item[child] = wk
            for iid, wk in bom_workshop_by_item.items():
                if iid and wk and iid not in item_workshop:
                    item_workshop[iid] = wk

            snap_row = s.execute(text("SELECT id, taken_at FROM stock_snapshot ORDER BY taken_at DESC LIMIT 1")).fetchone()
            stock_snapshot_id = snap_row[0] if snap_row else None
            stock_snapshot_taken_at = str(snap_row[1]) if snap_row and snap_row[1] is not None else None
            stock_map = {}
            if stock_snapshot_id is not None:
                stock_rows = s.execute(
                    text(
                        "SELECT item_id, COALESCE(workshop,'') AS workshop, stock_qty FROM stock_line WHERE snapshot_id = :sid"
                    ),
                    {"sid": stock_snapshot_id},
                ).mappings().all()
                for sr in stock_rows:
                    stock_map[(str(sr["item_id"]), str(sr["workshop"]))] = float(sr.get("stock_qty") or 0.0)
            stock_total_by_item: dict[str, float] = defaultdict(float)
            stock_has_named_wk_by_item: dict[str, bool] = defaultdict(bool)
            for (iid, _wk), qty in stock_map.items():
                stock_total_by_item[str(iid)] += float(qty or 0.0)
                if str(_wk or "").strip():
                    stock_has_named_wk_by_item[str(iid)] = True

            def stock_for_workshop(item_id: str, workshop: str | None) -> float:
                iid = str(item_id)
                wk = str(workshop or "")
                key = (iid, wk)
                if key in stock_map:
                    return float(stock_map[key] or 0.0)
                # If item has workshop-specific stock rows, missing workshop means zero on this workshop.
                if stock_has_named_wk_by_item.get(iid, False):
                    return 0.0
                return float(stock_total_by_item.get(iid, 0.0) or 0.0)

            try:
                df_date_from = pd.to_datetime(date_from).date() if date_from else None
            except Exception:
                df_date_from = None
            try:
                df_date_to = pd.to_datetime(date_to).date() if date_to else None
            except Exception:
                df_date_to = None

            def bucket_date(d: dt.date) -> dt.date:
                if period == "week":
                    return d - dt.timedelta(days=d.weekday())
                if period == "month":
                    return d.replace(day=1)
                return d

            prepared = {}
            processed_base_child: set[tuple[str, str]] = set()
            for _, r in orders.iterrows():
                child_item = str(r["item_id"])
                base_item = str(r["base_item"])
                base = str(r["base_order_id"])
                if child_item == base_item:
                    continue
                pair_key = (base, child_item)
                if pair_key in processed_base_child:
                    # One child can be split into multiple order ids (~1, ~2) inside the same base order.
                    # Parent demand for transfer must be counted once per (base_order_id, child_item).
                    continue
                processed_base_child.add(pair_key)
                parent_base_row = parent_by_base.get(base)
                parents_info = []
                for (p, c), q in bom_map.items():
                    if c != child_item:
                        continue
                    match = orders[(orders["base_order_id"] == base) & (orders["item_id"] == p)]
                    if match.empty:
                        continue
                    pid = str(p)
                    # Parent item can be split into multiple order ids (~1, ~2, ...).
                    # Each split contributes its own qty/duration window.
                    for _, parent_row in match.iterrows():
                        parents_info.append((parent_row, float(q or 0.0) or 1.0, pid))
                if not parents_info and parent_base_row is not None:
                    pr = parent_base_row
                    if isinstance(pr, pd.Series):
                        pr = pr.to_dict()
                    qty_per = (
                        bom_map.get((str(pr.get("item_id")), child_item))
                        or bom_map.get((base_item, child_item))
                        or (max([q for (p, c), q in bom_map.items() if c == child_item], default=0.0) or None)
                        or 1.0
                    )
                    parents_info.append((pr, float(qty_per or 1.0), str(pr.get("item_id") or base_item)))
                for parent_row, qty_per, parent_item_id in parents_info:
                    parent = parent_row
                    if isinstance(parent_row, pd.Series):
                        parent_row = parent_row.to_dict()
                    if isinstance(parent, pd.Series):
                        parent = parent.to_dict()
                    parent_qty = float(parent_row.get("qty") or 0.0)
                    duration = int(parent_row.get("duration_days") or 1)
                    if duration < 1:
                        duration = 1
                    if qty_per is None:
                        qty_per = (
                            bom_map.get((parent_item_id, child_item))
                            or bom_map.get((base_item, child_item))
                            or (max([q for (p, c), q in bom_map.items() if c == child_item], default=0.0) or None)
                            or 1.0
                        )
                    per_day = parent_qty * float(qty_per) / float(duration)
                    parent_start = (parent_row or {}).get("start_date") or (parent or {}).get("start_date")
                    parent_end = (parent_row or {}).get("end_date") or (parent or {}).get("end_date")
                    if parent_start is None or parent_end is None:
                        continue
                    start_date = pd.to_datetime(parent_start).date()
                    end_date = pd.to_datetime(parent_end).date()
                    dates = pd.date_range(start_date, end_date, freq="D").date
                    target_wk = str(
                        (parent_row or {}).get("workshop")
                        or (parent or {}).get("workshop")
                        or item_workshop.get(parent_item_id)
                        or bom_workshop_by_item.get(parent_item_id)
                        or ""
                    )
                    key = (child_item, target_wk)
                    bucket = prepared.get(key, defaultdict(float))
                    for d in dates:
                        if df_date_from and d < df_date_from:
                            continue
                        if df_date_to and d > df_date_to:
                            continue
                        b = bucket_date(d)
                        bucket[b] += float(per_day)
                    prepared[key] = bucket

            rows_raw = []
            all_buckets: set[dt.date] = set()
            for (item_id, target_wk), plan_bucket in prepared.items():
                if not plan_bucket:
                    continue
                source_wk = item_workshop.get(item_id) or bom_workshop_by_item.get(item_id)
                # Workshop filter applies only to source workshop
                if wk_tokens and not _match_workshop(source_wk):
                    continue
                all_buckets.update(plan_bucket.keys())
                source_stock = stock_for_workshop(item_id, source_wk)
                target_stock = stock_for_workshop(item_id, target_wk)
                rows_raw.append(
                    {
                        "item_id": item_id,
                        "item_name": item_name.get(item_id),
                        "source_workshop": source_wk or "",
                        "target_workshop": target_wk or "",
                        "stock_source": float(source_stock),
                        "stock_target": float(target_stock),
                        "plan": plan_bucket,
                    }
                )

            # Aggregate across all target workshops into a single row per item
            if rows_raw:
                aggregated = {}
                for r in rows_raw:
                    iid = str(r.get("item_id"))
                    agg = aggregated.get(iid)
                    if agg is None:
                        agg = {
                            "item_id": iid,
                            "item_name": r.get("item_name"),
                            "source_workshop": r.get("source_workshop") or "",
                            "target_workshops": set(),
                            "target_workshop": "",
                            "stock_source": float(r.get("stock_source") or 0.0),
                            "stock_target": 0.0,
                            "plan": defaultdict(float),
                        }
                        aggregated[iid] = agg
                    if r.get("target_workshop"):
                        agg["target_workshops"].add(str(r.get("target_workshop")))
                    agg["stock_target"] += float(r.get("stock_target") or 0.0)
                    for k, v in (r.get("plan") or {}).items():
                        agg["plan"][k] += float(v or 0.0)
                for agg in aggregated.values():
                    tset = agg.pop("target_workshops", set())
                    if len(tset) == 1:
                        agg["target_workshop"] = next(iter(tset))
                    elif len(tset) > 1:
                        agg["target_workshop"] = ", ".join(sorted(tset))
                rows_raw = list(aggregated.values())

            # If items filter was provided, keep only matching items (after aggregation)
            if item_filter:
                rows_raw = [r for r in rows_raw if str(r.get("item_id", "")).lower() in item_filter]

            if not rows_raw or not all_buckets:
                return _cache_return({
                    "period": period,
                    "dates": [],
                    "rows": [],
                    "stock_snapshot_id": stock_snapshot_id,
                    "stock_snapshot_taken_at": stock_snapshot_taken_at,
                })

            sorted_buckets = sorted(all_buckets)
            for row in rows_raw:
                remaining = float(row.get("stock_target") or 0.0)
                deficit = {}
                cum = 0.0
                for b in sorted_buckets:
                    need = float(row["plan"].get(b, 0.0) or 0.0)
                    if remaining >= need:
                        remaining -= need
                    else:
                        deficit_val = need - remaining
                        remaining = 0.0
                        cum += deficit_val
                    deficit[b] = cum
                row["deficit"] = deficit

            return _cache_return({
                "period": period,
                "dates": sorted_buckets,
                "rows": rows_raw,
                "stock_snapshot_id": stock_snapshot_id,
                "stock_snapshot_taken_at": stock_snapshot_taken_at,
            })
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


def _serialize_transfer_deficit(res: dict) -> dict:
    dates = res.get("dates") or []
    rows_json = []
    for r in res.get("rows", []):
        def _to_dict(src: dict) -> dict:
            return {str(k): float(src.get(k, 0.0)) for k in dates}
        rows_json.append(
            {
                "item_id": r.get("item_id"),
                "item_name": r.get("item_name"),
                "source_workshop": r.get("source_workshop"),
                "target_workshop": r.get("target_workshop"),
                "stock_source": float(r.get("stock_source") or 0.0),
                "stock_target": float(r.get("stock_target") or 0.0),
                "plan": _to_dict(r.get("plan", {})),
                "deficit": _to_dict(r.get("deficit", {})),
            }
        )
    return {
        "status": "ok",
        "period": res.get("period"),
        "dates": [str(d) for d in dates],
        "rows": rows_json,
        "stock_snapshot_id": res.get("stock_snapshot_id"),
        "stock_snapshot_taken_at": res.get("stock_snapshot_taken_at"),
    }


def _run_transfer_deficit_job(job_id: str) -> None:
    with _TRANSFER_DEFICIT_JOBS_LOCK:
        job = _TRANSFER_DEFICIT_JOBS.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["updated_at"] = dt.datetime.utcnow().isoformat()
        job["updated_ts"] = time.time()
        plan_id = int(job.get("plan_id"))
        params = deepcopy(job.get("params") or {})
    try:
        res = _compute_transfer_deficit(
            plan_id,
            items=params.get("items"),
            workshops=params.get("workshops"),
            date_from=params.get("date_from"),
            date_to=params.get("date_to"),
            period=params.get("period") or "day",
        )
        payload = _serialize_transfer_deficit(res)
        with _TRANSFER_DEFICIT_JOBS_LOCK:
            job = _TRANSFER_DEFICIT_JOBS.get(job_id)
            if job:
                job["status"] = "done"
                job["result"] = payload
                job["updated_at"] = dt.datetime.utcnow().isoformat()
                job["updated_ts"] = time.time()
                _transfer_job_cleanup_locked()
    except Exception as e:
        err = str(e)
        if isinstance(e, HTTPException):
            detail = e.detail
            if isinstance(detail, dict):
                err = str(detail.get("msg") or detail)
            else:
                err = str(detail)
        with _TRANSFER_DEFICIT_JOBS_LOCK:
            job = _TRANSFER_DEFICIT_JOBS.get(job_id)
            if job:
                job["status"] = "error"
                job["error"] = err
                job["updated_at"] = dt.datetime.utcnow().isoformat()
                job["updated_ts"] = time.time()
                _transfer_job_cleanup_locked()


@app.post("/reports/plans/{plan_id}/transfer_deficit/jobs")
def start_transfer_deficit_job(plan_id: int, body: TransferDeficitJobIn):
    params = {
        "items": body.items or [],
        "workshops": body.workshops or [],
        "date_from": body.date_from,
        "date_to": body.date_to,
        "period": body.period or "day",
    }
    now_iso = dt.datetime.utcnow().isoformat()
    now_ts = time.time()
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "queued",
        "plan_id": int(plan_id),
        "params": params,
        "result": None,
        "error": None,
        "created_at": now_iso,
        "updated_at": now_iso,
        "created_ts": now_ts,
        "updated_ts": now_ts,
    }
    with _TRANSFER_DEFICIT_JOBS_LOCK:
        _transfer_job_cleanup_locked()
        _TRANSFER_DEFICIT_JOBS[job_id] = job
    t = Thread(target=_run_transfer_deficit_job, args=(job_id,), daemon=True)
    t.start()
    return {
        "status": "queued",
        "job_id": job_id,
        "status_url": f"/reports/transfer_deficit/jobs/{job_id}",
    }


@app.get("/reports/transfer_deficit/jobs/{job_id}")
def get_transfer_deficit_job(job_id: str, include_result: bool = Query(default=False)):
    with _TRANSFER_DEFICIT_JOBS_LOCK:
        _transfer_job_cleanup_locked()
        job = _TRANSFER_DEFICIT_JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail={"msg": "job not found"})
        payload = _transfer_job_payload(deepcopy(job), include_result=include_result)
    return payload

@app.get("/reports/plans/{plan_id}/transfer_deficit")
def report_transfer_deficit(
    plan_id: int,
    items: Optional[List[str]] = Query(default=None, description="Артикулы, можно списком через запятую или перенос"),
    workshops: Optional[List[str]] = Query(default=None, description="Цеха-числа для фильтрации источника/получателя"),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    period: str = "day",
):
    res = _compute_transfer_deficit(plan_id, items=items, workshops=workshops, date_from=date_from, date_to=date_to, period=period)
    return _serialize_transfer_deficit(res)

@app.post("/reports/plans/{plan_id}/transfer_deficit/export")
def export_transfer_deficit(plan_id: int, body: TransferDeficitExport):
    res = _compute_transfer_deficit(
        plan_id,
        items=body.items,
        workshops=body.workshops,
        date_from=body.date_from,
        date_to=body.date_to,
        period=body.period or "day",
    )
    dates = res.get("dates") or []
    rows = res.get("rows") or []
    if not dates or not rows:
        return {"status": "ok", "path": None, "rows": 0}
    out_path = body.out_path or os.path.join("out", "transfer_deficit.xlsx")
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    cols = [
        "Артикул",
        "Наименование",
        "Цех",
        "Цех получатель",
        "Запас на цехе",
        "Запас на цехе получателе",
        "Тип",
    ] + [str(d) for d in dates]
    data = []
    for r in rows:
        base = [
            r.get("item_id"),
            r.get("item_name") or "",
            r.get("source_workshop") or "",
            r.get("target_workshop") or "",
            float(r.get("stock_source") or 0.0),
            float(r.get("stock_target") or 0.0),
        ]
        plan_vals = [float(r.get("plan", {}).get(d, 0.0) or 0.0) for d in dates]
        deficit_vals = [float(r.get("deficit", {}).get(d, 0.0) or 0.0) for d in dates]
        data.append(base + ["План к перемещению"] + plan_vals)
        data.append(["", "", "", "", "", ""] + ["Накопительный дефицит"] + deficit_vals)
    pd.DataFrame(data, columns=cols).to_excel(out_path, index=False)
    return {"status": "ok", "path": out_path, "rows": len(rows)}
