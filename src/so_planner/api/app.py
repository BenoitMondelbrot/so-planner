# === imports (чистим) ===
from fastapi import Query, FastAPI, UploadFile, File, HTTPException, Request
import logging
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import os, shutil, uuid, traceback
import pandas as pd
import numpy as np
import datetime as dt

from pydantic import BaseModel, Field
from typing import Literal, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import text, func, or_

from fastapi import Query, FastAPI, UploadFile, File, HTTPException, Depends


# ВАЖНО: не тянем Base из .models, а берём только SessionLocal/init_db из пакета db
from ..db import SessionLocal, init_db

from ..ingest.loader import load_excels, validate_files
from ..scheduling.greedy_scheduler import run_greedy, compute_orders_timeline
from ..export.report import export_excel
from ..analysis.bottlenecks import scan_bottlenecks
from .routers import plans, optimize

# ================== App ==================
app = FastAPI(title="S&O Planner API")

STATIC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ui"))
if os.path.isdir(STATIC_DIR):  # монтируем только если каталог существует
    app.mount("/ui", StaticFiles(directory=STATIC_DIR, html=True), name="ui")

app.include_router(plans.router)
app.include_router(optimize.router)



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
    "out": "schedule_out.xlsx",
    "receipts": None,  # optional: uploaded receipts Excel (ad-hoc)
}

# --- PATCH START: schemas ---

class PlanVersionCreate(BaseModel):
    name: str
    horizon_start: Optional[str] = None
    horizon_end: Optional[str] = None
    notes: Optional[str] = None

class PlanLineIn(BaseModel):
    item_id: str
    due_date: str
    qty: int
    priority: Optional[str] = None
    customer: Optional[str] = None
    workshop: Optional[str] = None
    source_tag: Optional[str] = None

class ReceiptsLineIn(BaseModel):
    item_id: str
    due_date: str
    qty: int
    workshop: Optional[str] = ""
    receipt_type: Literal["prod","purchase","transfer"] = "prod"
    source_ref: Optional[str] = None

class StockLineIn(BaseModel):
    item_id: str
    stock_qty: int
    workshop: Optional[str] = ""

class NettingRunIn(BaseModel):
    plan_version_id: Optional[int] = None
    stock_snapshot_id: int
    # add 'excel' to allow using uploaded receipts sheet
    receipts_from: Literal["plan","firmed","both","excel"] = "plan"
    # optional explicit path (if not set, server will use LAST_PATHS['receipts'] when receipts_from='excel')
    receipts_excel_path: Optional[str] = None
    bom_version_id: Optional[str] = None
    params: Optional[dict] = None

# --- PATCH END ---

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
    # Small helper to inject new modes and generic mode handling without touching the file on disk
    def _inject_ui_modes(html: str) -> str:
        try:
            # 1) Add Standard-Up option to selector if missing
            if ('id="selMode"' in html) and ('value="standard_up"' not in html):
                i0 = html.find('id="selMode"')
                j = html.find('</select>', i0)
                if j != -1:
                    opt = "\n              <option value=\"standard_up\">Стандартный-вверх</option>"
                    html = html[:j] + opt + html[j:]
            # 2) Replace hardcoded fetch targets with dynamic mapping
            mode_expr = "${encodeURIComponent(mode==='standard'?'':(mode==='netting'?'product_view':mode))}"
            html = html.replace(
                "fetch('/schedule/greedy?mode=', {method:'POST'})",
                f"fetch(`/schedule/greedy?mode={mode_expr}`, {{method:'POST'}})"
            )
            html = html.replace(
                "fetch('/schedule/greedy?mode=product_view', {method:'POST'})",
                f"fetch(`/schedule/greedy?mode={mode_expr}`, {{method:'POST'}})"
            )
            # 3) Inject receipts enhancements (option 'excel', upload control, and patched runGreedyJSON)
            inj = """
<script>
document.addEventListener('DOMContentLoaded', function(){
  try{
    // Add 'excel' option to receipts selector if missing
    var sel = document.getElementById('ng-receipts');
    if(sel){
      var hasExcel = false; for(var i=0;i<sel.options.length;i++){ if(sel.options[i].value==='excel'){ hasExcel=true; break; } }
      if(!hasExcel){ var opt=document.createElement('option'); opt.value='excel'; opt.textContent='excel'; sel.appendChild(opt); }
    }
  }catch(e){ console.warn('inject receipts option failed', e); }
  try{
    // Files tab: add Receipts upload control next to stock
    var stock = document.getElementById('fStock');
    if(stock && !document.getElementById('fReceipts')){
      var wrap = stock.parentElement.parentElement; // row
      var col = document.createElement('div'); col.className='col';
      col.innerHTML = '<label>План поступлений (receipts) (.xlsx)</label><input id="fReceipts" type="file" accept=".xlsx,.xls" />';
      wrap.appendChild(col);
      var col2 = document.createElement('div'); col2.className='col'; col2.style.flex='0 0 auto';
      col2.innerHTML = '<label>&nbsp;</label><button class="btn" id="btnUploadReceipts">Upload Receipts</button>';
      wrap.appendChild(col2);
      document.getElementById('btnUploadReceipts').addEventListener('click', async function(){
        try{
          var f = document.getElementById('fReceipts').files[0];
          if(!f){ alert('Выберите receipts.xlsx'); return; }
          var fd = new FormData(); fd.append('receipts', f);
          var r = await fetch('/upload/receipts', { method:'POST', body: fd });
          var j = await r.json();
          if(!r.ok){ alert('Ошибка загрузки: '+(j.error||j.detail?.msg||'unknown')); return; }
          window.activeReceiptsPath = j.stored_path;
          if(window.toast) toast('Receipts uploaded', true);
        }catch(e){ alert('Upload error: '+e); }
      });
    }
  }catch(e){ console.warn('inject receipts upload failed', e); }
  try{
    // Patch Run Greedy JSON button to include receipts_excel_path when receipts=='excel'
    var btn = document.getElementById('btnRunGreedyJSON');
    if(btn){
      var nb = btn.cloneNode(true); btn.parentNode.replaceChild(nb, btn);
      nb.addEventListener('click', async function(){
        try{
          const planRaw=document.getElementById('ng-plan').value; const plan=planRaw?parseInt(planRaw,10):null;
          const stock=parseInt(document.getElementById('ng-stock').value||'0',10);
          const receipts=(document.getElementById('ng-receipts').value||'plan');
          if(!stock){ alert('Укажите Stock Snapshot'); return; }
          const payload={ plan_version_id:plan, stock_snapshot_id:stock, receipts_from:receipts };
          if(receipts==='excel') payload.receipts_excel_path = (window.activeReceiptsPath||null);
          const r=await fetch('/schedule/greedy_json',{method:'POST',headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
          const j=await r.json();
          if(!r.ok){ alert('Ошибка: '+(j.detail?.msg||JSON.stringify(j))); return; }
          const meta=document.getElementById('ng-meta');
          if(meta) meta.innerHTML = `rows: <b>${j.rows}</b> · out: <b>${j.out||''}</b> · plan_id: <b>${j.plan_id??''}</b>`;
          if(j.plan_id) window.state && (window.state.selectedId=j.plan_id);
        }catch(e){ alert('Run error: '+e); }
      });
    }
  }catch(e){ console.warn('patch runGreedyJSON failed', e); }
  try{
    // Add 'Approve selected plan' button on Plans tab
    var runBtn = document.getElementById('btnRunGreedyPlan');
    if(runBtn && !document.getElementById('btnApprovePlan')){
      var row = runBtn.parentElement; var b=document.createElement('button'); b.className='btn'; b.id='btnApprovePlan'; b.textContent='Approve selected'; b.style.marginLeft='8px';
      row.appendChild(b);
      b.addEventListener('click', async function(){
        try{
          const id = (window.state&&window.state.selectedId)||null; if(!id){ alert('Select plan first'); return; }
          const r = await fetch(`/plans/${id}/approve`, {method:'POST'}); const j=await r.json();
          if(!r.ok){ alert('Approve failed'); return; }
          if(window.toast) toast('Plan approved', true);
        }catch(e){ alert('Approve error: '+e); }
      });
    }
  }catch(e){ console.warn('inject approve button failed', e); }
});
</script>
"""
            # append before </body>
            try:
                k = html.rfind('</body>')
                if k != -1 and inj not in html:
                    html = html[:k] + inj + html[k:]
            except Exception:
                pass
        except Exception:
            pass
        return html

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
                            stock: UploadFile | None = None):
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
        if stock is not None:
            spath = _save(stock)

        with SessionLocal() as s:
            cnts = load_excels(s, mpath, bpath, ppath)

        # обновим активные пути
        LAST_PATHS.update({"machines": mpath, "bom": bpath, "plan": ppath})
        if spath:
            LAST_PATHS.update({"stock": spath})

        payload = {
            "status": "ok",
            "stored_paths": {"machines": mpath, "bom": bpath, "plan": ppath, "stock": spath},
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

@app.post("/upload/receipts")
async def upload_receipts(receipts: UploadFile = File(...)):
    try:
        ext = os.path.splitext(receipts.filename or "")[1] or ".xlsx"
        path = os.path.join("uploads", f"{uuid.uuid4().hex}{ext}")
        with open(path, "wb") as out:
            shutil.copyfileobj(receipts.file, out)
        LAST_PATHS.update({"receipts": path})
        return JSONResponse(content={"status": "ok", "stored_path": path, "active_paths": LAST_PATHS})
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        return JSONResponse(status_code=400, content={"status": "error", "error": str(e), "trace": tb})


def ensure_tables(db: Session):
    from so_planner.scheduling.greedy_scheduler import _ensure_netting_tables
    _ensure_netting_tables(db)



# ================== Scheduling (Greedy) ==================
@app.post("/schedule/greedy")
def schedule_greedy(mode: str = ""):
    """
    Запускает greedy-планировщик по LAST_PATHS и возвращает сводку:
      - out (xlsx), rows, min/max даты, preview,
      - bottlenecks / hot_days,
      - active_paths,
      - warnings.
    """
    # Fast path: product_view via DB if requested (auto-pick latest IDs)
    if (mode or "").lower().strip() == "product_view":
        try:
            with SessionLocal() as s:
                ensure_tables(s)
                pv = s.execute(text("SELECT id FROM plan_version WHERE status='approved' ORDER BY id DESC LIMIT 1")).scalar()
                if pv is None:
                    pv = s.execute(text("SELECT id FROM plan_version ORDER BY id DESC LIMIT 1")).scalar()
                ss = s.execute(text("SELECT id FROM stock_snapshot ORDER BY id DESC LIMIT 1")).scalar()
                # Если нет снимка остатков — создаём пустой автоматически, чтобы можно было запустить неттинг «из коробки»
                auto_created_snapshot = False
                if ss is None:
                    ss = s.execute(text("""
                        INSERT INTO stock_snapshot (name, taken_at, notes)
                        VALUES ('auto', CURRENT_TIMESTAMP, 'created by /schedule/greedy?mode=product_view')
                        RETURNING id
                    """)).scalar()
                    s.commit()
                    auto_created_snapshot = True
                auto_created_plan = False
                if pv is None:
                    # Нет ни одной версии плана — создадим пустую и сразу утвердим,
                    # чтобы первый запуск неттинга мог состояться «из коробки».
                    name = f"auto {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}"
                    pv = s.execute(text(
                        """
                        INSERT INTO plan_version (name, created_at, status, origin)
                        VALUES (:name, CURRENT_TIMESTAMP, 'approved', 'ui')
                        RETURNING id
                        """
                    ), {"name": name}).scalar()
                    s.commit()
                    auto_created_plan = True

                from so_planner.scheduling.greedy_scheduler import run_product_view_from_db
                out_file, sched, plan_id = run_product_view_from_db(
                    db=s,
                    plan_version_id=(None if auto_created_plan else int(pv)),
                    stock_snapshot_id=int(ss),
                    receipts_from="plan",
                    bom_path=LAST_PATHS.get("bom"),
                    machines_path=LAST_PATHS.get("machines"),
                    out_xlsx=LAST_PATHS.get("out"),
                    user="ui",
                )

                summary, hot = scan_bottlenecks(s)

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
                "rows": rows_cnt,
                "plan_id": int(plan_id) if plan_id is not None else None,
                "min_date": min_date,
                "max_date": max_date,
                "preview": preview,
                "bottlenecks": any_to_jsonable(summary),
                "hot_days": any_to_jsonable(hot),
                "active_paths": LAST_PATHS,
                "warnings": warnings,
            }
            return JSONResponse(content=payload)
        except HTTPException:
            raise
        except Exception as e:
            tb = traceback.format_exc(limit=3)
            raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})

    try:
        with SessionLocal() as s:
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
                stock_path=LAST_PATHS.get("stock")
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

@app.post("/plans/versions")
def create_plan_version(payload: PlanVersionCreate, db: Session = Depends(get_db)):
    ensure_tables(db)
    q = text("""
      INSERT INTO plan_version (name, created_at, status, horizon_start, horizon_end, notes, origin)
      VALUES (:name, CURRENT_TIMESTAMP, 'draft', :hs, :he, :notes, 'ui')
      RETURNING id
    """)
    vid = db.execute(q, {"name": payload.name, "hs": payload.horizon_start, "he": payload.horizon_end, "notes": payload.notes}).scalar_one()
    db.commit()
    return {"id": int(vid), "status": "draft"}

@app.post("/plans/versions/{plan_version_id}/lines:bulk")
def bulk_plan_lines(plan_version_id: int, lines: List[PlanLineIn], db: Session = Depends(get_db)):
    ensure_tables(db)
    rows = [l.dict() for l in lines]
    for r in rows:
        r["plan_version_id"] = plan_version_id
    db.execute(text("""
      INSERT INTO plan_line (plan_version_id,item_id,due_date,qty,priority,customer,workshop,source_tag)
      VALUES (:plan_version_id,:item_id,:due_date,:qty,:priority,:customer,:workshop,:source_tag)
    """), rows)
    db.commit()
    return {"inserted": len(rows)}

@app.post("/plans/versions/{plan_version_id}/approve")
def approve_plan_version(plan_version_id: int, db: Session = Depends(get_db)):
    ensure_tables(db)
    db.execute(text("UPDATE plan_version SET status='approved' WHERE id=:id"), {"id": plan_version_id})
    db.commit()
    return {"id": plan_version_id, "status": "approved"}

@app.post("/receipts/plan/{plan_version_id}:bulk")
def bulk_receipts_plan(plan_version_id: int, rows: List[ReceiptsLineIn], db: Session = Depends(get_db)):
    ensure_tables(db)
    data = [dict(r, plan_version_id=plan_version_id) for r in [x.dict() for x in rows]]
    db.execute(text("""
      INSERT INTO receipts_plan (plan_version_id,item_id,due_date,qty,workshop,receipt_type,source_ref)
      VALUES (:plan_version_id,:item_id,:due_date,:qty,:workshop,:receipt_type,:source_ref)
    """), data)
    db.commit()
    return {"inserted": len(data)}

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

@app.post("/netting/run")
def netting_run(payload: NettingRunIn, db: Session = Depends(get_db)):
    """
    Запускает ветку product_view неттинга, сохраняет результат в БД,
    возвращает run_id + краткую сводку.
    """
    ensure_tables(db)

    # подготавливаем входы как сейчас делает /schedule/greedy, но запускаем только product_view
    from so_planner.scheduling.greedy_scheduler import run_pipeline

    # Считаем, что plan_df, bom, machines у вас собираются как обычно из загруженных источников.
    # Если уже есть ваш механизм "loaded", используйте его тут. Ниже — пример через kwargs.
    out_file, sched = run_pipeline(
        mode="product_view",
        plan_version_id=payload.plan_version_id,
        stock_snapshot_id=payload.stock_snapshot_id,
        receipts_from=payload.receipts_from,
        receipts_excel_path=(payload.receipts_excel_path or LAST_PATHS.get("receipts")) if payload.receipts_from == "excel" else None,
        bom_version_id=payload.bom_version_id,
        db=db,
        user="api",
        **{}  # <- ваш привычный набор: пути к excel или уже загруженные DataFrame'ы
    )

    # Внутри run_pipeline сохранён netting_run; ради ответа вернём последнее id
    rid = db.execute(text("SELECT id FROM netting_run ORDER BY id DESC LIMIT 1")).scalar()
    # Быстрая сводка
    orders_count = db.execute(text("SELECT COUNT(*) FROM netting_order WHERE netting_run_id=:rid"), {"rid": rid}).scalar()
    summary_count = db.execute(text("SELECT COUNT(*) FROM netting_summary_row WHERE netting_run_id=:rid"), {"rid": rid}).scalar()
    log_count = db.execute(text("SELECT COUNT(*) FROM netting_log_row WHERE netting_run_id=:rid"), {"rid": rid}).scalar()
    return {"run_id": int(rid), "orders": int(orders_count or 0), "summary": int(summary_count or 0), "log": int(log_count or 0)}



# Lightweight JSON scheduling endpoint (product_view)
@app.post("/schedule/greedy_json")
def schedule_greedy_json(payload: NettingRunIn, db: Session = Depends(get_db)):
    """Runs product_view scheduling and returns rows + out path for UI convenience."""
    try:
        ensure_tables(db)
        from so_planner.scheduling.greedy_scheduler import run_product_view_from_db
        out_file, sched, plan_id = run_product_view_from_db(
            db=db,
            plan_version_id=payload.plan_version_id,
            stock_snapshot_id=payload.stock_snapshot_id,
            receipts_from=payload.receipts_from,
            receipts_excel_path=(payload.receipts_excel_path or LAST_PATHS.get("receipts")) if payload.receipts_from == "excel" else None,
            bom_path=LAST_PATHS.get("bom"),
            machines_path=LAST_PATHS.get("machines"),
            out_xlsx=LAST_PATHS.get("out"),
            user="api",
        )
        rows_cnt = int(len(sched) if sched is not None else 0)
        return {"status": "ok", "rows": rows_cnt, "out": str(out_file) if out_file is not None else None, "plan_id": plan_id}
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})


# ================== Netting (runs, browse) ==================
@app.get("/netting/runs")
def list_netting_runs(plan_version_id: Optional[int] = None, db: Session = Depends(get_db)):
    ensure_tables(db)
    base = "SELECT id, started_at, finished_at, user, mode, plan_version_id, stock_snapshot_id, status FROM netting_run"
    if plan_version_id:
        base += " WHERE plan_version_id = :pv"
        rows = db.execute(text(base), {"pv": plan_version_id}).mappings().all()
    else:
        rows = db.execute(text(base)).mappings().all()
    # newest first
    rows = sorted(rows, key=lambda r: r["id"], reverse=True)
    return [{k: (str(v) if k in ("started_at","finished_at") and v is not None else v) for k, v in dict(r).items()} for r in rows]


@app.get("/netting/runs/{rid}")
def get_netting_run(rid: int, db: Session = Depends(get_db)):
    ensure_tables(db)
    head = db.execute(text("SELECT id, started_at, finished_at, user, mode, plan_version_id, stock_snapshot_id, status FROM netting_run WHERE id=:id"), {"id": rid}).mappings().first()
    if not head:
        raise HTTPException(status_code=404, detail={"msg": "run not found"})
    counts = {
        "orders": int(db.execute(text("SELECT COUNT(*) FROM netting_order WHERE netting_run_id=:id"), {"id": rid}).scalar() or 0),
        "summary": int(db.execute(text("SELECT COUNT(*) FROM netting_summary_row WHERE netting_run_id=:id"), {"id": rid}).scalar() or 0),
        "log": int(db.execute(text("SELECT COUNT(*) FROM netting_log_row WHERE netting_run_id=:id"), {"id": rid}).scalar() or 0),
    }
    return {"head": dict(head), "counts": counts}


@app.get("/netting/runs/{rid}/orders")
def get_netting_orders(rid: int,
                       item_id: Optional[str] = None,
                       workshop: Optional[str] = None,
                       date_from: Optional[str] = None,
                       date_to: Optional[str] = None,
                       db: Session = Depends(get_db)):
    ensure_tables(db)
    q = """
      SELECT order_id, item_id, due_date, qty, priority, workshop
      FROM netting_order
      WHERE netting_run_id = :rid
    """
    params = {"rid": rid}
    if item_id:
        q += " AND item_id = :it"; params["it"] = item_id
    if workshop:
        q += " AND COALESCE(workshop,'') = :wk"; params["wk"] = workshop
    if date_from:
        q += " AND date(due_date) >= date(:df)"; params["df"] = date_from
    if date_to:
        q += " AND date(due_date) <= date(:dt)"; params["dt"] = date_to
    q += " ORDER BY due_date, item_id"
    rows = db.execute(text(q), params).mappings().all()
    # stringify dates
    out = []
    for r in rows:
        rr = dict(r)
        if rr.get("due_date") is not None:
            rr["due_date"] = str(rr["due_date"])
        out.append(rr)
    return out


@app.get("/netting/runs/{rid}/summary")
def get_netting_summary(rid: int,
                        item_id: Optional[str] = None,
                        workshop: Optional[str] = None,
                        db: Session = Depends(get_db)):
    ensure_tables(db)
    q = """
      SELECT item_id, workshop,
             stock_used_total, receipts_used_total, orders_created_total,
             opening_exact_init, opening_generic_init
      FROM netting_summary_row
      WHERE netting_run_id = :rid
    """
    params = {"rid": rid}
    if item_id:
        q += " AND item_id = :it"; params["it"] = item_id
    if workshop:
        q += " AND COALESCE(workshop,'') = :wk"; params["wk"] = workshop
    q += " ORDER BY item_id, workshop"
    rows = db.execute(text(q), params).mappings().all()
    return [dict(r) for r in rows]


@app.get("/netting/runs/{rid}/log")
def get_netting_log(rid: int,
                    item_id: Optional[str] = None,
                    workshop: Optional[str] = None,
                    date_from: Optional[str] = None,
                    date_to: Optional[str] = None,
                    db: Session = Depends(get_db)):
    ensure_tables(db)
    q = """
      SELECT item_id, workshop, date, kind,
             opening_exact, opening_generic,
             stock_used_exact, stock_used_generic,
             receipts_used, order_created, available_after
      FROM netting_log_row
      WHERE netting_run_id = :rid
    """
    params = {"rid": rid}
    if item_id:
        q += " AND item_id = :it"; params["it"] = item_id
    if workshop:
        q += " AND COALESCE(workshop,'') = :wk"; params["wk"] = workshop
    if date_from:
        q += " AND date(date) >= date(:df)"; params["df"] = date_from
    if date_to:
        q += " AND date(date) <= date(:dt)"; params["dt"] = date_to
    q += " ORDER BY date, item_id"
    rows = db.execute(text(q), params).mappings().all()
    return [{**dict(r), "date": str(r["date"]) if r.get("date") is not None else None} for r in rows]


@app.get("/netting/runs/{rid}/coverage")
def get_coverage(rid: int,
                 item_id: str,
                 due_date: str,
                 workshop: Optional[str] = None,
                 db: Session = Depends(get_db)):
    """Return coverage snapshot for given item/due_date from netting_log_row."""
    ensure_tables(db)
    base = """
      SELECT item_id, COALESCE(workshop,'') AS workshop, date, kind,
             opening_exact, opening_generic,
             stock_used_exact, stock_used_generic,
             receipts_used, order_created, available_after
      FROM netting_log_row
      WHERE netting_run_id = :rid AND item_id = :it AND date(date) = date(:dd)
    """
    params = {"rid": rid, "it": item_id, "dd": due_date}
    if workshop:
        base += " AND COALESCE(workshop,'') = :wk"; params["wk"] = workshop
    base += " ORDER BY date"
    rows = db.execute(text(base), params).mappings().all()
    if not rows:
        return {"opening": {"exact": 0, "generic": 0}, "used": {"stock_exact": 0, "stock_generic": 0, "receipts": 0}, "order_created": 0, "available_after": 0, "receipts_breakdown": []}
    # take the first matched row (daily snapshot)
    r = dict(rows[0])
    return {
        "opening": {"exact": int(r.get("opening_exact") or 0), "generic": int(r.get("opening_generic") or 0)},
        "used": {
            "stock_exact": int(r.get("stock_used_exact") or 0),
            "stock_generic": int(r.get("stock_used_generic") or 0),
            "receipts": int(r.get("receipts_used") or 0),
        },
        "order_created": int(r.get("order_created") or 0),
        "available_after": int(r.get("available_after") or 0),
        "receipts_breakdown": [],
    }


# ================== Reports (DB-based) ==================
@app.get("/reports/plans/{plan_id}/orders_timeline")
def report_orders_timeline(plan_id: int, workshops: Optional[List[str]] = Query(default=None)):
    try:
        from ..db.models import ScheduleOp, DimMachine
        with SessionLocal() as s:
            tokens, prefixes = _normalize_workshop_tokens_list(workshops)
            q = (
                s.query(ScheduleOp.order_id, ScheduleOp.item_id, ScheduleOp.article_name, ScheduleOp.start_ts, ScheduleOp.end_ts)
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
            rows = q.all()
        if not rows:
            return {"status": "ok", "count": 0, "orders": []}
        import pandas as pd
        df = pd.DataFrame([
            {"order_id": r.order_id, "item_id": r.item_id, "article_name": r.article_name, "start_ts": pd.to_datetime(r.start_ts), "end_ts": pd.to_datetime(r.end_ts)}
            for r in rows
        ])
        g = df.groupby("order_id", as_index=False).agg(
            start_date=("start_ts", "min"),
            finish_date=("end_ts", "max"),
            item_id=("item_id", "first"),
            article_name=("article_name", "first"),
        )
        g["duration_days"] = (g["finish_date"].dt.normalize() - g["start_date"].dt.normalize()).dt.days + 1
        # due_date from plan_order_info (if present)
        due_map = {}
        try:
            with SessionLocal() as s:
                due_rows = s.execute(text("SELECT order_id,due_date FROM plan_order_info WHERE plan_id=:pid"), {"pid": plan_id}).mappings().all()
                for r in due_rows:
                    if r["order_id"]:
                        due_map[str(r["order_id"])]= str(r["due_date"]) if r["due_date"] is not None else None
        except Exception:
            due_map = {}

        out = []
        for _, rr in g.iterrows():
            oid = str(rr["order_id"]) if rr["order_id"] is not None else ""
            due = due_map.get(oid)
            try:
                lag = 0
                if due:
                    fd = pd.to_datetime(rr["finish_date"]).normalize()
                    dd = pd.to_datetime(due).normalize()
                    lag = int((fd - dd).days)
            except Exception:
                lag = 0
            out.append({
                "base_order_id": oid.split(":", 1)[0] if ":" in oid else oid,
                "order_id": oid,
                "item_id": str(rr["item_id"]) if rr["item_id"] is not None else "",
                "article_name": str(rr["article_name"]) if rr["article_name"] is not None else None,
                "item_name": str(rr["article_name"]) if rr["article_name"] is not None else None,
                "start_date": str(pd.to_datetime(rr["start_date"]).date()),
                "finish_date": str(pd.to_datetime(rr["finish_date"]).date()),
                "duration_days": int(rr["duration_days"]),
                "due_date": due,
                "finish_lag": lag,
            })
        return {"status": "ok", "count": len(out), "orders": out}
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

        # BOM from file (LAST_PATHS), filtered by present items
        try:
            bom = pd.read_excel(LAST_PATHS["bom"], sheet_name=0, dtype=object)
        except Exception:
            return {"status": "ok", "edges": []}

        def nc(s):
            return str(s).strip().lower().replace(" ", "").replace("_", "")

        cols = {nc(c): c for c in bom.columns}
        art = cols.get("article") or cols.get("item") or cols.get("item_id")
        root = cols.get("rootarticle") or cols.get("root article")
        if not art or not root:
            return {"status": "ok", "edges": []}

        pairs = bom[[art, root]].dropna().astype(str)
        pairs = pairs[(pairs[art] != "") & (pairs[root] != "")]
        pairs = pairs[(pairs[art].isin(present)) & (pairs[root].isin(present))]
        edges = []
        for base, items_in_base in base_map.items():
            # for each parent-child pair that both present in this base
            for _, r in pairs.iterrows():
                parent = str(r[root]); child = str(r[art])
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
        return {"status": "ok", "ops": out}
    except Exception as e:
        tb = traceback.format_exc(limit=3)
        raise HTTPException(status_code=400, detail={"msg": str(e), "trace": tb})
