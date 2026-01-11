# -*- coding: utf-8 -*-
"""
Greedy-планировщик для so-planner (совместим с API, вашими Excel и старым вызовом с Session).
"""
from __future__ import annotations

import numpy as np
import argparse
import datetime as dt
from collections import defaultdict
from pathlib import Path

from typing import Any, Optional, Tuple

from typing import Literal, Iterable
from sqlalchemy import text
from sqlalchemy.orm import Session
import json


import pandas as pd
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import ColorScaleRule
NETTING_LOG = None

from .greedy.loaders import (
    load_plan_of_sales as _L_load_plan_of_sales,
    load_bom as _L_load_bom,
    load_bom_article_name_map as _L_load_bom_article_name_map,
    load_machines as _L_load_machines,
    load_stock_any as _L_load_stock_any,
)

# Override monolith implementations with modular versions
load_plan_of_sales = _L_load_plan_of_sales
load_bom = _L_load_bom
load_machines = _L_load_machines
load_stock_any = _L_load_stock_any


# =========================
# Utils
# =========================
def _norm_col(s: str) -> str:
    return str(s).strip().lower().replace(" ", "").replace("_", "")


def _as_date(x: Any):
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except Exception:
        return pd.NaT


def _ensure_positive_int(x: Any) -> int:
    try:
        v = float(x)
        if pd.isna(v) or v <= 0:
            return 0
        return int(round(v))
    except Exception:
        return 0


# =========================
# Loaders
# =========================
def _dead_load_plan_of_sales(path: Path) -> pd.DataFrame:
    return _L_load_plan_of_sales(path)
    """
    Ожидаем wide-таблицу:
      - колонка артикула: 'article' (или item_id/Материал и т.п.)
      - остальные колонки — даты; значения — qty
    """
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    # Находим колонку артикула
    id_candidates = ["article", "item_id", "item", "материал", "артикул"]
    item_col = None
    for c in id_candidates:
        if _norm_col(c) in norm:
            item_col = norm[_norm_col(c)]
            break
    if item_col is None:
        # fallback — первая колонка считаем артикулом
        item_col = df.columns[0]

    # Дата-колонки — те, что парсятся в дату
    date_cols = []
    for c in df.columns:
        if c == item_col:
            continue
        d = _as_date(c)
        if pd.notna(d):
            date_cols.append(c)

    if not date_cols:
        raise ValueError(
            f"В плане не найдены колонки-даты. Нашлись: {list(df.columns)}"
        )

    long_df = df.melt(
        id_vars=[item_col],
        value_vars=date_cols,
        var_name="due_date",
        value_name="qty",
    )
    long_df["due_date"] = pd.to_datetime(long_df["due_date"], errors="coerce").dt.date
    long_df.rename(columns={item_col: "item_id"}, inplace=True)
    long_df["item_id"] = long_df["item_id"].astype(str).str.strip()
    long_df["qty"] = long_df["qty"].apply(_ensure_positive_int)
    long_df = long_df[
        (long_df["qty"] > 0)
        & long_df["item_id"].ne("")
        & long_df["due_date"].notna()
    ].copy()
    long_df["priority"] = pd.to_datetime(long_df["due_date"])
    long_df.sort_values(["priority", "item_id"], inplace=True, kind="stable")
    long_df.reset_index(drop=True, inplace=True)
    return long_df


def _legacy_load_machines(path: Path) -> pd.DataFrame:
    # ... существующий код выше ...
    # календарь (опционально)
    # ...

    # --- НОВОЕ: overload_pct на уровне машины (доля; 0.25 = +25%)
    overload_cols = ["overload_pct", "overload pct", "overload%", "перегрузка", "перегрузка%"]
    df["overload_pct"] = 0.0
    for oc in overload_cols:
        if _norm_col(oc) in norm:
            s = pd.to_numeric(df[norm[_norm_col(oc)]], errors="coerce").fillna(0.0).astype(float)
            # если кто-то дал проценты в 0..100 — переведём в долю
            df["overload_pct"] = np.where(s > 1.0, s / 100.0, s)
            break

    keep = ["machine_id", "capacity_per_day", "overload_pct"]
    if "calendar_date" in df.columns:
        keep.append("calendar_date")
    if "capacity_override" in df.columns:
        keep.append("capacity_override")
    return df[keep]



def _dead_load_bom(path: Path) -> pd.DataFrame:
    return _L_load_bom(path)
    """
    Поддерживает:
    B) вашу схему:
       - article
       - operations (шаг)
       - machine id
       - machine time (часы/ед) ИЛИ human time (часы/ед)
       - setting time (часы на операцию, опционально)
       - root article (иерархия)
    A) классическую:
       - item_id / step / machine_id / time_per_unit (мин/ед) [+ setup*] [+ root article?]

    Возвращает: item_id, step, machine_id, time_per_unit (мин/ед), setup_minutes (мин/оп), root_item_id
    """
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    def has(x: str) -> bool:
        return _norm_col(x) in norm

    def col(x: str) -> str:
        return norm[_norm_col(x)]

    # --- Схема B (ваши файлы)
    if has("article") and (has("machineid") or has("machine id")):
        out = pd.DataFrame()
        out["item_id"] = df[col("article")].astype(str).str.strip()
        out["step"] = pd.to_numeric(df[col("operations")], errors="coerce").fillna(1).astype(int) if has("operations") else 1
        mid_col = col("machineid") if has("machineid") else col("machine id")
        out["machine_id"] = df[mid_col].astype(str).str.strip()
        # время на ед. (часы -> минуты)
        if has("machinetime"):
            h = pd.to_numeric(df[col("machinetime")], errors="coerce").fillna(0).astype(float)
        elif has("humantime"):
            h = pd.to_numeric(df[col("humantime")], errors="coerce").fillna(0).astype(float)
        else:
            raise ValueError("BOM: нет 'machine time' или 'human time' (часы/ед).")
        out["time_per_unit"] = h * 60.0
        # наладка (часы -> минуты)
        if has("settingtime") or has("setting time"):
            sc = col("settingtime") if has("settingtime") else col("setting time")
            out["setup_minutes"] = pd.to_numeric(df[sc], errors="coerce").fillna(0).astype(float) * 60.0
        else:
            out["setup_minutes"] = 0.0
        # root
        if has("rootarticle") or has("root article"):
            rac = col("rootarticle") if has("rootarticle") else col("root article")
            out["root_item_id"] = df[rac].astype(str).str.strip().replace({"nan": "", "None": ""})
        else:
            out["root_item_id"] = ""

        # optional workshop
        if has("workshop"):
            out["workshop"] = df[col("workshop")].astype(str).str.strip()
        else:
            out["workshop"] = ""
        # optional qty_per_parent
        if has("qty_per_parent") or has("qtyperparent"):
            qcol = col("qty_per_parent") if has("qty_per_parent") else col("qtyperparent")
            out["qty_per_parent"] = pd.to_numeric(df[qcol], errors="coerce").fillna(1.0).astype(float)
        else:
            out["qty_per_parent"] = 1.0

        out = out.sort_values(["item_id", "step"], kind="stable").reset_index(drop=True)
        ret_cols = ["item_id","step","machine_id","time_per_unit","setup_minutes","root_item_id"]
        if "workshop" in out.columns: ret_cols.append("workshop")
        if "qty_per_parent" in out.columns: ret_cols.append("qty_per_parent")
        return out[ret_cols]

    # --- Схема A (классика)
    rename_map = {}
    candidates = {
        "item_id": ["item_id", "item", "article", "материал", "артикул"],
        "step": ["step", "operations", "operationseq", "opseq", "seq", "sequence", "порядок"],
        "machine_id": ["machine_id", "machine", "resource", "станок", "машина", "machine id"],
        "time_per_unit": ["time_per_unit", "proc_time", "duration", "minutes_per_unit", "мин_на_ед", "минутнаед", "machine time"],
    }
    for k, opts in candidates.items():
        for o in opts:
            if _norm_col(o) in norm:
                rename_map[norm[_norm_col(o)]] = k
                break
    df = df.rename(columns=rename_map)

    need = {"item_id", "machine_id", "time_per_unit"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"BOM: отсутствуют {missing}. Нашлись: {list(df.columns)}")

    if "step" not in df.columns:
        df["step"] = 1

    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["step"] = pd.to_numeric(df["step"], errors="coerce").fillna(1).astype(int)
    df["machine_id"] = df["machine_id"].astype(str).str.strip()
    df["machine_id"] = df["machine_id"] \
        .str.replace(r"\.0$", "", regex=True) \
        .str.replace(r"\s+", " ", regex=True)
    
    df["time_per_unit"] = pd.to_numeric(df["time_per_unit"], errors="coerce").fillna(0).astype(float)
    if df["time_per_unit"].median() < 1.0:
        df["time_per_unit"] *= 60.0  # часы -> минуты

    df["machine_id"] = df["machine_id"].astype(str).str.strip()
    df["machine_id"] = df["machine_id"] \
        .str.replace(r"\.0$", "", regex=True) \
        .str.replace(r"\s+", " ", regex=True)
    

    # setup (опц.)
    setup_series = None
    for cand in ["setup_minutes", "setting_time", "setup", "наладка", "setting time"]:
        if _norm_col(cand) in norm:
            setup_series = pd.to_numeric(df[norm[_norm_col(cand)]], errors="coerce").fillna(0).astype(float)
            if setup_series.median() < 1.0:
                setup_series *= 60.0
            break
    df["setup_minutes"] = setup_series if setup_series is not None else 0.0

    # root
    if _norm_col("root article") in norm or _norm_col("rootarticle") in norm:
        key = norm[_norm_col("root article")] if _norm_col("root article") in norm else norm[_norm_col("rootarticle")]
        df["root_item_id"] = df[key].astype(str).str.strip().replace({"nan": "", "None": ""})
    else:
        df["root_item_id"] = ""

    # optional A-schema workshop/qty_per_parent
    if _norm_col("workshop") in norm:
        df["workshop"] = df[norm[_norm_col("workshop")]].astype(str).str.strip()
    else:
        df["workshop"] = ""
    if _norm_col("qty_per_parent") in norm or _norm_col("qtyperparent") in norm:
        key = norm[_norm_col("qty_per_parent")] if _norm_col("qty_per_parent") in norm else norm[_norm_col("qtyperparent")]
        df["qty_per_parent"] = pd.to_numeric(df[key], errors="coerce").fillna(1.0).astype(float)
    else:
        df["qty_per_parent"] = 1.0

    df = df.sort_values(["item_id", "step"], kind="stable").reset_index(drop=True)
    ret_cols = ["item_id","step","machine_id","time_per_unit","setup_minutes","root_item_id"]
    if "workshop" in df.columns: ret_cols.append("workshop")
    if "qty_per_parent" in df.columns: ret_cols.append("qty_per_parent")
    return df[ret_cols]


def _dead_load_machines(path: Path) -> pd.DataFrame:
    return _L_load_machines(path)
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    def has(x: str) -> bool: return _norm_col(x) in norm
    def col(x: str) -> str:  return norm[_norm_col(x)]

    # machine_id
    if has("machine_id"): df = df.rename(columns={col("machine_id"): "machine_id"})
    elif has("machineid"): df = df.rename(columns={col("machineid"): "machine_id"})
    elif has("machine id"): df = df.rename(columns={col("machine id"): "machine_id"})
    else: raise ValueError("machines: нет колонки machine id / machine_id.")

    df["machine_id"] = df["machine_id"].astype(str).str.strip()
    df["machine_id"] = df["machine_id"] \
        .str.replace(r"\.0$", "", regex=True) \
        .str.replace(r"\s+", " ", regex=True)    
    

    # capacity_per_day
    # если есть capacity_per_day (в часах/день) — переведём в минуты
    if "capacity_per_day" in df.columns:
        df["capacity_per_day"] = pd.to_numeric(df["capacity_per_day"], errors="coerce").fillna(0.0)
        df["capacity_per_day"] = df["capacity_per_day"] * 60.0
    elif "count" in df.columns and ("available time" in df.columns or "available_time" in df.columns):
        at_col = "available time" if "available time" in df.columns else "available_time"
        cnt = pd.to_numeric(df["count"], errors="coerce").fillna(0.0)
        hrs = pd.to_numeric(df[at_col], errors="coerce").fillna(0.0)
        df["capacity_per_day"] = (cnt * hrs * 60.0)
    else:
        raise ValueError("machines: не найдены поля для расчёта мощности")

    if "capacity_override" in df.columns:
        df["capacity_override"] = pd.to_numeric(df["capacity_override"], errors="coerce").fillna(pd.NA)

    # календарь (опц.)
    if has("calendar_date") or has("date"):
        c = col("calendar_date") if has("calendar_date") else col("date")
        df = df.rename(columns={c: "calendar_date"})
        df["calendar_date"] = pd.to_datetime(df["calendar_date"], errors="coerce").dt.date
    if has("capacity_override") or has("override"):
        c = col("capacity_override") if has("capacity_override") else col("override")
        df = df.rename(columns={c: "capacity_override"})
        df["capacity_override"] = pd.to_numeric(df["capacity_override"], errors="coerce").astype(float)

    # overload_pct (доля; можно 0..100 → переведём)
    df["overload_pct"] = 0.0
    for oc in ["overload_pct", "overload pct", "overload%", "перегрузка", "перегрузка%"]:
        if has(oc):
            s = pd.to_numeric(df[col(oc)], errors="coerce").fillna(0.0).astype(float)
            df["overload_pct"] = np.where(s > 1.0, s / 100.0, s)
            break

    keep = ["machine_id", "capacity_per_day", "overload_pct"]
    if "calendar_date" in df.columns: keep.append("calendar_date")
    if "capacity_override" in df.columns: keep.append("capacity_override")
    return df[keep]




def _dead_load_stock_any(path: Path) -> pd.DataFrame:
    return _L_load_stock_any(path)
    """
    Загружает Excel с остатками, гибко распознавая названия колонок.
    Поддерживаемые синонимы:
      - ключ артикула: 'item_id','item','article','материал','артикул'
      - количество: 'stock_qty','qty','quantity','остаток','свободныйостаток','free_stock','on_hand','available'
    Возвращает df с колонками: item_id, stock_qty (float), агрегировано по item_id.
    """
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    key_opts = ["item_id","item","article","материал","артикул"]
    qty_opts = ["stock_qty","qty","quantity","остаток","свободныйостаток","free_stock","on_hand","available"]

    key_col = None
    for k in key_opts:
        if _norm_col(k) in norm:
            key_col = norm[_norm_col(k)]
            break
    if key_col is None:
        key_col = df.columns[0]

    qty_col = None
    for q in qty_opts:
        if _norm_col(q) in norm:
            qty_col = norm[_norm_col(q)]
            break
    if qty_col is None:
        raise ValueError("stock: не найдена колонка количества (например, 'stock_qty'/'qty'/'остаток').")

    out = pd.DataFrame({
        "item_id": df[key_col].astype(str).str.strip(),
        "stock_qty": pd.to_numeric(df[qty_col], errors="coerce").fillna(0.0).astype(float),
    })
    out = out[out["item_id"].ne("")].copy()
    out = out.groupby("item_id", as_index=False)["stock_qty"].sum()
    return out

# --- PATCH START: Netting SQL helpers ---

def _ensure_netting_tables(db: Session):
    """Создаём недостающие таблицы (минимальная миграция)"""
    # plan/receipts/stock
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS plan_version (
        id INTEGER PRIMARY KEY,
        name TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        author TEXT,
        status TEXT,
        horizon_start DATE,
        horizon_end DATE,
        origin TEXT,
        notes TEXT
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS plan_line (
        id INTEGER PRIMARY KEY,
        plan_version_id INTEGER NOT NULL,
        item_id TEXT NOT NULL,
        due_date DATE NOT NULL,
        qty INTEGER NOT NULL,
        priority DATETIME NULL,
        customer TEXT NULL,
        workshop TEXT NULL,
        source_tag TEXT NULL
    );
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_plan_line_main
    ON plan_line(plan_version_id,item_id,due_date);
    """))

    db.execute(text("""
    CREATE TABLE IF NOT EXISTS receipts_plan (
        id INTEGER PRIMARY KEY,
        plan_version_id INTEGER NOT NULL,
        item_id TEXT NOT NULL,
        due_date DATE NOT NULL,
        qty INTEGER NOT NULL,
        workshop TEXT NULL,
        receipt_type TEXT,
        source_ref TEXT NULL
    );
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_receipts_plan_main
    ON receipts_plan(plan_version_id,item_id,due_date);
    """))

    db.execute(text("""
    CREATE TABLE IF NOT EXISTS stock_snapshot (
        id INTEGER PRIMARY KEY,
        name TEXT,
        taken_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        notes TEXT
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS stock_line (
        id INTEGER PRIMARY KEY,
        snapshot_id INTEGER NOT NULL,
        item_id TEXT NOT NULL,
        workshop TEXT DEFAULT '',
        stock_qty INTEGER NOT NULL
    );
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_stock_line_main
    ON stock_line(snapshot_id,item_id,workshop);
    """))

    # netting results
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS netting_run (
        id INTEGER PRIMARY KEY,
        started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        finished_at DATETIME,
        user TEXT,
        mode TEXT,
        plan_version_id INTEGER,
        stock_snapshot_id INTEGER,
        bom_version_id TEXT,
        receipts_source_desc TEXT,
        params TEXT,
        status TEXT
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS netting_order (
        id INTEGER PRIMARY KEY,
        netting_run_id INTEGER NOT NULL,
        order_id TEXT,
        item_id TEXT,
        due_date DATE,
        qty INTEGER,
        priority DATETIME NULL,
        workshop TEXT NULL
    );
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_netting_order
    ON netting_order(netting_run_id,item_id,due_date);
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS netting_log_row (
        id INTEGER PRIMARY KEY,
        netting_run_id INTEGER NOT NULL,
        item_id TEXT,
        workshop TEXT,
        date DATE NULL,
        kind TEXT,
        opening_exact INTEGER NULL,
        opening_generic INTEGER NULL,
        stock_used_exact INTEGER,
        stock_used_generic INTEGER,
        receipts_used INTEGER,
        order_created INTEGER,
        available_after INTEGER
    );
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_netting_log_row
    ON netting_log_row(netting_run_id,item_id,workshop,date);
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS netting_summary_row (
        id INTEGER PRIMARY KEY,
        netting_run_id INTEGER NOT NULL,
        item_id TEXT,
        workshop TEXT,
        stock_used_total INTEGER,
        receipts_used_total INTEGER,
        orders_created_total INTEGER,
        opening_exact_init INTEGER NULL,
        opening_generic_init INTEGER NULL
    );
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_netting_summary_row
    ON netting_summary_row(netting_run_id,item_id,workshop);
    """))
    # demand linkage table
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS demand_linkage (
        id INTEGER PRIMARY KEY,
        netting_run_id INTEGER NOT NULL,
        parent_item_id TEXT,
        parent_due_date DATE,
        child_item_id TEXT,
        child_due_date DATE,
        qty_per_parent REAL,
        required_qty INTEGER
    );
    """))
    # order info (due_date) per plan for reports
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS plan_order_info (
        plan_id INTEGER,
        order_id TEXT,
        due_date DATE,
        PRIMARY KEY (plan_id, order_id)
    );
    """))
    db.commit()


def _load_receipts_from_db(
    db: Session,
    plan_version_id: int | None,
    receipts_from: Literal["plan", "firmed", "both"] = "plan",
) -> pd.DataFrame:
    """
    Строим existing_orders_df для неттинга из БД.
    Пока берём только receipts_plan (вариант 'plan' и часть 'both').
    Хук под 'firmed' оставлен на будущее (из таблиц расписания).
    """
    parts: list[pd.DataFrame] = []

    if receipts_from in ("plan", "both"):
        q = text("""
            SELECT item_id, due_date, qty, COALESCE(workshop,'') AS workshop
            FROM receipts_plan
            WHERE plan_version_id = :p
        """)
        rows = db.execute(q, {"p": plan_version_id}).mappings().all()
        if rows:
            parts.append(pd.DataFrame(rows))

    # TODO: if receipts_from in ("firmed","both"): подтянуть firmed-расписания

    if not parts:
        return pd.DataFrame(columns=["item_id","due_date","qty","workshop"])

    df = pd.concat(parts, ignore_index=True)
    df["item_id"]  = df["item_id"].astype(str).str.strip()
    df["workshop"] = df["workshop"].astype(str).fillna("")
    df["due_date"] = pd.to_datetime(df["due_date"]).dt.date
    df["qty"]      = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    return df


def _load_stock_snapshot(db: Session, stock_snapshot_id: int) -> pd.DataFrame:
    q = text("""
        SELECT item_id, COALESCE(workshop,'') AS workshop, stock_qty
        FROM stock_line
        WHERE snapshot_id = :s
    """)
    rows = db.execute(q, {"s": stock_snapshot_id}).mappings().all()
    if not rows:
        return pd.DataFrame(columns=["item_id","workshop","stock_qty"])
    df = pd.DataFrame(rows)
    df["item_id"]   = df["item_id"].astype(str).str.strip()
    df["workshop"]  = df["workshop"].astype(str).fillna("")
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)
    # схлопнем дубли
    return df.groupby(["item_id","workshop"], as_index=False)["stock_qty"].sum()


def _save_netting_results_to_db(
    db: Session,
    run_meta: dict,
    demand_net: pd.DataFrame,
    netting_log: pd.DataFrame,
    netting_summary: pd.DataFrame,
    linkage_df: pd.DataFrame | None = None,
) -> int:
    """
    Сохраняем все артефакты неттинга. Возвращаем netting_run.id
    """
    # run header
    ins = text("""
      INSERT INTO netting_run (started_at, finished_at, user, mode, plan_version_id,
                               stock_snapshot_id, bom_version_id, receipts_source_desc, params, status)
      VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :user, :mode, :plan_version_id,
              :stock_snapshot_id, :bom_version_id, :receipts_source_desc, :params, 'done')
      RETURNING id
    """)
    rid = db.execute(
        ins,
        {
            "user": run_meta.get("user","ui"),
            "mode": "product_view",
            "plan_version_id": run_meta.get("plan_version_id"),
            "stock_snapshot_id": run_meta.get("stock_snapshot_id"),
            "bom_version_id": run_meta.get("bom_version_id",""),
            "receipts_source_desc": run_meta.get("receipts_source_desc","plan"),
            "params": json.dumps(run_meta.get("params",{}), ensure_ascii=False),
        },
    ).scalar_one()

    # orders
    if not demand_net.empty:
        payload = demand_net.copy()
        for col in ("order_id","item_id","workshop"):
            if col not in payload.columns:
                payload[col] = ""
        # Normalize to string to avoid sqlite binding issues with pandas Timestamps
        payload["priority"] = pd.to_datetime(payload["priority"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        rows = payload[["order_id","item_id","due_date","qty","priority","workshop"]].to_dict("records")
        db.execute(text("""
            INSERT INTO netting_order (netting_run_id, order_id, item_id, due_date, qty, priority, workshop)
            VALUES (:rid, :order_id, :item_id, :due_date, :qty, :priority, :workshop)
        """), [dict(r, rid=rid) for r in rows])

    # log
    if not netting_log.empty:
        log = netting_log.copy()
        try:
            import pandas as _pd
            if "date" in log.columns:
                log["date"] = _pd.to_datetime(log["date"], errors="coerce").dt.date
                log["date"] = log["date"].where(log["date"].notna(), None)
            for c in [
                "opening_exact","opening_generic",
                "stock_used_exact","stock_used_generic",
                "receipts_used","order_created","available_after",
            ]:
                if c in log.columns:
                    log[c] = _pd.to_numeric(log[c], errors="coerce")
                    log[c] = log[c].where(log[c].notna(), None)
        except Exception:
            pass
        rows = log.to_dict("records")
        db.execute(text("""
            INSERT INTO netting_log_row
            (netting_run_id, item_id, workshop, date, kind,
             opening_exact, opening_generic,
             stock_used_exact, stock_used_generic,
             receipts_used, order_created, available_after)
            VALUES
            (:rid, :item_id, :workshop, :date, :kind,
             :opening_exact, :opening_generic,
             :stock_used_exact, :stock_used_generic,
             :receipts_used, :order_created, :available_after)
        """), [dict(r, rid=rid) for r in rows])

    # summary
    if not netting_summary.empty:
        rows = netting_summary.to_dict("records")
        db.execute(text("""
            INSERT INTO netting_summary_row
            (netting_run_id, item_id, workshop,
             stock_used_total, receipts_used_total, orders_created_total,
             opening_exact_init, opening_generic_init)
            VALUES
            (:rid, :item_id, :workshop,
             :stock_used_total, :receipts_used_total, :orders_created_total,
             :opening_exact_init, :opening_generic_init)
        """), [dict(r, rid=rid) for r in rows])

    # linkage (опционально)
    if linkage_df is not None and not linkage_df.empty:
        rows = linkage_df.to_dict("records")
        db.execute(text("""
            INSERT INTO demand_linkage
            (netting_run_id, parent_item_id, parent_due_date,
             child_item_id, child_due_date, qty_per_parent, required_qty)
            VALUES
            (:rid, :parent_item_id, :parent_due_date,
             :child_item_id, :child_due_date, :qty_per_parent, :required_qty)
        """), [dict(r, rid=rid) for r in rows])

    db.commit()
    return int(rid)

# --- PATCH END ---



def compute_orders_timeline(sched: pd.DataFrame) -> pd.DataFrame:
    """
    Таймлайн по каждому order_id:
      start_date, finish_date, duration_days, due_date, finish_lag.
    Ожидаемые колонки в sched: order_id, item_id, date, due_date.
    """
    if sched.empty:
        return pd.DataFrame(columns=[
            "order_id","item_id","start_date","finish_date","duration_days","due_date","finish_lag"
        ])

    # убеждаемся в корректных типах дат
    df = sched.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["due_date"] = pd.to_datetime(df["due_date"])

    g = df.groupby("order_id", as_index=False).agg(
        start_date=("date", "min"),
        finish_date=("date", "max"),
        due_date=("due_date", "max"),
        item_id=("item_id", "first"),
    )
    g["duration_days"] = (g["finish_date"] - g["start_date"]).dt.days + 1
    g["finish_lag"] = (g["finish_date"] - g["due_date"]).dt.days
    # приводим к date
    g["start_date"] = g["start_date"].dt.date
    g["finish_date"] = g["finish_date"].dt.date
    g["due_date"] = g["due_date"].dt.date

    return g.sort_values(
        ["start_date","finish_date","order_id"], kind="stable"
    ).reset_index(drop=True)


def compute_order_items_timeline(sched: pd.DataFrame) -> pd.DataFrame:
    """
    Таймлайн по каждой паре (order_id, item_id):
      start_date, finish_date, duration_days, due_date, finish_lag.
    Полезно, если включён split_child_orders и order_id = "<base>:<item>".
    """
    if sched.empty:
        return pd.DataFrame(columns=[
            "order_id","item_id","start_date","finish_date","duration_days","due_date","finish_lag"
        ])

    df = sched.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["due_date"] = pd.to_datetime(df["due_date"])

    g = df.groupby(["order_id","item_id"], as_index=False).agg(
        start_date=("date", "min"),
        finish_date=("date", "max"),
        due_date=("due_date", "max"),
    )
    g["duration_days"] = (g["finish_date"] - g["start_date"]).dt.days + 1
    g["finish_lag"] = (g["finish_date"] - g["due_date"]).dt.days
    g["start_date"] = g["start_date"].dt.date
    g["finish_date"] = g["finish_date"].dt.date
    g["due_date"] = g["due_date"].dt.date

    return g.sort_values(
        ["order_id","item_id","start_date"], kind="stable"
    ).reset_index(drop=True)



# =========================
# Demand / order_id
# =========================
def build_demand(plan_df: pd.DataFrame) -> pd.DataFrame:
    # ожидание: длинный формат с колонками item_id, due_date, qty
    # если пришёл «широкий», прогоните через load_plan_of_sales ДО вызова build_demand (см. run_pipeline ниже)
    g = plan_df.groupby(["item_id", "due_date"], as_index=False).agg(qty=("qty", "sum"))
    g = g.sort_values(["due_date", "item_id"]).reset_index(drop=True)

    # стабильный order_id: (item_id, due_date, seq)
    from collections import defaultdict
    seq = defaultdict(int)
    order_ids = []
    for _, r in g.iterrows():
        key = (r["item_id"], r["due_date"])
        seq[key] += 1
        oid = f'{r["item_id"]}-{pd.to_datetime(r["due_date"]).strftime("%Y%m%d")}-{seq[key]:04d}'
        order_ids.append(oid)
    g["order_id"] = order_ids
    g["priority"] = pd.to_datetime(g["due_date"])  # важно: реальная дата, а не 0
    return g[["order_id", "item_id", "due_date", "qty", "priority"]]



# =========================
# root->child
# =========================


def build_bom_hierarchy(bom: pd.DataFrame) -> pd.DataFrame:
    """
    Возвращает таблицу ссылок root->child и уровень (0=корень верхнего уровня).
    Учитывает только пары, где root_item_id непустой.
    """
    links = bom[["item_id","root_item_id"]].drop_duplicates()
    links = links[links["root_item_id"].fillna("").astype(str).str.len() > 0].copy()
    if links.empty:
        links["level"] = 0
        return links

    # оценим уровни рекурсивно (простая топология по цепочкам)
    parents = {r.item_id: r.root_item_id for r in links.itertuples(index=False)}
    level = {}
    def lvl(x, seen=None):
        seen = seen or set()
        if x in level: return level[x]
        p = parents.get(x, "")
        if not p or p == x or p in seen:
            level[x] = 0
            return 0
        seen.add(x)
        level[x] = 1 + lvl(p, seen)
        return level[x]

    items = set(links["item_id"]).union(set(links["root_item_id"]))
    for it in items:
        lvl(it)

    # уровень child = lvl(child), корень выше по числу
    links["level"] = links["item_id"].map(level).fillna(0).astype(int)
    return links.sort_values(["level","root_item_id","item_id"]).reset_index(drop=True)

# === Product View (SAP APO-style netting) =====================================
# Вверху файла рядом с импортами
NETTING_LOG = None

def product_view_generate_demand(
    plan_df: pd.DataFrame,
    bom: pd.DataFrame,
    stock_df: pd.DataFrame | None = None,
    existing_orders_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    """
    Product-View netting (time-phased) по ВСЕМ article:
      1) разворачиваем спрос по BOM (учитывая qty_per_parent),
      2) для каждой (item_id, workshop) идём по датам, покрывая: склад exact -> склад generic -> поступления <= t,
      3) остаток порождает новый order на дату t.

    Пишем детальный NETTING_LOG: opening*, stock_used*, receipts_used, order_created, available_after.
    """
    global NETTING_LOG

    # --- helpers ---
    def C(df, name):
        m = {str(c).lower(): c for c in df.columns}
        return m.get(name.lower(), name)

    # 0) Нормализуем и построим начальный demand
    need = ["item_id","due_date","qty"]
    miss = [n for n in need if C(plan_df, n) not in plan_df.columns]
    if miss:
        raise ValueError("plan_df must contain item_id, due_date, qty")

    base_plan = (plan_df
                 .rename(columns={C(plan_df,"item_id"):"item_id",
                                  C(plan_df,"due_date"):"due_date",
                                  C(plan_df,"qty"):"qty"})
                 .copy())
    base_plan["item_id"]  = base_plan["item_id"].astype(str).str.strip()
    base_plan["due_date"] = pd.to_datetime(base_plan["due_date"]).dt.date
    base_plan["qty"]      = pd.to_numeric(base_plan["qty"], errors="coerce").fillna(0).astype(int)


    
    # Превратим в формат demand с order_id/priority
    demand0 = build_demand(base_plan)  # order_id, item_id, due_date, qty, priority
    # 0.1) Защитимся от фиктивного "0" в плане
    base_plan["item_id"] = base_plan["item_id"].astype(str).str.strip()
    bad_zero = base_plan[base_plan["item_id"] == "0"]["qty"].sum()
    if bad_zero:
        logging.warning("Plan contains demand for item_id=0 (qty=%s). It will be ignored.", bad_zero)
        base_plan = base_plan[base_plan["item_id"] != "0"]
    
    # 1) Чистим BOM от фиктивных корней и нулевых позиций + схлопываем дубли
    b = bom.copy()
    b["root_item_id"] = b["root_item_id"].astype(str).str.strip()
    b["item_id"]      = b["item_id"].astype(str).str.strip()
    b = b[b["root_item_id"] != b["item_id"]]
    b = b[(b["root_item_id"] != "0") & (b["item_id"] != "0")]
    
    if "qty_per_parent" not in b.columns:
        b["qty_per_parent"] = 1
    b["qty_per_parent"] = pd.to_numeric(b["qty_per_parent"], errors="coerce").fillna(0)
    
    b = (b.groupby(["root_item_id","item_id"], as_index=False)["qty_per_parent"]
           .sum())
    
    # 1) Разворачиваем спрос по ЧИСТОМУ BOM
    exp = expand_demand_with_hierarchy(build_demand(base_plan), b, split_child_orders=True)

    # item -> workshop (если есть)
    item_workshop = {}
    if "workshop" in bom.columns:
        bw = bom[["item_id","workshop"]].dropna().drop_duplicates("item_id")
        item_workshop = dict(zip(bw["item_id"].astype(str), bw["workshop"].astype(str)))
    exp["workshop"] = exp["item_id"].map(item_workshop).fillna("")

    # Сводим потребности по (item, workshop, due_date)
    dem = (exp.groupby(["item_id","workshop","due_date"], as_index=False)["qty"]
              .sum().sort_values(["item_id","workshop","due_date"]))

    # 2) Подготовим склад (exact и generic)
    if stock_df is None:
        stock_df = pd.DataFrame(columns=["item_id","stock_qty","workshop"])
    s = stock_df.copy()
    if "workshop" not in s.columns:
        s["workshop"] = ""
    s["item_id"] = s["item_id"].astype(str).str.strip()
    s["workshop"] = s["workshop"].astype(str).fillna("")
    s["stock_qty"] = pd.to_numeric(s["stock_qty"], errors="coerce").fillna(0).astype(int)

    stock_exact = (s.groupby(["item_id","workshop"], as_index=False)["stock_qty"]
                     .sum().set_index(["item_id","workshop"])["stock_qty"].to_dict())
    stock_generic = (s[s["workshop"] == ""]
                       .groupby(["item_id","workshop"], as_index=False)["stock_qty"]
                       .sum().set_index(["item_id","workshop"])["stock_qty"].to_dict())

    # 3) Поступления (существующие заказы) накапливаем по времени
    if existing_orders_df is None:
        existing_orders_df = pd.DataFrame(columns=["item_id","due_date","qty","workshop"])
    rec = existing_orders_df.copy()
    if "workshop" not in rec.columns:
        rec["workshop"] = ""
    rec["item_id"]  = rec["item_id"].astype(str).str.strip()
    rec["workshop"] = rec["workshop"].astype(str).fillna("")
    rec["due_date"] = pd.to_datetime(rec["due_date"]).dt.date
    rec["qty"]      = pd.to_numeric(rec["qty"], errors="coerce").fillna(0).astype(int)
    receipts = (rec.groupby(["item_id","workshop","due_date"], as_index=False)["qty"]
                  .sum().sort_values(["item_id","workshop","due_date"]))

    out_orders, log_rows, seq = [], [], {}

    # 4) Идём по каждой паре (item, workshop) и по датам
    for (it, wk), block in dem.groupby(["item_id","workshop"]):
        block = block.sort_values("due_date")

        # стартовые остатки для пары
        remain_exact   = float(stock_exact.get((it, wk), 0.0))
        remain_generic = float(stock_generic.get((it, ""), 0.0))
        receipts_remain = 0.0

        # лог открытия
        log_rows.append({
            "item_id": it, "workshop": wk, "date": None, "kind": "opening",
            "opening_exact": int(remain_exact), "opening_generic": int(remain_generic),
            "stock_used_exact": 0, "stock_used_generic": 0,
            "receipts_used": 0, "order_created": 0, "available_after": int(remain_exact + remain_generic)
        })

        # блок поступлений по паре
        rb = receipts[(receipts["item_id"] == it) & (receipts["workshop"] == wk)].sort_values("due_date")
        r_dates = rb["due_date"].tolist()
        r_qtys  = rb["qty"].tolist()
        ridx = 0

        for r in block.itertuples(index=False):
            dd, need = r.due_date, int(r.qty)

            # начисляем все receipts со сроком <= dd
            while ridx < len(r_dates) and r_dates[ridx] <= dd:
                receipts_remain += float(r_qtys[ridx])
                ridx += 1

            # склад exact -> generic
            stock_used_exact = min(need, int(remain_exact))
            need -= stock_used_exact
            remain_exact -= stock_used_exact

            stock_used_generic = 0
            if need > 0:
                stock_used_generic = min(need, int(remain_generic))
                need -= stock_used_generic
                remain_generic -= stock_used_generic

            # поступления
            receipts_used = 0
            if need > 0 and receipts_remain > 0:
                take = min(need, int(receipts_remain))
                receipts_used = take
                receipts_remain -= take
                need -= take

            # остаток → новый order
            order_created = 0
            if need > 0:
                order_created = need
                key = (it, pd.to_datetime(dd).date())
                seq[key] = seq.get(key, 0) + 1
                oid = f"{it}-{pd.to_datetime(dd).strftime('%Y%m%d')}-PV{seq[key]:03d}"
                out_orders.append({
                    "order_id": oid, "item_id": it, "due_date": dd,
                    "qty": order_created, "priority": pd.to_datetime(dd), "workshop": wk,
                })
                need = 0

            available_after = int(remain_exact + remain_generic + receipts_remain)

            log_rows.append({
                "item_id": it, "workshop": wk, "date": dd, "kind": "day",
                "opening_exact": None, "opening_generic": None,
                "stock_used_exact": int(stock_used_exact),
                "stock_used_generic": int(stock_used_generic),
                "receipts_used": int(receipts_used),
                "order_created": int(order_created),
                "available_after": int(available_after),
            })

    NETTING_LOG = pd.DataFrame(log_rows, columns=[
        "item_id","workshop","date","kind",
        "opening_exact","opening_generic",
        "stock_used_exact","stock_used_generic",
        "receipts_used","order_created","available_after"
    ])

    return pd.DataFrame(out_orders, columns=["order_id","item_id","due_date","qty","priority","workshop"])



# === Unified pipeline with mode='product_view' =================================
def run_pipeline(
    plan_path,
    bom_path,
    machines_path,
    out_xlsx,
    stock_path=None,
    start_date=None,
    overload_pct: float = 0.0,
    split_child_orders: bool = True,
    align_roots_to_due: bool = True,
    mode: str = "",
):
    """
    Backward compatible pipeline.
    mode == 'product_view' -> do Product-View netting then call greedy_schedule.
    Otherwise -> delegate to original pipeline.
    """
    mode = (mode or "").lower().strip()
    # Optional kwargs for product_view mode
    receipts_from = str((kwargs or {}).get("receipts_from", "plan"))
    if mode != "product_view":
        return _orig_run_pipeline(
            plan_path, bom_path, machines_path, out_xlsx,
            stock_path=stock_path, start_date=start_date,
            overload_pct=overload_pct,
            split_child_orders=split_child_orders,
            align_roots_to_due=align_roots_to_due,
        )

    # --- Product View path ---
    plan = load_plan_of_sales(Path(plan_path))
    bom = load_bom(Path(bom_path))
    machines = load_machines(Path(machines_path))

    stock_df = None
    if stock_path:
        stock_df = load_stock_any(Path(stock_path))
        if stock_df is not None and "workshop" not in stock_df.columns:
            stock_df["workshop"] = ""

    # existing receipts from current out_xlsx (if present)
    existing_orders = None
    p = Path(out_xlsx)
    if p.exists():
        try:
            tmp = pd.read_excel(p, sheet_name="schedule")
            keep = [c for c in ["item_id","due_date","qty","workshop"] if c in tmp.columns]
            existing_orders = tmp[keep].copy() if keep else None
        except Exception:
            existing_orders = None

    # netting -> delta-orders
    demand_net = product_view_generate_demand(plan, bom, stock_df=stock_df, existing_orders_df=existing_orders)

    # convert to demand DF compatible with the existing pipeline
    demand = build_demand(demand_net)

    # optional: expand hierarchy here if твой build_demand не делает это сам
    demand = expand_demand_with_hierarchy(demand, bom, split_child_orders=split_child_orders, include_parents=False)

    # JIT alignment preserved
    start = None
    if start_date:
        try:
            start = pd.to_datetime(start_date).date()
        except Exception:
            start = None

    # Do NOT re-consume stock here (already netted)
    sched = greedy_schedule(
        demand, bom, machines,
        start_date=start,
        overload_pct=overload_pct,
        split_child_orders=split_child_orders,
        align_roots_to_due=align_roots_to_due,
        stock_map=None,
    )

    export_with_charts._machines_df = machines
    out_file = export_with_charts(sched, Path(out_xlsx), bom=bom)
    return out_file, sched


def run_greedy(*args, **kwargs):
    """
    Backward-compatible wrapper:
    - if mode='product_view' -> call our run_pipeline (above)
    - else -> delegate to original run_greedy()
    """
    mode = (kwargs.get("mode") or "").lower().strip()
    if mode != "product_view":
        return _orig_run_greedy(*args, **kwargs)

    # Build params in a backward compatible way
    plan_path     = kwargs.get("plan_path")
    bom_path      = kwargs.get("bom_path")
    machines_path = kwargs.get("machines_path")
    out_xlsx      = kwargs.get("out_xlsx")
    stock_path    = kwargs.get("stock_path")
    start_date    = kwargs.get("start_date")
    overload_pct  = float(kwargs.get("overload_pct", 0.0))
    split_child_orders = bool(kwargs.get("split_child_orders", True))
    align_roots_to_due = bool(kwargs.get("align_roots_to_due", True))

    return run_pipeline(
        plan_path=plan_path,
        bom_path=bom_path,
        machines_path=machines_path,
        out_xlsx=out_xlsx,
        stock_path=stock_path,
        start_date=start_date,
        overload_pct=overload_pct,
        split_child_orders=split_child_orders,
        align_roots_to_due=align_roots_to_due,
        mode="product_view",
    )


    def C(df, name):  # безопасный доступ к столбцам в любом регистре
        m = {c.lower(): c for c in df.columns}
        return m.get(name.lower(), name)

    # --- item -> workshop
    item_workshop = {}
    if "workshop" in bom.columns:
        bw = (bom[["item_id", "workshop"]]
              .dropna()
              .drop_duplicates("item_id"))
        item_workshop = dict(zip(bw["item_id"].astype(str), bw["workshop"].astype(str)))

    # --- агрегированный спрос
    demand = (plan_df
              .rename(columns={C(plan_df,"item_id"):"item_id",
                               C(plan_df,"due_date"):"due_date",
                               C(plan_df,"qty"):"qty"})
              .groupby(["item_id","due_date"], as_index=False)
              .agg(qty=("qty","sum")))
    demand["item_id"] = demand["item_id"].astype(str)
    demand["workshop"] = demand["item_id"].map(item_workshop).fillna("")

    # --- склад {(item, workshop)->qty}
    if stock_df is None:
        stock_df = pd.DataFrame(columns=["item_id","stock_qty","workshop"])
    stock_df = stock_df.copy()
    if "workshop" not in stock_df.columns:
        stock_df["workshop"] = ""
    stock_df["item_id"] = stock_df["item_id"].astype(str)
    stock_df["workshop"] = stock_df["workshop"].astype(str)
    stock_map = (stock_df.groupby(["item_id","workshop"], as_index=False)["stock_qty"]
                       .sum().set_index(["item_id","workshop"])["stock_qty"].to_dict())

    # --- уже созданные заказы как поступления {(item, due_date, workshop)->qty}
    if existing_orders_df is None:
        existing_orders_df = pd.DataFrame(columns=["item_id","due_date","qty","workshop"])
    rec = existing_orders_df.copy()
    if "workshop" not in rec.columns:
        rec["workshop"] = ""
    rec["item_id"] = rec["item_id"].astype(str)
    rec["workshop"] = rec["workshop"].astype(str)
    receipt_map = (rec.groupby(["item_id","due_date","workshop"], as_index=False)["qty"]
                     .sum().set_index(["item_id","due_date","workshop"])["qty"].to_dict())

    # --- чистая потребность → формируем новые заказы на дельту
    rows, seq = [], {}
    for r in demand.itertuples(index=False):
        it, dd, qty, wk = str(r.item_id), r.due_date, int(r.qty), str(r.workshop)

        # 1) склад (по цеху; если нужен фолбэк на общий, добавь ключ (it,""))
        key_sw = (it, wk)
        avail = float(stock_map.get(key_sw, 0.0))
        if avail >= qty:
            stock_map[key_sw] = avail - qty
            continue
        elif avail > 0:
            qty -= int(avail)
            stock_map[key_sw] = 0.0

        # 2) уже созданные заказы на эту дату (поступления)
        exist = float(receipt_map.get((it, dd, wk), 0.0))
        if exist >= qty:
            continue
        elif exist > 0:
            qty -= int(exist)

        if qty <= 0:
            continue

        # 3) создаём отдельный заказ на дельту
        seq_key = (it, pd.to_datetime(dd).date())
        seq[seq_key] = seq.get(seq_key, 0) + 1
        oid = f"{it}-{pd.to_datetime(dd).strftime('%Y%m%d')}-PV{seq[seq_key]:03d}"
        rows.append({
            "order_id": oid,
            "item_id": it,
            "due_date": dd,
            "qty": int(qty),
            "priority": pd.to_datetime(dd),
            "workshop": wk,
        })

    return pd.DataFrame(rows, columns=["order_id","item_id","due_date","qty","priority","workshop"])
# ============================================================================


def expand_demand_with_hierarchy(demand: pd.DataFrame, bom: pd.DataFrame, *, split_child_orders: bool = False, include_parents: bool = False) -> pd.DataFrame:
    # Build parent and children maps from BOM
    parents: dict[str, str] = {}
    children_map: dict[str, dict[str, float]] = {}
    for r in bom.itertuples(index=False):
        p = r.root_item_id
        c = r.item_id
        parents[c] = p
        if p and p != c:
            children_map.setdefault(p, {})[c] = float(getattr(r, "qty_per_parent", 1.0)) or 1.0

    # Вспомогательные обходы
    def ancestors(x: str) -> list[str]:
        out = []
        seen = set()
        cur = x
        for _ in range(100000):
            p = parents.get(cur, "")
            if not p or p in seen or p == cur:
                break
            out.append(p)
            seen.add(p)
            cur = p
        return out

    def descendants_with_factor(x: str) -> list[tuple[str, float]]:
        out = []
        stack = [(x, 1.0)]
        seen = set([x])
        while stack:
            cur, f = stack.pop()
            for ch, r in (children_map.get(cur, {}) or {}).items():
                if ch in seen:
                    continue
                f_new = f * (r if np.isfinite(r) and r > 0 else 1.0)
                out.append((ch, f_new))
                seen.add(ch)
                stack.append((ch, f_new))
        return out

    rows = []
    for r in demand.itertuples(index=False):
        base_oid = str(r.order_id)
        it = str(r.item_id)
        due = r.due_date
        qty = int(r.qty)
        pr = r.priority

        # сам спрос (FG)
        rows.append(dict(
            base_order_id=base_oid,
            order_id=(f"{base_oid}:{it}" if split_child_orders else base_oid),
            item_id=it, due_date=due, qty=qty, priority=pr, role="FG"
        ))
        # предки (без масштабирования)
        for a in ancestors(it):
            rows.append(dict(
                base_order_id=base_oid,
                order_id=(f"{base_oid}:{a}" if split_child_orders else base_oid),
                item_id=a, due_date=due, qty=qty, priority=pr, role="PARENT"
            ))
        # потомки (масштабируем вниз)
        for d, fmul in descendants_with_factor(it):
            rows.append(dict(
                base_order_id=base_oid,
                order_id=(f"{base_oid}:{d}" if split_child_orders else base_oid),
                item_id=d, due_date=due, qty=int(round(qty * float(fmul))), priority=pr, role="CHILD"
            ))

    exp = pd.DataFrame(rows)
    if exp.empty:
        raise ValueError("Greedy: expanded_demand is empty — проверьте BOM/план.")
    # depth для сортировки: строим уровни из BOM
    links = build_bom_hierarchy(bom)
    depth_map = {r.item_id: int(r.level) for r in links.itertuples(index=False)} if not links.empty else {}
    exp["depth"] = exp["item_id"].map(depth_map).fillna(0).astype(int)
    # Стабильная сортировка: сначала приоритет, затем depth
    exp = exp.sort_values(["priority","depth","item_id"], kind="stable").reset_index(drop=True)
    return exp

def greedy_schedule(
    demand: pd.DataFrame,
    bom: pd.DataFrame,
    machines: pd.DataFrame,
    start_date: dt.date | None = None,
    overload_pct: float = 0.0,
    split_child_orders: bool = True,
    align_roots_to_due: bool = True,
    guard_limit_days: int = 200 * 365,
    stock_map: dict | None = None,
    include_parents: bool = False,
    expand: bool = True,
) -> pd.DataFrame:
    """
    Планировщик с маршрутами (step), наладкой, иерархией BOM и перегрузкой.

    Режимы:
      - ASAP (align_roots_to_due=False): планируем вперёд от start_date.
      - JIT  (align_roots_to_due=True): корневой item (role='FG') заканчивает ровно в due_date,
        его потомки планируются НАЗАД так, чтобы закончить не позже старта родителя, и не стартовать ранее 'today'.

    В расписание добавляется 'base_order_id' (если split_child_orders=True).
    """
    
    if start_date is None:
        start_date = dt.date.today()
    warnings: list[str] = []   # ← будем собирать предупреждения для UI/логов

    # === 0) Подготовка входных данных и иерархии ===
    if expand:
        demand = expand_demand_with_hierarchy(demand, bom, split_child_orders=split_child_orders, include_parents=include_parents)
    print("[GREEDY DEBUG] expanded_demand rows:", len(demand))
    # --- map item -> workshop (from BOM)
    item_workshop = {}
    if "workshop" in bom.columns:
        try:
            _bw = bom[["item_id","workshop"]].dropna()
            _bw["item_id"] = _bw["item_id"].astype(str).str.strip()
            _bw["workshop"] = _bw["workshop"].astype(str).str.strip()
            item_workshop = _bw.drop_duplicates("item_id").set_index("item_id")["workshop"].to_dict()
        except Exception:
            item_workshop = {}
    else:
        item_workshop = {}
    # === STOCK CONSUMPTION (apply to all levels FG/PARENT/CHILD) ===
    if stock_map:
        # normalize stock map keys to tuple (item, workshop) or str item for legacy
        smap = {}
        for k, v in stock_map.items():
            if isinstance(k, tuple) and len(k) == 2:
                smap[(str(k[0]), str(k[1]))] = float(v or 0.0)
            else:
                smap[str(k)] = float(v or 0.0)

        adjusted = []
        demand = demand.sort_values(["priority","depth","item_id"], kind="stable").reset_index(drop=True)
        for r in demand.itertuples(index=False):
            item = str(r.item_id)
            wk = item_workshop.get(item, "")
            q = int(r.qty) if not pd.isna(r.qty) else 0
            if q <= 0:
                continue

            # try to consume stock: exact (item, wk) -> generic (item, "") -> legacy item -> any other workshop for item
            needed = q
            tried_keys = []
            for key in ((item, wk), (item, ""), item):
                if key in tried_keys:
                    continue
                tried_keys.append(key)
                avail = smap.get(key, 0.0)
                if avail <= 0:
                    continue
                take = min(needed, int(avail))
                smap[key] = avail - take
                needed -= take
                if needed <= 0:
                    break

            # Fallback: sweep other workshops for this item if nothing or not enough taken (covers BOM without workshop)
            if needed > 0:
                for key, avail in list(smap.items()):
                    if isinstance(key, tuple) and len(key) == 2 and str(key[0]) == item and key not in tried_keys:
                        if avail <= 0:
                            continue
                        take = min(needed, int(avail))
                        smap[key] = avail - take
                        needed -= take
                        if needed <= 0:
                            break

            if needed <= 0:
                continue  # fully covered by stock
            q_prod = needed

            if q_prod > 0:
                adjusted.append(dict(
                    base_order_id=getattr(r, "base_order_id", r.order_id),
                    order_id=r.order_id,
                    item_id=item,
                    due_date=r.due_date,
                    qty=int(q_prod),
                    priority=r.priority,
                    role=getattr(r, "role", "FG"),
                    depth=getattr(r, "depth", 0),
                    workshop=wk,
                ))
        demand = pd.DataFrame(adjusted) if adjusted else demand.iloc[0:0]

    if len(demand) == 0:
        raise ValueError("Greedy: expanded_demand is empty — нет маршрутов/BOM или qty_per_parent/идентификаторы не сошлись.")
    if "role" not in demand.columns:
        demand["role"] = "FG"

    # Карты предков/потомков для дерева
    parent_map = {}
    children_map: dict[str, set[str]] = {}
    if "root_item_id" in bom.columns:
        for r in bom[["item_id", "root_item_id"]].drop_duplicates().itertuples(index=False):
            item = str(r.item_id)
            par  = str(r.root_item_id) if pd.notna(r.root_item_id) else ""
            parent_map[item] = par or ""
            if par and par != item:
                children_map.setdefault(par, set()).add(item)
    else:
        # без иерархии — все без родителей
        parent_map = {}

    # === 1) Емкость машин и перегрузка ===
    base_cap = machines.groupby("machine_id", as_index=True)["capacity_per_day"].max().to_dict()
    per_machine_over = {}
    if "overload_pct" in machines.columns:
        per_machine_over = machines.groupby("machine_id", as_index=True)["overload_pct"].max().to_dict()

    overrides = defaultdict(dict)  # machine_id -> {date -> cap}
    if "calendar_date" in machines.columns and "capacity_override" in machines.columns:
        mcal = machines.dropna(subset=["calendar_date", "capacity_override"])
        for _, r in mcal.iterrows():
            overrides[str(r["machine_id"])][r["calendar_date"]] = float(r["capacity_override"])

    def effective_cap(machine_id: str, day: dt.date) -> float:
        cap = overrides.get(machine_id, {}).get(day, base_cap.get(machine_id, 0.0))
        ov = per_machine_over.get(machine_id, overload_pct)
        try:
            ov = float(ov)
        except Exception:
            ov = 0.0
        ov = max(0.0, ov)
        return float(cap) * (1.0 + ov)

    # === 2) Маршруты: item_id -> [(step, machine_id, t_per_unit, setup_once)], отсортированы по step ===
    route = defaultdict(list)
    has_setup = "setup_minutes" in bom.columns
    for _, r in bom.iterrows():
        route[str(r["item_id"])].append((
            int(r["step"]),
            str(r["machine_id"]),
            float(r["time_per_unit"]),
            float(r["setup_minutes"]) if has_setup else 0.0
        ))
    for k in route.keys():
        route[k].sort(key=lambda x: x[0])

    # Проверим совпадение machine_id между BOM и machines
    route_mids = sorted({m for steps in route.values() for (_, m, _, _) in steps})
    base_cap = machines.groupby("machine_id")["capacity_per_day"].max()
    missing = [m for m in route_mids if m not in base_cap.index]
    print(f"[GREEDY DEBUG] route_machines={len(route_mids)} missing_in_machines={len(missing)}")
    if missing[:10]: print("[GREEDY DEBUG] missing sample:", missing[:10])
    
    # Проверим кап на сегодня по первым 5 машинам из маршрутов
    sd = start_date
    for mid in route_mids[:5]:
        try:
            c = effective_cap(mid, sd)
        except Exception:
            c = None
        print(f"[GREEDY DEBUG] cap[{mid}] on {sd} = {c}")


    # === 3) Аккумуляторы ===
    used = defaultdict(lambda: defaultdict(float))  # machine_id -> date -> minutes_used
    rows = []

    # В JIT режиме нам важно знать старт/финиш каждого (base_order_id, item_id)
    item_start: dict[tuple[str, str], dt.date] = {}
    item_finish: dict[tuple[str, str], dt.date] = {}

    # Утилиты размещения одного ШАГА
    def alloc_forward_step(machine_id: str, minutes_total: float, day_from: dt.date, latest_allowed: dt.date | None) -> tuple[dt.date, dt.date]:
        """Размещает minutes_total вперёд от day_from. Если latest_allowed задан, не допускаем day > latest_allowed."""
        remaining = float(minutes_total)
        day = max(day_from, start_date)  # не раньше сегодняшнего дня
        first_day = None
        guard = 0
        while remaining > 1e-6:
            guard += 1
            if guard > guard_limit_days:
                raise RuntimeError("Превышен лимит дней при планировании вперёд (guard_limit_days).")
            if latest_allowed is not None and day > latest_allowed:
                raise RuntimeError("Не удалось уложиться в заданный дедлайн при планировании вперёд.")
            cap = effective_cap(machine_id, day)
            free = max(0.0, cap - used[machine_id][day])
            if free > 1e-6:
                take = min(free, remaining)
                used[machine_id][day] += take
                remaining -= take
                if first_day is None:
                    first_day = day
            if remaining > 1e-6:
                day = day + dt.timedelta(days=1)
        return first_day or day_from, day  # start_day, finish_day (последний день, где добили)

    def alloc_backward_step(machine_id: str, minutes_total: float, deadline: dt.date, earliest_allowed: dt.date) -> tuple[dt.date, dt.date]:
        """Размещает minutes_total НАЗАД, заканчивая в deadline (<=deadline), но не раньше earliest_allowed."""
        remaining = float(minutes_total)
        day = deadline
        last_day = None
        guard = 0
        while remaining > 1e-6:
            guard += 1
            if guard > guard_limit_days:
                raise RuntimeError("Превышен лимит дней при планировании вперёд (guard_limit_days).")
            if day < earliest_allowed:
                raise RuntimeError("Не удалось уложиться в окно при планировании назад.")
            cap = effective_cap(machine_id, day)
            free = max(0.0, cap - used[machine_id][day])
            if free > 1e-6:
                take = min(free, remaining)
                used[machine_id][day] += take
                remaining -= take
                if last_day is None:
                    last_day = day
            if remaining > 1e-6:
                day = day - dt.timedelta(days=1)
        # вернём (start_day, finish_day) для шага
        # start_day = день начала (самый ранний задействованный), это day+1 после последнего шага цикла,
        # но проще отследить: после цикла 'day' уже на 1 раньше фактического стартового дня
        start_day = day + dt.timedelta(days=1)
        finish_day = last_day or deadline
        return start_day, finish_day

    # Размещение одного ИЗДЕЛИЯ по всем его шагам
    def schedule_item_forward(base_oid: str, oid: str, item: str, qty: int, earliest: dt.date, latest: dt.date | None, due: dt.date):
        """ASAP: шагаем вперёд, optionally удерживая finish <= latest."""
        steps = route.get(item) or [(1, "UNKNOWN", 0.0, 0.0)]
        first_any = None
        cur_day = earliest
        for step, machine_id, tpu, setup_once in steps:
            total = qty * float(tpu) + float(setup_once)
            st, fin = alloc_forward_step(machine_id, total, cur_day, latest_allowed=latest)
            # Запишем по дням (там уже добавились minutes в used) — добавим строки
            # Чтобы не разбирать по минутам в пределах дня, мы уже пишем по факту в while-цикле выше;
            # здесь фиксируем только границы для зависимостей:
            cur_day = fin  # следующий шаг не раньше финиша текущего
            first_any = first_any or st
            # (Строки расписания уже собирались в alloc_* через used; добавим их здесь агрегированно по дням)
        # Для корректного экспорта и lag — нам нужны реальные строки по дням.
        # Мы уже использовали used[...] для резерва, но строчек не добавили. Добавим их сейчас постфактум:
        # (пройдём по диапазону дат и вычленим вклад этого item/oid — упростим: добавим строки во время распределения)
        # => Поэтому переносим формирование строк внутрь alloc_* (см. ниже обновлённый вариант).
        item_start[(base_oid, item)] = first_any or earliest
        item_finish[(base_oid, item)] = cur_day
        return item_start[(base_oid, item)], item_finish[(base_oid, item)]

    # Чтобы формировать строки расписания сразу, сделаем прокси вокруг alloc_*,
    # который принимает "контекст строки" и при каждом 'take' добавляет row.
    # ---------- Аллокаторы (вперёд / назад) ----------
    def alloc_forward_step_rows(machine_id, minutes_total, day_from, latest_allowed, row_ctx):
        # robust numeric
        try:
            remaining = float(minutes_total)
        except Exception:
            remaining = 0.0
        if not np.isfinite(remaining) or remaining < 0:
            remaining = 0.0

        day = max(day_from, start_date)
        first_day = None
        guard = 0

        while remaining > 1e-6:
            guard += 1
            if guard > guard_limit_days:
                raise RuntimeError("Превышен лимит дней при планировании вперёд (guard_limit_days).")
            if latest_allowed is not None and day > latest_allowed:
                raise RuntimeError("Не удалось уложиться в дедлайн при планировании вперёд.")

            cap = effective_cap(machine_id, day)
            free = max(0.0, cap - used[machine_id][day])

            if free > 1e-6:
                take = min(free, remaining)
                used[machine_id][day] += take
                rows.append({**row_ctx, "machine_id": machine_id, "date": day, "minutes": take})
                remaining -= take
                if first_day is None:
                    first_day = day

            if remaining > 1e-6:
                day = day + dt.timedelta(days=1)

        return first_day or day_from, day


    def alloc_backward_step_rows(machine_id, minutes_total, deadline, earliest_allowed, row_ctx):
        # robust numeric
        try:
            remaining = float(minutes_total)
        except Exception:
            remaining = 0.0
        if not np.isfinite(remaining) or remaining < 0:
            remaining = 0.0

        day = deadline
        last_day = None
        guard = 0

        while remaining > 1e-6:
            guard += 1
            if guard > guard_limit_days:
                raise RuntimeError("Превышен лимит дней при планировании назад (guard_limit_days).")

            cap = effective_cap(machine_id, day)
            free = max(0.0, cap - used[machine_id][day])

            if free > 1e-6:
                take = min(free, remaining)
                used[machine_id][day] += take
                rows.append({**row_ctx, "machine_id": machine_id, "date": day, "minutes": take})
                remaining -= take
                if last_day is None:
                    last_day = day

            if remaining > 1e-6:
                day = day - dt.timedelta(days=1)

        start_day = day + dt.timedelta(days=1)
        finish_day = last_day or deadline
        return start_day, finish_day


    def schedule_item_backward(base_oid: str, oid: str, item: str, qty: int,
                               deadline: dt.date, earliest_allowed: dt.date, due: dt.date):
        """JIT-хелпер: один item назад к deadline (<=deadline), без старта раньше earliest_allowed."""
        steps = route.get(item) or [(1, "UNKNOWN", 0.0, 0.0)]
        cur_deadline = deadline
        earliest_seen = None

        # ровно и безопасно считаем минуты по шагам
        def _num(x): 
            try:
                x = float(x)
            except Exception:
                x = 0.0
            if not np.isfinite(x): x = 0.0
            return x

        for step, machine_id, tpu, setup_once in reversed(steps):
            tpu = _num(tpu); setup_once = _num(setup_once); q = int(qty) if not pd.isna(qty) else 0
            total = q * tpu + setup_once

            row_ctx = {
                "base_order_id": base_oid,
                "order_id": oid,
                "item_id": item,
                "step": step,
                "qty": q,
                "due_date": due, "workshop": item_workshop.get(item, "")}
            st, fin = alloc_backward_step_rows(machine_id, total, cur_deadline, earliest_allowed, row_ctx)
            earliest_seen = st if earliest_seen is None else min(earliest_seen, st)
            # предыдущий по маршруту шаг должен финишировать не позже старта текущего - 1 день
            cur_deadline = st - dt.timedelta(days=1)

        item_start[(base_oid, item)] = earliest_seen or earliest_allowed
        item_finish[(base_oid, item)] = deadline
        return item_start[(base_oid, item)], item_finish[(base_oid, item)]


    # === 4) Основная логика: ASAP или JIT ===
    if not align_roots_to_due:
        # ---------- ASAP вперёд ----------
        sort_cols = [c for c in ["priority", "base_order_id", "item_id"] if c in demand.columns]
        demand_sorted = demand.sort_values(sort_cols, kind="stable").reset_index(drop=True)

        for _, job in demand_sorted.iterrows():
            base_oid = str(job.get("base_order_id", job["order_id"]))
            oid = str(job["order_id"])
            item = str(job["item_id"])
            qty = int(job["qty"]) if not pd.isna(job["qty"]) else 0
            due = job["due_date"]

            # старт не раньше финиша родителя
            earliest_day = start_date
            par = str(parent_map.get(item, "") or "")
            if par:
                pf = item_finish.get((base_oid, par))
                if pf:
                    earliest_day = max(earliest_day, pf)

            steps = route.get(item) or [(1, "UNKNOWN", 0.0, 0.0)]

            # аккуратно считаем суммарные минуты
            def _num(x):
                try:
                    x = float(x)
                except Exception:
                    x = 0.0
                if not np.isfinite(x): x = 0.0
                return x
            total_item_minutes = 0.0
            for _, _, tpu, setup in steps:
                total_item_minutes += qty * _num(tpu) + _num(setup)

            if total_item_minutes <= 1e-9:
                # Пустой маршрут — плейсхолдер
                rows.append({
                    "base_order_id": base_oid, "order_id": oid, "item_id": item,
                    "step": 1, "machine_id": "UNKNOWN", "date": earliest_day,
                    "minutes": 0.0, "qty": qty, "due_date": due,
                })
                item_start[(base_oid, item)] = earliest_day
                item_finish[(base_oid, item)] = earliest_day
                warnings.append(f"[ROUTE] {base_oid}/{item}: пустой маршрут (0 мин). Плейсхолдер {earliest_day}.")
                continue

            # собственно планирование вперёд
            first_any = None
            cur = earliest_day  # <--- пропущенная переменная была
            for step, machine_id, tpu, setup_once in steps:
                tpu = _num(tpu); setup_once = _num(setup_once)
                total = qty * tpu + setup_once
                row_ctx = {
                    "base_order_id": base_oid, "order_id": oid, "item_id": item,
                    "step": step, "qty": qty, "due_date": due,
                }
                st, fin = alloc_forward_step_rows(machine_id, total, cur, latest_allowed=None, row_ctx=row_ctx)
                cur = fin
                if first_any is None:
                    first_any = st

            item_start[(base_oid, item)] = first_any or earliest_day
            item_finish[(base_oid, item)] = cur

    else:
        # ---------- JIT: корневые (FG) упираются в due_date, дети назад до старта родителя ----------
        today = start_date  # не стартуем раньше today

        # группируем по base_order_id (если его нет — по order_id)
        grp_cols = ["base_order_id"] if "base_order_id" in demand.columns else ["order_id"]
        for base_oid, df_ord in demand.groupby(grp_cols):
            base_oid = str(base_oid if isinstance(base_oid, str) else base_oid[0])

            # корни по role='FG', иначе — у кого нет родителя
            roots = df_ord[df_ord["role"] == "FG"]["item_id"].astype(str).unique().tolist()
            if not roots:
                roots = [it for it in df_ord["item_id"].astype(str).unique().tolist() if not parent_map.get(it, "")]
                if not roots and len(df_ord):
                    roots = [str(df_ord["item_id"].astype(str).iloc[0])]

            # планировать каждый корень по КАЖДОМУ его due в этой группе
            for root in roots:
                # все due для root в этой группе
                cand_root = df_ord[df_ord["item_id"].astype(str) == root].copy()
                if cand_root.empty:
                    continue
                cand_root["due_date"] = pd.to_datetime(cand_root["due_date"])
                for due_ts in sorted(cand_root["due_date"].unique()):
                    deadline = due_ts.date()

                    def plan_down(item_id: str, deadline: dt.date):
                        # выбрать строку спроса для item_id с due==deadline (или ближайшую)
                        cc = df_ord[df_ord["item_id"].astype(str) == item_id].copy()
                        if cc.empty:
                            return
                        cc["due_date"] = pd.to_datetime(cc["due_date"])
                        target = pd.to_datetime(deadline)
                        row = cc.iloc[(cc["due_date"] - target).abs().argsort().iloc[0]]
                        q = int(row["qty"]) if not pd.isna(row["qty"]) else 0
                        due_loc = row["due_date"].date()
                        oid_loc = str(row["order_id"])

                        steps_loc = route.get(item_id) or [(1, "UNKNOWN", 0.0, 0.0)]
                        # суммарные минуты (robust)
                        def _num(x):
                            try:
                                x = float(x)
                            except Exception:
                                x = 0.0
                            if not np.isfinite(x): x = 0.0
                            return x
                        total_item_minutes = 0.0
                        for _, _, tpu_i, setup_i in steps_loc:
                            total_item_minutes += q * _num(tpu_i) + _num(setup_i)

                        if total_item_minutes <= 1e-9:
                            # пустой маршрут — плейсхолдер на дедлайн (или today, если в прошлом)
                            place_day = deadline if deadline >= today else today
                            rows.append({
                                "base_order_id": base_oid, "order_id": oid_loc, "item_id": item_id,
                                "step": 1, "machine_id": "UNKNOWN", "date": place_day,
                                "minutes": 0.0, "qty": q, "due_date": due_loc,
                            })
                            item_start[(base_oid, item_id)] = place_day
                            item_finish[(base_oid, item_id)] = place_day
                            # дети — к старту - 1 день
                            child_deadline = item_start[(base_oid, item_id)] - dt.timedelta(days=1)
                            for child in sorted(list(children_map.get(item_id, []))):
                                plan_down(child, deadline=child_deadline)
                            return

                        # полноценное планирование назад
                        start_idx = len(rows)
                        earliest_seen = None
                        cur_deadline = deadline

                        for step, machine_id, tpu, setup in reversed(steps_loc):
                            tpu = _num(tpu); setup = _num(setup)
                            total = q * tpu + setup
                            ctx = {"base_order_id": base_oid, "order_id": oid_loc,
                                   "item_id": item_id, "step": step, "qty": q, "due_date": due_loc, "workshop": item_workshop.get(item_id, "")}
                            st, fin = alloc_backward_step_rows(machine_id, total, cur_deadline, earliest_allowed=today, row_ctx=ctx)
                            earliest_seen = st if earliest_seen is None else min(earliest_seen, st)
                            cur_deadline = st - dt.timedelta(days=1)

                        start_fact = earliest_seen or today
                        finish_fact = deadline
                        item_start[(base_oid, item_id)] = start_fact
                        item_finish[(base_oid, item_id)] = finish_fact

                        # если ушли «в прошлое» — сдвигаем вперёд
                        if start_fact < today:
                            shift = (today - start_fact).days
                            if shift > 0:
                                for i in range(start_idx, len(rows)):
                                    if rows[i]["order_id"] == oid_loc and rows[i]["item_id"] == item_id:
                                        rows[i]["date"] = rows[i]["date"] + dt.timedelta(days=shift)
                                item_start[(base_oid, item_id)] = today
                                item_finish[(base_oid, item_id)] = finish_fact + dt.timedelta(days=shift)

                        # дети — дедлайн = старт родителя - 1 день
                        child_deadline = item_start[(base_oid, item_id)] - dt.timedelta(days=1)
                        for child in sorted(list(children_map.get(item_id, []))):
                            plan_down(child, deadline=child_deadline)

                    # планируем корень на этот конкретный due
                    plan_down(root, deadline=deadline)



    # === 5) Финализация расписания ===
    sched = pd.DataFrame(rows)
    if sched.empty:
        sched = pd.DataFrame([{
            "base_order_id": "NO_JOBS",
            "order_id": "NO_JOBS",
            "item_id": "",
            "step": 1,
            "machine_id": "UNKNOWN",
            "date": start_date,
            "minutes": 0.0,
            "qty": 0,
            "due_date": start_date,
        }])

    sched["lag_days"] = (pd.to_datetime(sched["date"]) - pd.to_datetime(sched["due_date"])).dt.days
    sched = sched.sort_values(["date","machine_id","order_id","step"], kind="stable").reset_index(drop=True)

    if not rows:
        print("[GREEDY INFO] Все потребности покрыты запасом — производственные операции не создавались.")
        sched = pd.DataFrame([{
            "base_order_id": "ALL_FROM_STOCK",
            "order_id": "ALL_FROM_STOCK",
            "item_id": "",
            "step": 1,
            "machine_id": "NONE",
            "date": start_date,
            "minutes": 0.0,
            "qty": 0,
            "due_date": start_date,
        }])
        sched["lag_days"] = 0
        try:
            sched.attrs["warnings"] = warnings + ["ALL_FROM_STOCK"]
        except Exception:
            pass
        return sched
    print("[GREEDY DEBUG] rows_appended:", len(rows))

    try:
        sched.attrs["warnings"] = warnings
    except Exception:
        pass

    return sched




# =========================
# Export (Excel + stacked bar)
# =========================
def _auto_width(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                max_len = max(max_len, len(str(cell.value)))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(60, max(12, max_len + 2))


from openpyxl.chart import BarChart, Reference
from openpyxl.chart.layout import Layout, ManualLayout

from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.utils import get_column_letter
import pandas as pd
from pathlib import Path

def compute_order_items_timeline(sched: pd.DataFrame) -> pd.DataFrame:
    """
    Таймлайн для каждой пары (order_id, item_id):
      start_date, finish_date, duration_days, due_date, finish_lag
    """
    if sched.empty:
        return pd.DataFrame(columns=["order_id","item_id","start_date","finish_date","duration_days","due_date","finish_lag"])

    g = sched.groupby(["order_id","item_id"], as_index=False).agg(
        start_date=("date", "min"),
        finish_date=("date", "max"),
        due_date=("due_date", "max"),
    )
    g["duration_days"] = (pd.to_datetime(g["finish_date"]) - pd.to_datetime(g["start_date"])).dt.days + 1
    g["finish_lag"] = (pd.to_datetime(g["finish_date"]) - pd.to_datetime(g["due_date"])).dt.days
    return g.sort_values(["order_id","item_id","start_date"], kind="stable").reset_index(drop=True)


def export_with_charts(sched: pd.DataFrame, out_xlsx: Path, bom: pd.DataFrame | None = None) -> Path:
    sched = sched.copy()
    sched = sched.copy()
    for c in ("minutes", "qty"):
        if c in sched.columns:
            sched[c] = pd.to_numeric(sched[c], errors="coerce").fillna(0.0)
    
    by_day = sched.groupby(["date", "machine_id"], as_index=False)["minutes"].sum()
    by_day["minutes"] = pd.to_numeric(by_day["minutes"], errors="coerce").fillna(0.0)
    gmat = by_day.pivot(index="machine_id", columns="date", values="minutes").fillna(0).sort_index(axis=0)

    wb = Workbook()
    ws_sched = wb.active; ws_sched.title = "schedule"
    cols = ["base_order_id","order_id","item_id","workshop","step","machine_id","date","minutes","qty","due_date","lag_days"]
    cols = [c for c in cols if c in sched.columns]
    ws_sched.append(cols)
    for _, r in sched[cols].iterrows():
        ws_sched.append(list(r.values))
    _auto_width(ws_sched)

    ws_alloc = wb.create_sheet("alloc_by_day")
    ws_alloc.append(["date","machine_id","minutes"])
    for _, r in by_day.iterrows():
        ws_alloc.append([r["date"], r["machine_id"], float(r["minutes"])])
    _auto_width(ws_alloc)

    ws_gmat = wb.create_sheet("gantt_matrix")
    ws_gmat.append(["machine_id"] + [d for d in gmat.columns.tolist()])
    for mid, row in gmat.iterrows():
        ws_gmat.append([mid] + [float(x) for x in row.tolist()])
    _auto_width(ws_gmat)
    if gmat.shape[0] > 0 and gmat.shape[1] > 0:
        chart = BarChart(); chart.type="col"; chart.grouping="stacked"
        chart.title = "Загрузка (минуты по машинам/дням)"; chart.y_axis.title="Минуты"; chart.x_axis.title="Дата"
        data_ref = Reference(ws_gmat, min_col=2, min_row=1, max_col=1+gmat.shape[1], max_row=1+gmat.shape[0])
        cats_ref = Reference(ws_gmat, min_col=2, min_row=1, max_col=1+gmat.shape[1], max_row=1)
        chart.add_data(data_ref, titles_from_data=True, from_rows=True); chart.set_categories(cats_ref)
        chart.height=18; chart.width=28
        ws_gmat.add_chart(chart, f"B{gmat.shape[0] + 4}")

    # --- util_heatmap_% ---
    ws_util = wb.create_sheet("util_heatmap_%")
    try:
        cap_tbl = _build_cap_matrix(export_with_charts._machines_df) if hasattr(export_with_charts, "_machines_df") else None
    except Exception:
        cap_tbl = None
    if cap_tbl is None or cap_tbl.empty:
        cap_tbl = pd.DataFrame({"machine_id": by_day["machine_id"].unique(), "cap_minutes": 0.0})
    
    if "calendar_date" in cap_tbl.columns:
        cap_by_date = cap_tbl.rename(columns={"calendar_date":"date"})
        cap_join = by_day.merge(cap_by_date[["machine_id","date","cap_minutes"]],
                                on=["machine_id","date"], how="left")
        base_cap = cap_tbl.groupby("machine_id", as_index=False)["cap_minutes"].max() \
                          .rename(columns={"cap_minutes":"base_cap"})
        cap_join = cap_join.merge(base_cap, on="machine_id", how="left")
        cap_join["cap_eff"] = cap_join["cap_minutes"].fillna(cap_join["base_cap"]).fillna(0.0)
    else:
        base_cap = cap_tbl.groupby("machine_id", as_index=False)["cap_minutes"].max() \
                          .rename(columns={"cap_minutes":"cap_eff"})
        cap_join = by_day.merge(base_cap, on="machine_id", how="left")
        cap_join["cap_eff"] = cap_join["cap_eff"].fillna(0.0)
    
    # <-- ВАЖНО: приведение к числам ДO ЛЮБОЙ АРИФМЕТИКИ
    cap_join["minutes"] = pd.to_numeric(cap_join["minutes"], errors="coerce").fillna(0.0)
    cap_join["cap_eff"] = pd.to_numeric(cap_join["cap_eff"], errors="coerce").fillna(0.0)
    
    # Считаем % загрузки один раз, безопасно
    cap_join["util_pct"] = np.where(
        cap_join["cap_eff"] > 0,
        (cap_join["minutes"] / cap_join["cap_eff"] * 100).round(1),
        0.0
    )
    
    util_piv = cap_join.pivot(index="machine_id", columns="date", values="util_pct").fillna(0)

    ws_util.append(["machine_id"] + [d for d in util_piv.columns.tolist()])
    for mid, row in util_piv.iterrows():
        ws_util.append([mid] + [float(x) for x in row.tolist()])
    _auto_width(ws_util)
    if util_piv.shape[0] > 0 and util_piv.shape[1] > 0:
        from openpyxl.utils import get_column_letter as _gcl
        min_row = 2; max_row = 1 + util_piv.shape[0]
        min_col = 2; max_col = 1 + util_piv.shape[1]
        cell_range = f"{_gcl(min_col)}{min_row}:{_gcl(max_col)}{max_row}"
        rule = ColorScaleRule(start_type='num', start_value=0, start_color='FFFFFF',
                              mid_type='num', mid_value=50, mid_color='FFF59D',
                              end_type='num', end_value=100, end_color='F44336')
        ws_util.conditional_formatting.add(cell_range, rule)
        ws_util.freeze_panes = "B2"

    orders = compute_orders_timeline(sched)
    ws_otl = wb.create_sheet("orders_timeline")
    ws_otl.append(["order_id","item_id","start_date","finish_date","duration_days","due_date","finish_lag"])
    for _, r in orders.iterrows():
        ws_otl.append(list(r.values))
    _auto_width(ws_otl)
    if not orders.empty:
        min_start = pd.to_datetime(orders["start_date"]).min().date()
        ws_otl.cell(row=1, column=8, value="offset_days")
        for i, (_, rr) in enumerate(orders.iterrows(), start=2):
            off = (pd.to_datetime(rr["start_date"]).date() - min_start).days
            ws_otl.cell(row=i, column=8, value=int(off))
        nrows = orders.shape[0]
        cats_ref = Reference(ws_otl, min_col=1, min_row=2, max_row=1 + nrows)
        data_ref1 = Reference(ws_otl, min_col=8, min_row=1, max_col=8, max_row=1 + nrows)
        data_ref2 = Reference(ws_otl, min_col=5, min_row=1, max_col=5, max_row=1 + nrows)
        gant = BarChart(); gant.type = "bar"; gant.grouping = "stacked"
        gant.title = f"Orders Gantt (t0 = {min_start})"; gant.y_axis.title = "order_id"; gant.x_axis.title = "days from t0"
        gant.add_data(data_ref1, titles_from_data=True); gant.add_data(data_ref2, titles_from_data=True); gant.set_categories(cats_ref)
        gant.height = 20; gant.width = 30
        try:
            ser_offset = gant.series[0]
            ser_offset.graphicalProperties.solidFill = "DDDDDD"
            ser_offset.graphicalProperties.line.solidFill = "DDDDDD"
        except Exception:
            pass
        ws_otl.add_chart(gant, "J2")

    oitl = compute_order_items_timeline(sched)
    ws_oitl = wb.create_sheet("order_items_timeline")
    ws_oitl.append(["order_id","item_id","start_date","finish_date","duration_days","due_date","finish_lag"])
    for _, r in oitl.iterrows():
        ws_oitl.append(list(r.values))
    _auto_width(ws_oitl)

    ws_bom = wb.create_sheet("BOM")
    if bom is not None and "root_item_id" in bom.columns:
        try:
            b = bom[["item_id","root_item_id","qty_per_parent"]].drop_duplicates()
            b["root_item_id"] = b["root_item_id"].fillna("").astype(str)
            ws_bom.append(["root_item_id","item_id","qty_per_parent"])
            for _, r in b.iterrows():
                ws_bom.append([r["root_item_id"], r["item_id"], float(r["qty_per_parent"])])
        except Exception:
            ws_bom.append(["info"]); ws_bom.append(["Не удалось собрать BOM view."])
    else:
        ws_bom.append(["info"]); ws_bom.append(["BOM без иерархии."])
    _auto_width(ws_bom)

    if "base_order_id" in sched.columns:
        link = (sched[["base_order_id","order_id","item_id"]].drop_duplicates().sort_values(["base_order_id","order_id","item_id"], kind="stable"))
        ws_link = wb.create_sheet("orders_linkage")
        ws_link.append(["base_order_id","order_id","item_id"])
        for _, r in link.iterrows():
            ws_link.append([r["base_order_id"], r["order_id"], r["item_id"]])
        _auto_width(ws_link)

    # Child-from-FG linkage (explicit mapping FG -> CHILD with quantities)
    try:
        if set(["base_order_id","order_id","item_id"]).issubset(set(sched.columns)):
            df = sched.copy()
            df["base_order_id"] = df["base_order_id"].astype(str)
            df["order_id"] = df["order_id"].astype(str)
            df["item_id"] = df["item_id"].astype(str)
            # Identify FG rows (order_id == base_order_id)
            fg_rows = df[df["order_id"] == df["base_order_id"]]
            fg_map = fg_rows.drop_duplicates("base_order_id")[["base_order_id","item_id"]] \
                             .set_index("base_order_id")["item_id"].to_dict()
            # Child rows where order_id != base_order_id
            ch = df[df["order_id"] != df["base_order_id"]].copy()
            if not ch.empty:
                ch["fg_item"] = ch["base_order_id"].map(fg_map).fillna("")
                # customer from base prefix if present (e.g., 'Petrol-YYYYMMDD-PV...')
                def _cust_from_base(s: str) -> str:
                    try:
                        return str(s).split("-", 1)[0]
                    except Exception:
                        return ""
                ch["customer"] = ch["base_order_id"].map(_cust_from_base)
                # aggregate by base, fg_item, child, due_date
                keep_due = "due_date" if "due_date" in ch.columns else None
                agg_cols = ["base_order_id","fg_item","item_id"] + ([keep_due] if keep_due else [])
                ch_qty = ch.copy()
                ch_qty["qty"] = pd.to_numeric(ch_qty.get("qty", 0), errors="coerce").fillna(0.0)
                grp = ch_qty.groupby(agg_cols, as_index=False)["qty"].sum()
                ws_cff = wb.create_sheet("child_from_fg")
                header = ["base_order_id","customer","fg_item","child_item","due_date","qty"]
                ws_cff.append(header)
                for _, r in grp.iterrows():
                    ws_cff.append([
                        r.get("base_order_id",""),
                        ch.loc[ch["base_order_id"]==r.get("base_order_id",""), "customer"].iloc[0] if not ch.empty else "",
                        r.get("fg_item",""),
                        r.get("item_id",""),
                        (str(r.get("due_date")) if keep_due else ""),
                        float(r.get("qty", 0.0)),
                    ])
                _auto_width(ws_cff)
    except Exception:
        pass

    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_xlsx)
    return out_xlsx


# =========================
# Pipeline
# =========================



# =========================
# Backward-compatible wrapper (tolerates SQLAlchemy Session)
# =========================


def run_pipeline(
    plan_path,
    bom_path,
    machines_path,
    out_xlsx,
    stock_path=None,
    start_date=None,
    overload_pct: float = 0.0,
    split_child_orders: bool = True,
    align_roots_to_due: bool = True,
    mode: str = "",
):


        # --- Product View netting branch (активируется только при mode="product_view")
    _mode = str(locals().get("mode", "")).lower().strip()
    if _mode == "product_view":
        from pathlib import Path as _P

        # существующие поступления: берём из out_xlsx (или оставь как хочешь)
        _existing = None
        try:
            if _P(out_xlsx).exists():
                _dfx = pd.read_excel(_P(out_xlsx), sheet_name="schedule")
                _keep = [c for c in ["item_id","due_date","qty","workshop"] if c in _dfx.columns]
                _existing = _dfx[_keep].copy() if _keep else None
        except Exception:
            _existing = None

        # склад
        _stock_df = None
        if stock_path:
            _stock_df = load_stock_any(_P(stock_path))
            if _stock_df is not None and "workshop" not in _stock_df.columns:
                _stock_df["workshop"] = ""

        # входы
        plan = load_plan_of_sales(_P(plan_path))
        bom  = load_bom(_P(bom_path))
        machines = load_machines(_P(machines_path))

        # неттинг → заказы на дельту
        demand_net = product_view_generate_demand(plan, bom, stock_df=_stock_df, existing_orders_df=_existing)

        # строим demand в твоём формате и идём дальше обычным путём
        demand = build_demand(demand_net)
        stock_map = None  # склад уже учли в неттинге

    """
    End-to-end pipeline: load inputs -> expand demand -> schedule -> export.
    Keeps backward-compatible API.
    """
    # 1) Load inputs
    plan = load_plan_of_sales(Path(plan_path))   # wide → long
    demand = build_demand(plan)
    bom = load_bom(Path(bom_path))
    machines = load_machines(Path(machines_path))

    # 2) Expand demand by BOM hierarchy (qty_per_parent applies only to descendants)
    demand = expand_demand_with_hierarchy(demand, bom, split_child_orders=split_child_orders)

    # 3) Optional stock
    stock_map = None
    if stock_path:
        try:
            _stock_df = load_stock_any(Path(stock_path))
            # support optional workshop column
            if "workshop" not in _stock_df.columns:
                _stock_df["workshop"] = ""
            # aggregate duplicates per (item, workshop) before mapping to dict
            _stock_df = (_stock_df
                         .assign(item_id=lambda x: x["item_id"].astype(str),
                                 workshop=lambda x: x["workshop"].astype(str))
                         .groupby(["item_id","workshop"], as_index=False)["stock_qty"].sum())
            stock_map = { (str(r.item_id), str(r.workshop)): float(r.stock_qty)
                          for r in _stock_df.itertuples(index=False) }
        except Exception as e:
            print("[GREEDY WARN] stock load failed:", e)
            stock_map = None

    # 4) Start date
    start = None
    if start_date:
        try:
            start = pd.to_datetime(start_date).date()
        except Exception:
            start = None

    # 5) Schedule
    sched = greedy_schedule(
        demand, bom, machines,
        start_date=start,
        overload_pct=overload_pct,
        split_child_orders=split_child_orders,
        align_roots_to_due=align_roots_to_due,
        stock_map=stock_map,
        include_parents=(mode == "standard_up"),
    )

    # 6) Export
    export_with_charts._machines_df = machines  # pass through for charts if needed
    out_file = export_with_charts(sched, Path(out_xlsx), bom=bom)
    return out_file, sched

def run_greedy(*args: Any, **kwargs: Any) -> Tuple[str, pd.DataFrame]:
    """
    Гибкая обёртка вокруг run_pipeline:
    - понимает позиционные/именованные аргументы,
    - глотает лишние флаги (в т.ч. guard_limit_days),
    - при save_to_plan_id + db сохраняет в БД.
    Возвращает (out_xlsx, sched_df).
    """
    # спец-параметры
    save_to_plan_id = kwargs.pop("save_to_plan_id", None)
    db = kwargs.pop("db", None) or kwargs.pop("session", None)

    # 1) Нормализация входных аргументов до любых других действий
    if "stock" in kwargs and "stock_path" not in kwargs:
        kwargs["stock_path"] = kwargs.pop("stock")

    # 2) Хелпер для определения SQLAlchemy Session
    def _looks_like_session(x: Any) -> bool:
        return hasattr(x, "execute") or (hasattr(x, "add") and hasattr(x, "commit"))

    # 3) Поддержка позиционного Session первым аргументом
    pos = list(args)
    db = kwargs.pop("db", None) or kwargs.pop("session", None)
    if pos and _looks_like_session(pos[0]):
        db = db or pos.pop(0)
    # сопоставим позиционные
    ordered_names = [
"plan_path", "bom_path", "machines_path", "out_xlsx", "stock_path",
        "start_date", "overload_pct", "split_child_orders", "align_roots_to_due",
        "guard_limit_days",  # этот ключ примем, но дальше НЕ прокидываем
    ]
    for i, name in enumerate(ordered_names):
        if i < len(pos) and name not in kwargs:
            kwargs[name] = pos[i]

    # дефолты
    plan_path          = kwargs.get("plan_path")          or "plan of sales.xlsx"
    bom_path           = kwargs.get("bom_path")           or "BOM.xlsx"
    machines_path      = kwargs.get("machines_path")      or "machines.xlsx"
    out_xlsx           = kwargs.get("out_xlsx")           or "schedule_out.xlsx"
    start_date         = kwargs.get("start_date")         or None
    stock_path        = kwargs.get("stock_path")        or None
    overload_pct       = float(kwargs.get("overload_pct") or 0.0)
    split_child_orders = bool(kwargs.get("split_child_orders") or False)
    align_roots_to_due = bool(kwargs.get("align_roots_to_due") or False)
    _guard_limit_days  = kwargs.get("guard_limit_days", None)  # игнорируем для run_pipeline
    mode = kwargs.get("mode", "")

    # ← ПЕРЕЧЕНЬ, КОТОРЫЙ ПОДДЕРЖИВАЕТ run_pipeline
    safe_kwargs = dict(
        stock_path=stock_path,
        start_date=start_date,
        overload_pct=overload_pct,
        split_child_orders=split_child_orders,
        align_roots_to_due=align_roots_to_due,
    )

    # запуск основного пайплайна
    out_xlsx_path, sched_df = run_pipeline(
        plan_path, bom_path, machines_path, out_xlsx,
        mode=mode,  # ← пробрасываем режим
        **safe_kwargs
    )

    # при необходимости сохранить в БД
    if save_to_plan_id and db is not None:
        from ..db.models import ScheduleOp, MachineLoadDaily, PlanVersion
        from .utils import compute_daily_loads

        plan = db.get(PlanVersion, int(save_to_plan_id))
        if plan is None:
            raise ValueError(f"PlanVersion id={save_to_plan_id} not found")
        if sched_df is None or getattr(sched_df, "empty", True):
            raise RuntimeError("Greedy produced empty schedule, nothing to save.")

        df_ops = sched_df.copy()
        df_ops["start_ts"] = pd.to_datetime(df_ops["date"])
        df_ops["end_ts"] = pd.to_datetime(df_ops["date"]) + pd.to_timedelta(1, unit="D")
        df_ops["duration_sec"] = (
            pd.to_numeric(df_ops.get("minutes", 0), errors="coerce").fillna(0.0) * 60
        ).astype(int)
        df_ops["setup_sec"] = 0
        df_ops["op_index"] = (
            pd.to_numeric(df_ops.get("step", 1), errors="coerce").fillna(1).astype(int)
        )
        df_ops["batch_id"] = ""
        df_ops["qty"] = pd.to_numeric(df_ops.get("qty", 0), errors="coerce").fillna(0.0)

        article_name_map = {}
        try:
            if bom_path:
                article_name_map = _L_load_bom_article_name_map(Path(bom_path))
        except Exception:
            article_name_map = {}

        bulk_ops = []
        for r in df_ops.itertuples(index=False):
            article_name = article_name_map.get(str(getattr(r, "item_id", "") or "")) or None
            bulk_ops.append(
                ScheduleOp(
                    plan_id=plan.id,
                    order_id=str(r.order_id),
                    item_id=str(r.item_id),
                    article_name=article_name,
                    machine_id=str(r.machine_id),
                    start_ts=r.start_ts,
                    end_ts=r.end_ts,
                    qty=float(getattr(r, "qty", 0) or 0.0),
                    duration_sec=int(r.duration_sec),
                    setup_sec=int(getattr(r, "setup_sec", 0) or 0),
                    op_index=int(getattr(r, "op_index", 0) or 0),
                    batch_id=str(getattr(r, "batch_id", "") or ""),
                )
            )
        db.bulk_save_objects(bulk_ops)
        db.commit()

        loads_df = compute_daily_loads(df_ops)
        bulk_loads = [
            MachineLoadDaily(
                plan_id=plan.id,
                machine_id=row.machine_id,
                work_date=row.work_date,
                load_sec=int(row.load_sec),
                cap_sec=int(row.cap_sec),
                util=float(row.util),
            )
            for row in loads_df.itertuples(index=False)
        ]
        db.bulk_save_objects(bulk_loads)
        db.commit()

        try:
            plan.origin = plan.origin or "greedy"
            plan.status = "ready"
            db.commit()
        except Exception:
            db.rollback()

    return out_xlsx_path, sched_df
# =========================
# CLI (локальный запуск)
# =========================
def _parse_args():
    p = argparse.ArgumentParser("Greedy planner (so-planner)")
    p.add_argument("--plan", default="plan of sales.xlsx", help="Путь к plan of sales.xlsx")
    p.add_argument("--bom", default="BOM.xlsx", help="Путь к BOM.xlsx")
    p.add_argument("--machines", default="machines.xlsx", help="Путь к machines.xlsx")
    p.add_argument("--out", default="schedule_out.xlsx", help="Куда сохранить Excel с планом")
    p.add_argument("--stock", default=None, help="Путь к Excel с остатками (article/item_id + qty)")
    p.add_argument("--start", default=None, help="Стартовая дата (YYYY-MM-DD)")
    p.add_argument("--overload-pct", type=float, default=0.0, help="Глобальная перегрузка (0..1 или 0..100%)")
    p.add_argument("--split-child-orders", action="store_true", help="Каждый article в отдельный order (<base>:<item>)")
    p.add_argument("--stock", default=None, help="Путь к Excel с остатками (article/item_id + qty)")

    return p.parse_args()

def main():
    args = _parse_args()
    overload = args.overload_pct
    if overload > 1.0:
        overload = overload / 100.0
    out, _ = run_pipeline(
        args.plan, args.bom, args.machines, args.out,
        stock_path=args.stock,
        start_date=args.start,
        overload_pct=overload,
        split_child_orders=bool(args.split_child_orders),
    )
    print(f"Готово: {out}")



if __name__ == "__main__":
    main()

# --- DB helpers for product_view/netting ---
def _ensure_netting_tables(db: Session) -> None:
    """Create minimal tables used by product_view netting if missing (SQLite)."""
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS plan_version (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'draft',
            horizon_start TEXT,
            horizon_end TEXT,
            notes TEXT,
            origin TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS plan_line (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_version_id INTEGER,
            item_id TEXT,
            due_date TEXT,
            qty INTEGER,
            priority TEXT,
            customer TEXT,
            workshop TEXT,
            source_tag TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS receipts_plan (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_version_id INTEGER,
            item_id TEXT,
            due_date TEXT,
            qty INTEGER,
            workshop TEXT,
            receipt_type TEXT,
            source_ref TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            taken_at TEXT DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_line (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER,
            item_id TEXT,
            workshop TEXT,
            stock_qty INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS netting_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            user TEXT,
            mode TEXT,
            plan_version_id INTEGER,
            stock_snapshot_id INTEGER,
            bom_version_id TEXT,
            receipts_source_desc TEXT,
            params TEXT,
            status TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS netting_order (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            netting_run_id INTEGER,
            order_id TEXT,
            item_id TEXT,
            due_date TEXT,
            qty INTEGER,
            priority TEXT,
            workshop TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS netting_log_row (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            netting_run_id INTEGER,
            item_id TEXT,
            workshop TEXT,
            date TEXT,
            kind TEXT,
            opening_exact INTEGER,
            opening_generic INTEGER,
            stock_used_exact INTEGER,
            stock_used_generic INTEGER,
            receipts_used INTEGER,
            order_created INTEGER,
            available_after INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS netting_summary_row (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            netting_run_id INTEGER,
            item_id TEXT,
            workshop TEXT,
            stock_used_total INTEGER,
            receipts_used_total INTEGER,
            orders_created_total INTEGER,
            opening_exact_init INTEGER,
            opening_generic_init INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS demand_linkage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            netting_run_id INTEGER,
            parent_item_id TEXT,
            parent_due_date TEXT,
            child_item_id TEXT,
            child_due_date TEXT,
            qty_per_parent REAL,
            required_qty REAL
        )
        """,
    ]
    for sql in stmts:
        db.execute(text(sql))
    db.commit()

def _load_receipts_from_db(
    db: Session,
    plan_version_id: int,
    receipts_from: Literal["plan", "firmed", "both"] = "plan",
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []

    if receipts_from in ("plan", "both"):
        if plan_version_id is None:
            q = text(
                """
                SELECT item_id, due_date, qty, COALESCE(workshop,'') AS workshop
                FROM receipts_plan
                """
            )
            rows = db.execute(q).mappings().all()
        else:
            q = text(
                """
                SELECT item_id, due_date, qty, COALESCE(workshop,'') AS workshop
                FROM receipts_plan
                WHERE plan_version_id = :p
                """
            )
            rows = db.execute(q, {"p": plan_version_id}).mappings().all()
        if rows:
            parts.append(pd.DataFrame(rows))

    if receipts_from in ("firmed", "both"):
        try:
            chk = db.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='schedule_version'")
            ).scalar()
            if chk:
                qv = text(
                    """
                    SELECT sv.id
                    FROM schedule_version sv
                    WHERE sv.status = 'firmed'
                      AND (sv.plan_version_id = :p OR :p IS NULL)
                    """
                )
                sv_ids = [r[0] for r in db.execute(qv, {"p": plan_version_id}).all()]
                if sv_ids:
                    qo = text(
                        f"""
                        SELECT item_id, due_date, qty, COALESCE(workshop,'') AS workshop
                        FROM schedule_order
                        WHERE schedule_version_id IN ({','.join(map(str, sv_ids))})
                        """
                    )
                    rows = db.execute(qo).mappings().all()
                    if rows:
                        parts.append(pd.DataFrame(rows))
        except Exception:
            pass

    if not parts:
        return pd.DataFrame(columns=["item_id", "due_date", "qty", "workshop"])

    df = pd.concat(parts, ignore_index=True)
    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["workshop"] = df["workshop"].astype(str).fillna("")
    df["due_date"] = pd.to_datetime(df["due_date"]).dt.date
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df = df.groupby(["item_id", "workshop", "due_date"], as_index=False)["qty"].sum()
    return df

def _load_stock_snapshot(db: Session, snapshot_id: int) -> pd.DataFrame:
    q = text(
        """
        SELECT item_id, COALESCE(workshop,'') AS workshop, stock_qty
        FROM stock_line
        WHERE snapshot_id = :sid
        """
    )
    rows = db.execute(q, {"sid": snapshot_id}).mappings().all()
    if not rows:
        return pd.DataFrame(columns=["item_id", "workshop", "stock_qty"])
    df = pd.DataFrame(rows)
    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["workshop"] = df["workshop"].astype(str).fillna("")
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)
    return df

# --- Optional: load receipts from Excel (ad-hoc) ---
def _load_receipts_excel(path: str | None) -> pd.DataFrame:
    if not path:
        return pd.DataFrame(columns=["item_id", "due_date", "qty", "workshop"])
    try:
        df = pd.read_excel(Path(path), sheet_name=0, dtype=object)
    except Exception:
        return pd.DataFrame(columns=["item_id", "due_date", "qty", "workshop"])

    def _nc(s: str) -> str:
        return str(s).strip().lower().replace(" ", "").replace("_", "")

    cols = {_nc(c): c for c in df.columns}
    item = cols.get("item_id") or cols.get("item") or cols.get("article")
    due = cols.get("due_date") or cols.get("date")
    qty = cols.get("qty") or cols.get("quantity")
    wk = cols.get("workshop") or cols.get("wk")
    if not item or not due or not qty:
        return pd.DataFrame(columns=["item_id", "due_date", "qty", "workshop"])
    out = pd.DataFrame({
        "item_id": df[item].astype(str).str.strip(),
        "due_date": pd.to_datetime(df[due], errors="coerce").dt.date,
        "qty": pd.to_numeric(df[qty], errors="coerce").fillna(0).astype(int),
        "workshop": df[wk].astype(str).fillna("") if wk else "",
    })
    out = out[(out["item_id"] != "") & out["due_date"].notna() & (out["qty"] > 0)].copy()
    if "workshop" not in out.columns:
        out["workshop"] = ""
    return out.groupby(["item_id", "workshop", "due_date"], as_index=False)["qty"].sum()

# --- Product-View from DB end-to-end ---
def run_product_view_from_db(
    db: Session,
    plan_version_id: int | None,
    stock_snapshot_id: int,
    receipts_from: str = "plan",
    receipts_excel_path: str | None = None,
    bom_path: str | None = None,
    machines_path: str | None = None,
    out_xlsx: str | None = None,
    user: str = "api",
    plan_name: str | None = None,
):
    """
    Runs Product-View netting using DB tables (plan_line, receipts_plan, stock_line) + greedy schedule.
    Returns (out_file_path, schedule_df).
    """
    import pandas as pd  # ensure no scope issues with global import
    _ensure_netting_tables(db)

    # Load inputs
    if (receipts_from or "plan").lower().strip() == "excel" and receipts_excel_path:
        existing_orders_df = _load_receipts_excel(receipts_excel_path)
    else:
        existing_orders_df = _load_receipts_from_db(db, plan_version_id, receipts_from)
    stock_df = _load_stock_snapshot(db, stock_snapshot_id)

    try:
        bom = load_bom(Path(bom_path or "BOM.xlsx"))
    except Exception:
        bom = pd.DataFrame(columns=["item_id","root_item_id","qty_per_parent","workshop"])
    try:
        machines = load_machines(Path(machines_path or "machines.xlsx"))
    except Exception:
        machines = pd.DataFrame(columns=["machine_id","capacity_per_day"])  # minimal stub

    # Plan from DB (optional)
    if plan_version_id is None:
        plan_df = pd.DataFrame(columns=["item_id","due_date","qty","workshop"])
    else:
        q = text("""
          SELECT item_id, due_date, qty, COALESCE(workshop,'') AS workshop
          FROM plan_line WHERE plan_version_id = :p
        """)
        rows = db.execute(q, {"p": plan_version_id}).mappings().all()
        plan_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["item_id","due_date","qty","workshop"])
    if not plan_df.empty:
        plan_df["item_id"] = plan_df["item_id"].astype(str).str.strip()
        plan_df["workshop"] = plan_df["workshop"].astype(str).fillna("")
        plan_df["due_date"] = pd.to_datetime(plan_df["due_date"]).dt.date
        plan_df["qty"] = pd.to_numeric(plan_df["qty"], errors="coerce").fillna(0).astype(int)

    # Netting: build residual demand
    demand_net = product_view_generate_demand(plan_df=plan_df, bom=bom, stock_df=stock_df, existing_orders_df=existing_orders_df)

    # Copy out netting log and build summary
    netting_log = NETTING_LOG.copy() if NETTING_LOG is not None else pd.DataFrame()
    if not netting_log.empty and "date" in netting_log.columns:
        netting_log["date"] = pd.to_datetime(netting_log["date"]).dt.date

    g = netting_log.copy()
    # Orders summary from residual demand (more reliable than log aggregation)
    gb_orders = ["item_id", "workshop"] + (["customer"] if (demand_net is not None and "customer" in demand_net.columns) else [])
    orders_sum = (
        demand_net.groupby(gb_orders, as_index=False)["qty"].sum().rename(columns={"qty": "orders_created_total"})
        if demand_net is not None and not demand_net.empty else pd.DataFrame(columns=gb_orders + ["orders_created_total"])
    )
    if not g.empty:
        g["stock_used_total"] = g.get("stock_used_exact", 0).fillna(0).astype(int) + g.get("stock_used_generic", 0).fillna(0).astype(int)
        g["receipts_used_total"] = g.get("receipts_used", 0).fillna(0).astype(int)
        gb = ["item_id","workshop"] + (["customer"] if "customer" in g.columns else [])
        openings = (
            g[g["kind"] == "opening"]
            .groupby(gb, as_index=False)[["opening_exact","opening_generic"]]
            .max()
            .rename(columns={"opening_exact":"opening_exact_init","opening_generic":"opening_generic_init"})
        )
        stock_receipts = (
            g[g["kind"] == "day"]
            .groupby(gb, as_index=False)[["stock_used_total","receipts_used_total"]]
            .sum()
        )
        # Merge stock/receipts from log with orders from demand
        netting_summary = (
            stock_receipts
            .merge(orders_sum, on=gb if set(gb) == set(gb_orders) else [c for c in gb if c in gb_orders], how="outer")
            .merge(openings, on=gb if set(gb) == set(gb_orders) else [c for c in gb if c in gb_orders], how="left")
            .fillna({"opening_exact_init": 0, "opening_generic_init": 0, "stock_used_total": 0, "receipts_used_total": 0, "orders_created_total": 0})
        )
    else:
        netting_summary = orders_sum.copy()
        if not netting_summary.empty:
            netting_summary["stock_used_total"] = 0
            netting_summary["receipts_used_total"] = 0
            netting_summary["opening_exact_init"] = 0
            netting_summary["opening_generic_init"] = 0
        else:
            netting_summary = pd.DataFrame(columns=[
                "item_id","workshop","stock_used_total","receipts_used_total","orders_created_total","opening_exact_init","opening_generic_init"
            ])

    # Save run results to DB
    run_meta = dict(
        user=user,
        mode="product_view",
        plan_version_id=plan_version_id,
        stock_snapshot_id=stock_snapshot_id,
        bom_version_id="",
        receipts_source_desc=receipts_from,
        params={"mode":"product_view"}
    )
    _ = _save_netting_results_to_db(
        db=db,
        run_meta=run_meta,
        demand_net=demand_net,
        netting_log=netting_log,
        netting_summary=netting_summary,
        linkage_df=None,
    )

    # If residual demand is zero, skip greedy schedule gracefully
    if demand_net is None or demand_net.empty:
        return None, pd.DataFrame()

    # Schedule using greedy (no additional stock)
    demand = build_demand(demand_net)
    sched = greedy_schedule(
        demand, bom, machines,
        start_date=None,
        overload_pct=0.0,
        split_child_orders=True,
        align_roots_to_due=True,
        stock_map=None,
    )

    out_path = Path(out_xlsx or "schedule_out.xlsx")
    export_with_charts._machines_df = machines
    out_file = export_with_charts(sched, out_path, bom=bom)

    # Append netting_log and summary to workbook (best effort)
    try:
        cols = [
            "item_id","workshop","customer","date","kind",
            "opening_exact","opening_generic",
            "stock_used_exact","stock_used_generic",
            "receipts_used","order_created","available_after",
        ]
        log_df = NETTING_LOG.copy() if NETTING_LOG is not None else pd.DataFrame(columns=cols)
        if "date" in log_df.columns:
            log_df["date"] = pd.to_datetime(log_df["date"]).dt.date
        with pd.ExcelWriter(out_path, engine="openpyxl", mode="a", if_sheet_exists="replace", date_format="yyyy-mm-dd") as xw:
            log_df.to_excel(xw, index=False, sheet_name="netting_log")
            g = log_df.copy()
            gb = ["item_id","workshop"] + (["customer"] if "customer" in g.columns else [])
            if not g.empty and "kind" in g.columns:
                summary = (
                    g[g["kind"]=="day"]
                    .groupby(gb, as_index=False)[["stock_used_exact","stock_used_generic","receipts_used"]]
                    .sum()
                )
                summary["stock_used_total"] = summary["stock_used_exact"].fillna(0).astype(int) + summary["stock_used_generic"].fillna(0).astype(int)
            else:
                summary = pd.DataFrame(columns=gb + [
                    "stock_used_exact","stock_used_generic","receipts_used","stock_used_total"
                ])
            # Inject order_created from actual residual demand
            try:
                dn = demand_net.copy() if demand_net is not None else pd.DataFrame(columns=["item_id","workshop","qty"])  # type: ignore[name-defined]
                if not dn.empty:
                    if "workshop" not in dn.columns:
                        dn["workshop"] = ""
                    gb2 = ["item_id","workshop"] + (["customer"] if "customer" in dn.columns else [])
                    orders_summary = dn.groupby(gb2, as_index=False)["qty"].sum().rename(columns={"qty":"order_created"})
                    summary = (
                        summary.drop(columns=["order_created"], errors="ignore")
                               .merge(orders_summary, on=gb, how="outer")
                               .fillna({"stock_used_exact":0, "stock_used_generic":0, "receipts_used":0, "stock_used_total":0, "order_created":0})
                    )
            except Exception:
                pass
            summary.to_excel(xw, index=False, sheet_name="netting_summary")
    except Exception:
        pass

    # Persist Greedy result into PlanVersion for reports
    try:
        from so_planner.db.models import PlanVersion, ScheduleOp, MachineLoadDaily
        from so_planner.scheduling.utils import compute_daily_loads
        import pandas as pd

        pname = plan_name or f"Netting {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M')}"
        plan = PlanVersion(name=pname, origin="product_view", status="draft")
        db.add(plan)
        db.commit()
        db.refresh(plan)

        df_ops = sched.copy()
        df_ops["start_ts"] = pd.to_datetime(df_ops["date"])
        df_ops["end_ts"] = pd.to_datetime(df_ops["date"]) + pd.to_timedelta(1, unit="D")
        df_ops["duration_sec"] = (
            pd.to_numeric(df_ops.get("minutes", 0), errors="coerce").fillna(0.0) * 60
        ).astype(int)
        df_ops["setup_sec"] = 0
        df_ops["op_index"] = (
            pd.to_numeric(df_ops.get("step", 1), errors="coerce").fillna(1).astype(int)
        )
        df_ops["batch_id"] = ""
        df_ops["qty"] = pd.to_numeric(df_ops.get("qty", 0), errors="coerce").fillna(0.0)

        article_name_map = {}
        try:
            if bom_path:
                article_name_map = _L_load_bom_article_name_map(Path(bom_path))
        except Exception:
            article_name_map = {}

        bulk_ops = []
        for r in df_ops.itertuples(index=False):
            article_name = article_name_map.get(str(getattr(r, "item_id", "") or "")) or None
            bulk_ops.append(
                ScheduleOp(
                    plan_id=plan.id,
                    order_id=str(r.order_id),
                    item_id=str(r.item_id),
                    article_name=article_name,
                    machine_id=str(r.machine_id),
                    start_ts=r.start_ts,
                    end_ts=r.end_ts,
                    qty=float(getattr(r, "qty", 0) or 0.0),
                    duration_sec=int(r.duration_sec),
                    setup_sec=int(getattr(r, "setup_sec", 0) or 0),
                    op_index=int(getattr(r, "op_index", 0) or 0),
                    batch_id=str(getattr(r, "batch_id", "") or ""),
                )
            )
        if bulk_ops:
            db.bulk_save_objects(bulk_ops)
            db.commit()

        loads_df = compute_daily_loads(df_ops)
        bulk_loads = [
            MachineLoadDaily(
                plan_id=plan.id,
                machine_id=row.machine_id,
                work_date=row.work_date,
                load_sec=int(row.load_sec),
                cap_sec=int(row.cap_sec),
                util=float(row.util),
            )
            for row in loads_df.itertuples(index=False)
        ]
        if bulk_loads:
            db.bulk_save_objects(bulk_loads)
            db.commit()

        try:
            plan.status = "ready"
            db.commit()
        except Exception:
            db.rollback()

        plan_id = int(plan.id)

        # Persist due_date per order for reports (from residual demand)
        try:
            if 'order_id' in demand_net.columns and 'due_date' in demand_net.columns:
                _rows = (
                    demand_net[['order_id','due_date']]
                    .dropna()
                    .drop_duplicates()
                    .to_dict('records')
                )
                if _rows:
                    ins = text("""
                        INSERT OR REPLACE INTO plan_order_info (plan_id, order_id, due_date)
                        VALUES (:plan_id, :order_id, :due_date)
                    """)
                    payload = [
                        { 'plan_id': plan_id, 'order_id': str(r['order_id']), 'due_date': str(pd.to_datetime(r['due_date']).date()) }
                        for r in _rows if str(r.get('order_id','')).strip()!=''
                    ]
                    if payload:
                        db.execute(ins, payload)
                        db.commit()
        except Exception:
            db.rollback()
    except Exception:
        plan_id = None

    return out_file, sched, plan_id
# === Merged Product-View aware pipeline ===

def run_pipeline(
    plan_path: str | None = None,
    bom_path: str | None = None,
    machines_path: str | None = None,
    out_xlsx: str | None = None,
    stock_path: str | None = None,
    start_date: str | None = None,
    overload_pct: float = 0.0,
    split_child_orders: bool = True,
    align_roots_to_due: bool = True,
    mode: str = "",
    **kwargs,
):
    """
    End-to-end pipeline.
    mode == 'product_view' -> Product-View netting (time-phased, stock fallback, receipts <= due) + greedy.
    otherwise -> standard pipeline (as before).
    """
    mode = (mode or "").lower().strip()
  
    if mode == "product_view":
        # ожидаем, что нам пробросят db: Session, plan_version_id, stock_snapshot_id, receipts_from
        db: Session | None = kwargs.get("db")
        plan_version_id: int | None = kwargs.get("plan_version_id")
        stock_snapshot_id: int | None = kwargs.get("stock_snapshot_id")
        receipts_from: str = kwargs.get("receipts_from", "plan")
    
        if db is None or plan_version_id is None or stock_snapshot_id is None:
            raise ValueError("product_view requires db, plan_version_id and stock_snapshot_id")
    
        # гарантируем таблицы
        _ensure_netting_tables(db)
    
        # загрузим склад и поступления из БД
        if (receipts_from or "plan").lower().strip() == "excel" and kwargs.get("receipts_excel_path"):
            existing_orders_df = _load_receipts_excel(kwargs.get("receipts_excel_path"))
        else:
            existing_orders_df = _load_receipts_from_db(db, plan_version_id, receipts_from)  # df �? item_id,due_date,qty,workshop
        stock_df = _load_stock_snapshot(db, stock_snapshot_id)  # df с item_id,workshop,stock_qty
    
        # неттинг: уже разворачивает BOM внутри себя и возвращает дельта-заказы
        demand_net = product_view_generate_demand(
            plan_df=plan_df,  # как и раньше: расплавленный план продаж (FG)
            bom=bom,
            stock_df=stock_df,
            existing_orders_df=existing_orders_df
        )
    
        # Лог/сводка (как у вас уже делалось для Excel) — считаем сразу для БД
        netting_log = NETTING_LOG.copy()
        if "date" in netting_log.columns:
            netting_log["date"] = pd.to_datetime(netting_log["date"]).dt.date
    
        # summary
        def _safe_int(x): 
            return int(pd.to_numeric(x, errors="coerce").fillna(0))
        g = netting_log.copy()
        g["stock_used_total"]    = g["stock_used_exact"].fillna(0).astype(int) + g["stock_used_generic"].fillna(0).astype(int)
        g["receipts_used_total"] = g["receipts_used"].fillna(0).astype(int)
        g["orders_created_total"]= g["order_created"].fillna(0).astype(int)
        # opening (первая opening по паре для отчёта)
        openings = (g[g["kind"]=="opening"]
                    .groupby(["item_id","workshop"], as_index=False)[["opening_exact","opening_generic"]]
                    .max().rename(columns={"opening_exact":"opening_exact_init","opening_generic":"opening_generic_init"}))
        netting_summary = (g[g["kind"]=="day"]
            .groupby(["item_id","workshop"], as_index=False)[["stock_used_total","receipts_used_total","orders_created_total"]]
            .sum()
            .merge(openings, on=["item_id","workshop"], how="left")
            .fillna({"opening_exact_init":0,"opening_generic_init":0})
        )
    
        # (опционально) linkage: если у вас есть функция возврата связей — сюда
        linkage_df = None  # заполним позже при необходимости
    
        # сохраним всё в БД (и получим run_id)
        run_meta = dict(
            user=kwargs.get("user","ui"),
            plan_version_id=plan_version_id,
            stock_snapshot_id=stock_snapshot_id,
            bom_version_id=kwargs.get("bom_version_id",""),
            receipts_source_desc=receipts_from,
            params={"mode":"product_view"}
        )
        netting_run_id = _save_netting_results_to_db(
            db=db,
            run_meta=run_meta,
            demand_net=demand_net,
            netting_log=netting_log,
            netting_summary=netting_summary,
            linkage_df=linkage_df
        )
    
        # готовим demand для посадки на мощности (без повторного expand и без stock_map)
        demand = build_demand(demand_net)  # уже child/fg как в обычной ветке
        stock_map = None  # склад уже «съеден» неттингом


        # Netting -> delta orders
        demand_net = product_view_generate_demand(plan, bom, stock_df=stock_df, existing_orders_df=existing_orders)
        demand = build_demand(demand_net)

        # Start date
        start = None
        if start_date:
            try:
                start = pd.to_datetime(start_date).date()
            except Exception:
                start = None

        # Schedule (stock is already netted)
        sched = greedy_schedule(
            demand, bom, machines,
            start_date=start,
            overload_pct=overload_pct,
            split_child_orders=split_child_orders,
            align_roots_to_due=align_roots_to_due,
            stock_map=None,
        )

        export_with_charts._machines_df = machines
        out_file = export_with_charts(sched, Path(out_xlsx), bom=bom)
        # --- Append netting_log (+summary) ---
        try:
            cols = [
                "item_id","workshop","customer","date","kind",
                "opening_exact","opening_generic",
                "stock_used_exact","stock_used_generic",
                "receipts_used","order_created","available_after",
            ]
            log_df = NETTING_LOG.copy() if NETTING_LOG is not None else pd.DataFrame(columns=cols)
            if "date" in log_df.columns:
                log_df["date"] = pd.to_datetime(log_df["date"]).dt.date
            with pd.ExcelWriter(Path(out_xlsx), engine="openpyxl", mode="a", if_sheet_exists="replace", date_format="yyyy-mm-dd") as xw:
                log_df.to_excel(xw, index=False, sheet_name="netting_log")
                g = log_df.copy()
                gb = ["item_id","workshop"] + (["customer"] if "customer" in g.columns else [])
                if not g.empty and "kind" in g.columns:
                    summary = (
                        g[g["kind"]=="day"]
                        .groupby(gb, as_index=False)[["stock_used_exact","stock_used_generic","receipts_used","order_created"]]
                        .sum()
                    )
                    summary["stock_used_total"] = summary["stock_used_exact"] + summary["stock_used_generic"]
                else:
                    summary = pd.DataFrame(columns=gb + [
                        "stock_used_exact","stock_used_generic","receipts_used","order_created","stock_used_total"
                    ])
                summary.to_excel(xw, index=False, sheet_name="netting_summary")
        except Exception as e:
            print("[NETTING] export failed:", e)
        return out_file, sched

    # -------- Standard pipeline (with optional two-phase stock netting) --------
    plan = load_plan_of_sales(Path(plan_path))
    bom = load_bom(Path(bom_path))
    machines = load_machines(Path(machines_path))

    # If stock file provided, perform two-phase netting like Product View (no receipts):
    # 1) Net FG plan by stock -> residual FG orders; 2) schedule expanded residuals.
    demand: pd.DataFrame
    did_netting = False
    if stock_path:
        try:
            stock_df = load_stock_any(Path(stock_path))
        except Exception as e:
            print("[GREEDY WARN] stock load failed:", e)
            stock_df = pd.DataFrame(columns=["item_id","stock_qty","workshop"])
        try:
            demand_net = product_view_generate_demand(plan_df=plan, bom=bom, stock_df=stock_df, existing_orders_df=None)
            # Use netted orders directly to avoid creating artificial FG for components
            demand = demand_net.copy()
            stock_map = None  # already netted
            did_netting = True
        except Exception as e:
            print("[GREEDY WARN] product_view-like netting failed, fallback to one-pass:", e)
            demand = build_demand(plan)
            # build legacy stock_map for greedy fallback
            stock_map = None
            try:
                if not stock_df.empty:
                    if "workshop" not in stock_df.columns:
                        stock_df["workshop"] = ""
                    stock_map = { (str(r.item_id), str(r.workshop)): float(r.stock_qty) for r in stock_df.itertuples(index=False) }
            except Exception:
                stock_map = None
    else:
        demand = build_demand(plan)
        stock_map = None

    start = None
    if start_date:
        try:
            start = pd.to_datetime(start_date).date()
        except Exception:
            start = None

    # If user requests Standard-Up and we already netted by stock, augment demand with parents only
    if (mode == "standard_up") and did_netting:
        try:
            # minimal BOM slice for expansion
            b2 = bom.copy()
            if "qty_per_parent" not in b2.columns:
                b2["qty_per_parent"] = 1.0
            # seed columns
            need_cols = ["order_id","item_id","due_date","qty","priority"]
            opt_cols = [c for c in ["customer","workshop"] if c in demand.columns]
            seed = demand[[c for c in need_cols if c in demand.columns] + opt_cols].copy()
            exp = expand_demand_with_hierarchy(seed, b2, split_child_orders=True, include_parents=True)
            parents_only = exp[exp.get("role","FG").eq("PARENT")]
            cols = [c for c in ["order_id","item_id","due_date","qty","priority","workshop","customer"] if c in parents_only.columns]
            if not parents_only.empty:
                demand = pd.concat([demand, parents_only[cols]], ignore_index=True)
        except Exception as _e:
            # keep going without parents
            pass

    sched = greedy_schedule(
        demand, bom, machines,
        start_date=start,
        overload_pct=overload_pct,
        split_child_orders=split_child_orders,
        align_roots_to_due=align_roots_to_due,
        stock_map=stock_map,
        include_parents=(mode == "standard_up"),
        expand=(not did_netting),
    )

    export_with_charts._machines_df = machines
    out_file = export_with_charts(sched, Path(out_xlsx), bom=bom)

    # Append netting_log and summary to workbook (standard mode)
    try:
        cols = [
            "item_id","workshop","customer","date","kind",
            "opening_exact","opening_generic",
            "stock_used_exact","stock_used_generic",
            "receipts_used","order_created","available_after",
        ]
        net_log = NETTING_LOG.copy() if NETTING_LOG is not None else pd.DataFrame(columns=cols)
        if "date" in net_log.columns:
            net_log["date"] = pd.to_datetime(net_log["date"]).dt.date
        with pd.ExcelWriter(Path(out_xlsx), engine="openpyxl", mode="a", if_sheet_exists="replace", date_format="yyyy-mm-dd") as xw:
            net_log.to_excel(xw, index=False, sheet_name="netting_log")
            g = net_log.copy()
            gb = ["item_id","workshop"] + (["customer"] if "customer" in g.columns else [])
            if not g.empty and "kind" in g.columns:
                summary = (
                    g[g["kind"]=="day"]
                    .groupby(gb, as_index=False)[
                        ["stock_used_exact","stock_used_generic","receipts_used"]
                    ].sum()
                )
                summary["stock_used_total"] = summary["stock_used_exact"].fillna(0).astype(int) + summary["stock_used_generic"].fillna(0).astype(int)
            else:
                summary = pd.DataFrame(columns=gb + [
                    "stock_used_exact","stock_used_generic","receipts_used","stock_used_total"
                ])
            # Override order_created from residual demand if available in scope
            try:
                dn = demand_net.copy() if 'demand_net' in locals() else None
                if dn is not None and not dn.empty:
                    if "workshop" not in dn.columns:
                        dn["workshop"] = ""
                    gb2 = ["item_id","workshop"] + (["customer"] if "customer" in dn.columns else [])
                    orders_summary = dn.groupby(gb2, as_index=False)["qty"].sum().rename(columns={"qty":"order_created"})
                    summary = (
                        summary.drop(columns=["order_created"], errors="ignore")
                               .merge(orders_summary, on=gb, how="outer")
                               .fillna({"stock_used_exact":0, "stock_used_generic":0, "receipts_used":0, "stock_used_total":0, "order_created":0})
                    )
            except Exception:
                pass
            summary.to_excel(xw, index=False, sheet_name="netting_summary")
    except Exception as e:
        print("[NETTING] append failed:", e)
    return out_file, sched


def run_greedy(*args, **kwargs):
    """
    Backward-compatible wrapper for run_pipeline with extra 'mode' passthrough.
    """
    # Support old positional signature
    save_to_plan_id = kwargs.pop("save_to_plan_id", None)
    db = kwargs.pop("db", None) or kwargs.pop("session", None)

    def _looks_like_session(x):
        return hasattr(x, "execute") or (hasattr(x, "add") and hasattr(x, "commit"))

    pos = list(args)
    if pos and _looks_like_session(pos[0]):
        db = db or pos.pop(0)

    ordered_names = [
        "plan_path", "bom_path", "machines_path", "out_xlsx", "stock_path",
        "start_date", "overload_pct", "split_child_orders", "align_roots_to_due",
        "guard_limit_days",
    ]
    for i, name in enumerate(ordered_names):
        if i < len(pos) and name not in kwargs:
            kwargs[name] = pos[i]

    # Defaults
    plan_path          = kwargs.get("plan_path")          or "plan of sales.xlsx"
    bom_path           = kwargs.get("bom_path")           or "BOM.xlsx"
    machines_path      = kwargs.get("machines_path")      or "machines.xlsx"
    out_xlsx           = kwargs.get("out_xlsx")           or "schedule_out.xlsx"
    start_date         = kwargs.get("start_date")         or None
    stock_path         = kwargs.get("stock_path")         or "stock_path.xlsx"
    overload_pct       = float(kwargs.get("overload_pct") or 0.0)
    split_child_orders = bool(kwargs.get("split_child_orders") or False)
    align_roots_to_due = bool(kwargs.get("align_roots_to_due") or False)
    mode               = (kwargs.get("mode") or "").lower().strip()

    out_file, sched = run_pipeline(
        plan_path, bom_path, machines_path, out_xlsx,
        stock_path=stock_path,
        start_date=start_date,
        overload_pct=overload_pct,
        split_child_orders=split_child_orders,
        align_roots_to_due=align_roots_to_due,
        mode=mode,
    )

    # Optional DB save path omitted here to keep merged file simple; can be re-added if needed.
    return out_file, sched

# Final override: ensure modular loaders are used
load_plan_of_sales = _L_load_plan_of_sales
load_bom = _L_load_bom
load_machines = _L_load_machines
load_stock_any = _L_load_stock_any

# Drop dead/legacy loader symbols from namespace to avoid accidental use
try:
    del _dead_load_plan_of_sales
except Exception:
    pass
try:
    del _dead_load_bom
except Exception:
    pass
try:
    del _dead_load_machines
except Exception:
    pass
try:
    del _dead_load_stock_any
except Exception:
    pass
try:
    del _legacy_load_machines
except Exception:
    pass


# --- Overrides added to support 'customer' on plan/netting and arbitrary item ids ---
def build_demand(plan_df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
    """Aggregate demand.

    If a 'customer' column exists in plan_df, group by it and keep it in the
    output; order_id will use customer as prefix when present, otherwise item_id.
    """
    group_keys = ["item_id", "due_date"] + (["customer"] if "customer" in plan_df.columns else [])
    g = plan_df.groupby(group_keys, as_index=False).agg(qty=("qty", "sum"))
    sort_keys = ["due_date"] + (["customer"] if "customer" in g.columns else []) + ["item_id"]
    g = g.sort_values(sort_keys, kind="stable").reset_index(drop=True)

    from collections import defaultdict
    seq: dict[tuple[str, object], int] = defaultdict(int)
    order_ids: list[str] = []
    for _, r in g.iterrows():
        base = str(r.get("customer", "") or r["item_id"])
        key = (base, r["due_date"])  # sequence per (customer|item, date)
        seq[key] = seq.get(key, 0) + 1
        oid = f"{base}-{pd.to_datetime(r['due_date']).strftime('%Y%m%d')}-{seq[key]:04d}"
        order_ids.append(oid)
    g["order_id"] = order_ids
    g["priority"] = pd.to_datetime(g["due_date"])  # default priority
    cols = ["order_id", "item_id", "due_date", "qty", "priority"]
    if "customer" in g.columns:
        cols.append("customer")
    return g[cols]


def expand_demand_with_hierarchy(demand: pd.DataFrame, bom: pd.DataFrame, *, split_child_orders: bool = False, include_parents: bool = False) -> pd.DataFrame:
    # Build parent and children maps from BOM
    parents: dict[str, str] = {}
    children_map: dict[str, dict[str, float]] = {}
    for r in bom.itertuples(index=False):
        p = r.root_item_id; c = r.item_id
        parents[str(c)] = str(p)
        if p and p != c:
            children_map.setdefault(str(p), {})[str(c)] = float(getattr(r, "qty_per_parent", 1.0)) or 1.0

    def ancestors(x: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        cur = x
        for _ in range(100000):
            p = parents.get(cur, "")
            if not p or p in seen or p == cur:
                break
            out.append(p)
            seen.add(p)
            cur = p
        return out

    def descendants_with_factor(x: str) -> list[tuple[str, float]]:
        out: list[tuple[str, float]] = []
        stack: list[tuple[str, float]] = [(x, 1.0)]
        seen: set[str] = {x}
        while stack:
            cur, f = stack.pop()
            for ch, r in (children_map.get(cur, {}) or {}).items():
                if ch in seen:
                    continue
                f_new = f * (r if np.isfinite(r) and r > 0 else 1.0)
                out.append((ch, f_new))
                seen.add(ch)
                stack.append((ch, f_new))
        return out

    rows: list[dict[str, object]] = []
    for r in demand.itertuples(index=False):
        base_oid = str(r.order_id)
        it = str(r.item_id)
        due = r.due_date
        qty = int(r.qty)
        pr = r.priority
        cust = getattr(r, "customer", None)

        rows.append({
            "base_order_id": base_oid,
            "order_id": (f"{base_oid}:{it}" if split_child_orders else base_oid),
            "item_id": it,
            "due_date": due,
            "qty": qty,
            "priority": pr,
            "role": "FG",
            "customer": (str(cust) if cust is not None else None),
        })
        if include_parents:
            for a in ancestors(it):
                rows.append({
                    "base_order_id": base_oid,
                    "order_id": (f"{base_oid}:{a}" if split_child_orders else base_oid),
                    "item_id": a,
                    "due_date": due,
                    "qty": qty,
                    "priority": pr,
                    "role": "PARENT",
                    "customer": (str(cust) if cust is not None else None),
                })
        for d, fmul in descendants_with_factor(it):
            rows.append({
                "base_order_id": base_oid,
                "order_id": (f"{base_oid}:{d}" if split_child_orders else base_oid),
                "item_id": d,
                "due_date": due,
                "qty": int(round(qty * float(fmul))),
                "priority": pr,
                "role": "CHILD",
                "customer": (str(cust) if cust is not None else None),
            })

    exp = pd.DataFrame(rows)
    if exp.empty:
        raise ValueError("Greedy: expanded_demand is empty (check BOM/qty_per_parent)")
    links = build_bom_hierarchy(bom)
    depth_map = {r.item_id: int(r.level) for r in links.itertuples(index=False)} if not links.empty else {}
    exp["depth"] = exp["item_id"].map(depth_map).fillna(0).astype(int)
    exp = exp.sort_values(["priority", "depth", "item_id"], kind="stable").reset_index(drop=True)
    return exp


def product_view_generate_demand(  # type: ignore[override]
    plan_df: pd.DataFrame,
    bom: pd.DataFrame,
    stock_df: pd.DataFrame | None = None,
    existing_orders_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Time-phased netting in two phases to avoid over-planning children.

    Phase 1: net only FG (plan items) -> get residual FG orders.
    Phase 2: expand residual FG orders via BOM to children and net them.
    """
    def C(df, name):
        m = {str(c).lower(): c for c in df.columns}
        return m.get(name.lower(), name)

    need = ["item_id", "due_date", "qty"]
    miss = [n for n in need if C(plan_df, n) not in plan_df.columns]
    if miss:
        raise ValueError("plan_df must contain item_id, due_date, qty")

    base_plan = (plan_df
                 .rename(columns={C(plan_df, "item_id"): "item_id",
                                  C(plan_df, "due_date"): "due_date",
                                  C(plan_df, "qty"): "qty"})
                 .copy())
    base_plan["item_id"] = base_plan["item_id"].astype(str).str.strip()
    base_plan["due_date"] = pd.to_datetime(base_plan["due_date"]).dt.date
    base_plan["qty"] = pd.to_numeric(base_plan["qty"], errors="coerce").fillna(0).astype(int)
    if C(plan_df, "customer") in plan_df.columns:
        base_plan["customer"] = plan_df[C(plan_df, "customer")].astype(str).fillna("").str.strip()

    # Default workshop per item (optional)
    item_workshop: dict[str, str] = {}
    if "workshop" in bom.columns:
        bw = bom[["item_id", "workshop"]].dropna().drop_duplicates("item_id")
        item_workshop = dict(zip(bw["item_id"].astype(str), bw["workshop"].astype(str)))

    # Build stock and receipts maps
    if stock_df is None:
        stock_df = pd.DataFrame(columns=["item_id", "stock_qty", "workshop", "customer"])
    s = stock_df.copy()
    if "workshop" not in s.columns:
        s["workshop"] = ""
    s["item_id"] = s["item_id"].astype(str).str.strip()
    s["workshop"] = s["workshop"].astype(str).fillna("")
    if "customer" not in s.columns:
        s["customer"] = ""
    else:
        s["customer"] = s["customer"].astype(str).fillna("").str.strip()
    s["stock_qty"] = pd.to_numeric(s["stock_qty"], errors="coerce").fillna(0).astype(int)
    # Exact per (item,workshop,customer)
    stock_exact = (s.groupby(["item_id", "workshop", "customer"], as_index=False)["stock_qty"].sum()
                     .set_index(["item_id", "workshop", "customer"])["stock_qty"].to_dict())
    # Global stock pool per (item,customer) (sum across all workshops)
    stock_pool = (s.groupby(["item_id", "customer"], as_index=False)["stock_qty"].sum()
                    .set_index(["item_id", "customer"])["stock_qty"].to_dict())

    if existing_orders_df is None:
        existing_orders_df = pd.DataFrame(columns=["item_id", "due_date", "qty", "workshop"])
    rec = existing_orders_df.copy()
    if "workshop" not in rec.columns:
        rec["workshop"] = ""
    rec["item_id"] = rec["item_id"].astype(str).str.strip()
    rec["workshop"] = rec["workshop"].astype(str).fillna("")
    rec["due_date"] = pd.to_datetime(rec["due_date"]).dt.date
    rec["qty"] = pd.to_numeric(rec["qty"], errors="coerce").fillna(0).astype(int)
    receipts = (rec.groupby(["item_id", "workshop", "due_date"], as_index=False)["qty"].sum()
                  .sort_values(["item_id", "workshop", "due_date"]))

    # Helper: netting for a demand table grouped by (item_id, workshop, due_date[, customer])
    def net_pass(dem_df: pd.DataFrame) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        out: list[dict[str, object]] = []
        logs: list[dict[str, object]] = []
        seq: dict[tuple[str, object], int] = {}
        outer_keys = ["item_id", "workshop"] + (["customer"] if "customer" in dem_df.columns else [])
        for key_vals, block in dem_df.groupby(outer_keys):
            if isinstance(key_vals, tuple):
                it = str(key_vals[0]); wk = str(key_vals[1]); cust = (str(key_vals[2]) if len(key_vals) > 2 else "")
            else:
                it = str(key_vals); wk = ""; cust = ""
            block = block.sort_values("due_date")

            # Available exact for this workshop is limited by customer-specific global pool
            pool = float(stock_pool.get((it, cust), 0.0))
            avail_exact = min(float(stock_exact.get((it, wk, cust), 0.0)), pool)
            receipts_remain = 0.0

            logs.append({
                "item_id": it, "workshop": wk, "customer": cust, "date": None, "kind": "opening",
                "opening_exact": int(avail_exact), "opening_generic": int(max(0.0, pool - avail_exact)),
                "stock_used_exact": 0, "stock_used_generic": 0,
                "receipts_used": 0, "order_created": 0, "available_after": int(pool)
            })

            rb = receipts[(receipts["item_id"] == it) & (receipts["workshop"] == wk)].sort_values("due_date")
            r_dates = rb["due_date"].tolist(); r_qtys = rb["qty"].tolist(); ridx = 0

            for r in block.itertuples(index=False):
                dd, need = r.due_date, int(r.qty)
                while ridx < len(r_dates) and r_dates[ridx] <= dd:
                    receipts_remain += float(r_qtys[ridx]); ridx += 1

                # Use exact first (bounded by pool), then global pool as generic
                pool = float(stock_pool.get((it, cust), 0.0))
                take_exact_cap = min(float(stock_exact.get((it, wk, cust), 0.0)), pool)
                stock_used_exact = min(need, int(take_exact_cap))
                need -= stock_used_exact
                stock_pool[(it, cust)] = max(0.0, pool - stock_used_exact)

                stock_used_generic = 0
                if need > 0 and stock_pool.get((it, cust), 0.0) > 0:
                    gen_avail = float(stock_pool.get((it, cust), 0.0))
                    stock_used_generic = min(need, int(gen_avail))
                    need -= stock_used_generic
                    stock_pool[(it, cust)] = max(0.0, gen_avail - stock_used_generic)

                receipts_used = 0
                if need > 0 and receipts_remain > 0:
                    take = min(need, int(receipts_remain)); receipts_used = take; receipts_remain -= take; need -= take

                order_created = 0
                if need > 0:
                    order_created = need
                    base_prefix = str(cust) if ("customer" in dem_df.columns and str(cust) != "") else str(it)
                    key = (base_prefix, pd.to_datetime(dd).date())
                    seq[key] = seq.get(key, 0) + 1
                    oid = f"{base_prefix}-{pd.to_datetime(dd).strftime('%Y%m%d')}-PV{seq[key]:03d}"
                    out.append({
                        "order_id": oid, "item_id": it, "due_date": dd,
                        "qty": order_created, "priority": pd.to_datetime(dd), "workshop": wk,
                        "customer": str(cust) if ("customer" in dem_df.columns) else "",
                    })
                    need = 0

            available_after = int(max(0.0, float(stock_pool.get((it, cust), 0.0)) + receipts_remain))
            logs.append({
                    "item_id": it, "workshop": wk, "customer": cust, "date": dd, "kind": "day",
                    "opening_exact": None, "opening_generic": None,
                    "stock_used_exact": int(stock_used_exact),
                    "stock_used_generic": int(stock_used_generic),
                    "receipts_used": int(receipts_used),
                    "order_created": int(order_created),
                    "available_after": int(available_after),
                })
        return out, logs

    # Phase 1: net FG (plan-level) demand only
    fg = base_plan.copy()
    fg["workshop"] = fg["item_id"].map(item_workshop).fillna("")
    group_keys = ["item_id", "workshop", "due_date"] + (["customer"] if "customer" in fg.columns else [])
    fg_dem = (fg.groupby(group_keys, as_index=False)["qty"].sum().sort_values(group_keys))
    fg_orders, log_fg = net_pass(fg_dem)
    fg_orders_df = pd.DataFrame(fg_orders, columns=["order_id","item_id","due_date","qty","priority","workshop","customer"])

    # Prepare BOM as parent->child with ratios
    b = bom.copy()
    if "root_item_id" not in b.columns:
        b["root_item_id"] = ""
    b["root_item_id"] = b["root_item_id"].astype(str).str.strip()
    b["item_id"] = b["item_id"].astype(str).str.strip()
    if "qty_per_parent" not in b.columns:
        b["qty_per_parent"] = 1.0
    b["qty_per_parent"] = pd.to_numeric(b["qty_per_parent"], errors="coerce").fillna(1.0).astype(float)

    # Phase 2: expand residual FG orders to children and net them
    if not fg_orders_df.empty:
        exp2 = expand_demand_with_hierarchy(fg_orders_df, b, split_child_orders=True, include_parents=False)
        exp2["workshop"] = exp2["item_id"].map(item_workshop).fillna("")
        dem_children = exp2[exp2.get("role","CHILD").eq("CHILD")].copy()
        if dem_children.empty:
            child_orders = []
            log_child = []
        else:
            group_keys2 = ["item_id","workshop","due_date"] + (["customer"] if "customer" in dem_children.columns else [])
            dem_children = (dem_children.groupby(group_keys2, as_index=False)["qty"].sum().sort_values(group_keys2))
            child_orders, log_child = net_pass(dem_children)
    else:
        child_orders = []
        log_child = []

    # Combine
    all_orders = fg_orders + child_orders
    order_cols = ["order_id", "item_id", "due_date", "qty", "priority", "workshop", "customer"]
    orders_df = pd.DataFrame(all_orders, columns=order_cols)

    # Logs
    global NETTING_LOG
    NETTING_LOG = pd.DataFrame(log_fg + log_child, columns=[
        "item_id","workshop","customer","date","kind",
        "opening_exact","opening_generic",
        "stock_used_exact","stock_used_generic",
        "receipts_used","order_created","available_after",
    ])

    return orders_df


def _ensure_netting_tables(db: Session) -> None:  # type: ignore[override]
    """Ensure minimal netting tables exist; add missing 'customer' column if needed."""
    # base tables (keep simple; SQLite dialect)
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS plan_version (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT,
        horizon_start DATE,
        horizon_end DATE,
        notes TEXT,
        origin TEXT
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS plan_line (
        id INTEGER PRIMARY KEY,
        plan_version_id INTEGER NOT NULL,
        item_id TEXT NOT NULL,
        due_date DATE NOT NULL,
        qty INTEGER NOT NULL,
        priority DATETIME NULL,
        customer TEXT NULL,
        workshop TEXT NULL,
        source_tag TEXT NULL
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS receipts_plan (
        id INTEGER PRIMARY KEY,
        plan_version_id INTEGER NOT NULL,
        item_id TEXT NOT NULL,
        due_date DATE NOT NULL,
        qty INTEGER NOT NULL,
        workshop TEXT NULL,
        receipt_type TEXT,
        source_ref TEXT NULL
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS stock_snapshot (
        id INTEGER PRIMARY KEY,
        name TEXT,
        taken_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        notes TEXT
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS stock_line (
        id INTEGER PRIMARY KEY,
        snapshot_id INTEGER NOT NULL,
        item_id TEXT NOT NULL,
        workshop TEXT DEFAULT '',
        stock_qty INTEGER NOT NULL
    );
    """))

    db.execute(text("""
    CREATE TABLE IF NOT EXISTS netting_run (
        id INTEGER PRIMARY KEY,
        started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        finished_at DATETIME,
        user TEXT,
        mode TEXT,
        plan_version_id INTEGER,
        stock_snapshot_id INTEGER,
        bom_version_id TEXT,
        receipts_source_desc TEXT,
        params TEXT,
        status TEXT
    );
    """))
    db.execute(text("""
    CREATE TABLE IF NOT EXISTS netting_order (
        id INTEGER PRIMARY KEY,
        netting_run_id INTEGER NOT NULL,
        order_id TEXT,
        item_id TEXT,
        due_date DATE,
        qty INTEGER,
        priority DATETIME NULL,
        workshop TEXT NULL,
        customer TEXT NULL
    );
    """))

    # add customer column if missing (for already created tables)
    try:
        cols = db.execute(text("PRAGMA table_info(netting_order)")).mappings().all()
        names = {str(r.name) if hasattr(r, "name") else str(r[1]) for r in cols}  # support SQLite row shapes
        if "customer" not in names:
            db.execute(text("ALTER TABLE netting_order ADD COLUMN customer TEXT"))
    except Exception:
        pass

    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_plan_line_main ON plan_line(plan_version_id,item_id,due_date);
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_receipts_plan_main ON receipts_plan(plan_version_id,item_id,due_date);
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_stock_line_main ON stock_line(snapshot_id,item_id,workshop);
    """))
    db.execute(text("""
    CREATE INDEX IF NOT EXISTS ix_netting_order ON netting_order(netting_run_id,item_id,due_date);
    """))
    db.commit()


def _save_netting_results_to_db(  # type: ignore[override]
    db: Session,
    run_meta: dict,
    demand_net: pd.DataFrame,
    netting_log: pd.DataFrame,
    netting_summary: pd.DataFrame,
    linkage_df: pd.DataFrame | None = None,
) -> int:
    ins = text("""
      INSERT INTO netting_run (started_at, finished_at, user, mode, plan_version_id,
                               stock_snapshot_id, bom_version_id, receipts_source_desc, params, status)
      VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :user, :mode, :plan_version_id,
              :stock_snapshot_id, :bom_version_id, :receipts_source_desc, :params, 'done')
      RETURNING id
    """)
    rid = db.execute(
        ins,
        {
            "user": run_meta.get("user", "ui"),
            "mode": "product_view",
            "plan_version_id": run_meta.get("plan_version_id"),
            "stock_snapshot_id": run_meta.get("stock_snapshot_id"),
            "bom_version_id": run_meta.get("bom_version_id", ""),
            "receipts_source_desc": run_meta.get("receipts_source_desc", "plan"),
            "params": json.dumps(run_meta.get("params", {}), ensure_ascii=False),
        },
    ).scalar_one()

    if not demand_net.empty:
        payload = demand_net.copy()
        for col in ("order_id", "item_id", "workshop", "customer"):
            if col not in payload.columns:
                payload[col] = ""
        payload["priority"] = pd.to_datetime(payload["priority"]).apply(lambda x: x.to_pydatetime())
        rows = [
            {
                "order_id": str(r.order_id),
                "item_id": str(r.item_id),
                "due_date": r.due_date,  # already date
                "qty": int(r.qty),
                "priority": (str(r.priority) if getattr(r, "priority", None) is not None else None),
                "workshop": str(getattr(r, "workshop", "") or ""),
                "customer": str(getattr(r, "customer", "") or ""),
            }
            for r in payload.itertuples(index=False)
        ]
        db.execute(text("""
            INSERT INTO netting_order (netting_run_id, order_id, item_id, due_date, qty, priority, workshop, customer)
            VALUES (:rid, :order_id, :item_id, :due_date, :qty, :priority, :workshop, :customer)
        """), [dict(r, rid=rid) for r in rows])

    if not netting_log.empty:
        _log = netting_log.copy()
        try:
            import pandas as _pd
            if "date" in _log.columns:
                _log["date"] = _pd.to_datetime(_log["date"], errors="coerce").dt.date
                _log["date"] = _log["date"].where(_log["date"].notna(), None)
            for c in [
                "opening_exact","opening_generic",
                "stock_used_exact","stock_used_generic",
                "receipts_used","order_created","available_after",
            ]:
                if c in _log.columns:
                    _log[c] = _pd.to_numeric(_log[c], errors="coerce")
                    _log[c] = _log[c].where(_log[c].notna(), None)
        except Exception:
            pass
        rows = _log.to_dict("records")
        db.execute(text("""
            INSERT INTO netting_log_row
            (netting_run_id, item_id, workshop, date, kind,
             opening_exact, opening_generic,
             stock_used_exact, stock_used_generic,
             receipts_used, order_created, available_after)
            VALUES
            (:rid, :item_id, :workshop, :date, :kind,
             :opening_exact, :opening_generic,
             :stock_used_exact, :stock_used_generic,
             :receipts_used, :order_created, :available_after)
        """), [dict(r, rid=rid) for r in rows])

    if not netting_summary.empty:
        rows = netting_summary.to_dict("records")
        db.execute(text("""
            INSERT INTO netting_summary_row
            (netting_run_id, item_id, workshop,
             stock_used_total, receipts_used_total, orders_created_total,
             opening_exact_init, opening_generic_init)
            VALUES
            (:rid, :item_id, :workshop,
             :stock_used_total, :receipts_used_total, :orders_created_total,
             :opening_exact_init, :opening_generic_init)
        """), [dict(r, rid=rid) for r in rows])

    if linkage_df is not None and not linkage_df.empty:
        rows = linkage_df.to_dict("records")
        db.execute(text("""
            INSERT INTO demand_linkage
            (netting_run_id, parent_item_id, parent_due_date,
             child_item_id, child_due_date, qty_per_parent, required_qty)
            VALUES
            (:rid, :parent_item_id, :parent_due_date,
             :child_item_id, :child_due_date, :qty_per_parent, :required_qty)
        """), [dict(r, rid=rid) for r in rows])

    db.commit()
    return int(rid)
