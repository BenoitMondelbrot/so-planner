"""Loaders used by greedy scheduling.

This module now contains the actual implementations originally defined
in the monolithic `greedy_scheduler.py` to avoid duplication and enable
modular imports.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


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


def _clean_text(v: Any) -> str:
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def load_plan_of_sales(path: Path) -> pd.DataFrame:
    """Read wide plan-of-sales and return normalized long-form DataFrame.

    Output columns: item_id, due_date, qty, priority[, customer]
    If a "customer" column is present in the source sheet, it is preserved
    and replicated to the long-form rows and returned as a string column.
    """
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    id_candidates = ["article", "item_id", "item"]
    item_col = None
    for c in id_candidates:
        if _norm_col(c) in norm:
            item_col = norm[_norm_col(c)]
            break
    if item_col is None:
        item_col = df.columns[0]

    # optional customer column (keep as string)
    customer_col = None
    for c in ("customer",):
        if _norm_col(c) in norm:
            customer_col = norm[_norm_col(c)]
            break

    date_cols = []
    for c in df.columns:
        if c == item_col or (customer_col is not None and c == customer_col):
            continue
        d = _as_date(c)
        if pd.notna(d):
            date_cols.append(c)
    if not date_cols:
        raise ValueError(f"No date-like columns found. Got: {list(df.columns)}")

    id_vars = [item_col] + ([customer_col] if customer_col is not None else [])
    long_df = df.melt(id_vars=id_vars, value_vars=date_cols, var_name="due_date", value_name="qty")
    long_df["due_date"] = pd.to_datetime(long_df["due_date"], errors="coerce").dt.date
    long_df.rename(columns={item_col: "item_id"}, inplace=True)
    long_df["item_id"] = long_df["item_id"].astype(str).str.strip()
    if customer_col is not None:
        long_df.rename(columns={customer_col: "customer"}, inplace=True)
        long_df["customer"] = long_df["customer"].astype(str).fillna("").str.strip()
    long_df["qty"] = long_df["qty"].apply(_ensure_positive_int)
    long_df = long_df[(long_df["qty"] > 0) & long_df["item_id"].ne("") & long_df["due_date"].notna()].copy()
    long_df["priority"] = pd.to_datetime(long_df["due_date"])  # default priority by due_date
    sort_cols = ["priority", "item_id"]
    if "customer" in long_df.columns:
        sort_cols = ["priority", "customer", "item_id"]
    long_df.sort_values(sort_cols, inplace=True, kind="stable")
    long_df.reset_index(drop=True, inplace=True)
    return long_df


def load_bom_article_name_map(path: Path) -> dict[str, str]:
    """Read BOM and return item_id -> article_name mapping (best-effort)."""
    try:
        df = pd.read_excel(path, sheet_name=0, dtype=object)
    except Exception:
        return {}

    norm = {_norm_col(c): c for c in df.columns}
    item_col = None
    for cand in ("article", "item_id", "item"):
        if _norm_col(cand) in norm:
            item_col = norm[_norm_col(cand)]
            break
    if item_col is None:
        return {}

    name_col = None
    for cand in ("article name", "article_name", "item name", "item_name", "name"):
        if _norm_col(cand) in norm:
            name_col = norm[_norm_col(cand)]
            break
    if name_col is None:
        return {}

    tmp = df[[item_col, name_col]].copy()
    tmp[item_col] = tmp[item_col].apply(_clean_text)
    tmp[name_col] = tmp[name_col].apply(_clean_text)
    tmp = tmp[(tmp[item_col] != "") & (tmp[name_col] != "")]
    tmp = tmp.drop_duplicates(subset=[item_col])
    return dict(zip(tmp[item_col], tmp[name_col]))


def load_bom(path: Path) -> pd.DataFrame:
    """Normalize BOM to a canonical schema.

    Returns columns: item_id, step, machine_id, time_per_unit (min),
    setup_minutes (min), root_item_id, optional: workshop, qty_per_parent, article_name
    """
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    def has(x: str) -> bool:
        return _norm_col(x) in norm

    def col(x: str) -> str:
        return norm[_norm_col(x)]

    name_col = None
    for cand in ("article name", "article_name", "item name", "item_name", "name"):
        if _norm_col(cand) in norm:
            name_col = norm[_norm_col(cand)]
            break

    # Schema B: article + machine id + {machine/human} time
    if has("article") and (has("machineid") or has("machine id")):
        out = pd.DataFrame()
        out["item_id"] = df[col("article")].astype(str).str.strip()
        out["step"] = pd.to_numeric(df[col("operations")], errors="coerce").fillna(1).astype(int) if has("operations") else 1
        mid_col = col("machineid") if has("machineid") else col("machine id")
        out["machine_id"] = df[mid_col].astype(str).str.strip()
        if has("machinetime"):
            h = pd.to_numeric(df[col("machinetime")], errors="coerce").fillna(0).astype(float)
        elif has("humantime"):
            h = pd.to_numeric(df[col("humantime")], errors="coerce").fillna(0).astype(float)
        else:
            raise ValueError("BOM: missing machine/human time")
        out["time_per_unit"] = h * 60.0
        if has("settingtime") or has("setting time"):
            sc = col("settingtime") if has("settingtime") else col("setting time")
            out["setup_minutes"] = pd.to_numeric(df[sc], errors="coerce").fillna(0).astype(float) * 60.0
        else:
            out["setup_minutes"] = 0.0
        if has("rootarticle") or has("root article"):
            rac = col("rootarticle") if has("rootarticle") else col("root article")
            out["root_item_id"] = df[rac].astype(str).str.strip().replace({"nan": "", "None": ""})
        else:
            out["root_item_id"] = ""
        if has("workshop"):
            out["workshop"] = df[col("workshop")].astype(str).str.strip()
        else:
            out["workshop"] = ""
        if has("qty_per_parent") or has("qtyperparent"):
            qcol = col("qty_per_parent") if has("qty_per_parent") else col("qtyperparent")
            out["qty_per_parent"] = pd.to_numeric(df[qcol], errors="coerce").fillna(1.0).astype(float)
        else:
            out["qty_per_parent"] = 1.0
        lag_col = None
        for cand in ("lag time", "lag_time", "lag", "lagdays", "lag day", "lag_days"):
            if has(cand):
                lag_col = col(cand)
                break
        if lag_col is not None:
            out["lag_days"] = pd.to_numeric(df[lag_col], errors="coerce").fillna(0.0).astype(int)
        else:
            out["lag_days"] = 0
        if name_col is not None:
            out["article_name"] = df[name_col].apply(_clean_text)
        out = out.sort_values(["item_id", "step"], kind="stable").reset_index(drop=True)
        ret_cols = ["item_id","step","machine_id","time_per_unit","setup_minutes","root_item_id"]
        if "workshop" in out.columns: ret_cols.append("workshop")
        if "qty_per_parent" in out.columns: ret_cols.append("qty_per_parent")
        if "lag_days" in out.columns: ret_cols.append("lag_days")
        if "article_name" in out.columns: ret_cols.append("article_name")
        return out[ret_cols]

    # Schema A: already close to canonical — rename columns
    rename_map = {}
    candidates = {
        "item_id": ["item_id", "item", "article"],
        "step": ["step", "operations", "operationseq", "opseq", "seq", "sequence"],
        "machine_id": ["machine_id", "machine", "resource", "machine id"],
        "time_per_unit": ["time_per_unit", "proc_time", "duration", "minutes_per_unit", "machine time"],
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
        raise ValueError(f"BOM: missing {missing}. Got: {list(df.columns)}")

    if "step" not in df.columns:
        df["step"] = 1

    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["step"] = pd.to_numeric(df["step"], errors="coerce").fillna(1).astype(int)
    df["machine_id"] = df["machine_id"].astype(str).str.strip()
    df["machine_id"] = df["machine_id"].str.replace(r"\.0$", "", regex=True).str.replace(r"\s+", " ", regex=True)

    df["time_per_unit"] = pd.to_numeric(df["time_per_unit"], errors="coerce").fillna(0).astype(float)
    if df["time_per_unit"].median() < 1.0:
        df["time_per_unit"] *= 60.0

    # setup
    setup_series = None
    for cand in ["setup_minutes", "setting_time", "setup", "setting time"]:
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

    if name_col is not None:
        df["article_name"] = df[name_col].apply(_clean_text)

    # optional workshop/qty_per_parent
    if _norm_col("workshop") in norm:
        df["workshop"] = df[norm[_norm_col("workshop")]].astype(str).str.strip()
    else:
        df["workshop"] = ""
    if _norm_col("qty_per_parent") in norm or _norm_col("qtyperparent") in norm:
        key = norm[_norm_col("qty_per_parent")] if _norm_col("qty_per_parent") in norm else norm[_norm_col("qtyperparent")]
        df["qty_per_parent"] = pd.to_numeric(df[key], errors="coerce").fillna(1.0).astype(float)
    else:
        df["qty_per_parent"] = 1.0
    lag_key = None
    for cand in ("lag time", "lag_time", "lag", "lagdays", "lag day", "lag_days"):
        if _norm_col(cand) in norm:
            lag_key = norm[_norm_col(cand)]
            break
    if lag_key is not None:
        df["lag_days"] = pd.to_numeric(df[lag_key], errors="coerce").fillna(0.0).astype(int)
    else:
        df["lag_days"] = 0

    df = df.sort_values(["item_id", "step"], kind="stable").reset_index(drop=True)
    ret_cols = ["item_id","step","machine_id","time_per_unit","setup_minutes","root_item_id"]
    if "workshop" in df.columns: ret_cols.append("workshop")
    if "qty_per_parent" in df.columns: ret_cols.append("qty_per_parent")
    if "lag_days" in df.columns: ret_cols.append("lag_days")
    if "article_name" in df.columns: ret_cols.append("article_name")
    return df[ret_cols]


def load_machines(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    def has(x: str) -> bool: return _norm_col(x) in norm
    def col(x: str) -> str:  return norm[_norm_col(x)]

    # machine_id
    if has("machine_id"): df = df.rename(columns={col("machine_id"): "machine_id"})
    elif has("machineid"): df = df.rename(columns={col("machineid"): "machine_id"})
    elif has("machine id"): df = df.rename(columns={col("machine id"): "machine_id"})
    else: raise ValueError("machines: missing machine id / machine_id.")

    df["machine_id"] = df["machine_id"].astype(str).str.strip()
    df["machine_id"] = df["machine_id"].str.replace(r"\.0$", "", regex=True).str.replace(r"\s+", " ", regex=True)

    # capacity_per_day in minutes
    if "capacity_per_day" in df.columns:
        df["capacity_per_day"] = pd.to_numeric(df["capacity_per_day"], errors="coerce").fillna(0.0)
        df["capacity_per_day"] = df["capacity_per_day"] * 60.0
    elif "count" in df.columns and ("available time" in df.columns or "available_time" in df.columns):
        at_col = "available time" if "available time" in df.columns else "available_time"
        cnt = pd.to_numeric(df["count"], errors="coerce").fillna(0.0)
        hrs = pd.to_numeric(df[at_col], errors="coerce").fillna(0.0)
        df["capacity_per_day"] = (cnt * hrs * 60.0)
    else:
        raise ValueError("machines: no capacity columns found")

    if "capacity_override" in df.columns:
        df["capacity_override"] = pd.to_numeric(df["capacity_override"], errors="coerce").fillna(pd.NA)

    # calendar date
    if has("calendar_date") or has("date"):
        c = col("calendar_date") if has("calendar_date") else col("date")
        df = df.rename(columns={c: "calendar_date"})
        df["calendar_date"] = pd.to_datetime(df["calendar_date"], errors="coerce").dt.date
    if has("capacity_override") or has("override"):
        c = col("capacity_override") if has("capacity_override") else col("override")
        df = df.rename(columns={c: "capacity_override"})
        df["capacity_override"] = pd.to_numeric(df["capacity_override"], errors="coerce").astype(float)

    # overload_pct (0..1 or 0..100%)
    df["overload_pct"] = 0.0
    for oc in ["overload_pct", "overload pct", "overload%"]:
        if has(oc):
            s = pd.to_numeric(df[col(oc)], errors="coerce").fillna(0.0).astype(float)
            df["overload_pct"] = np.where(s > 1.0, s / 100.0, s)
            break

    keep = ["machine_id", "capacity_per_day", "overload_pct"]
    if "calendar_date" in df.columns: keep.append("calendar_date")
    if "capacity_override" in df.columns: keep.append("capacity_override")
    return df[keep]


def load_stock_any(path: Path) -> pd.DataFrame:
    """Read stock Excel with flexible column names -> item_id, stock_qty[, workshop][, customer].
    Supports optional per-workshop and per-customer segmentation. If columns are missing,
    fills them with empty strings to represent generic pools.
    """
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    norm = {_norm_col(c): c for c in df.columns}

    key_opts = [
        "item_id","item","article",
        "артикул","товар","изделие"
    ]
    qty_opts = [
        "stock_qty","qty","quantity","free_stock","on_hand","available",
        "остаток","наличие","свободныйостаток","доступно"
    ]

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
        raise ValueError("stock: missing stock_qty-like column")

    out = pd.DataFrame({
        "item_id": df[key_col].astype(str).str.strip(),
        "stock_qty": pd.to_numeric(df[qty_col], errors="coerce").fillna(0.0).astype(float),
    })
    # Workshop synonyms
    ws_col = None
    for c in ("workshop","цех","участок"):
        if _norm_col(c) in norm:
            ws_col = norm[_norm_col(c)]
            break
    if ws_col is not None:
        out["workshop"] = df[ws_col].astype(str).fillna("")
    else:
        out["workshop"] = ""
    # Optional customer segmentation (support synonyms)
    cust_col = None
    for c in ("customer","client","клиент","заказчик","покупатель"):
        if _norm_col(c) in norm:
            cust_col = norm[_norm_col(c)]
            break
    if cust_col is not None:
        out["customer"] = df[cust_col].astype(str).fillna("").str.strip()
    else:
        out["customer"] = ""
    out = out[out["item_id"].ne("")].copy()
    grp_cols = ["item_id", "workshop", "customer"]
    out = out.groupby(grp_cols, as_index=False)["stock_qty"].sum()
    return out


__all__ = ["load_plan_of_sales", "load_bom", "load_bom_article_name_map", "load_machines", "load_stock_any"]
