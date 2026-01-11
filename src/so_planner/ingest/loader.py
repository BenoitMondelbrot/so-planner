# src/so_planner/ingest/loader.py
from __future__ import annotations

import math
from datetime import datetime, date
from typing import Tuple, Dict, Any, Set

import numpy as np
import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from ..models import DimMachine, BOM, Demand

# ===================== Синонимы заголовков (lowercase) =====================

MACH_SYNONYMS: Dict[str, Set[str]] = {
    "machine_id": {"machine_id", "machine id", "id", "machine", "код", "код_оборудования", "станок id"},
    "name": {"name", "название", "наименование", "оборудование", "станок"},
    "family": {"family", "семейство", "группа", "цех", "линия", "workshop", "shop"},
    "capacity_per_shift": {
        "capacity_per_shift", "capacity", "available time", "available_time",
        "мощность_смена", "мощность/смена", "минут/смену"
    },
    "setup_time": {"setup_time", "setup", "переналадка", "переналадка, мин", "setup (min)"},
    "count": {"count", "кол-во", "machines", "шт"},
}

# Классический BOM и «маршрутка» (article/root article/operations/machine id/времена)
BOM_SYNONYMS: Dict[str, Set[str]] = {
    "item_id": {"item_id", "item", "материал", "изделие", "артикул", "артикул дсе", "product", "article"},
    "component_id": {
        "component_id", "component", "компонент", "сырьё", "сырье",
        "root article", "root_article", "parent", "parent item"
    },
    "qty_per": {"qty_per", "норма", "норматив", "кол-во на ед", "количество на единицу", "qty per"},
    "routing_step": {"routing_step", "этап", "операция", "шаг", "стадия", "operations", "operation"},
    "machine_family": {
        "machine_family", "семейство", "семейство машин", "группа",
        "machine id", "machine_id", "станок", "станок id"
    },
    "proc_time_std": {"proc_time_std", "время_операции", "такт", "нормочасы", "мин/ед"},
    "article_name": {"article name", "article_name", "item name", "item_name"},
}

# Временные колонки — если нет proc_time_std, суммируем их (мин/ед)
BOM_TIME_COLS: Set[str] = {
    "human time", "human_time", "человеко-время", "человеко время",
    "machine time", "machine_time", "машинное время", "машинное-время",
    "setting time", "setting_time", "переналадка", "переналадка, мин",
}

DEMAND_SYNONYMS: Dict[str, Set[str]] = {
    "order_id": {"order_id", "order", "заказ", "номер заказа", "документ"},
    "item_id": {"item_id", "item", "материал", "артикул", "изделие", "article"},
    "due_date": {"due_date", "due", "дата", "срок", "дата отгрузки", "дата_поставки", "дата_сдачи"},
    "qty": {"qty", "количество", "объем", "объём", "шт"},
    "priority": {"priority", "приоритет", "вес"},
    "customer": {"customer", "клиент", "заказчик", "покупатель"},
}

# ===================== Helpers =====================

EXCEL_ORIGIN = pd.Timestamp("1899-12-30")  # origin для Excel serial

def _read_xlsx(path: str) -> pd.DataFrame:
    return pd.read_excel(path, engine="openpyxl")

def _rename_by_synonyms(df: pd.DataFrame, synonyms: dict[str, Set[str]]) -> pd.DataFrame:
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    rename = {}
    for canon, syns in synonyms.items():
        for s in syns:
            if s in lower_map:
                rename[lower_map[s]] = canon
                break
    return df.rename(columns=rename)

# --------------------- Machines ---------------------

def _canonicalize_machines(df: pd.DataFrame) -> pd.DataFrame:
    original_cols = list(df.columns)
    df = _rename_by_synonyms(df, MACH_SYNONYMS)

    if "machine_id" not in df.columns:
        raise ValueError(f"machines: нет обязательной колонки ['machine_id']. Обнаружены: {original_cols}")

    if "name" not in df.columns:
        df["name"] = df["machine_id"].astype(str)

    # capacity_per_shift: если значение <=24 — трактуем как часы, конвертируем в минуты
    if "capacity_per_shift" in df.columns:
        cap = pd.to_numeric(df["capacity_per_shift"], errors="coerce")
        df["capacity_per_shift"] = cap.where(cap > 24, cap * 60.0).fillna(480.0)
    else:
        df["capacity_per_shift"] = 480.0  # дефолт 8 часов

    # count -> разворачиваем строки: mid -> mid_1..mid_n
    if "count" in df.columns:
        df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(1).astype(int).clip(lower=1)
        expanded = []
        for _, r in df.iterrows():
            c = int(r["count"])
            base = str(r["machine_id"])
            for k in range(1, c + 1):
                rr = r.copy()
                rr["machine_id"] = f"{base}_{k}" if c > 1 else base
                expanded.append(rr.drop(labels=["count"]))
        df = pd.DataFrame(expanded)

    if "family" not in df.columns:
        df["family"] = None
    if "setup_time" not in df.columns:
        df["setup_time"] = None

    cols = ["machine_id", "name", "family", "capacity_per_shift", "setup_time"]
    return df.reindex(columns=cols).assign(machine_id=lambda x: x["machine_id"].astype(str))

# --------------------- BOM (classic + routing) ---------------------

def _canonicalize_bom(df: pd.DataFrame) -> pd.DataFrame:
    """
    Поддерживает:
      - Классический BOM: item_id, component_id, qty_per (+ optional route cols)
      - «Маршрутку»: article/root article/operations/machine id/... с суммарным временем
    """
    original_cols = list(df.columns)
    df = _rename_by_synonyms(df, BOM_SYNONYMS)

    # Классический комплект готов?
    has_classic = all(col in df.columns for col in ("item_id", "component_id", "qty_per"))
    if not has_classic:
        # Синтезируем из маршрутки
        if "item_id" not in df.columns:
            raise ValueError(
                f"bom: отсутствует колонка с изделием (например 'article'/'item_id'). Обнаружены: {original_cols}"
            )
        if "component_id" not in df.columns:
            df["component_id"] = df["item_id"]
        if "qty_per" not in df.columns:
            df["qty_per"] = 1

    # routing_step
    if "routing_step" in df.columns:
        df["routing_step"] = pd.to_numeric(df["routing_step"], errors="coerce").fillna(1).astype(int)
    else:
        df["routing_step"] = 1

    # machine_family
    if "machine_family" in df.columns:
        df["machine_family"] = df["machine_family"].astype(str)

    if "article_name" in df.columns:
        df["article_name"] = df["article_name"].astype(str).str.strip()

    # proc_time_std: если нет — суммируем известные time-колонки
    if "proc_time_std" not in df.columns:
        lower_cols = {str(c).strip().lower(): c for c in df.columns}
        time_src_cols = [lower_cols[c] for c in BOM_TIME_COLS if c in lower_cols]
        if time_src_cols:
            tmp = df[time_src_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
            df["proc_time_std"] = tmp.sum(axis=1)

    keep_cols = [c for c in ["item_id", "component_id", "qty_per", "routing_step", "machine_family", "proc_time_std", "article_name"]
                 if c in df.columns]
    required_min = {"item_id", "component_id", "qty_per"}
    missing = sorted([c for c in required_min if c not in keep_cols])
    if missing:
        raise ValueError(f"bom: после нормализации нет обязательных колонок {missing}. Обнаружены: {original_cols}")

    return df[keep_cols]

# --------------------- Demand (TALL + WIDE) ---------------------

def _as_date_from_header(h) -> date | None:
    """Преобразовать заголовок столбца в дату, если возможно.
       Поддерживает Timestamp/date/datetime, строки, Excel-serial (int/float)."""
    # pandas/py datetime
    if isinstance(h, (pd.Timestamp, datetime, date, np.datetime64)):
        try:
            ts = pd.to_datetime(h, errors="coerce")
            return None if pd.isna(ts) else ts.date()
        except Exception:
            return None
    # Excel serial (int/float)
    if isinstance(h, (int, float)) and not (isinstance(h, float) and math.isnan(h)):
        try:
            day = int(h)
            if 1 <= day <= 80000:  # разумный диапазон
                return (EXCEL_ORIGIN + pd.to_timedelta(day, unit="D")).date()
        except Exception:
            pass
    # Строка (в т.ч. локальные форматы)
    if isinstance(h, str):
        s = h.strip()
        if not s:
            return None
        for dayfirst in (True, False):
            try:
                ts = pd.to_datetime(s, errors="raise", dayfirst=dayfirst)
                return ts.date()
            except Exception:
                continue
    return None

def _lift_first_row_as_headers_if_dates(df: pd.DataFrame, id_col_name: str) -> tuple[pd.DataFrame, bool]:
    """Если даты лежат в ПЕРВОЙ строке данных, а заголовки 'Unnamed',
       поднимаем первую строку как заголовки и возвращаем новый df."""
    if df.empty or df.shape[0] < 1:
        return df, False

    first_idx = df.index[0]
    probe = df.loc[first_idx]

    # собрать возможные даты из первой строки
    new_cols: list[object] = [id_col_name]
    date_like_found = False
    for c in df.columns:
        if c == id_col_name:
            continue
        dt_candidate = _as_date_from_header(probe[c])
        if dt_candidate is not None:
            new_cols.append(dt_candidate)
            date_like_found = True
        else:
            new_cols.append(None)

    if not date_like_found:
        return df, False

    # назначим новые имена; None → __junk_i
    final_cols: list[str | date] = []
    junk_i = 1
    for val in new_cols:
        if val is None:
            final_cols.append(f"__junk_{junk_i}")
            junk_i += 1
        else:
            final_cols.append(val)

    df2 = df.iloc[1:].copy()
    df2.columns = final_cols
    return df2, True

def _canonicalize_demand(df: pd.DataFrame) -> pd.DataFrame:
    """
    Поддерживает:
      1) TALL: (item_id, due_date, qty[, ...])
      2) WIDE: 'article'/'item_id' + множество столбцов-дат с qty.
         Даты могут быть: Timestamp, строки, Excel-serial; либо лежать в первой строке данных.
    """
    original_cols = list(df.columns)
    df_ren = _rename_by_synonyms(df, DEMAND_SYNONYMS)

    # ---- Случай 1: уже TALL ----
    if all(c in df_ren.columns for c in ("item_id", "due_date", "qty")):
        df_ren["due_date"] = pd.to_datetime(df_ren["due_date"], errors="coerce").dt.date
        df_ren["qty"] = pd.to_numeric(df_ren["qty"], errors="coerce")
        df_ren = df_ren.dropna(subset=["item_id", "due_date", "qty"])
        df_ren = df_ren[df_ren["qty"] > 0]
        keep = [c for c in ["order_id", "item_id", "due_date", "qty", "priority", "customer"] if c in df_ren.columns]
        if not keep:
            keep = ["item_id", "due_date", "qty"]
        return df_ren[keep]

    # ---- Случай 2: WIDE ----
    # обеспечим item_id (через article → item_id)
    if "item_id" not in df_ren.columns:
        candidates = [c for c in df.columns if str(c).strip().lower() in {"article", "изделие", "артикул", "item"}]
        if candidates:
            df_ren = df_ren.rename(columns={candidates[0]: "item_id"})
        else:
            raise ValueError(
                f"demand: не найден идентификатор изделия (ожидали 'item_id' или 'article'). Обнаружены: {original_cols}"
            )

    # 2.1 Даты в заголовках?
    date_cols: list[object] = []
    date_name_map: dict[object, date] = {}
    for c in df_ren.columns:
        if c == "item_id":
            continue
        d = _as_date_from_header(c)
        if d is not None:
            date_cols.append(c)
            date_name_map[c] = d

    # 2.2 Если не нашли — возможно, даты лежат в первой строке данных
    if not date_cols:
        df_try, lifted = _lift_first_row_as_headers_if_dates(df_ren, "item_id")
        if lifted:
            df_ren = df_try
            # пересоберём карту дат
            for c in df_ren.columns:
                if c == "item_id" or (isinstance(c, str) and c.startswith("__junk_")):
                    continue
                if isinstance(c, date):
                    date_cols.append(c)
                    date_name_map[c] = c

    if not date_cols:
        raise ValueError(
            "demand: не найдены столбцы-даты. "
            "Поддерживаются: реальные datetime заголовки, Excel serial (числа), строки (парсимые как даты), "
            "или даты в первой строке данных."
        )

    # item_id → str
    df_ren["item_id"] = df_ren["item_id"].astype(str)

    # отфильтруем только item_id + дата-столбцы
    keep_cols = ["item_id"] + [c for c in df_ren.columns if c in date_name_map]
    work = df_ren[keep_cols].copy()

    # melt → long
    long = work.melt(id_vars=["item_id"], var_name="__due_col", value_name="qty")

    # var_name → реальная дата
    def _map_due(v):
        if v in date_name_map:
            return date_name_map[v]
        if isinstance(v, date):
            return v
        return _as_date_from_header(v)

    long["due_date"] = long["__due_col"].apply(_map_due)
    long = long.drop(columns=["__due_col"])

    # qty → число; фильтрация NaN/<=0
    long["qty"] = pd.to_numeric(long["qty"], errors="coerce")
    long = long.dropna(subset=["due_date", "qty"])
    long = long[long["qty"] > 0]

    # сортировка
    long = long.sort_values(["item_id", "due_date"]).reset_index(drop=True)

    # order_id отсутствует — генерим позже в планировщике
    return long[["item_id", "due_date", "qty"]]

# ===================== Публичные функции =====================

def validate_files(machines_xlsx: str, bom_xlsx: str, plan_xlsx: str) -> Dict[str, Any]:
    """Сухая валидация: читать/нормализовать, вернуть отчёт (без записи в БД)."""
    report: Dict[str, Any] = {"status": "ok", "issues": [], "counts": {}, "preview": {}}

    # Machines
    mdf = _canonicalize_machines(_read_xlsx(machines_xlsx))
    report["counts"]["machines_rows"] = int(len(mdf))
    if mdf["machine_id"].duplicated().any():
        report["issues"].append({
            "level": "warning", "where": "machines",
            "msg": f"дубли machine_id: {int(mdf['machine_id'].duplicated().sum())}"
        })
    if (mdf["capacity_per_shift"] <= 0).any():
        bad = int((mdf["capacity_per_shift"] <= 0).sum())
        report["issues"].append({"level": "error", "where": "machines", "msg": f"capacity_per_shift <= 0 у {bad} строк"})

    # BOM
    bdf = _canonicalize_bom(_read_xlsx(bom_xlsx))
    report["counts"]["bom_rows"] = int(len(bdf))
    if (pd.to_numeric(bdf["qty_per"], errors="coerce") < 0).any():
        bad = int((pd.to_numeric(bdf["qty_per"], errors="coerce") < 0).sum())
        report["issues"].append({"level": "error", "where": "bom", "msg": f"qty_per < 0 у {bad} строк"})
    if "routing_step" not in bdf.columns:
        report["issues"].append({"level": "info", "where": "bom", "msg": "routing_step отсутствует — принят 1"})

    # Demand (WIDE/TALL)
    ddf = _canonicalize_demand(_read_xlsx(plan_xlsx))
    report["counts"]["demand_rows"] = int(len(ddf))
    if ddf["due_date"].isna().any():
        report["issues"].append({"level": "error", "where": "demand", "msg": "найдены пустые due_date после нормализации"})
    if (pd.to_numeric(ddf["qty"], errors="coerce") <= 0).any():
        bad = int((pd.to_numeric(ddf["qty"], errors="coerce") <= 0).sum())
        report["issues"].append({"level": "error", "where": "demand", "msg": f"qty <= 0 у {bad} строк"})
    # info: если нет order_id — теперь это норма
    report["issues"].append({"level": "info", "where": "demand", "msg": "order_id отсутствует — будет сгенерирован в планировщике"})

    # Cross-check: demand.item_id ∉ bom.item_id (warning)
    missing_items = sorted(set(ddf["item_id"].astype(str)) - set(bdf["item_id"].astype(str)))
    if missing_items:
        report["issues"].append({
            "level": "warning", "where": "cross",
            "msg": f"{len(missing_items)} item_id из demand отсутствуют в BOM (пример: {missing_items[:5]})"
        })

    # Preview
    report["preview"] = {
        "machines": mdf.head(5).to_dict(orient="records"),
        "bom": bdf.head(5).to_dict(orient="records"),
        "demand": ddf.head(5).to_dict(orient="records"),
    }
    if any(i["level"] == "error" for i in report["issues"]):
        report["status"] = "error"
    return report

def load_excels(session: Session, machines_xlsx: str, bom_xlsx: str, plan_xlsx: str, dry_run: bool = False) -> Tuple[int, int, int]:
    """Импорт в БД (или dry_run=True для подсчётов)."""
    mdf = _canonicalize_machines(_read_xlsx(machines_xlsx))
    bdf = _canonicalize_bom(_read_xlsx(bom_xlsx))
    ddf = _canonicalize_demand(_read_xlsx(plan_xlsx))

    if dry_run:
        return len(mdf), len(bdf), len(ddf)

    # truncate + insert
    session.execute(delete(DimMachine))
    session.execute(delete(BOM))
    session.execute(delete(Demand))

    session.bulk_insert_mappings(
        DimMachine,
        mdf[["machine_id", "name", "family", "capacity_per_shift", "setup_time"]].to_dict(orient="records"),
    )
    session.bulk_insert_mappings(
        BOM,
        bdf[[c for c in ["item_id", "component_id", "qty_per", "routing_step", "machine_family", "proc_time_std", "article_name"]
             if c in bdf.columns]].to_dict(orient="records"),
    )
    session.bulk_insert_mappings(
        Demand,
        ddf[[c for c in ["order_id", "item_id", "due_date", "qty", "priority", "customer"] if c in ddf.columns]]
        .to_dict(orient="records"),
    )
    session.commit()
    return len(mdf), len(bdf), len(ddf)
