from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from .bom_versioning import (
    article_name_map_from_df,
    get_resolved_bom_version,
    get_version_rows_df,
)
from .db.models import SalesPlanLine, SalesPlanVersion


def _norm_col(s: str) -> str:
    return str(s).strip().lower().replace(" ", "").replace("_", "")


def _clean_text(v: Any) -> str:
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def _norm_customer(v: Any) -> str | None:
    s = _clean_text(v)
    return s or None


def normalize_sales_plan_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    norm = {_norm_col(c): c for c in out.columns}

    item_col = None
    for c in ("article", "item_id", "item", "артикул"):
        key = _norm_col(c)
        if key in norm:
            item_col = norm[key]
            break
    if item_col is None:
        item_col = out.columns[0]

    customer_col = None
    for c in ("customer", "client", "клиент", "заказчик", "покупатель"):
        key = _norm_col(c)
        if key in norm:
            customer_col = norm[key]
            break

    date_cols: list[Any] = []
    for c in out.columns:
        if c == item_col or (customer_col is not None and c == customer_col):
            continue
        d = pd.to_datetime(c, errors="coerce")
        if pd.notna(d):
            date_cols.append(c)
    if not date_cols:
        raise ValueError(f"No date-like columns found in sales plan: {list(out.columns)}")

    id_vars = [item_col] + ([customer_col] if customer_col is not None else [])
    long_df = out.melt(id_vars=id_vars, value_vars=date_cols, var_name="due_date", value_name="qty")
    long_df["due_date"] = pd.to_datetime(long_df["due_date"], errors="coerce").dt.date
    long_df.rename(columns={item_col: "item_id"}, inplace=True)
    long_df["item_id"] = long_df["item_id"].map(_clean_text)
    if customer_col is not None:
        long_df.rename(columns={customer_col: "customer"}, inplace=True)
        long_df["customer"] = long_df["customer"].map(_norm_customer)
    else:
        long_df["customer"] = None

    qty_num = pd.to_numeric(long_df["qty"], errors="coerce")
    frac = (qty_num - qty_num.round()).abs()
    if frac.dropna().gt(1e-9).any():
        raise ValueError("Sales plan qty must be integer")
    long_df["qty"] = qty_num.fillna(0).round().astype(int)

    long_df = long_df[
        (long_df["item_id"] != "")
        & long_df["due_date"].notna()
        & (long_df["qty"] > 0)
    ].copy()
    long_df = (
        long_df.groupby(["item_id", "customer", "due_date"], as_index=False, dropna=False)["qty"]
        .sum()
        .sort_values(["due_date", "item_id", "customer"], kind="stable")
        .reset_index(drop=True)
    )
    return long_df[["item_id", "customer", "due_date", "qty"]]


def _article_name_map_for_bom(db: Session, bom_version_id: int | None = None) -> tuple[int, dict[str, str]]:
    bom_ver = get_resolved_bom_version(db, bom_version_id)
    bom_df = get_version_rows_df(db, int(bom_ver.id))
    return int(bom_ver.id), article_name_map_from_df(bom_df)


def list_sales_plan_versions(db: Session) -> list[SalesPlanVersion]:
    return (
        db.query(SalesPlanVersion)
        .order_by(SalesPlanVersion.created_at.desc(), SalesPlanVersion.id.desc())
        .all()
    )


def get_resolved_sales_plan_version(db: Session, version_id: int | None = None) -> SalesPlanVersion:
    ver: SalesPlanVersion | None = None
    if version_id is not None:
        ver = db.get(SalesPlanVersion, int(version_id))
    else:
        ver = (
            db.query(SalesPlanVersion)
            .order_by(SalesPlanVersion.created_at.desc(), SalesPlanVersion.id.desc())
            .first()
        )
    if ver is None:
        raise ValueError("Sales plan version not found")
    return ver


def create_sales_plan_version(
    db: Session,
    *,
    name: str | None = None,
    notes: str | None = None,
    source_file: str | None = None,
    source_version_id: int | None = None,
) -> SalesPlanVersion:
    ver = SalesPlanVersion(
        name=(name or f"Sales plan {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}").strip(),
        notes=notes,
        source_file=source_file,
        row_count=0,
    )
    db.add(ver)
    db.flush()

    if source_version_id is not None:
        src = get_resolved_sales_plan_version(db, int(source_version_id))
        rows = (
            db.query(SalesPlanLine)
            .filter(SalesPlanLine.version_id == int(src.id))
            .order_by(SalesPlanLine.id.asc())
            .all()
        )
        if rows:
            db.bulk_insert_mappings(
                SalesPlanLine,
                [
                    {
                        "version_id": int(ver.id),
                        "item_id": str(r.item_id),
                        "article_name": r.article_name,
                        "customer": r.customer,
                        "due_date": r.due_date,
                        "qty": int(r.qty),
                    }
                    for r in rows
                ],
            )
            ver.row_count = int(len(rows))
            ver.bom_version_id = src.bom_version_id

    db.commit()
    db.refresh(ver)
    return ver


def replace_sales_plan_lines(
    db: Session,
    version_id: int,
    rows_df: pd.DataFrame,
    *,
    bom_version_id: int | None = None,
) -> SalesPlanVersion:
    ver = get_resolved_sales_plan_version(db, int(version_id))
    in_df = rows_df.copy() if rows_df is not None else pd.DataFrame(columns=["item_id", "customer", "due_date", "qty"])
    if "customer" not in in_df.columns:
        in_df["customer"] = None
    if "item_id" not in in_df.columns:
        raise ValueError("item_id is required")
    if "due_date" not in in_df.columns:
        raise ValueError("due_date is required")
    if "qty" not in in_df.columns:
        raise ValueError("qty is required")

    in_df["item_id"] = in_df["item_id"].map(_clean_text)
    in_df["customer"] = in_df["customer"].map(_norm_customer)
    in_df["due_date"] = pd.to_datetime(in_df["due_date"], errors="coerce").dt.date
    qty_num = pd.to_numeric(in_df["qty"], errors="coerce")
    frac = (qty_num - qty_num.round()).abs()
    if frac.dropna().gt(1e-9).any():
        raise ValueError("Sales plan qty must be integer")
    in_df["qty"] = qty_num.fillna(0).round().astype(int)

    in_df = in_df[(in_df["item_id"] != "") & in_df["due_date"].notna() & (in_df["qty"] > 0)].copy()
    if not in_df.empty:
        in_df = (
            in_df.groupby(["item_id", "customer", "due_date"], as_index=False, dropna=False)["qty"]
            .sum()
            .sort_values(["due_date", "item_id", "customer"], kind="stable")
            .reset_index(drop=True)
        )

    resolved_bom_version_id, article_map = _article_name_map_for_bom(db, bom_version_id or ver.bom_version_id)
    item_ids = {str(x) for x in in_df["item_id"].astype(str).tolist()}
    missing = sorted([x for x in item_ids if x not in article_map])
    if missing:
        sample = ", ".join(missing[:10])
        suffix = "..." if len(missing) > 10 else ""
        raise ValueError(f"Unknown items not found in active BOM: {sample}{suffix}")

    db.execute(delete(SalesPlanLine).where(SalesPlanLine.version_id == int(ver.id)))
    if not in_df.empty:
        db.bulk_insert_mappings(
            SalesPlanLine,
            [
                {
                    "version_id": int(ver.id),
                    "item_id": str(r.item_id),
                    "article_name": article_map.get(str(r.item_id)),
                    "customer": (str(r.customer) if pd.notna(r.customer) and str(r.customer).strip() else None),
                    "due_date": r.due_date,
                    "qty": int(r.qty),
                }
                for r in in_df.itertuples(index=False)
            ],
        )
    ver.row_count = int(len(in_df))
    ver.bom_version_id = int(resolved_bom_version_id)
    db.commit()
    db.refresh(ver)
    return ver


def get_sales_plan_demand_df(db: Session, version_id: int) -> pd.DataFrame:
    rows = (
        db.query(SalesPlanLine.item_id, SalesPlanLine.customer, SalesPlanLine.due_date, SalesPlanLine.qty)
        .filter(SalesPlanLine.version_id == int(version_id))
        .order_by(SalesPlanLine.due_date.asc(), SalesPlanLine.item_id.asc(), SalesPlanLine.id.asc())
        .all()
    )
    if not rows:
        return pd.DataFrame(columns=["item_id", "due_date", "qty", "priority", "customer"])
    out = pd.DataFrame(rows, columns=["item_id", "customer", "due_date", "qty"])
    out["item_id"] = out["item_id"].astype(str).str.strip()
    out["customer"] = (
        out["customer"]
        .astype(str)
        .replace({"None": "", "none": "", "nan": "", "NaN": "", "<NA>": "", "NULL": "", "null": ""})
        .str.strip()
    )
    out["due_date"] = pd.to_datetime(out["due_date"], errors="coerce").dt.date
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0).astype(int)
    out = out[(out["item_id"] != "") & out["due_date"].notna() & (out["qty"] > 0)].copy()
    if out.empty:
        return pd.DataFrame(columns=["item_id", "due_date", "qty", "priority", "customer"])
    out = (
        out.groupby(["item_id", "due_date", "customer"], as_index=False, dropna=False)["qty"]
        .sum()
        .sort_values(["due_date", "customer", "item_id"], kind="stable")
        .reset_index(drop=True)
    )
    out["priority"] = pd.to_datetime(out["due_date"], errors="coerce")
    return out[["item_id", "due_date", "qty", "priority", "customer"]]


def fetch_sales_plan_matrix(db: Session, version_id: int) -> tuple[list[str], list[dict]]:
    ver = get_resolved_sales_plan_version(db, int(version_id))
    _, article_map = _article_name_map_for_bom(db, ver.bom_version_id)
    rows = (
        db.query(SalesPlanLine)
        .filter(SalesPlanLine.version_id == int(ver.id))
        .order_by(SalesPlanLine.item_id.asc(), SalesPlanLine.customer.asc(), SalesPlanLine.due_date.asc())
        .all()
    )
    date_keys = sorted({str(r.due_date) for r in rows if r.due_date is not None})
    grouped: dict[tuple[str, str | None], dict] = {}
    for r in rows:
        key = (str(r.item_id), (str(r.customer) if r.customer is not None else None))
        rec = grouped.get(key)
        if rec is None:
            rec = {
                "item_id": str(r.item_id),
                "article_name": (r.article_name or article_map.get(str(r.item_id)) or ""),
                "customer": (str(r.customer) if r.customer is not None else ""),
                "quantities": {},
            }
            grouped[key] = rec
        rec["quantities"][str(r.due_date)] = int(r.qty or 0)
    out_rows = sorted(grouped.values(), key=lambda x: (str(x.get("item_id") or ""), str(x.get("customer") or "")))
    return date_keys, out_rows
