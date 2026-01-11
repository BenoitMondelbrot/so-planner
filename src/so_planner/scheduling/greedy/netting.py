"""Product-View netting helpers (facade)."""
from __future__ import annotations

import pandas as pd  # type: ignore
from sqlalchemy.orm import Session

from ..greedy_scheduler import (
    product_view_generate_demand as _product_view_generate_demand,
)

# DB helpers inside monolith use private names; expose friendlier aliases here
from ..greedy_scheduler import (
    _ensure_netting_tables as _ensure_netting_tables_impl,
    _load_receipts_from_db as _load_receipts_from_db_impl,
    _load_stock_snapshot as _load_stock_snapshot_impl,
    _save_netting_results_to_db as _save_netting_results_to_db_impl,
)


def product_view_generate_demand(
    plan_df: pd.DataFrame,
    bom: pd.DataFrame,
    *,
    stock_df: pd.DataFrame | None = None,
    existing_orders_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    return _product_view_generate_demand(plan_df, bom, stock_df=stock_df, existing_orders_df=existing_orders_df)


def ensure_netting_tables(db: Session) -> None:
    _ensure_netting_tables_impl(db)


def load_receipts_from_db(db: Session, plan_version_id: int | None, receipts_from: str = "plan") -> pd.DataFrame:
    return _load_receipts_from_db_impl(db, plan_version_id, receipts_from)  # type: ignore[arg-type]


def load_stock_snapshot(db: Session, stock_snapshot_id: int) -> pd.DataFrame:
    return _load_stock_snapshot_impl(db, stock_snapshot_id)


def save_netting_results_to_db(
    db: Session,
    run_meta: dict,
    demand_net: pd.DataFrame,
    netting_log: pd.DataFrame,
    netting_summary: pd.DataFrame,
    linkage_df: pd.DataFrame | None = None,
) -> int:
    return _save_netting_results_to_db_impl(db, run_meta, demand_net, netting_log, netting_summary, linkage_df)


__all__ = [
    "product_view_generate_demand",
    "ensure_netting_tables",
    "load_receipts_from_db",
    "load_stock_snapshot",
    "save_netting_results_to_db",
]

