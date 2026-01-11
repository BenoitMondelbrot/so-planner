"""BOM-related helpers (facade)."""
from __future__ import annotations

import pandas as pd  # type: ignore

from ..greedy_scheduler import (
    build_bom_hierarchy as _build_bom_hierarchy,
    expand_demand_with_hierarchy as _expand_demand_with_hierarchy,
)


def build_bom_hierarchy(bom: pd.DataFrame) -> pd.DataFrame:
    return _build_bom_hierarchy(bom)


def expand_demand_with_hierarchy(
    demand: pd.DataFrame,
    bom: pd.DataFrame,
    *,
    split_child_orders: bool = False,
) -> pd.DataFrame:
    return _expand_demand_with_hierarchy(demand, bom, split_child_orders=split_child_orders)


__all__ = ["build_bom_hierarchy", "expand_demand_with_hierarchy"]

