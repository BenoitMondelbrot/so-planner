"""Demand-related helpers (facade)."""
from __future__ import annotations

import pandas as pd  # type: ignore
from typing import Iterable

from ..greedy_scheduler import build_demand as _build_demand


def build_demand(plan_df: pd.DataFrame, *, reserved_order_ids: Iterable[str] | None = None) -> pd.DataFrame:
    return _build_demand(plan_df, reserved_order_ids=reserved_order_ids)


__all__ = ["build_demand"]
