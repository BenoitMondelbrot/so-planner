"""Demand-related helpers (facade)."""
from __future__ import annotations

import pandas as pd  # type: ignore

from ..greedy_scheduler import build_demand as _build_demand


def build_demand(plan_df: pd.DataFrame) -> pd.DataFrame:
    return _build_demand(plan_df)


__all__ = ["build_demand"]

