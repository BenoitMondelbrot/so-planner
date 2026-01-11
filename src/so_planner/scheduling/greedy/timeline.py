"""Timeline metrics (facade)."""
from __future__ import annotations

import pandas as pd  # type: ignore

from ..greedy_scheduler import (
    compute_orders_timeline as _compute_orders_timeline,
    compute_order_items_timeline as _compute_order_items_timeline,
)


def compute_orders_timeline(sched: pd.DataFrame) -> pd.DataFrame:
    return _compute_orders_timeline(sched)


def compute_order_items_timeline(sched: pd.DataFrame) -> pd.DataFrame:
    return _compute_order_items_timeline(sched)


__all__ = [
    "compute_orders_timeline", "compute_order_items_timeline",
]

