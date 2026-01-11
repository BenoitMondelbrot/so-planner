"""Export helpers (facade)."""
from __future__ import annotations

from pathlib import Path
import pandas as pd  # type: ignore

from ..greedy_scheduler import export_with_charts as _export_with_charts


def export_with_charts(sched: pd.DataFrame, out_xlsx: Path, bom: pd.DataFrame | None = None) -> Path:
    return _export_with_charts(sched, out_xlsx, bom=bom)


__all__ = ["export_with_charts"]

