from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import delete, func
from sqlalchemy.orm import Session

from .db.models import BOM, BOMLine, BOMVersion


def _clean_text(v: Any) -> str:
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def normalize_bom_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    expected = [
        "item_id",
        "component_id",
        "qty_per",
        "loss",
        "routing_step",
        "machine_family",
        "proc_time_std",
        "article_name",
        "workshop",
        "time_per_unit",
        "machine_time",
        "setting_time",
        "source_step",
        "setup_minutes",
        "lag_time",
    ]
    for col in expected:
        if col not in out.columns:
            out[col] = None

    out["item_id"] = out["item_id"].map(_clean_text)
    out["component_id"] = out["component_id"].map(_clean_text)
    out["qty_per"] = pd.to_numeric(out["qty_per"], errors="coerce").fillna(1.0).astype(float)
    out["loss"] = pd.to_numeric(out["loss"], errors="coerce").fillna(1.0).astype(float)
    out.loc[out["loss"] <= 0, "loss"] = 1.0
    out["routing_step"] = pd.to_numeric(out["routing_step"], errors="coerce").fillna(1).astype(int)
    out["machine_family"] = out["machine_family"].map(_clean_text)
    out["proc_time_std"] = pd.to_numeric(out["proc_time_std"], errors="coerce")
    out["article_name"] = out["article_name"].map(_clean_text)
    out["workshop"] = out["workshop"].map(_clean_text)
    out["time_per_unit"] = pd.to_numeric(out["time_per_unit"], errors="coerce")
    out["machine_time"] = pd.to_numeric(out["machine_time"], errors="coerce")
    out["setting_time"] = pd.to_numeric(out["setting_time"], errors="coerce")
    out["source_step"] = out["source_step"].map(_clean_text)
    out["setup_minutes"] = pd.to_numeric(out["setup_minutes"], errors="coerce")
    out["lag_time"] = pd.to_numeric(out["lag_time"], errors="coerce")

    out = out[(out["item_id"] != "") & (out["component_id"] != "")]
    out = out.reset_index(drop=True)
    return out[expected]


def _to_line_mappings(df: pd.DataFrame, version_id: int) -> list[dict]:
    return [
        {
            "version_id": int(version_id),
            "item_id": str(r.item_id),
            "component_id": str(r.component_id),
            "qty_per": float(r.qty_per),
            "loss": float(getattr(r, "loss", 1.0) or 1.0),
            "routing_step": int(r.routing_step),
            "machine_family": (str(r.machine_family) if r.machine_family else None),
            "proc_time_std": (float(r.proc_time_std) if pd.notna(r.proc_time_std) else None),
            "article_name": (str(r.article_name) if r.article_name else None),
            "workshop": (str(r.workshop) if r.workshop else None),
            "time_per_unit": (float(r.time_per_unit) if pd.notna(r.time_per_unit) else None),
            "machine_time": (float(r.machine_time) if pd.notna(r.machine_time) else None),
            "setting_time": (float(r.setting_time) if pd.notna(r.setting_time) else None),
            "source_step": (str(r.source_step) if r.source_step else None),
            "setup_minutes": (float(r.setup_minutes) if pd.notna(r.setup_minutes) else None),
            "lag_time": (float(r.lag_time) if pd.notna(r.lag_time) else None),
        }
        for r in df.itertuples(index=False)
    ]


def sync_legacy_bom_from_version(db: Session, version_id: int) -> int:
    rows = (
        db.query(BOMLine)
        .filter(BOMLine.version_id == int(version_id))
        .order_by(BOMLine.id.asc())
        .all()
    )
    db.execute(delete(BOM))
    if rows:
        db.bulk_insert_mappings(
            BOM,
            [
                {
                    "item_id": str(r.item_id),
                    "component_id": str(r.component_id),
                    "qty_per": float(r.qty_per),
                    "loss": float(getattr(r, "loss", 1.0) or 1.0),
                    "routing_step": int(r.routing_step or 1),
                    "machine_family": r.machine_family,
                    "proc_time_std": r.proc_time_std,
                    "article_name": r.article_name,
                    "workshop": r.workshop,
                    "time_per_unit": r.time_per_unit,
                    "machine_time": r.machine_time,
                    "setting_time": r.setting_time,
                    "source_step": r.source_step,
                    "setup_minutes": r.setup_minutes,
                    "lag_time": r.lag_time,
                }
                for r in rows
            ],
        )
    db.flush()
    return len(rows)


def _seed_from_legacy_bom(db: Session) -> BOMVersion | None:
    has_versions = int(db.query(func.count(BOMVersion.id)).scalar() or 0) > 0
    if has_versions:
        return None
    legacy_rows = db.query(BOM).order_by(BOM.id.asc()).all()
    if not legacy_rows:
        return None

    ver = BOMVersion(
        name="Legacy BOM snapshot",
        source_file=None,
        is_active=True,
        row_count=len(legacy_rows),
        notes="Auto-created from legacy bom table",
    )
    db.add(ver)
    db.flush()
    db.bulk_insert_mappings(
        BOMLine,
        [
            {
                "version_id": int(ver.id),
                "item_id": str(r.item_id),
                "component_id": str(r.component_id),
                "qty_per": float(r.qty_per),
                "loss": float(getattr(r, "loss", 1.0) or 1.0),
                "routing_step": int(r.routing_step or 1),
                "machine_family": r.machine_family,
                "proc_time_std": r.proc_time_std,
                "article_name": getattr(r, "article_name", None),
                "workshop": getattr(r, "workshop", None),
                "time_per_unit": getattr(r, "time_per_unit", None),
                "machine_time": getattr(r, "machine_time", None),
                "setting_time": getattr(r, "setting_time", None),
                "source_step": getattr(r, "source_step", None),
                "setup_minutes": getattr(r, "setup_minutes", None),
                "lag_time": getattr(r, "lag_time", None),
            }
            for r in legacy_rows
        ],
    )
    db.commit()
    db.refresh(ver)
    return ver


def ensure_seed_bom_version(db: Session) -> BOMVersion | None:
    try:
        return _seed_from_legacy_bom(db)
    except Exception:
        db.rollback()
        return None


def list_bom_versions(db: Session) -> list[BOMVersion]:
    ensure_seed_bom_version(db)
    return db.query(BOMVersion).order_by(BOMVersion.created_at.desc(), BOMVersion.id.desc()).all()


def get_resolved_bom_version(db: Session, version_id: int | None = None) -> BOMVersion:
    ensure_seed_bom_version(db)

    version: BOMVersion | None = None
    if version_id is not None:
        version = db.get(BOMVersion, int(version_id))
        if version is None:
            raise ValueError(f"BOM version {version_id} not found")
    else:
        version = (
            db.query(BOMVersion)
            .filter(BOMVersion.is_active == True)  # noqa: E712
            .order_by(BOMVersion.created_at.desc(), BOMVersion.id.desc())
            .first()
        )
        if version is None:
            version = (
                db.query(BOMVersion)
                .order_by(BOMVersion.created_at.desc(), BOMVersion.id.desc())
                .first()
            )
    if version is None:
        raise ValueError("No BOM versions found. Import BOM from Excel first.")
    return version


def create_bom_version(
    db: Session,
    bom_df: pd.DataFrame,
    *,
    name: str | None = None,
    source_file: str | None = None,
    notes: str | None = None,
    activate: bool = True,
) -> BOMVersion:
    normalized = normalize_bom_dataframe(bom_df)
    if normalized.empty:
        raise ValueError("BOM is empty after normalization")

    ver = BOMVersion(
        name=(name or f"BOM {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}").strip(),
        source_file=source_file,
        is_active=False,
        row_count=int(len(normalized)),
        notes=notes,
    )
    db.add(ver)
    db.flush()

    db.bulk_insert_mappings(BOMLine, _to_line_mappings(normalized, int(ver.id)))
    if activate:
        db.query(BOMVersion).filter(BOMVersion.id != int(ver.id)).update({"is_active": False})
        ver.is_active = True
        sync_legacy_bom_from_version(db, int(ver.id))
    db.commit()
    db.refresh(ver)
    return ver


def activate_bom_version(db: Session, version_id: int) -> BOMVersion:
    ver = db.get(BOMVersion, int(version_id))
    if ver is None:
        raise ValueError(f"BOM version {version_id} not found")
    has_rows = int(
        db.query(func.count(BOMLine.id)).filter(BOMLine.version_id == int(version_id)).scalar() or 0
    )
    if has_rows <= 0:
        raise ValueError(f"BOM version {version_id} has no rows")

    db.query(BOMVersion).update({"is_active": False})
    ver.is_active = True
    sync_legacy_bom_from_version(db, int(version_id))
    db.commit()
    db.refresh(ver)
    return ver


def fetch_bom_rows(
    db: Session,
    version_id: int,
    *,
    item_id: str | None = None,
    component_id: str | None = None,
    machine_family: str | None = None,
    limit: int = 500,
    offset: int = 0,
) -> tuple[list[dict], int]:
    q = db.query(BOMLine).filter(BOMLine.version_id == int(version_id))
    if item_id:
        q = q.filter(BOMLine.item_id.ilike(f"%{item_id.strip()}%"))
    if component_id:
        q = q.filter(BOMLine.component_id.ilike(f"%{component_id.strip()}%"))
    if machine_family:
        q = q.filter(BOMLine.machine_family.ilike(f"%{machine_family.strip()}%"))

    total = int(q.count())
    rows = (
        q.order_by(BOMLine.id.asc())
        .offset(max(0, int(offset or 0)))
        .limit(max(1, min(5000, int(limit or 500))))
        .all()
    )
    return (
        [
            {
                "id": int(r.id),
                "version_id": int(r.version_id),
                "item_id": str(r.item_id),
                "component_id": str(r.component_id),
                "qty_per": float(r.qty_per),
                "loss": float(getattr(r, "loss", 1.0) or 1.0),
                "routing_step": int(r.routing_step or 1),
                "machine_family": r.machine_family,
                "proc_time_std": r.proc_time_std,
                "article_name": r.article_name,
                "workshop": r.workshop,
                "time_per_unit": r.time_per_unit,
                "machine_time": r.machine_time,
                "setting_time": r.setting_time,
                "source_step": r.source_step,
                "setup_minutes": r.setup_minutes,
                "lag_time": r.lag_time,
                "recorded_at": str(r.recorded_at) if r.recorded_at is not None else None,
            }
            for r in rows
        ],
        total,
    )


def get_version_rows_df(db: Session, version_id: int) -> pd.DataFrame:
    rows = (
        db.query(BOMLine)
        .filter(BOMLine.version_id == int(version_id))
        .order_by(BOMLine.id.asc())
        .all()
    )
    if not rows:
        return pd.DataFrame(
            columns=[
                "item_id",
                "component_id",
                "qty_per",
                "loss",
                "routing_step",
                "machine_family",
                "proc_time_std",
                "article_name",
                "workshop",
                "time_per_unit",
                "machine_time",
                "setting_time",
                "source_step",
                "setup_minutes",
                "lag_time",
            ]
        )
    return pd.DataFrame(
        [
            {
                "item_id": str(r.item_id),
                "component_id": str(r.component_id),
                "qty_per": float(r.qty_per),
                "loss": float(getattr(r, "loss", 1.0) or 1.0),
                "routing_step": int(r.routing_step or 1),
                "machine_family": (str(r.machine_family) if r.machine_family is not None else ""),
                "proc_time_std": (float(r.proc_time_std) if r.proc_time_std is not None else None),
                "article_name": (str(r.article_name) if r.article_name is not None else ""),
                "workshop": (str(r.workshop) if r.workshop is not None else ""),
                "time_per_unit": (float(r.time_per_unit) if r.time_per_unit is not None else None),
                "machine_time": (float(r.machine_time) if r.machine_time is not None else None),
                "setting_time": (float(r.setting_time) if r.setting_time is not None else None),
                "source_step": (str(r.source_step) if r.source_step is not None else ""),
                "setup_minutes": (float(r.setup_minutes) if r.setup_minutes is not None else None),
                "lag_time": (float(r.lag_time) if r.lag_time is not None else None),
            }
            for r in rows
        ]
    )


def bom_df_to_scheduler_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "item_id",
                "step",
                "machine_id",
                "time_per_unit",
                "setup_minutes",
                "root_item_id",
                "qty_per_parent",
                "loss",
                "article_name",
            ]
        )
    out = pd.DataFrame()
    out["item_id"] = df["item_id"].astype(str).str.strip()
    out["step"] = pd.to_numeric(df["routing_step"], errors="coerce").fillna(1).astype(int)
    out["machine_id"] = df["machine_family"].fillna("").astype(str).str.strip()
    out["time_per_unit"] = pd.to_numeric(df["proc_time_std"], errors="coerce").fillna(0.0).astype(float)
    out["setup_minutes"] = 0.0
    out["root_item_id"] = df["component_id"].fillna("").astype(str).str.strip()
    out["qty_per_parent"] = pd.to_numeric(df["qty_per"], errors="coerce").fillna(1.0).astype(float)
    if "loss" in df.columns:
        out["loss"] = pd.to_numeric(df["loss"], errors="coerce").fillna(1.0).astype(float)
        out.loc[out["loss"] <= 0, "loss"] = 1.0
    else:
        out["loss"] = 1.0
    out["article_name"] = df["article_name"].fillna("").astype(str).str.strip()
    out = out[(out["item_id"] != "") & (out["machine_id"] != "")]
    out = out.sort_values(["item_id", "step"], kind="stable").reset_index(drop=True)
    return out


def article_name_map_from_df(df: pd.DataFrame) -> dict[str, str]:
    if df is None or df.empty:
        return {}
    tmp = df[["item_id", "article_name"]].copy()
    tmp["item_id"] = tmp["item_id"].astype(str).str.strip()
    tmp["article_name"] = tmp["article_name"].astype(str).str.strip()
    tmp = tmp[(tmp["item_id"] != "") & (tmp["article_name"] != "")]
    tmp = tmp.drop_duplicates(subset=["item_id"], keep="first")
    return {str(r.item_id): str(r.article_name) for r in tmp.itertuples(index=False)}
