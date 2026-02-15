from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from ...bom_versioning import (
    activate_bom_version,
    create_bom_version,
    fetch_bom_rows,
    get_resolved_bom_version,
    get_version_rows_df,
    list_bom_versions,
)
from ...db import get_db
from ...ingest.loader import _canonicalize_bom, _read_xlsx

router = APIRouter(prefix="/bom", tags=["bom"])

DEFAULT_DIFF_CRITERIA = {
    "qty_per",
    "loss",
    "proc_time_std",
    "article_name",
    "row_count",
    "multiplicity",
    "norm_combined",
    "workshop",
    "time_per_unit",
    "machine_time",
    "setting_time",
    "source_step",
    "setup_minutes",
    "lag_time",
}


def _norm_text(v) -> str:
    s = str(v or "").strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def _norm_float(v) -> float | None:
    if v is None:
        return None
    try:
        x = float(v)
    except Exception:
        return None
    if x != x:  # NaN
        return None
    return float(x)


def _pct_delta(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None
    if abs(before) < 1e-12:
        return None
    return ((after - before) / abs(before)) * 100.0


def _parse_criteria(raw: str | None) -> set[str]:
    if not raw:
        return set(DEFAULT_DIFF_CRITERIA)
    out: set[str] = set()
    for part in str(raw).replace(";", ",").split(","):
        t = part.strip().lower()
        if not t:
            continue
        out.add(t)
    return out or set(DEFAULT_DIFF_CRITERIA)


def _aggregate_rows(df):
    out: dict[tuple, dict] = {}
    if df is None or df.empty:
        return out
    for r in df.itertuples(index=False):
        item_id = _norm_text(getattr(r, "item_id", ""))
        component_id = _norm_text(getattr(r, "component_id", ""))
        routing_step = int(getattr(r, "routing_step", 1) or 1)
        machine_family = _norm_text(getattr(r, "machine_family", ""))
        key = (item_id, component_id, routing_step, machine_family)
        qty = _norm_float(getattr(r, "qty_per", None)) or 0.0
        loss = _norm_float(getattr(r, "loss", None))
        proc = _norm_float(getattr(r, "proc_time_std", None))
        article = _norm_text(getattr(r, "article_name", ""))
        workshop = _norm_text(getattr(r, "workshop", ""))
        time_per_unit = _norm_float(getattr(r, "time_per_unit", None))
        machine_time = _norm_float(getattr(r, "machine_time", None))
        setting_time = _norm_float(getattr(r, "setting_time", None))
        source_step = _norm_text(getattr(r, "source_step", ""))
        setup_minutes = _norm_float(getattr(r, "setup_minutes", None))
        lag_time = _norm_float(getattr(r, "lag_time", None))

        cur = out.get(key)
        if cur is None:
            out[key] = {
                "item_id": item_id,
                "component_id": component_id,
                "routing_step": routing_step,
                "machine_family": machine_family,
                "qty_per": float(qty),
                "loss": loss,
                "proc_time_std": proc,
                "article_name": article,
                "workshop": workshop,
                "time_per_unit": time_per_unit,
                "machine_time": machine_time,
                "setting_time": setting_time,
                "source_step": source_step,
                "setup_minutes": setup_minutes,
                "lag_time": lag_time,
                "row_count": 1,
            }
            continue
        cur["qty_per"] = float(cur["qty_per"] or 0.0) + float(qty)
        cur["row_count"] = int(cur.get("row_count") or 1) + 1
        cur_proc = _norm_float(cur.get("proc_time_std"))
        if cur_proc is None and proc is not None:
            cur["proc_time_std"] = proc
        elif cur_proc is not None and proc is not None and proc > cur_proc:
            cur["proc_time_std"] = proc
        if not _norm_text(cur.get("article_name")) and article:
            cur["article_name"] = article
        if not _norm_text(cur.get("workshop")) and workshop:
            cur["workshop"] = workshop
        if not _norm_text(cur.get("source_step")) and source_step:
            cur["source_step"] = source_step

        for field, val in (
            ("loss", loss),
            ("time_per_unit", time_per_unit),
            ("machine_time", machine_time),
            ("setting_time", setting_time),
            ("setup_minutes", setup_minutes),
            ("lag_time", lag_time),
        ):
            cur_val = _norm_float(cur.get(field))
            if cur_val is None and val is not None:
                cur[field] = val
            elif cur_val is not None and val is not None and val > cur_val:
                cur[field] = val
    return out


@router.get("/versions", summary="List BOM versions")
def bom_versions(db: Session = Depends(get_db)):
    versions = list_bom_versions(db)
    return [
        {
            "id": int(v.id),
            "name": v.name,
            "source_file": v.source_file,
            "is_active": bool(v.is_active),
            "row_count": int(v.row_count or 0),
            "created_at": str(v.created_at) if v.created_at is not None else None,
            "notes": v.notes,
        }
        for v in versions
    ]


@router.post("/versions/import", summary="Import BOM version from Excel")
async def bom_import(
    file: UploadFile = File(...),
    name: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    activate: bool = Form(default=True),
    db: Session = Depends(get_db),
):
    suffix = os.path.splitext(file.filename or "")[1] or ".xlsx"
    tmp_file = tempfile.NamedTemporaryFile(prefix="bom_", suffix=suffix, delete=False)
    tmp_path = Path(tmp_file.name)
    tmp_file.close()
    try:
        with tmp_path.open("wb") as out:
            shutil.copyfileobj(file.file, out)
        bom_df = _canonicalize_bom(_read_xlsx(str(tmp_path)))
        ver = create_bom_version(
            db,
            bom_df,
            name=name or f"BOM import: {file.filename or 'uploaded.xlsx'}",
            source_file=file.filename or None,
            notes=notes,
            activate=bool(activate),
        )
        return {
            "status": "ok",
            "version": {
                "id": int(ver.id),
                "name": ver.name,
                "source_file": ver.source_file,
                "is_active": bool(ver.is_active),
                "row_count": int(ver.row_count or 0),
                "created_at": str(ver.created_at) if ver.created_at is not None else None,
                "notes": ver.notes,
            },
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail={"msg": str(e)})
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


@router.post("/versions/{version_id}/activate", summary="Set active BOM version")
def bom_activate(version_id: int, db: Session = Depends(get_db)):
    try:
        ver = activate_bom_version(db, version_id)
        return {
            "status": "ok",
            "version": {
                "id": int(ver.id),
                "name": ver.name,
                "is_active": bool(ver.is_active),
                "row_count": int(ver.row_count or 0),
                "created_at": str(ver.created_at) if ver.created_at is not None else None,
            },
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail={"msg": str(e)})


@router.get("/versions/{version_id}/rows", summary="Get BOM rows for one version")
def bom_rows(
    version_id: int,
    item_id: str | None = Query(default=None),
    component_id: str | None = Query(default=None),
    machine_family: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    version = get_resolved_bom_version(db, version_id)
    rows, total = fetch_bom_rows(
        db,
        int(version.id),
        item_id=item_id,
        component_id=component_id,
        machine_family=machine_family,
        limit=limit,
        offset=offset,
    )
    return {
        "status": "ok",
        "version": {
            "id": int(version.id),
            "name": version.name,
            "source_file": version.source_file,
            "is_active": bool(version.is_active),
            "row_count": int(version.row_count or 0),
            "created_at": str(version.created_at) if version.created_at is not None else None,
        },
        "count": len(rows),
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "rows": rows,
    }


@router.get("/report", summary="BOM report by version")
def bom_report(
    version_id: int | None = Query(default=None),
    item_id: str | None = Query(default=None),
    component_id: str | None = Query(default=None),
    machine_family: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    version = get_resolved_bom_version(db, version_id)
    rows, total = fetch_bom_rows(
        db,
        int(version.id),
        item_id=item_id,
        component_id=component_id,
        machine_family=machine_family,
        limit=limit,
        offset=offset,
    )
    return {
        "status": "ok",
        "version_id": int(version.id),
        "version_name": version.name,
        "is_active": bool(version.is_active),
        "created_at": str(version.created_at) if version.created_at is not None else None,
        "count": len(rows),
        "total": int(total),
        "rows": rows,
    }


@router.get("/diff", summary="Compare two BOM versions")
def bom_diff(
    from_version_id: int = Query(..., ge=1),
    to_version_id: int = Query(..., ge=1),
    criteria: str | None = Query(
        default=None,
        description=(
            "Comma-separated criteria: "
            "qty_per,loss,proc_time_std,article_name,row_count,multiplicity,norm_combined,"
            "workshop,time_per_unit,machine_time,setting_time,source_step,setup_minutes,lag_time"
        ),
    ),
    qty_pct_threshold: float = Query(default=0.0, ge=0.0, le=100000.0),
    proc_pct_threshold: float = Query(default=0.0, ge=0.0, le=100000.0),
    norm_pct_threshold: float = Query(default=0.0, ge=0.0, le=100000.0),
    multiplicity_dev_pct_threshold: float = Query(default=1.0, ge=0.0, le=100000.0),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    criteria_set = _parse_criteria(criteria)
    v_from = get_resolved_bom_version(db, from_version_id)
    v_to = get_resolved_bom_version(db, to_version_id)

    from_df = get_version_rows_df(db, int(v_from.id))
    to_df = get_version_rows_df(db, int(v_to.id))
    a = _aggregate_rows(from_df)
    b = _aggregate_rows(to_df)
    pairs_from = {(k[0], k[1]) for k in a.keys()}
    pairs_to = {(k[0], k[1]) for k in b.keys()}

    keys = sorted(set(a.keys()) | set(b.keys()))
    rows = []
    tol = 1e-9

    def _num_change(v_left, v_right):
        left_num = _norm_float(v_left)
        right_num = _norm_float(v_right)
        delta_abs = None if (left_num is None or right_num is None) else (right_num - left_num)
        delta_pct = _pct_delta(left_num, right_num)
        changed = False
        if left_num is None and right_num is not None:
            changed = True
        elif left_num is not None and right_num is None:
            changed = True
        elif left_num is not None and right_num is not None:
            changed = abs(delta_abs or 0.0) > tol
        return left_num, right_num, changed, delta_abs, delta_pct

    for key in keys:
        left = a.get(key)
        right = b.get(key)
        if left is None and right is not None:
            pair = (right["item_id"], right["component_id"])
            is_structure = pair in pairs_from
            rows.append(
                {
                    "change_type": ("changed_structure" if is_structure else "added"),
                    "item_id": right["item_id"],
                    "component_id": right["component_id"],
                    "routing_step": right["routing_step"],
                    "machine_family": right["machine_family"],
                    "reasons": (["structure"] if is_structure else ["added"]),
                    "severity": ("high" if is_structure else "low"),
                    "qty_per_before": None,
                    "qty_per_after": right["qty_per"],
                    "qty_per_delta_abs": None,
                    "qty_per_delta_pct": None,
                    "loss_before": None,
                    "loss_after": right.get("loss"),
                    "loss_delta_abs": None,
                    "loss_delta_pct": None,
                    "qty_ratio": None,
                    "qty_ratio_nearest_int": None,
                    "qty_ratio_dev_pct": None,
                    "proc_time_std_before": None,
                    "proc_time_std_after": right["proc_time_std"],
                    "proc_time_std_delta_abs": None,
                    "proc_time_std_delta_pct": None,
                    "combined_norm_before": None,
                    "combined_norm_after": None,
                    "combined_norm_delta_abs": None,
                    "combined_norm_delta_pct": None,
                    "article_name_before": None,
                    "article_name_after": right["article_name"],
                    "workshop_before": None,
                    "workshop_after": right.get("workshop"),
                    "time_per_unit_before": None,
                    "time_per_unit_after": right.get("time_per_unit"),
                    "time_per_unit_delta_abs": None,
                    "time_per_unit_delta_pct": None,
                    "machine_time_before": None,
                    "machine_time_after": right.get("machine_time"),
                    "machine_time_delta_abs": None,
                    "machine_time_delta_pct": None,
                    "setting_time_before": None,
                    "setting_time_after": right.get("setting_time"),
                    "setting_time_delta_abs": None,
                    "setting_time_delta_pct": None,
                    "source_step_before": None,
                    "source_step_after": right.get("source_step"),
                    "setup_minutes_before": None,
                    "setup_minutes_after": right.get("setup_minutes"),
                    "setup_minutes_delta_abs": None,
                    "setup_minutes_delta_pct": None,
                    "lag_time_before": None,
                    "lag_time_after": right.get("lag_time"),
                    "lag_time_delta_abs": None,
                    "lag_time_delta_pct": None,
                    "row_count_before": 0,
                    "row_count_after": right["row_count"],
                }
            )
            continue
        if right is None and left is not None:
            pair = (left["item_id"], left["component_id"])
            is_structure = pair in pairs_to
            rows.append(
                {
                    "change_type": ("changed_structure" if is_structure else "removed"),
                    "item_id": left["item_id"],
                    "component_id": left["component_id"],
                    "routing_step": left["routing_step"],
                    "machine_family": left["machine_family"],
                    "reasons": (["structure"] if is_structure else ["removed"]),
                    "severity": ("high" if is_structure else "low"),
                    "qty_per_before": left["qty_per"],
                    "qty_per_after": None,
                    "qty_per_delta_abs": None,
                    "qty_per_delta_pct": None,
                    "loss_before": left.get("loss"),
                    "loss_after": None,
                    "loss_delta_abs": None,
                    "loss_delta_pct": None,
                    "qty_ratio": None,
                    "qty_ratio_nearest_int": None,
                    "qty_ratio_dev_pct": None,
                    "proc_time_std_before": left["proc_time_std"],
                    "proc_time_std_after": None,
                    "proc_time_std_delta_abs": None,
                    "proc_time_std_delta_pct": None,
                    "combined_norm_before": None,
                    "combined_norm_after": None,
                    "combined_norm_delta_abs": None,
                    "combined_norm_delta_pct": None,
                    "article_name_before": left["article_name"],
                    "article_name_after": None,
                    "workshop_before": left.get("workshop"),
                    "workshop_after": None,
                    "time_per_unit_before": left.get("time_per_unit"),
                    "time_per_unit_after": None,
                    "time_per_unit_delta_abs": None,
                    "time_per_unit_delta_pct": None,
                    "machine_time_before": left.get("machine_time"),
                    "machine_time_after": None,
                    "machine_time_delta_abs": None,
                    "machine_time_delta_pct": None,
                    "setting_time_before": left.get("setting_time"),
                    "setting_time_after": None,
                    "setting_time_delta_abs": None,
                    "setting_time_delta_pct": None,
                    "source_step_before": left.get("source_step"),
                    "source_step_after": None,
                    "setup_minutes_before": left.get("setup_minutes"),
                    "setup_minutes_after": None,
                    "setup_minutes_delta_abs": None,
                    "setup_minutes_delta_pct": None,
                    "lag_time_before": left.get("lag_time"),
                    "lag_time_after": None,
                    "lag_time_delta_abs": None,
                    "lag_time_delta_pct": None,
                    "row_count_before": left["row_count"],
                    "row_count_after": 0,
                }
            )
            continue
        if left is None or right is None:
            continue

        qty_before = float(left["qty_per"] or 0.0)
        qty_after = float(right["qty_per"] or 0.0)
        qty_delta_abs = qty_after - qty_before
        qty_delta_pct = _pct_delta(qty_before, qty_after)
        qty_changed = abs(qty_delta_abs) > tol

        loss_before, loss_after, loss_changed, loss_delta_abs, loss_delta_pct = _num_change(
            left.get("loss"), right.get("loss")
        )

        proc_left, proc_right, proc_changed, proc_delta_abs, proc_delta_pct = _num_change(
            left.get("proc_time_std"), right.get("proc_time_std")
        )

        combined_before = None if proc_left is None else (qty_before * proc_left)
        combined_after = None if proc_right is None else (qty_after * proc_right)
        combined_delta_abs = None
        if combined_before is not None and combined_after is not None:
            combined_delta_abs = combined_after - combined_before
        combined_delta_pct = _pct_delta(combined_before, combined_after)

        qty_ratio = None
        qty_ratio_nearest_int = None
        qty_ratio_dev_pct = None
        if abs(qty_before) > tol:
            qty_ratio = qty_after / qty_before
            qty_ratio_nearest_int = int(round(qty_ratio))
            base = max(1, abs(qty_ratio_nearest_int))
            qty_ratio_dev_pct = abs(qty_ratio - float(qty_ratio_nearest_int)) / float(base) * 100.0

        article_changed = _norm_text(left.get("article_name")) != _norm_text(right.get("article_name"))
        workshop_changed = _norm_text(left.get("workshop")) != _norm_text(right.get("workshop"))
        source_step_changed = _norm_text(left.get("source_step")) != _norm_text(right.get("source_step"))
        count_changed = int(left.get("row_count") or 0) != int(right.get("row_count") or 0)
        (
            time_per_unit_before,
            time_per_unit_after,
            time_per_unit_changed,
            time_per_unit_delta_abs,
            time_per_unit_delta_pct,
        ) = _num_change(left.get("time_per_unit"), right.get("time_per_unit"))
        (
            machine_time_before,
            machine_time_after,
            machine_time_changed,
            machine_time_delta_abs,
            machine_time_delta_pct,
        ) = _num_change(left.get("machine_time"), right.get("machine_time"))
        (
            setting_time_before,
            setting_time_after,
            setting_time_changed,
            setting_time_delta_abs,
            setting_time_delta_pct,
        ) = _num_change(left.get("setting_time"), right.get("setting_time"))
        (
            setup_minutes_before,
            setup_minutes_after,
            setup_minutes_changed,
            setup_minutes_delta_abs,
            setup_minutes_delta_pct,
        ) = _num_change(left.get("setup_minutes"), right.get("setup_minutes"))
        (
            lag_time_before,
            lag_time_after,
            lag_time_changed,
            lag_time_delta_abs,
            lag_time_delta_pct,
        ) = _num_change(left.get("lag_time"), right.get("lag_time"))

        reasons: list[str] = []
        if "qty_per" in criteria_set and qty_changed:
            trig = False
            if qty_delta_pct is None:
                trig = True
            else:
                trig = abs(qty_delta_pct) > (float(qty_pct_threshold) + tol)
            if trig:
                reasons.append("qty_per")
        if "loss" in criteria_set and loss_changed:
            reasons.append("loss")
        if "proc_time_std" in criteria_set and proc_changed:
            trig = False
            if proc_delta_pct is None:
                trig = True
            else:
                trig = abs(proc_delta_pct) > (float(proc_pct_threshold) + tol)
            if trig:
                reasons.append("proc_time_std")
        if "article_name" in criteria_set and article_changed:
            reasons.append("article_name")
        if "workshop" in criteria_set and workshop_changed:
            reasons.append("workshop")
        if "source_step" in criteria_set and source_step_changed:
            reasons.append("source_step")
        if "row_count" in criteria_set and count_changed:
            reasons.append("row_count")
        if "time_per_unit" in criteria_set and time_per_unit_changed:
            reasons.append("time_per_unit")
        if "machine_time" in criteria_set and machine_time_changed:
            reasons.append("machine_time")
        if "setting_time" in criteria_set and setting_time_changed:
            reasons.append("setting_time")
        if "setup_minutes" in criteria_set and setup_minutes_changed:
            reasons.append("setup_minutes")
        if "lag_time" in criteria_set and lag_time_changed:
            reasons.append("lag_time")
        if "multiplicity" in criteria_set:
            if qty_ratio is None:
                if qty_changed:
                    reasons.append("multiplicity")
            elif qty_ratio_dev_pct is not None and qty_ratio_dev_pct > (float(multiplicity_dev_pct_threshold) + tol):
                reasons.append("multiplicity")
        if "norm_combined" in criteria_set:
            trig = False
            if combined_before is None and combined_after is None:
                trig = False
            elif combined_delta_pct is None:
                trig = (combined_delta_abs is not None and abs(combined_delta_abs) > tol)
            else:
                trig = abs(combined_delta_pct) > (float(norm_pct_threshold) + tol)
            if trig:
                reasons.append("norm_combined")

        if reasons:
            pct_vals = [
                abs(v)
                for v in [
                    qty_delta_pct,
                    loss_delta_pct,
                    proc_delta_pct,
                    combined_delta_pct,
                    time_per_unit_delta_pct,
                    machine_time_delta_pct,
                    setting_time_delta_pct,
                    setup_minutes_delta_pct,
                    lag_time_delta_pct,
                ]
                if v is not None
            ]
            max_pct = max(pct_vals) if pct_vals else 0.0
            if "multiplicity" in reasons or max_pct >= 100.0:
                severity = "high"
            elif max_pct >= 20.0 or len(reasons) >= 2:
                severity = "medium"
            else:
                severity = "low"
            rows.append(
                {
                    "change_type": "changed",
                    "item_id": left["item_id"],
                    "component_id": left["component_id"],
                    "routing_step": left["routing_step"],
                    "machine_family": left["machine_family"],
                    "reasons": reasons,
                    "severity": severity,
                    "qty_per_before": left["qty_per"],
                    "qty_per_after": right["qty_per"],
                    "qty_per_delta_abs": qty_delta_abs,
                    "qty_per_delta_pct": qty_delta_pct,
                    "loss_before": loss_before,
                    "loss_after": loss_after,
                    "loss_delta_abs": loss_delta_abs,
                    "loss_delta_pct": loss_delta_pct,
                    "qty_ratio": qty_ratio,
                    "qty_ratio_nearest_int": qty_ratio_nearest_int,
                    "qty_ratio_dev_pct": qty_ratio_dev_pct,
                    "proc_time_std_before": left["proc_time_std"],
                    "proc_time_std_after": right["proc_time_std"],
                    "proc_time_std_delta_abs": proc_delta_abs,
                    "proc_time_std_delta_pct": proc_delta_pct,
                    "combined_norm_before": combined_before,
                    "combined_norm_after": combined_after,
                    "combined_norm_delta_abs": combined_delta_abs,
                    "combined_norm_delta_pct": combined_delta_pct,
                    "article_name_before": left["article_name"],
                    "article_name_after": right["article_name"],
                    "workshop_before": left.get("workshop"),
                    "workshop_after": right.get("workshop"),
                    "time_per_unit_before": time_per_unit_before,
                    "time_per_unit_after": time_per_unit_after,
                    "time_per_unit_delta_abs": time_per_unit_delta_abs,
                    "time_per_unit_delta_pct": time_per_unit_delta_pct,
                    "machine_time_before": machine_time_before,
                    "machine_time_after": machine_time_after,
                    "machine_time_delta_abs": machine_time_delta_abs,
                    "machine_time_delta_pct": machine_time_delta_pct,
                    "setting_time_before": setting_time_before,
                    "setting_time_after": setting_time_after,
                    "setting_time_delta_abs": setting_time_delta_abs,
                    "setting_time_delta_pct": setting_time_delta_pct,
                    "source_step_before": left.get("source_step"),
                    "source_step_after": right.get("source_step"),
                    "setup_minutes_before": setup_minutes_before,
                    "setup_minutes_after": setup_minutes_after,
                    "setup_minutes_delta_abs": setup_minutes_delta_abs,
                    "setup_minutes_delta_pct": setup_minutes_delta_pct,
                    "lag_time_before": lag_time_before,
                    "lag_time_after": lag_time_after,
                    "lag_time_delta_abs": lag_time_delta_abs,
                    "lag_time_delta_pct": lag_time_delta_pct,
                    "row_count_before": left["row_count"],
                    "row_count_after": right["row_count"],
                }
            )

    added = sum(1 for r in rows if r["change_type"] == "added")
    removed = sum(1 for r in rows if r["change_type"] == "removed")
    changed = sum(1 for r in rows if r["change_type"] == "changed")
    changed_structure = sum(1 for r in rows if r["change_type"] == "changed_structure")
    criteria_counts: dict[str, int] = {}
    for r in rows:
        for reason in (r.get("reasons") or []):
            criteria_counts[reason] = int(criteria_counts.get(reason, 0) or 0) + 1
    total = len(rows)
    rows = rows[: int(limit)]

    return {
        "status": "ok",
        "from_version": {
            "id": int(v_from.id),
            "name": v_from.name,
            "created_at": str(v_from.created_at) if v_from.created_at is not None else None,
        },
        "to_version": {
            "id": int(v_to.id),
            "name": v_to.name,
            "created_at": str(v_to.created_at) if v_to.created_at is not None else None,
        },
        "summary": {
            "added": int(added),
            "removed": int(removed),
            "changed": int(changed),
            "changed_structure": int(changed_structure),
            "total": int(total),
            "returned": int(len(rows)),
            "limit": int(limit),
            "active_criteria": sorted(criteria_set),
            "criteria_hits": criteria_counts,
            "thresholds": {
                "qty_pct_threshold": float(qty_pct_threshold),
                "proc_pct_threshold": float(proc_pct_threshold),
                "norm_pct_threshold": float(norm_pct_threshold),
                "multiplicity_dev_pct_threshold": float(multiplicity_dev_pct_threshold),
            },
        },
        "rows": rows,
    }
