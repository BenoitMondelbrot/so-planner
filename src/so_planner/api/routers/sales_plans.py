from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ...db import get_db
from ...sales_plan_versioning import (
    create_sales_plan_version,
    fetch_sales_plan_matrix,
    get_resolved_sales_plan_version,
    list_sales_plan_versions,
    normalize_sales_plan_dataframe,
    replace_sales_plan_lines,
)
from ...bom_versioning import article_name_map_from_df, get_resolved_bom_version, get_version_rows_df

router = APIRouter(prefix="/sales-plans", tags=["sales-plans"])


class SalesPlanCreate(BaseModel):
    name: str
    notes: str | None = None
    source_version_id: int | None = None


class SalesPlanMatrixRowIn(BaseModel):
    item_id: str
    customer: str | None = None
    quantities: Dict[str, int | float | None] = Field(default_factory=dict)


class SalesPlanMatrixSave(BaseModel):
    rows: List[SalesPlanMatrixRowIn] = Field(default_factory=list)
    bom_version_id: int | None = None


class SalesPlanArticleLookup(BaseModel):
    items: List[str] = Field(default_factory=list)
    bom_version_id: int | None = None


def _read_uploaded_sales_plan(file: UploadFile) -> pd.DataFrame:
    suffix = os.path.splitext(file.filename or "")[1] or ".xlsx"
    tmp_file = tempfile.NamedTemporaryFile(prefix="sales_plan_", suffix=suffix, delete=False)
    tmp_path = Path(tmp_file.name)
    tmp_file.close()
    try:
        with tmp_path.open("wb") as out:
            shutil.copyfileobj(file.file, out)
        raw_df = pd.read_excel(tmp_path, sheet_name=0, dtype=object)
        return normalize_sales_plan_dataframe(raw_df)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _version_payload(v) -> dict:
    return {
        "id": int(v.id),
        "name": v.name,
        "source_file": v.source_file,
        "row_count": int(v.row_count or 0),
        "bom_version_id": int(v.bom_version_id) if v.bom_version_id is not None else None,
        "created_at": str(v.created_at) if v.created_at is not None else None,
        "notes": v.notes,
    }


@router.get("/versions", summary="List sales plan versions")
def versions(db: Session = Depends(get_db)):
    out = list_sales_plan_versions(db)
    return [_version_payload(v) for v in out]


@router.post("/versions", summary="Create sales plan version")
def create_version(body: SalesPlanCreate, db: Session = Depends(get_db)):
    try:
        v = create_sales_plan_version(
            db,
            name=body.name,
            notes=body.notes,
            source_version_id=body.source_version_id,
        )
        return {"status": "ok", "version": _version_payload(v)}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail={"msg": str(e)})


@router.post("/versions/import", summary="Import sales plan version from Excel")
async def import_version(
    file: UploadFile = File(...),
    name: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    bom_version_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
):
    try:
        normalized = _read_uploaded_sales_plan(file)
        v = create_sales_plan_version(
            db,
            name=name or f"Sales plan import: {file.filename or 'uploaded.xlsx'}",
            notes=notes,
            source_file=file.filename or None,
        )
        v = replace_sales_plan_lines(db, int(v.id), normalized, bom_version_id=bom_version_id)
        return {"status": "ok", "version": _version_payload(v)}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail={"msg": str(e)})


@router.post("/versions/{version_id}/import", summary="Import Excel into existing sales plan version")
async def import_into_existing_version(
    version_id: int,
    file: UploadFile = File(...),
    bom_version_id: int | None = Form(default=None),
    db: Session = Depends(get_db),
):
    try:
        _ = get_resolved_sales_plan_version(db, int(version_id))
        normalized = _read_uploaded_sales_plan(file)
        v = replace_sales_plan_lines(db, int(version_id), normalized, bom_version_id=bom_version_id)
        if file.filename:
            v.source_file = str(file.filename)
            db.commit()
            db.refresh(v)
        return {"status": "ok", "version": _version_payload(v), "saved_rows": int(v.row_count or 0)}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail={"msg": str(e)})


@router.get("/versions/{version_id}/matrix", summary="Get sales plan wide matrix")
def get_matrix(version_id: int, db: Session = Depends(get_db)):
    try:
        v = get_resolved_sales_plan_version(db, int(version_id))
        dates, rows = fetch_sales_plan_matrix(db, int(v.id))
        return {
            "status": "ok",
            "version": _version_payload(v),
            "dates": dates,
            "rows": rows,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail={"msg": str(e)})


@router.put("/versions/{version_id}/matrix", summary="Replace sales plan matrix rows")
def save_matrix(version_id: int, body: SalesPlanMatrixSave, db: Session = Depends(get_db)):
    try:
        line_rows = []
        for row in body.rows or []:
            item_id = str(row.item_id or "").strip()
            customer = str(row.customer or "").strip() or None
            for due_date, qty in (row.quantities or {}).items():
                line_rows.append(
                    {
                        "item_id": item_id,
                        "customer": customer,
                        "due_date": due_date,
                        "qty": qty,
                    }
                )
        df = pd.DataFrame(line_rows, columns=["item_id", "customer", "due_date", "qty"])
        v = replace_sales_plan_lines(db, int(version_id), df, bom_version_id=body.bom_version_id)
        return {"status": "ok", "version": _version_payload(v), "saved_rows": int(v.row_count or 0)}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail={"msg": str(e)})


@router.post("/article-names", summary="Resolve article names from active BOM")
def article_names(body: SalesPlanArticleLookup, db: Session = Depends(get_db)):
    try:
        bom_ver = get_resolved_bom_version(db, body.bom_version_id)
        bom_df = get_version_rows_df(db, int(bom_ver.id))
        name_map = article_name_map_from_df(bom_df)
        wanted = [str(x).strip() for x in (body.items or []) if str(x).strip()]
        if wanted:
            filtered = {k: v for k, v in name_map.items() if k in set(wanted)}
        else:
            filtered = name_map
        return {"status": "ok", "bom_version_id": int(bom_ver.id), "names": filtered}
    except Exception as e:
        raise HTTPException(status_code=400, detail={"msg": str(e)})
