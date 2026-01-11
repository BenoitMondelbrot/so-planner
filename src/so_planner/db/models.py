# src/so_planner/db/models.py  — unified
from sqlalchemy import (
    String, Integer, Float, Boolean, Date, DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func
from . import Base  

# ---------- Справочники / входные таблицы (как раньше) ----------
class DimMachine(Base):
    __tablename__ = "dim_machine"
    machine_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    family: Mapped[str] = mapped_column(String, nullable=True)
    shift_calendar: Mapped[str] = mapped_column(String, nullable=True)
    capacity_per_shift: Mapped[float] = mapped_column(Float, nullable=True)
    setup_time: Mapped[float] = mapped_column(Float, nullable=True)

class BOM(Base):
    __tablename__ = "bom"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[str] = mapped_column(String, index=True)
    article_name: Mapped[str | None] = mapped_column(String, nullable=True)
    component_id: Mapped[str] = mapped_column(String, index=True)
    qty_per: Mapped[float] = mapped_column(Float)
    routing_step: Mapped[int] = mapped_column(Integer, default=1)
    machine_family: Mapped[str] = mapped_column(String, nullable=True)
    proc_time_std: Mapped[float] = mapped_column(Float, nullable=True)

class Demand(Base):
    __tablename__ = "demand"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[str] = mapped_column(String, index=True, nullable=False)
    due_date: Mapped[Date] = mapped_column(Date, nullable=False)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    # было nullable=False — оставляем nullable=True, как в старой правке
    order_id: Mapped[str | None] = mapped_column(String, index=True, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    customer: Mapped[str | None] = mapped_column(String, nullable=True)

# ---------- Версионирование планов ----------
class PlanVersion(Base):
    __tablename__ = "plan_versions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)                  # имя из UI
    origin: Mapped[str] = mapped_column(String, nullable=False, default="greedy")  # greedy|milp|import
    status: Mapped[str] = mapped_column(String, nullable=False, default="ready")   # draft|running|ready|failed
    parent_plan_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("plan_versions.id"), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now())
    notes: Mapped[str | None] = mapped_column(String, nullable=True)
    input_hash: Mapped[str | None] = mapped_column(String, nullable=True)

    parent = relationship("PlanVersion", remote_side=[id])
    ops = relationship("ScheduleOp", back_populates="plan", cascade="all, delete-orphan")
    loads = relationship("MachineLoadDaily", back_populates="plan", cascade="all, delete-orphan")

# ---------- Расписания операций (объединяем старое и новое) ----------
class ScheduleOp(Base):
    __tablename__ = "schedule_ops"
    # Сохраняем имя PK как раньше (op_id), чтобы не ломать старый код
    op_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Новое: версия плана (обязательная)
    plan_id: Mapped[int] = mapped_column(Integer, ForeignKey("plan_versions.id"), index=True, nullable=False)

    # Поля, которые были раньше
    order_id: Mapped[str] = mapped_column(String, index=True)
    item_id: Mapped[str] = mapped_column(String, index=True)
    article_name: Mapped[str | None] = mapped_column(String, nullable=True)
    machine_id: Mapped[str] = mapped_column(String, index=True)
    start_ts: Mapped[DateTime] = mapped_column(DateTime, index=True)
    end_ts: Mapped[DateTime] = mapped_column(DateTime, index=True)
    setup_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    lateness_min: Mapped[float] = mapped_column(Float, default=0.0)

    # Новые поля из версии с greedy/MILP
    qty: Mapped[float | None] = mapped_column(Float, nullable=True)
    duration_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    setup_sec: Mapped[int] = mapped_column(Integer, nullable=True, default=0)
    batch_id: Mapped[str | None] = mapped_column(String, nullable=True)
    op_index: Mapped[int | None] = mapped_column(Integer, nullable=True)

    plan = relationship("PlanVersion", back_populates="ops")

    __table_args__ = (
        Index("ix_ops_machine_day", "machine_id", "start_ts"),
    )

# ---------- Нагрузка по дням (новая таблица для heatmap) ----------
class MachineLoadDaily(Base):
    __tablename__ = "machine_load_daily"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[int] = mapped_column(Integer, ForeignKey("plan_versions.id"), index=True, nullable=False)
    machine_id: Mapped[str] = mapped_column(String, index=True, nullable=False)
    work_date: Mapped[DateTime] = mapped_column(DateTime, index=True, nullable=False)  # 00:00
    load_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    cap_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    util: Mapped[float] = mapped_column(Float, nullable=False)  # load_sec / cap_sec

    plan = relationship("PlanVersion", back_populates="loads")
    __table_args__ = (
        UniqueConstraint("plan_id", "machine_id", "work_date", name="uq_plan_machine_day"),
    )

# ---------- Совместимость со старым Loads (оставляем как «устаревшее») ----------
class Loads(Base):
    __tablename__ = "loads"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    machine_id: Mapped[str] = mapped_column(String, index=True)
    date: Mapped[Date] = mapped_column(Date, index=True)
    minutes_used: Mapped[float] = mapped_column(Float)
    minutes_free: Mapped[float] = mapped_column(Float)
    queue_len: Mapped[int] = mapped_column(Integer, default=0)

Index("idx_loads_machine_date", Loads.machine_id, Loads.date)
