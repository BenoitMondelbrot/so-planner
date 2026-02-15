# src/so_planner/db/__init__.py
from __future__ import annotations
import os
import time
import logging
import contextvars
from contextlib import contextmanager
from typing import Generator, Iterable

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ---- Config -----------------------------------------------------------------
def _default_db_url() -> str:
    # Файл БД рядом с процессом; можно переопределить через env
    return os.getenv("DATABASE_URL", "sqlite:///./so_planner.db")

DATABASE_URL = _default_db_url()

# ---- Logging ---------------------------------------------------------------
# Controlled by env var DB_LOG:
#   off | summary | sql | full
# - summary: one-line per query (op, rows, ms)
# - sql: SQL text + trimmed params
# - full: summary + SQL + errors
_db_log_mode = os.getenv("DB_LOG", "off").strip().lower()
_LOG_SUMMARY = _db_log_mode in {"summary", "full"}
_LOG_SQL = _db_log_mode in {"sql", "full"}
_LOG_ERRORS = _db_log_mode in {"summary", "sql", "full"}

_logger = logging.getLogger("so_planner.sql")
if _db_log_mode != "off" and not _logger.handlers:
    # inherit root handlers; user can configure formatters globally
    _logger.setLevel(logging.INFO)

# correlation id for request-scoped logs
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")

def set_request_id(rid: str) -> None:
    try:
        _request_id_ctx.set(str(rid))
    except Exception:
        pass

def _short_params(p):
    try:
        if p is None:
            return None
        if isinstance(p, (list, tuple)):
            return [str(x)[:120] for x in p]
        if isinstance(p, dict):
            return {k: (str(v)[:120]) for k, v in p.items()}
        return str(p)[:120]
    except Exception:
        return "<params>"

# Для SQLite нужен спец-параметр
_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

# ---- Core objects ------------------------------------------------------------
engine: Engine = create_engine(
    DATABASE_URL,
    future=True,
    echo=os.getenv("SQL_ECHO", "0") == "1",
    connect_args={**_connect_args, **({"timeout": 60} if DATABASE_URL.startswith("sqlite") else {})},
    pool_pre_ping=True,
)

# Включаем внешние ключи в SQLite
if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
        except Exception:
            pass
        # Improve concurrency for readers/writers
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        try:
            cursor.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        try:
            cursor.execute("PRAGMA busy_timeout=60000")  # ms
        except Exception:
            pass
        cursor.close()

    @event.listens_for(engine, "before_cursor_execute")
    def _log_before_execute(conn, cursor, statement, parameters, context, executemany):  # noqa: D401
        if _db_log_mode == "off":
            return
        stack = conn.info.setdefault("_query_start_time", [])
        stack.append(time.perf_counter())
        if _LOG_SQL:
            rid = _request_id_ctx.get()
            _logger.info("[%s] SQL: %s | params=%s", rid, statement, _short_params(parameters))

    @event.listens_for(engine, "after_cursor_execute")
    def _log_after_execute(conn, cursor, statement, parameters, context, executemany):
        if _db_log_mode == "off":
            return
        try:
            start = conn.info.get("_query_start_time", []).pop()
        except Exception:
            start = None
        dur_ms = (time.perf_counter() - start) * 1000 if start else None
        if _LOG_SUMMARY:
            rid = _request_id_ctx.get()
            # crude op detection
            op = statement.strip().split(" ", 1)[0].upper() if statement else "SQL"
            _logger.info("[%s] %s rows=%s ms=%.2f", rid, op, getattr(cursor, "rowcount", None), (dur_ms or 0.0))

    @event.listens_for(engine, "handle_error")
    def _log_error(context):  # pragma: no cover
        if _db_log_mode == "off":
            return
        if _LOG_ERRORS:
            rid = _request_id_ctx.get()
            err = context.original_exception
            _logger.warning("[%s] DB-ERROR: %s | stmt=%s | params=%s", rid, err, context.statement, _short_params(context.parameters))

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
    class_=Session,
)

Base = declarative_base()

# ---- Public API --------------------------------------------------------------
def init_db() -> None:
    """
    Регистрирует все модели и создаёт недостающие таблицы.
    Важно: импортировать models до create_all, чтобы таблицы попали в metadata.
    """
    # Импорт моделей один раз при инициализации: регистрирует их в Base.metadata
    from . import models  # noqa: F401  # pylint: disable=unused-import

    Base.metadata.create_all(bind=engine)
    _ensure_sqlite_columns()


def _ensure_sqlite_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    def _col_name(row) -> str:
        if isinstance(row, dict):
            return str(row.get("name") or row.get("column") or "")
        try:
            return str(row[1])
        except Exception:
            return ""

    def _ensure_column(table: str, col: str, ddl: str) -> None:
        try:
            with engine.begin() as conn:
                rows = conn.execute(text(f"PRAGMA table_info({table})")).mappings().all()
                names = {_col_name(r) for r in rows}
                if col not in names:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}"))
        except Exception:
            pass

    _ensure_column("bom", "article_name", "TEXT")
    _ensure_column("bom", "loss", "REAL")
    _ensure_column("bom", "workshop", "TEXT")
    _ensure_column("bom", "time_per_unit", "REAL")
    _ensure_column("bom", "machine_time", "REAL")
    _ensure_column("bom", "setting_time", "REAL")
    _ensure_column("bom", "source_step", "TEXT")
    _ensure_column("bom", "setup_minutes", "REAL")
    _ensure_column("bom", "lag_time", "REAL")
    _ensure_column("bom_lines", "workshop", "TEXT")
    _ensure_column("bom_lines", "loss", "REAL")
    _ensure_column("bom_lines", "time_per_unit", "REAL")
    _ensure_column("bom_lines", "machine_time", "REAL")
    _ensure_column("bom_lines", "setting_time", "REAL")
    _ensure_column("bom_lines", "source_step", "TEXT")
    _ensure_column("bom_lines", "setup_minutes", "REAL")
    _ensure_column("bom_lines", "lag_time", "REAL")
    _ensure_column("schedule_ops", "article_name", "TEXT")
    _ensure_column("plan_versions", "bom_version_id", "INTEGER")
    _ensure_column("plan_versions", "sales_plan_version_id", "INTEGER")

@contextmanager
def session_scope() -> Iterable[Session]:
    """
    Контекстный менеджер для короткоживущих операций БД:
        with session_scope() as db:
            db.add(obj); ...
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def get_db() -> Generator[Session, None, None]:
    """
    Зависимость для FastAPI (если удобно тянуть прямо отсюда).
    Идентична тому, что обычно лежит в api/deps.py.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def run_raw(sql: str, **params):
    """Быстрый helper для отладки SQL."""
    with engine.begin() as conn:
        return conn.execute(text(sql), params)
