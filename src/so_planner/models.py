# src/so_planner/models.py
# Совместимость со старым кодом: реэкспорт ORM-моделей из нового места.
from .db.models import *  # noqa: F401,F403

# (опционально) зафиксировать __all__, чтобы linters не ругались
try:
    from .db.models import __all__ as _ALL  # если вы его объявляете
    __all__ = list(_ALL)  # type: ignore
except Exception:
    # если в db.models нет __all__, просто ничего не делаем
    pass
