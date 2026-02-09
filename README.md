# SO Planner

Система производственного планирования с API на FastAPI, загрузкой Excel-данных,
жадным планировщиком и блоком оптимизации (MILP / jobshop).

## Возможности
- Загрузка исходных данных из Excel: `machines.xlsx`, `BOM.xlsx`, `plan of sales.xlsx`, `stock.xlsx`, `receipts.xlsx`.
- Планирование и пересчёт графика через API (`/schedule/*`, `/plans/*`).
- Экспорт расписания и отчётов в Excel.
- Анализ узких мест и загрузки оборудования.
- Работа по умолчанию с SQLite, опционально с PostgreSQL.

## Требования
- Python 3.11+
- pip
- (Опционально) Docker для PostgreSQL

## Быстрый старт
```bash
git clone https://github.com/BenoitMondelbrot/so-planner.git
cd so-planner
python -m venv .venv
# Windows
.\\.venv\\Scripts\\activate
# Linux/macOS
# source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

## Запуск API
```bash
uvicorn src.so_planner.api.app:app --reload --host 0.0.0.0 --port 8000
```

После запуска:
- UI: `http://localhost:8000/`
- OpenAPI: `http://localhost:8000/docs`

## Переменные окружения
- `PYTHONPATH=src`
- `DATABASE_URL` (опционально):
  - SQLite по умолчанию: `sqlite:///./so_planner.db`
  - PostgreSQL: `postgresql+psycopg2://so_user:so_pass@localhost:5432/so_planner`
- `DB_LOG`: `off | summary | sql | full`

## Зависимости
- Основные зависимости описаны в `requirements.txt` и `pyproject.toml`.
- Для установки в editable-режиме также можно использовать:
```bash
pip install -e .
pip install -e .[runtime]
```

## База данных
- По умолчанию используется локальный файл `so_planner.db`.
- Для PostgreSQL используйте `docker-compose.yml`:
```bash
docker-compose up -d
```

## Полезные команды
```bash
python -m py_compile src/so_planner/api/app.py
git status
```
