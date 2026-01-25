# SO Planner

Производственное планирование с эвристиками, отчётами и API на FastAPI. Работает из коробки на SQLite, при желании может использовать PostgreSQL (docker-compose.yml).

## Требования
- Python 3.11+
- pip + виртуальное окружение (`python -m venv .venv`)
- Опционально: Docker для PostgreSQL (compose), Git
- Основной стек: FastAPI, SQLAlchemy, Pandas, NumPy, OpenPyXL, OR-Tools, Matplotlib

## Установка и запуск
1. Клонировать репозиторий:  
   `git clone https://github.com/BenoitMondelbrot/so-planner.git && cd so-planner`
2. Создать и активировать окружение:  
   `python -m venv .venv && .\.venv\Scripts\activate` (Windows)  
   `python -m venv .venv && source .venv/bin/activate` (Linux/macOS)
3. Установить зависимости (включая рантайм-опции для запуска API/Excel):  
   `pip install --upgrade pip`  
   `pip install -e .[runtime]`
4. Настроить переменные окружения (можно через `.env`):
   - `PYTHONPATH=src` (см. `.env.example`)
   - `DATABASE_URL` — опционально. Без значения используется SQLite `./so_planner.db`. Для PostgreSQL: `postgresql+psycopg2://so_user:so_pass@localhost:5432/so_planner`
   - `DB_LOG` — `off|summary|sql|full` для логов SQLAlchemy (опционально).
5. Запустить API:  
   `uvicorn src.so_planner.api.app:app --reload --host 0.0.0.0 --port 8000`
6. UI доступен по `http://localhost:8000/` (статические файлы монтируются из `src/so_planner/ui`).

## Работа с БД
- По умолчанию используется SQLite файл `so_planner.db` (лежит в корне).
- Для PostgreSQL можно поднять контейнер: `docker-compose up -d` (логин/пароль/БД см. `docker-compose.yml`) и выставить `DATABASE_URL` как выше.

## Загрузка данных
Через UI можно загрузить Excel-файлы (`machines.xlsx`, `BOM.xlsx`, `plan of sales.xlsx`, `stock.xlsx`, `receipts.xlsx`). В API реализованы загрузка, планирование, экспорт расписания и отчётов (в т.ч. отчёт «Дефицит к перемещению»).  

## Полезные команды
- Статус git: `git status`
- Формат установки зависимостей: `pip install -e .` или `pip install -e .[runtime]` (добавляет uvicorn, python-dotenv, xlsxwriter).
- Проверка синтаксиса API: `python -m py_compile src/so_planner/api/app.py`

