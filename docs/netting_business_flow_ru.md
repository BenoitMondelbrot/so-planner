Обновление неттинга и UI под типичный бизнес‑кейс

Цель: единая кнопка «Запустить неттинг + расписание» по заранее выбранным входам (machines, BOM, stock, план продаж, и план фиксированных поступлений), с автоматическим сохранением результата как временного плана в БД и быстрыми отчётами.

1) Входные данные и как их указывать

- Файлы как и раньше (вкладка «Файлы»):
  - machines.xlsx — мощности/оборудование
  - BOM.xlsx — спецификации
  - plan of sales.xlsx — план продаж
  - stock.xlsx — склад (опционально)
  - receipts.xlsx — план фиксированных поступлений (новое; опционально)
- Альтернативно к receipts.xlsx — использовать план поступлений из БД:
  - receipts_from: plan | firmed | both
  - plan_version_id — версия плана, к которой относятся записи в receipts_plan (для режима plan/both)
- UI: добавляется поле загрузки «План поступлений (Excel)» и кнопка «Upload Receipts». На вкладке «Неттинг» селектор Receipts From содержит варианты plan, firmed, both, excel.

2) Запуск прогона и учёт фиксированных поступлений

- Роут: POST /schedule/greedy_json
- Тело:
  - plan_version_id: int | null
  - stock_snapshot_id: int
  - receipts_from: "plan" | "firmed" | "both" | "excel"
  - receipts_excel_path: string | null (если receipts_from="excel"; иначе игнорируется)
- Бэкенд:
  - Если receipts_from="excel" и передан receipts_excel_path (или загружен через /upload/receipts) — загрузить поступления из Excel (ожидаемые колонки: item_id, due_date, qty[, workshop]).
  - Иначе — загрузить поступления из БД (receipts_plan и/или firmed schedule).
  - Выполнить Product-View неттинг: покрыть план продаж запасами и поступлениями до даты, остаток → заказы.

3) Построение расписания по операциям

- После расчёта residual-заказов (demand_net) вызывается greedy‑планировщик:
  - На вход — demand_net, BOM, machines.
  - Без повторного списания склада (stock_map=None), т.к. склад учтён в неттинге.
  - На выход — расписание операций по датам/станкам и Excel‑отчёт (schedule_out.xlsx).

4) Фиксированные заказы и многоуровневый неттинг

- Фиксированные корневые заказы (`plan_order_info.status='fixed'`) больше не подмешиваются в план спроса. Они учитываются как поступления на своём уровне и как обязательные заказы, которые разворачиваются вниз по BOM.
- Неттинг идёт по уровням BOM: сначала закрываем родителя складом/поступлениями/фиксами, создаём дельта‑заказ (если нужно), после чего формируем спрос на детей.
- `order_id`/`base_order_id` протягиваются неизменными, чтобы в расписании была видна цепочка fixed/delta и родитель‑потомок.

5) Автосохранение в БД как «временный план»

- Функция run_product_view_from_db создаёт PlanVersion с полями:
  - name: "Netting <UTC‑timestamp>"
  - origin: "product_view"
  - status: "draft" (временный план)
- Сохраняются:
  - Операции расписания (ScheduleOp)
  - Суточные загрузки (MachineLoadDaily) для heatmap
  - Дополнительно — due_date по заказам в plan_order_info для отчётов
- UI: в вкладке «Планы» выбранный (draft) план доступен для просмотра heatmap и Ганта. Добавлен API для утверждения: POST /plans/{id}/approve (перевод статуса в "approved").

5) Отчёты (heatmap и Gantt)

- Построение из БД (по plan_id), доступно сразу после прогона:
  - Heatmap: GET /plans/{plan_id}/heatmap
  - Гант: GET /reports/plans/{plan_id}/orders_timeline
- UI: на вкладке «Планы» — просмотр матрицы загрузок; на вкладке «Отчёты» — Гант и экспорт в Excel.

Новые/изменённые API

- POST /upload/receipts — загрузить Excel c поступлениями. Ответ: stored_path и обновлённые active_paths.
- POST /schedule/greedy_json — добавлена поддержка receipts_from="excel" и поля receipts_excel_path.
- POST /netting/run — аналогичная поддержка receipts_excel_path (для записей netting_* в БД без построения JSON‑результата).
- POST /plans/{plan_id}/approve — перевести план в статус "approved".

Изменения бэкенда (реализация)

- so_planner/scheduling/greedy_scheduler.py
  - run_product_view_from_db(db, plan_version_id, stock_snapshot_id, receipts_from, receipts_excel_path, ...): если receipts_from="excel" — загрузка поступлений из Excel (_load_receipts_excel). План сохраняется как status="draft".
  - _load_receipts_excel(path): читает item_id,due_date,qty[,workshop]; нормализация названий столбцов; агрегирование по (item_id,workshop,due_date).
  - Формирование netting_summary: orders_created_total берётся из demand_net (а не из суммирования лога).
- so_planner/api/app.py
  - LAST_PATHS дополнен ключом receipts.
  - NettingRunIn: receipts_from расширен значением "excel"; добавлено поле receipts_excel_path.
  - POST /upload/receipts: сохранение загруженного Excel и обновление LAST_PATHS['receipts'].
  - /schedule/greedy_json и /netting/run передают receipts_excel_path в неттинг при выборе "excel".
- so_planner/api/routers/plans.py
  - POST /plans/{id}/approve — перевод плана в "approved".

Мини‑гайд по формату receipts.xlsx

- Минимум колонок: item_id, due_date, qty
- Необязательно: workshop
- Типы: item_id — строка; due_date — дата; qty — целое > 0
- Можно грузить одним листом; берётся первый лист.

Пользовательский сценарий (UI)

1) Загрузить machines, BOM, plan, (опционально) stock и (опционально) receipts (кнопка Upload Receipts)
2) На вкладке «Неттинг» указать plan_version_id (если нужно), stock_snapshot_id, выбрать Receipts From (plan/firmed/both/excel).
3) Нажать «Запустить (JSON)». Получить plan_id черновика.
4) Посмотреть heatmap/Гант для выбранного draft‑плана. При необходимости нажать «Approve» (сохранить как версию).
