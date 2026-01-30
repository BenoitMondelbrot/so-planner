НЕТТИНГ В ОБЪЁМАХ (Product-View): подробное описание

Кратко: неттинг рассчитывает остаточный спрос (residual demand) по датам без учёта мощностей и длительностей. Он вычитает из плана продаж доступные запасы и подтверждённые поступления до даты, а остаток превращает в заказы (order_id, item_id, due_date, qty). Эти заказы затем идут в расписание.

1) Что именно считается (без мощностей и длительностей)

- Цель: получить объёмы к производству по датам (сколько нужной продукции, к какой дате), не планируя операции по станкам.
- Поуровневый алгоритм (product_view_generate_demand):
  - Для каждого уровня BOM спрос гасится складом/поступлениями/фиксами, остаток даёт дельта‑заказ.
  - Фиксированные заказы не добавляются в план спроса, но учитываются как поступления на своём уровне и как обязательное производство, которое разворачивается вниз по BOM.
  - После обработки уровня формируется спрос на дочерние позиции (qty_per_parent) и цикл повторяется на следующем уровне.
- Для каждой группы (item_id, workshop, due_date[, customer]) спрос проходит по датам в порядке возрастания due_date и покрывается:
  1) Точным складом в данном цехе (exact stock) — ограниченным ещё и глобальным пулом по клиенту.
  2) «Глобальным» пулом по клиенту (generic stock) — суммарный склад по данному item_id и customer через все цеха.
  3) Поступлениями (receipts) с due_date ≤ текущей даты потребности.
  4) Остаток -> создаётся заказ (order_created). Формат order_id: "<customer или item>-YYYYMMDD-PV###".
- Для каждой даты пишется строка лога с фактами покрытия и созданной потребностью.

2) Откуда берётся информация

- План спроса (plan_df):
  - Источник (DB-путь): таблица plan_line (item_id, due_date, qty[, workshop, customer]).
  - Источник (файлы): Excel «plan of sales.xlsx». Парсинг приводится к колонкам item_id, due_date, qty; workshop — по умолчанию из BOM для item_id; customer — опционально.
- Фиксированные заказы (plan_order_info.status='fixed'):
  - order_id, item_id, due_date, qty[, workshop]; customer в фиксах не используется.
  - Учитываются как поступления на уровне родителя и как обязательные заказы, которые разворачиваются вниз по BOM.
- Склад (stock_df):
  - Источник (DB): stock_line по snapshot_id (item_id, workshop, stock_qty). Опционально есть customer — если столбца нет, считается пустым.
  - Карты запасов:
    - stock_exact[(item_id, workshop, customer)] — суммарный точный склад в цехе и для клиента.
    - stock_pool[(item_id, customer)] — глобальный пул по клиенту, сумма по всем цехам (используется как «generic»).
- Поступления (existing_orders_df / receipts):
  - Источник (DB): receipts_plan по plan_version_id; режим "receipts_from":
    - "plan" — только receipts_plan;
    - "firmed" — из зафиксированных расписаний (schedule_version.status='firmed'), если таблицы есть;
    - "both" — объединение.
  - Источник (файлы): при наличии текущего schedule_out.xlsx берутся заказы из листа "schedule" (item_id,due_date,qty,workshop) как поступления.
- Спецификации (BOM):
  - Источник: Excel «BOM.xlsx».
  - Используется для:
    - Связи parent->child с коэффициентами qty_per_parent.
    - Маппинга workshop по item_id (если в BOM задан столбец workshop). Этот workshop применяется к спросу по item_id при неттинге.

3) Когда и как пишется в БД

- Основной вход: run_product_view_from_db(db, plan_version_id, stock_snapshot_id, receipts_from, ...). Запускается из API /netting/run и /schedule/greedy_json.
- Таблицы результатов:
  - netting_run: заголовок прогона (started_at, finished_at, user, mode='product_view', plan_version_id, stock_snapshot_id, receipts_source_desc, params, status).
  - netting_order: по каждой созданной residual-потребности: (netting_run_id, order_id, item_id, due_date, qty, priority, workshop[, customer]).
  - netting_log_row: подробный лог по дням: (netting_run_id, item_id, workshop, date, kind, opening_exact, opening_generic, stock_used_exact, stock_used_generic, receipts_used, order_created, available_after).
  - netting_summary_row: агрегаты по (item_id, workshop): stock_used_total, receipts_used_total, orders_created_total, opening_exact_init, opening_generic_init.
- Последовательность:
  1) После расчёта demand_net и формирования NETTING_LOG собирается summary.
  2) Вставляется заголовок в netting_run и далее bulk-вставка в netting_order / netting_log_row / netting_summary_row.
  3) Если demand_net пуст, расписание не строится; при непустом — дальше строится расписание и Excel-отчёт.

Примечание: orders_created_total в summary берётся из demand_net (сумма qty по ключам), а не из суммирования netting_log, чтобы избежать занижения при частичных лог-строках.

4) Отличия от schedule (расписания)

- Неттинг:
  - Считает только объёмы к датам (item_id, due_date, qty[, workshop, customer]).
  - Не учитывает мощности и длительности; не назначает операции на станки.
  - Вычитает склад и поступления только до даты потребности.
  - Выдаёт итоговую таблицу residual-заказов (demand_net / netting_order).
- Расписание (schedule):
  - Преобразует заказы и операции BOM в операции на станках (machine_id, step) с минутами и датами.
  - Учитывает мощности, очередность и т.п.; может дробить один заказ на множество операций/строк.
  - На листе "schedule" qty в строках повторяется по операциям; для сопоставления с неттингом объёмы следует агрегировать по уникальному base_order_id.
- Связь арифметики:
  - Сумма order_created_total из netting_summary (или суммы qty из demand_net по item_id/workshop[/customer]) должна совпадать с суммой qty заказов в расписании, если суммировать по уникальным base_order_id (а не по операциям).

5) Вкладки Excel: "netting_log" и "netting_summary"

- netting_log (покрытие по дням):
  - Колонки: item_id, workshop, customer, date, kind ('opening' или 'day'), opening_exact, opening_generic, stock_used_exact, stock_used_generic, receipts_used, order_created, available_after.
  - Opening-строка (kind='opening', date=None): стартовые доступности по группе (точный и «generic» склад), available_after = общий пул.
  - Day-строка (kind='day', date=due_date): факт покрытия этой даты (склад exact, затем generic, затем receipts) и величина order_created для остатка. available_after = остаток пула + неиспользованные receipts на дату.
- netting_summary (агрегаты):
  - Ключи: item_id, workshop (если customer используется в логе, в Excel-итоге может присутствовать разрез по customer; в DB-таблице netting_summary_row — без customer).
  - Поля:
    - stock_used_total = stock_used_exact + stock_used_generic (сумма по строкам kind='day').
    - receipts_used_total = сумма receipts_used по day-строкам.
    - orders_created_total = сумма qty из demand_net по ключам (а не из лога).
    - opening_exact_init / opening_generic_init — максимум из opening-строк по группе.

Доп. детали алгоритма покрытия по дате (внутри net_pass):

- Перед началом для группы (item_id, workshop[, customer]) рассчитывается:
  - pool = stock_pool[(item_id, customer)] — глобальный складовой пул по клиенту.
  - avail_exact = min(stock_exact[(item_id, workshop, customer)], pool) — точный доступ в цехе, ограниченный пулом.
- Для каждой даты due_date:
  1) Накапливаются receipts с due_date ≤ текущей дате в receipts_remain.
  2) Сначала списывается из avail_exact (и уменьшает общий pool).
  3) Затем «generic» — из остатка pool (снимается независимо от цеха).
  4) Затем из receipts_remain.
  5) Остаток превращается в order_created; формируется order_id (с префиксом customer, если есть, иначе item_id).
  6) Пишется строка "day" в лог с фактами; после покрытий available_after = текущий pool + receipts_remain.

Поля и форматы вводов/выводов (минимум):

- plan_df: item_id (str), due_date (date), qty (int), [workshop (str), customer (str)] — workshop подставляется из BOM, если есть.
- stock_df: item_id (str), stock_qty (int), [workshop (str, по умолчанию ''), customer (str, по умолчанию '')].
- existing_orders_df (receipts): item_id (str), due_date (date), qty (int), [workshop (str, по умолчанию '')].
- demand_net (результат): order_id, item_id, due_date, qty, priority (ts), workshop, [customer].

Практические замечания:

- Если в этом же запуске будет строиться расписание, то объёмы из demand_net не должны повторно списывать склад — greedy_schedule вызывается со stock_map=None.
- Для сопоставления с "schedule" используйте уникальные base_order_id на листе расписания: суммируйте qty по base_order_id, а не по операциям/строкам.
