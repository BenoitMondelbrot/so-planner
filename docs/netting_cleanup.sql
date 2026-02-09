-- Cleanup of removed netting/product_view tables (SQLite).
-- WARNING: This is destructive. Make a backup before running.

PRAGMA foreign_keys = OFF;

-- Core netting artifacts
DROP TABLE IF EXISTS netting_log_row;
DROP TABLE IF EXISTS netting_summary_row;
DROP TABLE IF EXISTS netting_order;
DROP TABLE IF EXISTS netting_run;
DROP TABLE IF EXISTS demand_linkage;

-- Optional: leftover indexes (usually dropped with tables, kept for safety)
DROP INDEX IF EXISTS ix_netting_order;
DROP INDEX IF EXISTS ix_netting_log_row;
DROP INDEX IF EXISTS ix_netting_summary_row;
DROP INDEX IF EXISTS ix_plan_line_main;
DROP INDEX IF EXISTS ix_receipts_plan_main;

PRAGMA foreign_keys = ON;

-- Optional: reclaim space after dropping large tables
-- VACUUM;
