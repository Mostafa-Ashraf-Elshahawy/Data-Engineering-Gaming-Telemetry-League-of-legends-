-- ====================================================
-- Phase 2: Raw Data Verification Queries (pgAdmin Demo)
-- ====================================================

-- 1. Check the ingestion execution audit log
SELECT
    ingestion_id,
    ingested_at,
    file_name,
    target_table,
    source_status,
    rows_fetched,
    columns_count,
    remake_matches_count,
    duration_seconds,
    status
FROM raw.ingestion_audit_log
ORDER BY ingestion_id DESC;

-- 2. Verify row counts across all 5 untouched raw tables (64,556 each = 322,780 total)
SELECT 'raw.full_data_20'  AS raw_table, COUNT(*) AS row_count FROM raw.full_data_20
UNION ALL
SELECT 'raw.full_data_40'  AS raw_table, COUNT(*) AS row_count FROM raw.full_data_40
UNION ALL
SELECT 'raw.full_data_60'  AS raw_table, COUNT(*) AS row_count FROM raw.full_data_60
UNION ALL
SELECT 'raw.full_data_80'  AS raw_table, COUNT(*) AS row_count FROM raw.full_data_80
UNION ALL
SELECT 'raw.full_data_100' AS raw_table, COUNT(*) AS row_count FROM raw.full_data_100;

-- 3. Preview untouched raw rows (all 42 original columns intact)
SELECT *
FROM raw.full_data_100
LIMIT 20;
