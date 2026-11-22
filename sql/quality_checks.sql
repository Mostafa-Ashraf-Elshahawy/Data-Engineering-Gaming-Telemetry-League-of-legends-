-- ====================================================================
-- Project: League of Legends Telemetry Data Engineering Pipeline
-- File: sql/quality_checks.sql
-- Phase 3: Post-Load SQL Data Quality Checks & Fail-Fast Enforcement
-- ====================================================================

-- 1. Create or replace a view summarizing all post-load data quality checks
CREATE OR REPLACE VIEW public.vw_data_quality_report AS
-- Check 1: Primary Key Uniqueness in matches
SELECT
    1 AS check_id,
    'PK_UNIQUENESS_MATCHES' AS check_name,
    'matches.match_id has 0 duplicate or NULL keys' AS rule_description,
    COUNT(*) - COUNT(DISTINCT match_id) + SUM(CASE WHEN match_id IS NULL THEN 1 ELSE 0 END) AS violations,
    CASE
        WHEN COUNT(*) > 0
         AND (COUNT(*) - COUNT(DISTINCT match_id) + SUM(CASE WHEN match_id IS NULL THEN 1 ELSE 0 END)) = 0
        THEN 'PASS' ELSE 'FAIL'
    END AS status
FROM public.matches

UNION ALL

-- Check 2: Composite Key Uniqueness in team_stats
SELECT
    2 AS check_id,
    'COMPOSITE_KEY_UNIQUENESS_TEAM_STATS' AS check_name,
    'team_stats (match_id, snapshot_time_pct, team_color) is 100% unique' AS rule_description,
    COUNT(*) - COUNT(DISTINCT (match_id, snapshot_time_pct, team_color)) AS violations,
    CASE
        WHEN COUNT(*) > 0
         AND (COUNT(*) - COUNT(DISTINCT (match_id, snapshot_time_pct, team_color))) = 0
        THEN 'PASS' ELSE 'FAIL'
    END AS status
FROM public.team_stats

UNION ALL

-- Check 3: Foreign Key Integrity (team_stats -> matches)
SELECT
    3 AS check_id,
    'FK_INTEGRITY_TEAM_STATS_TO_MATCHES' AS check_name,
    'Every team_stats.match_id exists in matches.match_id' AS rule_description,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM public.team_stats ts
LEFT JOIN public.matches m ON ts.match_id = m.match_id
WHERE m.match_id IS NULL

UNION ALL

-- Check 4: Foreign Key Integrity (objectives_control -> matches)
SELECT
    4 AS check_id,
    'FK_INTEGRITY_OBJECTIVES_TO_MATCHES' AS check_name,
    'Every objectives_control.match_id exists in matches.match_id' AS rule_description,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM public.objectives_control oc
LEFT JOIN public.matches m ON oc.match_id = m.match_id
WHERE m.match_id IS NULL

UNION ALL

-- Check 5: Null Threshold Check across critical columns (0 NULLs allowed)
SELECT
    5 AS check_id,
    'NULL_THRESHOLD_CRITICAL_COLUMNS' AS check_name,
    '0 NULL values in match_id, total_gold, snapshot_time_pct, team_color, win, first_blood' AS rule_description,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM public.team_stats
WHERE match_id IS NULL
   OR snapshot_time_pct IS NULL
   OR team_color IS NULL
   OR total_gold IS NULL
   OR first_blood IS NULL
   OR win IS NULL

UNION ALL

-- Check 6: Valid Match Duration Range (No remakes under 3 minutes / 180,000 ms)
SELECT
    6 AS check_id,
    'VALID_RANGE_MATCH_DURATION' AS check_name,
    'matches.full_time_ms >= 180000 (3.0 minutes minimum)' AS rule_description,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM public.matches
WHERE full_time_ms < 180000 OR game_duration_min < 3.00

UNION ALL

-- Check 7: Valid Numerical Ranges in team_stats (gold >= 2500, kills >= 0, towers 0..11)
SELECT
    7 AS check_id,
    'VALID_RANGE_TEAM_STATS_METRICS' AS check_name,
    'total_gold >= 2500, champion_kills >= 0, towers_destroyed BETWEEN 0 AND 11' AS rule_description,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM public.team_stats
WHERE total_gold < 2500
   OR champion_kills < 0
   OR towers_destroyed < 0
   OR towers_destroyed > 11

UNION ALL

-- Check 8: Valid Numerical Ranges in objectives_control (non-negative monster kills)
SELECT
    8 AS check_id,
    'VALID_RANGE_OBJECTIVES_METRICS' AS check_name,
    'dragons_killed >= 0, barons_killed >= 0, heralds_killed >= 0' AS rule_description,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM public.objectives_control
WHERE dragons_killed < 0
   OR barons_killed < 0
   OR heralds_killed < 0

UNION ALL

-- Check 9: Raw vs. Clean Row-Count Reconciliation
-- Clean matches (61,893) + Raw remakes (<180,000 ms: 2,663) MUST equal Raw total matches (64,556)
SELECT
    9 AS check_id,
    'RAW_VS_CLEAN_ROW_COUNT_RECONCILIATION' AS check_name,
    'clean_matches + raw_remakes == raw_total_matches AND fact_rows == clean_matches * 10' AS rule_description,
    ABS(
        (SELECT COUNT(*) FROM public.matches)
        + (SELECT COUNT(*) FROM raw.full_data_100 WHERE "fullTimeMS" < 180000)
        - (SELECT COUNT(*) FROM raw.full_data_100)
    )
    + ABS((SELECT COUNT(*) FROM public.team_stats) - (SELECT COUNT(*) * 10 FROM public.matches))
    + ABS((SELECT COUNT(*) FROM public.objectives_control) - (SELECT COUNT(*) * 10 FROM public.matches)) AS violations,
    CASE
        WHEN (SELECT COUNT(*) FROM public.matches) > 0
         AND (
            ABS(
                (SELECT COUNT(*) FROM public.matches)
                + (SELECT COUNT(*) FROM raw.full_data_100 WHERE "fullTimeMS" < 180000)
                - (SELECT COUNT(*) FROM raw.full_data_100)
            )
            + ABS((SELECT COUNT(*) FROM public.team_stats) - (SELECT COUNT(*) * 10 FROM public.matches))
            + ABS((SELECT COUNT(*) FROM public.objectives_control) - (SELECT COUNT(*) * 10 FROM public.matches))
         ) = 0
        THEN 'PASS' ELSE 'FAIL'
    END AS status;

-- 2. Automated Fail-Fast Block: raises a PostgreSQL exception if any check fails
DO $$
DECLARE
    failed_check RECORD;
    failure_count INT := 0;
BEGIN
    FOR failed_check IN
        SELECT check_id, check_name, violations, rule_description
        FROM public.vw_data_quality_report
        WHERE status = 'FAIL'
    LOOP
        failure_count := failure_count + 1;
        RAISE WARNING 'DATA QUALITY CHECK FAILED [#% - %]: % violations (%).',
            failed_check.check_id,
            failed_check.check_name,
            failed_check.violations,
            failed_check.rule_description;
    END LOOP;

    IF failure_count > 0 THEN
        RAISE EXCEPTION 'POST-LOAD DATA QUALITY VALIDATION FAILED: % check(s) did not pass. Halting pipeline.', failure_count;
    END IF;
END $$;

-- 3. Display the full quality check report table in pgAdmin
SELECT * FROM public.vw_data_quality_report ORDER BY check_id;
