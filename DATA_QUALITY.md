# Data Quality Framework & Validation Rules (`DATA_QUALITY.md`)

**Project:** League of Legends Ranked Match Telemetry Analysis  
**Phase:** Phase 3 — Data Cleaning, Relational Modeling, PostgreSQL & Data Quality

---

## 1. Overview of Fail-Fast Architecture

Our data pipeline enforces **two layers of automated data quality gates**:
1. **Layer 1 — Pre-Load Python Validation (`scripts/clean_and_load.py`)**: Validates raw file/column completeness, null counts, duplicate keys, and cleaned value domains **before** opening a database write transaction.
2. **Layer 2 — Post-Load SQL Validation (`sql/quality_checks.sql`)**: Queries the live PostgreSQL tables (`public.matches`, `public.team_stats`, `public.objectives_control`) via `public.vw_data_quality_report` and executes an automated PL/pgSQL `DO $$ ... RAISE EXCEPTION ... $$` block that halts execution and returns a non-zero exit code (`1`) if any rule registers `violations > 0`.

---

## 2. Complete List of Python & SQL Data Quality Rules and Thresholds

### A. Pre-Load Python Data Quality Rules (`run_preload_checks` & `validate_clean_data_preload`)

| Rule ID | Check Name | Target | Threshold / Expected Condition | Failure Behavior |
| :---: | :--- | :--- | :--- | :--- |
| **PY-01** | Snapshot Completeness | `data/raw/full_data_*.csv` | All 5 intervals (`20, 40, 60, 80, 100`) must exist and be non-empty. | Raises `ValueError`, logs `PRE-LOAD FAIL`, exits with code `1`. |
| **PY-02** | Cross-Snapshot Row Alignment | All 5 raw DataFrames | Every snapshot file must contain the exact same number of raw rows (`64,556`). | Raises `ValueError`, halts before transformation. |
| **PY-03** | Required Schema Columns | All 5 raw DataFrames | All 19 core telemetry columns (`matchID`, `fullTimeMS`, `blue*`, `red*`) must be present. | Raises `ValueError` listing missing columns, exits with code `1`. |
| **PY-04** | Pre-Load Null Threshold | Required raw columns | `0` NULL cells allowed (`0%` null tolerance). | Raises `ValueError` with null count, exits with code `1`. |
| **PY-05** | Pre-Load Duplicate Match IDs | `matchID` per snapshot | `0` duplicate `matchID` rows within any snapshot interval. | Raises `ValueError` with duplicate count, exits with code `1`. |
| **PY-06** | Pre-Insert Domain & Cardinality Check | Cleaned DataFrames | `full_time_ms >= 180000`, `total_gold >= 2500`, `towers_destroyed IN [0..11]`, and `len(team_stats) == len(matches) * 10`. | Raises `ValueError` (`CLEAN CHECK FAIL`), aborts DB load. |

### B. Post-Load PostgreSQL Data Quality Rules (`sql/quality_checks.sql`)

| Check ID | Check Name (`vw_data_quality_report`) | Target Table(s) | SQL Validation Rule & Threshold | Failure Behavior |
| :---: | :--- | :--- | :--- | :--- |
| **SQL-01** | `PK_UNIQUENESS_MATCHES` | `public.matches` | `COUNT(*) - COUNT(DISTINCT match_id) = 0` and `0` NULL `match_id`s (`Threshold: 0 violations`). | `RAISE EXCEPTION` in PL/pgSQL block; pipeline exits with code `1`. |
| **SQL-02** | `COMPOSITE_KEY_UNIQUENESS_TEAM_STATS` | `public.team_stats` | `COUNT(*) - COUNT(DISTINCT (match_id, snapshot_time_pct, team_color)) = 0` (`Threshold: 0 violations`). | `RAISE EXCEPTION`; transaction rolled back; exit code `1`. |
| **SQL-03** | `FK_INTEGRITY_TEAM_STATS_TO_MATCHES` | `team_stats` ⟕ `matches` | `LEFT JOIN public.matches` finds `0` orphan `match_id` rows (`Threshold: 0 violations`). | `RAISE EXCEPTION`; pipeline exits with code `1`. |
| **SQL-04** | `FK_INTEGRITY_OBJECTIVES_TO_MATCHES` | `objectives_control` ⟕ `matches` | `LEFT JOIN public.matches` finds `0` orphan `match_id` rows (`Threshold: 0 violations`). | `RAISE EXCEPTION`; pipeline exits with code `1`. |
| **SQL-05** | `NULL_THRESHOLD_CRITICAL_COLUMNS` | `public.team_stats` | `0` NULLs in `match_id`, `snapshot_time_pct`, `team_color`, `total_gold`, `first_blood`, `win` (`Threshold: 0 violations`). | `RAISE EXCEPTION`; pipeline exits with code `1`. |
| **SQL-06** | `VALID_RANGE_MATCH_DURATION` | `public.matches` | `0` rows with `full_time_ms < 180000` or `game_duration_min < 3.00` (`Threshold: 0 violations`). | `RAISE EXCEPTION`; pipeline exits with code `1`. |
| **SQL-07** | `VALID_RANGE_TEAM_STATS_METRICS` | `public.team_stats` | `total_gold >= 2500`, `champion_kills >= 0`, `towers_destroyed BETWEEN 0 AND 11` (`Threshold: 0 violations`). | `RAISE EXCEPTION`; pipeline exits with code `1`. |
| **SQL-08** | `VALID_RANGE_OBJECTIVES_METRICS` | `public.objectives_control` | `dragons_killed >= 0`, `barons_killed >= 0`, `heralds_killed >= 0` (`Threshold: 0 violations`). | `RAISE EXCEPTION`; pipeline exits with code `1`. |
| **SQL-09** | `RAW_VS_CLEAN_ROW_COUNT_RECONCILIATION` | `raw.full_data_100` vs `public.*` | `clean_matches (61,893) + raw_remakes (2,663) = raw_total (64,556)` AND `fact_rows (618,930) = clean_matches * 10` (`Threshold: 0 diff`). | `RAISE EXCEPTION`; pipeline exits with code `1`. |

---

## 3. Pass & Fail Scenario Verification Logs

### Scenario A: Clean Pass Run (`python scripts/clean_and_load.py`)
Command:
```bash
docker exec lol_ingestion python scripts/clean_and_load.py
# Or locally: python scripts/clean_and_load.py
```
Console Output (Exit Code `0`):
```text
INFO     | ==============================================================================
INFO     | STARTING PHASE 3: DATA CLEANING, RELATIONAL MODELING & DATA QUALITY
INFO     | ==============================================================================
INFO     | Running Pre-Load Data Quality Checks on raw data...
INFO     | Pre-Load Checks PASSED: 5 snapshots verified (64556 rows each, 322780 total raw rows, 0 nulls, 0 duplicates).
INFO     | Cleaning Summary: Raw Matches=64556 | Remakes Removed (<3m)=2663 | Clean Matches=61893
INFO     | Row Counts: Raw Snapshots=322780 -> Clean Wide Snapshots=309465 -> Unpivoted Team Rows=618930 (per fact table) | FirstBlood Fixed=15
INFO     | Pre-Insert Validation on Cleaned Tables PASSED.
INFO     | Database Load Complete: matches=61893 rows | team_stats=618930 rows | objectives_control=618930 rows | Views created.
INFO     | Post-Load SQL Data Quality Report:
INFO     |   [Check #1] PK_UNIQUENESS_MATCHES                    | Violations: 0 | Status: PASS
INFO     |   [Check #2] COMPOSITE_KEY_UNIQUENESS_TEAM_STATS      | Violations: 0 | Status: PASS
INFO     |   [Check #3] FK_INTEGRITY_TEAM_STATS_TO_MATCHES       | Violations: 0 | Status: PASS
INFO     |   [Check #4] FK_INTEGRITY_OBJECTIVES_TO_MATCHES       | Violations: 0 | Status: PASS
INFO     |   [Check #5] NULL_THRESHOLD_CRITICAL_COLUMNS          | Violations: 0 | Status: PASS
INFO     |   [Check #6] VALID_RANGE_MATCH_DURATION               | Violations: 0 | Status: PASS
INFO     |   [Check #7] VALID_RANGE_TEAM_STATS_METRICS           | Violations: 0 | Status: PASS
INFO     |   [Check #8] VALID_RANGE_OBJECTIVES_METRICS           | Violations: 0 | Status: PASS
INFO     |   [Check #9] RAW_VS_CLEAN_ROW_COUNT_RECONCILIATION    | Violations: 0 | Status: PASS
INFO     | ==============================================================================
INFO     | PHASE 3 CLEANING, LOADING & QUALITY CHECKS COMPLETED
INFO     | ==============================================================================
```

### Scenario B: Intentional Bad-Data Failure Test (`python scripts/clean_and_load.py --test-fail`)
Command:
```bash
python scripts/clean_and_load.py --test-fail
```
Console Output (Exit Code `1` — Halts Execution & Rolls Back Corrupt Data):
```text
INFO     | ==============================================================================
INFO     | STARTING PHASE 3: DATA CLEANING, RELATIONAL MODELING & DATA QUALITY
INFO     | ==============================================================================
INFO     | ==============================================================================
INFO     | RUNNING INTENTIONAL BAD-DATA FAILURE TEST (--test-fail)
INFO     | ==============================================================================
WARNING  | Injected bad record: team_stats.total_gold = -9999 (stat_id=1)
ERROR    | QUALITY CHECK CAUGHT CORRUPT DATA -> [Check #7] VALID_RANGE_TEAM_STATS_METRICS | Violations=1 | Status=FAIL
ERROR    | Pipeline halted as expected during bad-data test: FAIL-FAST TRIGGERED: 1 post-load data quality check(s) failed! Rolling back and halting pipeline.
```
