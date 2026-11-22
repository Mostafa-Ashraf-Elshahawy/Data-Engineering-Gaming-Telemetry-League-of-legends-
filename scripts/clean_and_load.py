"""
Phase 3: Data Cleaning, Relational Modeling & Data Quality Pipeline
Reads raw data, runs pre-load checks, cleans and normalizes into 3 tables,
loads into PostgreSQL, builds analytical views, and runs post-load SQL checks.
"""

import io
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> None:
        pass

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    Engine = object  # type: ignore
    SQLAlchemyError = Exception  # type: ignore

# Set up simple console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("lol_phase3")

# The 5 match snapshot percentages
SNAPSHOT_INTERVALS: List[int] = [20, 40, 60, 80, 100]

# Minimum valid match duration: 3 minutes (180,000 ms). Shorter games are /remake matches.
MIN_DURATION_MS: int = 180000

# Required raw columns needed for our 3 relational tables
REQUIRED_RAW_COLUMNS: List[str] = [
    "matchID",
    "fullTimeMS",
    "timePercent",
    "blueChampionKill",
    "blueFirstBlood",
    "blueDragonKill",
    "blueRiftHeraldKill",
    "blueBaronKill",
    "blueTowerKill",
    "blueTotalGold",
    "blueWin",
    "redChampionKill",
    "redFirstBlood",
    "redDragonKill",
    "redRiftHeraldKill",
    "redBaronKill",
    "redTowerKill",
    "redTotalGold",
    "redWin",
]


def load_config() -> Dict[str, str]:
    """Load database and folder settings from environment variables."""
    load_dotenv()
    base_dir = Path(__file__).resolve().parent.parent
    default_raw_dir = (
        "/app/data/raw" if Path("/app/data/raw").exists() else str(base_dir / "data" / "raw")
    )
    default_sql_dir = (
        "/app/sql" if Path("/app/sql").exists() else str(base_dir / "sql")
    )
    return {
        "user": os.getenv("POSTGRES_USER", "lol_admin"),
        "password": os.getenv("POSTGRES_PASSWORD", "change_me"),
        "db": os.getenv("POSTGRES_DB", "lol_analytics"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "raw_dir": os.getenv("RAW_DATA_DIR", default_raw_dir),
        "sql_dir": default_sql_dir,
    }


def get_engine(config: Dict[str, str]) -> Engine:
    """Connect to PostgreSQL database."""
    url = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['db']}"
    )
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def run_preload_checks(raw_dfs: Dict[int, pd.DataFrame]) -> None:
    """
    Step 1 (Pre-Load Checks): Verify raw files are complete, have all required
    columns, and contain no nulls or duplicate matchIDs before cleaning.
    """
    logger.info("Running Pre-Load Data Quality Checks on raw data...")

    # Check 1: All 5 snapshot intervals must exist
    if set(raw_dfs.keys()) != set(SNAPSHOT_INTERVALS):
        raise ValueError(f"PRE-LOAD FAIL: Expected intervals {SNAPSHOT_INTERVALS}, got {list(raw_dfs.keys())}")

    expected_row_count = len(raw_dfs[100])
    if expected_row_count == 0:
        raise ValueError("PRE-LOAD FAIL: Raw dataset is empty (0 rows).")

    for pct, df in raw_dfs.items():
        # Check 2: Row count consistency across all 5 snapshot files
        if len(df) != expected_row_count:
            raise ValueError(
                f"PRE-LOAD FAIL: Snapshot {pct}% has {len(df)} rows, expected {expected_row_count}."
            )

        # Check 3: All required columns must exist
        missing_cols = [col for col in REQUIRED_RAW_COLUMNS if col not in df.columns]
        if missing_cols:
            raise ValueError(f"PRE-LOAD FAIL: Snapshot {pct}% is missing columns: {missing_cols}")

        # Check 4: Zero NULL values allowed in required columns
        null_count = int(df[REQUIRED_RAW_COLUMNS].isnull().sum().sum())
        if null_count > 0:
            raise ValueError(f"PRE-LOAD FAIL: Snapshot {pct}% contains {null_count} NULL values.")

        # Check 5: No duplicate matchIDs within the same snapshot
        dup_count = int(df["matchID"].duplicated().sum())
        if dup_count > 0:
            raise ValueError(f"PRE-LOAD FAIL: Snapshot {pct}% contains {dup_count} duplicate matchIDs.")

    logger.info(
        "Pre-Load Checks PASSED: 5 snapshots verified (%d rows each, %d total raw rows, 0 nulls, 0 duplicates).",
        expected_row_count,
        expected_row_count * len(SNAPSHOT_INTERVALS),
    )


def clean_and_normalize(
    raw_dfs: Dict[int, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Step 1 & 2 (Data Cleaning & Normalization):
    1. Drop redundant 'Unnamed: 0' index column.
    2. Filter out aborted /remake matches under 3 minutes (fullTimeMS < 180,000).
    3. Fix FirstBlood contradiction where kills > 0 but both FirstBlood flags are False.
    4. Enforce data types and reshape wide Blue/Red columns into 3 relational tables:
       - matches (Dimension)
       - team_stats (Fact)
       - objectives_control (Fact)
    """
    raw_total_rows = sum(len(df) for df in raw_dfs.values())
    raw_unique_matches = len(raw_dfs[100])

    team_stats_frames: List[pd.DataFrame] = []
    objectives_frames: List[pd.DataFrame] = []
    total_fb_fixed = 0

    for pct in SNAPSHOT_INTERVALS:
        # Work on a copy so raw data remains untouched
        df = raw_dfs[pct].copy()

        # Rule 1: Drop the unnamed CSV index column if present
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        # Rule 2: Remove early /remake matches (< 3 minutes / 180,000 ms)
        df = df[df["fullTimeMS"] >= MIN_DURATION_MS].copy()

        # Rule 3: Fix FirstBlood contradiction (kills > 0 but both FirstBlood flags are False)
        no_fb_mask = (~df["blueFirstBlood"].astype(bool)) & (~df["redFirstBlood"].astype(bool))
        blue_scored_mask = no_fb_mask & (df["blueChampionKill"] > 0) & (df["redChampionKill"] == 0)
        red_scored_mask = no_fb_mask & (df["redChampionKill"] > 0) & (df["blueChampionKill"] >= 0) & (~blue_scored_mask)

        fixed_count = int(blue_scored_mask.sum() + red_scored_mask.sum())
        total_fb_fixed += fixed_count
        df.loc[blue_scored_mask, "blueFirstBlood"] = True
        df.loc[red_scored_mask, "redFirstBlood"] = True

        # Rule 4: Fix invalid tower counts (> 11) by capping to the physical map limit of 11 towers
        df["blueTowerKill"] = df["blueTowerKill"].clip(lower=0, upper=11)
        df["redTowerKill"] = df["redTowerKill"].clip(lower=0, upper=11)

        # Rule 5: Unpivot wide Blue and Red columns into normalized team rows
        blue_stats = pd.DataFrame({
            "match_id": df["matchID"].astype(str),
            "snapshot_time_pct": int(pct),
            "team_color": "Blue",
            "total_gold": df["blueTotalGold"].astype(int),
            "champion_kills": df["blueChampionKill"].astype(int),
            "towers_destroyed": df["blueTowerKill"].astype(int),
            "first_blood": df["blueFirstBlood"].astype(bool),
            "win": df["blueWin"].astype(bool),
        })
        red_stats = pd.DataFrame({
            "match_id": df["matchID"].astype(str),
            "snapshot_time_pct": int(pct),
            "team_color": "Red",
            "total_gold": df["redTotalGold"].astype(int),
            "champion_kills": df["redChampionKill"].astype(int),
            "towers_destroyed": df["redTowerKill"].astype(int),
            "first_blood": df["redFirstBlood"].astype(bool),
            "win": df["redWin"].astype(bool),
        })
        team_stats_frames.extend([blue_stats, red_stats])

        blue_obj = pd.DataFrame({
            "match_id": df["matchID"].astype(str),
            "snapshot_time_pct": int(pct),
            "team_color": "Blue",
            "dragons_killed": df["blueDragonKill"].astype(int),
            "barons_killed": df["blueBaronKill"].astype(int),
            "heralds_killed": df["blueRiftHeraldKill"].astype(int),
        })
        red_obj = pd.DataFrame({
            "match_id": df["matchID"].astype(str),
            "snapshot_time_pct": int(pct),
            "team_color": "Red",
            "dragons_killed": df["redDragonKill"].astype(int),
            "barons_killed": df["redBaronKill"].astype(int),
            "heralds_killed": df["redRiftHeraldKill"].astype(int),
        })
        objectives_frames.extend([blue_obj, red_obj])

    # Build the matches dimension table (1 row per valid match)
    df100_clean = raw_dfs[100][raw_dfs[100]["fullTimeMS"] >= MIN_DURATION_MS].copy()
    matches_df = pd.DataFrame({
        "match_id": df100_clean["matchID"].astype(str),
        "full_time_ms": df100_clean["fullTimeMS"].astype("int64"),
        "game_duration_min": (df100_clean["fullTimeMS"] / 60000.0).round(2),
    }).drop_duplicates(subset=["match_id"])

    team_stats_df = pd.concat(team_stats_frames, ignore_index=True)
    objectives_df = pd.concat(objectives_frames, ignore_index=True)

    remakes_removed = raw_unique_matches - len(matches_df)
    logger.info(
        "Cleaning Summary: Raw Matches=%d | Remakes Removed (<3m)=%d | Clean Matches=%d",
        raw_unique_matches,
        remakes_removed,
        len(matches_df),
    )
    logger.info(
        "Row Counts: Raw Snapshots=%d -> Clean Wide Snapshots=%d -> Unpivoted Team Rows=%d (per fact table) | FirstBlood Fixed=%d",
        raw_total_rows,
        len(matches_df) * 5,
        len(team_stats_df),
        total_fb_fixed,
    )

    return matches_df, team_stats_df, objectives_df


def validate_clean_data_preload(
    matches_df: pd.DataFrame,
    team_stats_df: pd.DataFrame,
    objectives_df: pd.DataFrame,
) -> None:
    """Verify cleaned DataFrames meet all PK, FK, and range rules before DB load."""
    if matches_df["match_id"].duplicated().any():
        raise ValueError("CLEAN CHECK FAIL: Duplicate match_id in matches_df.")

    if (matches_df["full_time_ms"] < MIN_DURATION_MS).any():
        raise ValueError("CLEAN CHECK FAIL: Match with duration < 180,000 ms found.")

    if (team_stats_df["total_gold"] < 2500).any():
        raise ValueError("CLEAN CHECK FAIL: Invalid total_gold (< 2500) detected.")

    if (team_stats_df["towers_destroyed"] < 0).any() or (team_stats_df["towers_destroyed"] > 11).any():
        raise ValueError("CLEAN CHECK FAIL: towers_destroyed out of range [0, 11].")

    if len(team_stats_df) != len(matches_df) * 10 or len(objectives_df) != len(matches_df) * 10:
        raise ValueError("CLEAN CHECK FAIL: Fact table row count does not equal matches * 10.")

    logger.info("Pre-Insert Validation on Cleaned Tables PASSED.")


def copy_df_to_postgres(
    raw_conn,
    df: pd.DataFrame,
    table_name: str,
    columns: List[str],
) -> None:
    """Fast bulk load a DataFrame into a PostgreSQL table using COPY."""
    buffer = io.StringIO()
    df[columns].to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    col_list = ", ".join(columns)
    copy_sql = f"COPY public.{table_name} ({col_list}) FROM STDIN WITH (FORMAT CSV)"
    with raw_conn.cursor() as cur:
        cur.copy_expert(copy_sql, buffer)


def load_to_postgres(
    engine: Engine,
    sql_dir: Path,
    matches_df: pd.DataFrame,
    team_stats_df: pd.DataFrame,
    objectives_df: pd.DataFrame,
) -> None:
    """Step 2 & 3: Create schema tables, load cleaned data, and create analytical views."""
    ddl_sql = (sql_dir / "create_tables.sql").read_text(encoding="utf-8")
    views_sql = (sql_dir / "analytical_views.sql").read_text(encoding="utf-8")

    # 1. Ensure tables exist and truncate old rows cleanly
    with engine.begin() as conn:
        conn.execute(text(ddl_sql))
        conn.execute(
            text("TRUNCATE TABLE public.objectives_control, public.team_stats, public.matches RESTART IDENTITY CASCADE;")
        )

    # 2. Bulk load the 3 tables in a single atomic transaction
    raw_conn = engine.raw_connection()
    try:
        copy_df_to_postgres(
            raw_conn,
            matches_df,
            "matches",
            ["match_id", "full_time_ms", "game_duration_min"],
        )
        copy_df_to_postgres(
            raw_conn,
            team_stats_df,
            "team_stats",
            [
                "match_id",
                "snapshot_time_pct",
                "team_color",
                "total_gold",
                "champion_kills",
                "towers_destroyed",
                "first_blood",
                "win",
            ],
        )
        copy_df_to_postgres(
            raw_conn,
            objectives_df,
            "objectives_control",
            [
                "match_id",
                "snapshot_time_pct",
                "team_color",
                "dragons_killed",
                "barons_killed",
                "heralds_killed",
            ],
        )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    # 3. Create analytical SQL views for the 3 project questions
    with engine.begin() as conn:
        conn.execute(text(views_sql))

    logger.info(
        "Database Load Complete: matches=%d rows | team_stats=%d rows | objectives_control=%d rows | Views created.",
        len(matches_df),
        len(team_stats_df),
        len(objectives_df),
    )


def run_postload_quality_checks(engine: Engine, sql_dir: Path) -> None:
    """Step 4: Run post-load SQL quality checks and halt immediately if any check fails."""
    quality_sql = (sql_dir / "quality_checks.sql").read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(quality_sql))
        rows = conn.execute(
            text("SELECT check_id, check_name, violations, status FROM public.vw_data_quality_report ORDER BY check_id;")
        ).fetchall()

    logger.info("Post-Load SQL Data Quality Report:")
    for row in rows:
        logger.info(
            "  [Check #%d] %-40s | Violations: %d | Status: %s",
            row[0],
            row[1],
            row[2],
            row[3],
        )


def run_intentional_bad_data_test(engine: Engine) -> int:
    """
    Step 5 (Intentional Bad-Data Fail Test):
    Simulates corrupt data injection inside a rolled-back transaction to prove
    that our quality checks catch errors and halt execution with non-zero exit code.
    """
    logger.info("=" * 78)
    logger.info("RUNNING INTENTIONAL BAD-DATA FAILURE TEST (--test-fail)")
    logger.info("=" * 78)

    try:
        with engine.begin() as conn:
            # Drop the table CHECK constraint temporarily inside the transaction to let bad data reach the SQL quality validator
            conn.execute(text("ALTER TABLE public.team_stats DROP CONSTRAINT IF EXISTS team_stats_total_gold_check;"))
            conn.execute(
                text("UPDATE public.team_stats SET total_gold = -9999 WHERE stat_id = 1;")
            )
            logger.warning("Injected bad record: team_stats.total_gold = -9999 (stat_id=1)")

            # Query the SQL quality report view inside the same transaction
            failed = conn.execute(
                text("SELECT check_id, check_name, violations, status FROM public.vw_data_quality_report WHERE status = 'FAIL';")
            ).fetchall()

            if failed:
                for row in failed:
                    logger.error(
                        "QUALITY CHECK CAUGHT CORRUPT DATA -> [Check #%d] %s | Violations=%d | Status=%s",
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                    )
                raise RuntimeError(
                    f"FAIL-FAST TRIGGERED: {len(failed)} post-load data quality check(s) failed! Rolling back and halting pipeline."
                )
    except RuntimeError as exc:
        logger.error("Pipeline halted as expected during bad-data test: %s", exc)
        return 1

    return 0


def main() -> int:
    """Run the full Phase 3 cleaning, modeling, loading, and validation pipeline."""
    start_time = time.perf_counter()
    test_fail_mode = "--test-fail" in sys.argv

    logger.info("=" * 78)
    logger.info("STARTING PHASE 3: DATA CLEANING, RELATIONAL MODELING & DATA QUALITY")
    logger.info("=" * 78)

    config = load_config()
    raw_dir = Path(config["raw_dir"])
    sql_dir = Path(config["sql_dir"])

    try:
        engine = get_engine(config)

        # If running the intentional bad-data test, run it immediately
        if test_fail_mode:
            return run_intentional_bad_data_test(engine)

        # 1. Load untouched raw snapshot files
        raw_dfs: Dict[int, pd.DataFrame] = {}
        for pct in SNAPSHOT_INTERVALS:
            csv_path = raw_dir / f"full_data_{pct}.csv"
            raw_dfs[pct] = pd.read_csv(csv_path)

        # 2. Run Pre-Load Data Quality Checks
        run_preload_checks(raw_dfs)

        # 3. Clean data and normalize into 3 relational tables
        matches_df, team_stats_df, objectives_df = clean_and_normalize(raw_dfs)

        # 4. Validate cleaned DataFrames before inserting into PostgreSQL
        validate_clean_data_preload(matches_df, team_stats_df, objectives_df)

        # 5. Load cleaned tables into PostgreSQL and build Analytical SQL Views
        load_to_postgres(engine, sql_dir, matches_df, team_stats_df, objectives_df)

        # 6. Execute Post-Load SQL Data Quality Checks
        run_postload_quality_checks(engine, sql_dir)

    except (ValueError, FileNotFoundError, SQLAlchemyError, RuntimeError) as exc:
        logger.error("PIPELINE HALTED DUE TO ERROR: %s", exc)
        return 1

    elapsed = round(time.perf_counter() - start_time, 2)
    logger.info("=" * 78)
    logger.info("PHASE 3 CLEANING, LOADING & QUALITY CHECKS COMPLETED IN %.2f SECONDS", elapsed)
    logger.info("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
