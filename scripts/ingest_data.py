"""
Phase 2: Modular Data Ingestion & Raw Landing Pipeline
Project: League of Legends Ranked Match Telemetry Analysis
Source: Zenodo Record 8303397 (DOI: 10.5281/zenodo.8303397)
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Configure structured console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("lol_ingestion")

SNAPSHOT_INTERVALS: List[int] = [20, 40, 60, 80, 100]
EXPECTED_COLUMNS: int = 42


def load_configuration() -> Dict[str, str]:
    """Load pipeline configuration from environment variables with safe defaults."""
    load_dotenv()

    default_raw_dir = (
        "/app/data/raw"
        if Path("/app/data/raw").exists()
        else str(Path(__file__).resolve().parent.parent / "data" / "raw")
    )

    return {
        "postgres_user": os.getenv("POSTGRES_USER", "lol_admin"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD", "change_me"),
        "postgres_db": os.getenv("POSTGRES_DB", "lol_analytics"),
        "postgres_host": os.getenv("POSTGRES_HOST", "localhost"),
        "postgres_port": os.getenv("POSTGRES_PORT", "5432"),
        "raw_data_dir": os.getenv("RAW_DATA_DIR", default_raw_dir),
        "raw_schema": os.getenv("RAW_SCHEMA", "raw"),
        "zenodo_record_id": os.getenv("ZENODO_RECORD_ID", "8303397"),
        "force_download": os.getenv("FORCE_DOWNLOAD", "false").lower() == "true",
    }


def check_zenodo_source(record_id: str, timeout: int = 10) -> Tuple[bool, str]:
    """
    Check upstream Zenodo API availability for record metadata.
    Gracefully handles offline environments (such as in-person offline lab defenses).
    """
    api_url = f"https://zenodo.org/api/records/{record_id}"
    try:
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        metadata = response.json()
        title = metadata.get("metadata", {}).get("title", "League of Legends Dataset")
        logger.info(
            "Source Check: Upstream Zenodo API reachable (HTTP %d) | Record %s: '%s'",
            response.status_code,
            record_id,
            title,
        )
        return True, f"ONLINE (HTTP {response.status_code})"
    except requests.RequestException as exc:
        logger.warning(
            "Source Check: Upstream Zenodo API unreachable (%s). "
            "Operating in offline-resilient mode using local raw landing directory.",
            exc.__class__.__name__,
        )
        return False, "OFFLINE_FALLBACK (Local Raw Cache)"


def fetch_or_verify_file(
    interval_pct: int,
    raw_dir: Path,
    record_id: str,
    force_download: bool = False,
) -> Tuple[Path, str]:
    """
    Ensure the raw snapshot CSV exists in the landing directory.
    Downloads from Zenodo if missing or if force_download is enabled;
    otherwise verifies the local raw file integrity.
    """
    filename = f"full_data_{interval_pct}.csv"
    file_path = raw_dir / filename
    download_url = (
        f"https://zenodo.org/records/{record_id}/files/{filename}?download=1"
    )

    raw_dir.mkdir(parents=True, exist_ok=True)

    if force_download or not file_path.exists():
        logger.info(
            "Fetching '%s' from Zenodo (%s)...", filename, download_url
        )
        try:
            with requests.get(download_url, stream=True, timeout=60) as response:
                response.raise_for_status()
                temp_path = file_path.with_suffix(".csv.part")
                with open(temp_path, "wb") as out_file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            out_file.write(chunk)
                temp_path.replace(file_path)
            source_status = "DOWNLOADED_FROM_ZENODO"
        except requests.RequestException as exc:
            if file_path.exists() and file_path.stat().st_size > 0:
                logger.warning(
                    "Download failed for '%s' (%s); using existing local copy.",
                    filename,
                    exc,
                )
                source_status = "LOCAL_RAW_FALLBACK"
            else:
                raise RuntimeError(
                    f"Failed to fetch '{filename}' from Zenodo and no local copy exists: {exc}"
                ) from exc
    else:
        source_status = "VERIFIED_LOCAL_RAW"

    if not file_path.exists() or file_path.stat().st_size == 0:
        raise FileNotFoundError(
            f"Raw file '{file_path}' is missing or empty (0 bytes)."
        )

    return file_path, source_status


def get_db_engine(
    config: Dict[str, str], max_retries: int = 5, retry_delay: int = 3
) -> Engine:
    """Create a SQLAlchemy engine and verify PostgreSQL connectivity with retries."""
    db_url = (
        f"postgresql+psycopg2://{config['postgres_user']}:{config['postgres_password']}"
        f"@{config['postgres_host']}:{config['postgres_port']}/{config['postgres_db']}"
    )

    for attempt in range(1, max_retries + 1):
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(
                "Database Connection: Connected to PostgreSQL at %s:%s/%s (attempt %d/%d)",
                config["postgres_host"],
                config["postgres_port"],
                config["postgres_db"],
                attempt,
                max_retries,
            )
            return engine
        except OperationalError as exc:
            logger.warning(
                "Database Connection: Attempt %d/%d failed (%s). Retrying in %ds...",
                attempt,
                max_retries,
                exc.__class__.__name__,
                retry_delay,
            )
            if attempt == max_retries:
                raise RuntimeError(
                    f"Could not connect to PostgreSQL after {max_retries} attempts."
                ) from exc
            time.sleep(retry_delay)

    raise RuntimeError("Unexpected database connection state.")


def initialize_raw_schema(engine: Engine, raw_schema: str) -> None:
    """Create the raw landing schema and ingestion audit log table in PostgreSQL."""
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {raw_schema};"
    create_audit_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_schema}.ingestion_audit_log (
        ingestion_id BIGSERIAL PRIMARY KEY,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        file_name VARCHAR(100) NOT NULL,
        target_table VARCHAR(100) NOT NULL,
        source_status VARCHAR(50) NOT NULL,
        file_size_bytes BIGINT NOT NULL,
        rows_fetched INT NOT NULL,
        columns_count INT NOT NULL,
        null_values_count INT NOT NULL,
        duplicate_rows_count INT NOT NULL,
        remake_matches_count INT NOT NULL,
        duration_seconds NUMERIC(8, 3) NOT NULL,
        status VARCHAR(30) NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_schema_sql))
        conn.execute(text(create_audit_table_sql))
    logger.info(
        "Raw Storage Init: Ensured schema '%s' and table '%s.ingestion_audit_log' exist.",
        raw_schema,
        raw_schema,
    )


def profile_raw_dataframe(df: pd.DataFrame) -> Dict[str, int]:
    """
    Inspect the untouched raw DataFrame and compute data quality metrics
    without modifying any data.
    """
    total_rows, total_cols = df.shape
    null_count = int(df.isnull().sum().sum())
    duplicate_rows = int(df.duplicated().sum())
    duplicate_match_ids = (
        int(df["matchID"].duplicated().sum()) if "matchID" in df.columns else 0
    )
    remake_count = (
        int((df["fullTimeMS"] < 180000).sum()) if "fullTimeMS" in df.columns else 0
    )
    zero_time_pct = (
        int((df["timePercent"] == 0).sum()) if "timePercent" in df.columns else 0
    )
    no_first_blood = (
        int(((~df["blueFirstBlood"]) & (~df["redFirstBlood"])).sum())
        if {"blueFirstBlood", "redFirstBlood"}.issubset(df.columns)
        else 0
    )
    kills_without_fb = (
        int(
            (
                ((df["blueChampionKill"] + df["redChampionKill"]) > 0)
                & (~df["blueFirstBlood"])
                & (~df["redFirstBlood"])
            ).sum()
        )
        if {
            "blueChampionKill",
            "redChampionKill",
            "blueFirstBlood",
            "redFirstBlood",
        }.issubset(df.columns)
        else 0
    )

    return {
        "rows": total_rows,
        "columns": total_cols,
        "null_count": null_count,
        "duplicate_rows": duplicate_rows,
        "duplicate_match_ids": duplicate_match_ids,
        "remake_count": remake_count,
        "zero_time_pct": zero_time_pct,
        "no_first_blood": no_first_blood,
        "kills_without_fb": kills_without_fb,
    }


def load_raw_csv_to_postgres(
    engine: Engine,
    file_path: Path,
    df: pd.DataFrame,
    raw_schema: str,
    table_name: str,
    source_status: str,
    profile_metrics: Dict[str, int],
    file_start_time: float,
) -> None:
    """
    Store the extracted raw dataset intact (as-is, all 42 columns untouched)
    into the PostgreSQL raw schema using fast PostgreSQL COPY protocol.
    """
    # 1. Create/replace the raw table structure matching the exact CSV columns as-is
    df.head(0).to_sql(
        name=table_name,
        con=engine,
        schema=raw_schema,
        if_exists="replace",
        index=False,
    )

    # 2. Stream untouched raw CSV rows directly into the raw table via COPY
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            with open(file_path, "r", encoding="utf-8") as csv_file:
                copy_sql = (
                    f'COPY "{raw_schema}"."{table_name}" '
                    f"FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                )
                cursor.copy_expert(copy_sql, csv_file)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    duration_sec = round(time.perf_counter() - file_start_time, 3)
    file_size_bytes = file_path.stat().st_size

    # 3. Record execution metrics in raw.ingestion_audit_log
    insert_log_sql = text(
        f"""
        INSERT INTO {raw_schema}.ingestion_audit_log (
            file_name, target_table, source_status, file_size_bytes,
            rows_fetched, columns_count, null_values_count,
            duplicate_rows_count, remake_matches_count,
            duration_seconds, status
        ) VALUES (
            :file_name, :target_table, :source_status, :file_size_bytes,
            :rows_fetched, :columns_count, :null_values_count,
            :duplicate_rows_count, :remake_matches_count,
            :duration_seconds, :status
        );
        """
    )
    with engine.begin() as conn:
        conn.execute(
            insert_log_sql,
            {
                "file_name": file_path.name,
                "target_table": f"{raw_schema}.{table_name}",
                "source_status": source_status,
                "file_size_bytes": file_size_bytes,
                "rows_fetched": profile_metrics["rows"],
                "columns_count": profile_metrics["columns"],
                "null_values_count": profile_metrics["null_count"],
                "duplicate_rows_count": profile_metrics["duplicate_rows"],
                "remake_matches_count": profile_metrics["remake_count"],
                "duration_seconds": duration_sec,
                "status": "SUCCESS",
            },
        )


def run_ingestion_pipeline(skip_db: bool = False) -> int:
    """Execute the end-to-end Phase 2 raw data ingestion and profiling pipeline."""
    pipeline_start_ts = datetime.now(timezone.utc)
    pipeline_start_perf = time.perf_counter()

    logger.info("=" * 78)
    logger.info("STARTING PHASE 2 RAW DATA INGESTION PIPELINE")
    logger.info("Execution Start Timestamp (UTC): %s", pipeline_start_ts.isoformat())
    logger.info("=" * 78)

    config = load_configuration()
    raw_dir = Path(config["raw_data_dir"])
    raw_schema = config["raw_schema"]

    # Step 1: Check upstream source availability
    _, upstream_status = check_zenodo_source(config["zenodo_record_id"])
    logger.info("Upstream Source Status: %s | Landing Dir: %s", upstream_status, raw_dir)

    # Connect to PostgreSQL unless explicitly skipped
    engine: Optional[Engine] = None
    if not skip_db:
        try:
            engine = get_db_engine(config)
            initialize_raw_schema(engine, raw_schema)
        except (SQLAlchemyError, RuntimeError) as exc:
            logger.error("Database Initialization Failed: %s", exc)
            return 1

    total_records_fetched = 0
    total_bytes_processed = 0
    file_summaries: List[Dict[str, object]] = []

    # Step 2 & 3: Fetch/verify each snapshot file, profile raw quality, and store intact
    for interval_pct in SNAPSHOT_INTERVALS:
        file_start = time.perf_counter()
        table_name = f"full_data_{interval_pct}"

        try:
            file_path, source_status = fetch_or_verify_file(
                interval_pct=interval_pct,
                raw_dir=raw_dir,
                record_id=config["zenodo_record_id"],
                force_download=config["force_download"],
            )
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            total_bytes_processed += file_path.stat().st_size

            # Read raw CSV as-is without any cleaning or transformation
            df = pd.read_csv(file_path)
            if df.shape[1] != EXPECTED_COLUMNS:
                logger.warning(
                    "Schema Notice: '%s' has %d columns (expected %d).",
                    file_path.name,
                    df.shape[1],
                    EXPECTED_COLUMNS,
                )

            metrics = profile_raw_dataframe(df)
            total_records_fetched += metrics["rows"]

            if engine is not None:
                load_raw_csv_to_postgres(
                    engine=engine,
                    file_path=file_path,
                    df=df,
                    raw_schema=raw_schema,
                    table_name=table_name,
                    source_status=source_status,
                    profile_metrics=metrics,
                    file_start_time=file_start,
                )
                storage_target = f"{raw_schema}.{table_name}"
            else:
                storage_target = str(file_path)

            elapsed = round(time.perf_counter() - file_start, 2)
            logger.info(
                "Ingested %-18s | Status: %-18s | Size: %.2f MB | Rows: %6d | Cols: %2d | "
                "Target: %-20s | Time: %.2fs",
                file_path.name,
                source_status,
                file_size_mb,
                metrics["rows"],
                metrics["columns"],
                storage_target,
                elapsed,
            )
            logger.info(
                "  -> Raw Profile [%s]: Nulls=%d | ExactDupes=%d | Remakes(<3m)=%d | "
                "TimePct=0:%d | NoFirstBlood=%d | KillsWithoutFB=%d",
                file_path.name,
                metrics["null_count"],
                metrics["duplicate_rows"],
                metrics["remake_count"],
                metrics["zero_time_pct"],
                metrics["no_first_blood"],
                metrics["kills_without_fb"],
            )

            file_summaries.append(
                {
                    "file": file_path.name,
                    "status": "SUCCESS",
                    "rows": metrics["rows"],
                    "cols": metrics["columns"],
                    "target": storage_target,
                }
            )

        except (FileNotFoundError, pd.errors.ParserError, SQLAlchemyError, RuntimeError) as exc:
            logger.error("Failed to ingest snapshot %d%%: %s", interval_pct, exc)
            return 1

    pipeline_end_ts = datetime.now(timezone.utc)
    total_duration = round(time.perf_counter() - pipeline_start_perf, 2)

    logger.info("=" * 78)
    logger.info("PHASE 2 INGESTION PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("Execution End Timestamp (UTC)  : %s", pipeline_end_ts.isoformat())
    logger.info("Total Execution Duration       : %.2f seconds", total_duration)
    logger.info("Total Files Processed          : %d / %d", len(file_summaries), len(SNAPSHOT_INTERVALS))
    logger.info("Total Records Fetched & Stored : %d rows", total_records_fetched)
    logger.info(
        "Total Raw Data Volume          : %.2f MB",
        total_bytes_processed / (1024 * 1024),
    )
    logger.info("=" * 78)
    return 0


if __name__ == "__main__":
    skip_db_flag = "--skip-db" in sys.argv
    sys.exit(run_ingestion_pipeline(skip_db=skip_db_flag))
