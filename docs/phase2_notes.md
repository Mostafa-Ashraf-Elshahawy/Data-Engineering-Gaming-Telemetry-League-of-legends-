# Phase 2 Technical Summary: Data Ingestion, Raw Storage & Profiling

**Course:** Data Engineering and Cloud Computing  
**Project:** League of Legends Ranked Match Telemetry Analysis  
**Phase:** Phase 2 — Data Ingestion, Raw Storage & Containerization  
**Source Dataset:** Zenodo Record `8303397` ([DOI: 10.5281/zenodo.8303397](https://doi.org/10.5281/zenodo.8303397))

---

## 1. Source Connection & Failure Handling (`scripts/ingest_data.py`)

The ingestion pipeline is implemented as a modular, reusable Python script (`scripts/ingest_data.py`) rather than an interactive notebook. It is structured around isolated functions for configuration loading, source verification/fetching, database connectivity, raw schema initialization, raw data profiling, and PostgreSQL loading.

### How the Script Connects to the Source
1. **Upstream Metadata Check (`check_zenodo_source`)**:
   - Queries the Zenodo REST API endpoint (`https://zenodo.org/api/records/8303397`) with an explicit HTTP timeout (`timeout=10s`) and validates the HTTP status code via `response.raise_for_status()`.
   - Designed for **offline resilience**: if the container runs in an offline lab defense environment without external internet access, the script catches `requests.RequestException`, logs an `OFFLINE_FALLBACK` warning, and seamlessly proceeds using the local raw landing directory (`/app/data/raw`).
2. **Dataset Extraction & Verification (`fetch_or_verify_file`)**:
   - Iterates over all five match progression intervals (`full_data_20.csv`, `full_data_40.csv`, `full_data_60.csv`, `full_data_80.csv`, `full_data_100.csv`).
   - If a file is absent locally (or `FORCE_DOWNLOAD=true`), the script streams the file in 1 MB chunks from `https://zenodo.org/records/8303397/files/{filename}?download=1` into a temporary `.part` file before atomically renaming it to prevent partial/corrupted files on interrupted downloads.
   - If the file is present locally, the script verifies that the file exists, has a non-zero byte size, and contains all expected `42` raw columns.

### Error & Failure Handling Mechanisms
- **Network & HTTP Failures (`requests.RequestException`)**: Caught during both API metadata checks and file downloads. Partial downloads are written to `.csv.part` files so interrupted transfers never overwrite valid raw CSV files.
- **File System & Parsing Errors (`FileNotFoundError`, `pd.errors.ParserError`)**: Validates non-zero file sizes before reading and traps malformed CSV syntax cleanly, logging the failing snapshot interval and exiting with non-zero status code (`1`).
- **Database Connection Retries (`get_db_engine`)**: Uses SQLAlchemy with `pool_pre_ping=True` and a retry loop (`max_retries=5`, `retry_delay=3s`) that catches `OperationalError` and executes a `SELECT 1` health check before starting ingestion.
- **Transactional Integrity**: Raw table loads use PostgreSQL's native `COPY ... FROM STDIN WITH (FORMAT CSV, HEADER TRUE)` inside an explicit try/except transaction block that issues `raw_conn.rollback()` on any error and always closes the connection in `finally`.

---

## 2. Where and How Raw Data Is Stored

### Dual-Layer Raw Landing Architecture
To guarantee reproducibility and fast SQL inspection during lab verification, untouched raw data is preserved in two locations before any Phase 3 cleaning or normalization:

1. **Raw File Landing Layer (`data/raw/*.csv` mounted to `/app/data/raw`)**:
   - All five original Zenodo CSV snapshots (`~43.35 MB` total) are kept intact on disk via a Docker bind mount (`./data:/app/data`).
2. **PostgreSQL Raw Schema Layer (`raw` schema in database `lol_analytics`)**:
   - The ingestion script automatically executes `CREATE SCHEMA IF NOT EXISTS raw;` and loads each CSV **as-is** (preserving all 42 raw columns, original column names, and raw types without modification) into five raw tables:
     - `raw.full_data_20` (`64,556` rows, `42` columns)
     - `raw.full_data_40` (`64,556` rows, `42` columns)
     - `raw.full_data_60` (`64,556` rows, `42` columns)
     - `raw.full_data_80` (`64,556` rows, `42` columns)
     - `raw.full_data_100` (`64,556` rows, `42` columns)
   - **Total Raw Records Stored:** `322,780` untouched snapshot records.
   - **Execution Audit Table (`raw.ingestion_audit_log`)**: Records execution metadata for every ingested file, including `ingested_at` timestamp, `file_name`, `target_table`, `source_status`, `file_size_bytes`, `rows_fetched`, `columns_count`, data quality counters, `duration_seconds`, and `status`.

### Why Preserving Untouched Raw Data Is Critical in Data Engineering
- **Reproducibility & Replayability**: If a bug is discovered in downstream Phase 3 cleaning or SQL transformation logic, the pipeline can re-run transformations directly from the immutable `raw` layer without re-fetching from external sources.
- **Auditability & Debugging**: Comparing transformed analytical tables (`public.matches`, `public.team_stats`, `public.objectives_control`) against `raw.full_data_*` allows engineers to verify row counts and trace anomalies back to the source.
- **Decoupling Extraction from Transformation (ELT)**: Separating raw ingestion (Phase 2) from cleaning/modeling (Phase 3) ensures source extraction failures and transformation errors are isolated.

---

## 3. Raw Data Profiling & Data Quality Issues (To Be Cleaned in Phase 3)

Profiling all five raw files (`64,556` rows × `5` files = `322,780` total rows, `42` columns per file) revealed zero `NULL` cells and zero exact duplicate rows within individual files, but uncovered **7 critical structural, semantic, and logical data quality issues** that must be cleaned in Phase 3:

| Snapshot File | Rows | Columns | Null Values | Exact Duplicate Rows | Remakes (`<3 min`) | `timePercent == 0` | Neither Team First Blood | Kills > 0 Without First Blood |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `full_data_20.csv` | 64,556 | 42 | 0 | 0 | 2,663 | 2,935 | 5,923 | 19 |
| `full_data_40.csv` | 64,556 | 42 | 0 | 0 | 2,663 | 2,377 | 3,059 | 3 |
| `full_data_60.csv` | 64,556 | 42 | 0 | 0 | 2,663 | 0 | 2,714 | 3 |
| `full_data_80.csv` | 64,556 | 42 | 0 | 0 | 2,663 | 0 | 2,604 | 3 |
| `full_data_100.csv` | 64,556 | 42 | 0 | 0 | 2,663 | 0 | 2,061 | 14 |
| **Total / Aggregate** | **322,780** | **42** | **0** | **0** | **13,315** | **5,312** | **16,361** | **42** |

### Bulleted List of Data Quality Issues for Phase 3 Cleaning
- **Issue 1 — Unnamed Redundant Index Column (`Unnamed: 0`)**:
  - Every raw CSV begins with a leading comma (`,matchID,fullTimeMS,...`), causing parsers to create an unnamed integer index column (`Unnamed: 0` with values `0` to `64,555`). This column carries no domain meaning and must be dropped in Phase 3.
- **Issue 2 — Misleading Column Name (`timePercent`) & Missing Snapshot Interval Identifier**:
  - Despite being named `timePercent`, the column does **not** store the match completion percentage (`20, 40, 60, 80, 100`). Instead, it stores the truncated integer **elapsed game time in minutes** at that snapshot (ranging from `0` to `13` in `full_data_20.csv` and `2` to `68` in `full_data_100.csv`).
  - Meanwhile, the true snapshot percentage (`20%`, `40%`, `60%`, `80%`, `100%`) is only encoded in the filename (`full_data_20.csv`, etc.) and must be explicitly injected as `snapshot_time_pct` during Phase 3 transformation.
- **Issue 3 — Early Match Remakes / Aborted Games (`fullTimeMS < 180,000` ms / `< 3` minutes)**:
  - Exactly **2,663 matches per file** (`13,315` rows across all 5 files, ~4.1% of the dataset) have a total match duration under 3 minutes (minimum `101,182` ms = `1.69` minutes).
  - In League of Legends, matches terminated under 3 minutes are `/remake` games caused by a disconnected player at match start. In these rows, teams sit at base starting values (`totalGold = 2500`, `0` kills, `0` minions, `avgPlayerLevel = 1.0`), yet one side is still marked `Win = True`. Retaining these remakes will distort win-rate and gold-scaling analytics in Phase 3.
- **Issue 4 — Zero-Minute Timestamp Truncation (`timePercent == 0`)**:
  - Because `timePercent` truncates elapsed minutes to an integer, **2,935 rows** in `full_data_20.csv` and **2,377 rows** in `full_data_40.csv` (`5,312` rows total) record an elapsed snapshot time of `0` minutes.
- **Issue 5 — Logical Telemetry Contradiction (`ChampionKill > 0` While Both `FirstBlood == False`)**:
  - Across the raw files, **42 snapshot records** (`19` in 20%, `3` in 40%, `3` in 60%, `3` in 80%, and `14` in 100%) record positive champion kills (`blueChampionKill + redChampionKill > 0`), yet both `blueFirstBlood` and `redFirstBlood` are `False`. By game rules, the first champion kill of any match must trigger First Blood.
  - Additionally, at 100% match completion (`full_data_100.csv`), **2,061 matches** have neither team securing First Blood (`blueFirstBlood = False` and `redFirstBlood = False`), predominantly due to early remakes with zero kills.
- **Issue 6 — Surrender Matches with Zero Towers Destroyed (`3,554` matches at 100%)**:
  - In `full_data_100.csv`, **3,554 winning teams** won the match while destroying `0` towers (`TowerKill = 0` and `InhibitorKill = 0`) due to remakes or early surrenders (`/ff`). Phase 3 cleaning must filter remakes and account for surrenders when answering Analytical Question 2 (towers destroyed by winning vs. losing teams).
- **Issue 7 — Denormalized Wide Schema & Cross-File Redundancy**:
  - Each raw row stores Blue and Red team attributes side-by-side (`blue*` and `red*` across 42 columns), and static match-level fields (`matchID`, `fullTimeMS`, `blueWin`, `redWin`) are repeated identically across all 5 snapshot files (`64,556` unique `matchID`s repeated 5 times = `322,780` rows). Furthermore, `redWin` is strictly `NOT blueWin`. Phase 3 will unpivot (melt) team columns by `team_color` (`'Blue'` / `'Red'`) and normalize the data into 3NF tables (`matches`, `team_stats`, `objectives_control`).

---

## 4. Containerization & Docker Networking Reference (Lab Defense Prep)

### `Dockerfile` Breakdown
- `FROM python:3.11-slim`: Uses the official lightweight Debian-based Python 3.11 image to minimize image size and attack surface.
- `ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1`: Prevents `.pyc` bytecode clutter and forces unbuffered standard output so ingestion logs appear in real time during `docker compose up`.
- `WORKDIR /app`: Sets `/app` as the working directory inside the container.
- `COPY requirements.txt /app/requirements.txt` & `RUN pip install --no-cache-dir -r /app/requirements.txt`: Copies `requirements.txt` before application code so Docker caches the installed Python dependencies unless requirements change.
- `COPY scripts/ /app/scripts/` & `RUN mkdir -p /app/data/raw`: Copies the ingestion code and creates the raw landing mount point.
- `CMD ["python", "scripts/ingest_data.py"]`: Runs the ingestion pipeline automatically when the container starts.

### Docker Networking & Service Orchestration (`docker-compose.yml`)
- **Shared Bridge Network (`lol_network`)**: All three containers (`lol_postgres`, `lol_pgadmin`, `lol_ingestion`) are attached to the custom bridge network `lol_network`. Docker's embedded DNS server resolves service names (`postgres`) to their internal container IP addresses, allowing `lol_ingestion` and `lol_pgadmin` to connect to `postgres:5432` internally while host applications connect via `localhost:5432`.
- **Healthcheck & Startup Ordering (`depends_on: condition: service_healthy`)**: The `postgres` service runs `pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}` every 10 seconds. Both `pgadmin` and `ingestion` wait until `postgres` reports `healthy` before starting, preventing race-condition connection failures.
