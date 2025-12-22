"""Query the local SQLite database for historical MAUDE data.

Includes build function to ingest MAUDE files into a local SQLite database.

The function downloads MAUDE data files and
classifies them by record type (device, foitext, foidev), and ingests them into
a SQLite database at maudecli/resources/historical-incidents.sqlite3.

The process is idempotent
- re-running on the same files will not create duplicates.

"""

from __future__ import annotations

# Python imports
import asyncio
import hashlib
import logging
import sqlite3
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Literal

# Module imports
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

# Local imports
from maudecli.utils import compute_file_hash

logger = logging.getLogger(__name__)

# Global variables
DB_PATH = Path().home() / ".maudecli" / "historical-incidents.sqlite3"
CACHE_DIR = Path().home() / ".cache" / ".maudecli"
DATAFILE_URLS = (
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foidevthru1997.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foidev1998.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foidev1999.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2000.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2001.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2002.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2003.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2004.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2005.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2006.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2007.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/device2008.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitextthru1995.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext1996.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext1997.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext1998.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext1999.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2000.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2001.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2002.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2003.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2004.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2005.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2006.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2007.zip",
    "https://www.accessdata.fda.gov/MAUDE/ftparea/foitext2008.zip",
)

RecordType = Literal["device", "foitext", "foidev"]


def compute_row_hash(row: pd.Series) -> str:
    """Compute hash for a row to enable deduplication.

    Args:
        row: Pandas series representing a row.

    Returns:
        Hexadecimal string of the row hash.

    """
    # Create a stable string representation of the row
    row_str = "|".join(str(v) if pd.notna(v) else "" for v in row)
    return hashlib.sha256(row_str.encode()).hexdigest()

def classify_file(filename: str) -> RecordType | None:
    """Classify a file into its record type based on filename.

    Args:
        filename: Name of the file.

    Returns:
        Record type (device, foitext, or foidev) or None if not recognized.

    """
    filename_lower = filename.lower()

    if "foitext" in filename_lower:
        return "foitext"

    if "foidev" in filename_lower:
        return "foidev"

    if "device" in filename_lower:
        return "device"

    return None


def create_tables(conn: sqlite3.Connection) -> None:
    """Create database tables if they don't exist.

    Args:
        conn: SQLite database connection.

    """
    cursor = conn.cursor()

    # Create ingestion log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            file_name TEXT PRIMARY KEY,
            file_hash TEXT NOT NULL,
            record_type TEXT NOT NULL,
            rows_ingested INTEGER NOT NULL,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create device table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS device (
            row_hash TEXT PRIMARY KEY
        )
    """)

    # Create foitext table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS foitext (
            row_hash TEXT PRIMARY KEY
        )
    """)

    # Create foidev table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS foidev (
            row_hash TEXT PRIMARY KEY
        )
    """)

    # Create indexes
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table}_{field} ON {table}({field})",
    )

    conn.commit()
    logger.info("Database tables initialized")


def is_file_ingested(
    conn: sqlite3.Connection, file_name: str, file_hash: str,
) -> bool:
    """Check if a file has already been ingested.

    Args:
        conn: SQLite database connection.
        file_name: Name of the file.
        file_hash: Hash of the file content.

    Returns:
        True if the file has been ingested with the same hash, False otherwise.

    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT file_hash FROM ingestion_log WHERE file_name = ?",
        (file_name,),
    )
    result = cursor.fetchone()

    if result is None:
        return False

    return result[0] == file_hash


def add_columns_if_needed(
    conn: sqlite3.Connection, table_name: str, columns: list[str],
) -> None:
    """Add columns to a table if they don't exist.

    Args:
        conn: SQLite database connection.
        table_name: Name of the table.
        columns: List of column names to add.

    """
    cursor = conn.cursor()

    # Get existing columns
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}

    # Add missing columns
    for col in columns:
        if col not in existing_columns and col != "row_hash":
            # Sanitize column name for SQL
            safe_col = col.replace("-", "_").replace(".", "_")
            if safe_col != col:
                logger.debug(
                    "Sanitized column name '%s' to '%s'",
                    col,
                    safe_col,
                )
            try:
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN [{safe_col}] TEXT",
                )
                logger.debug(
                    "Added column '%s' to table '%s'",
                    safe_col,
                    table_name,
                )
            except sqlite3.OperationalError as e:
                logger.warning(
                    "Could not add column '%s' to '%s': %s",
                    safe_col,
                    table_name,
                    e,
                )

    conn.commit()


def ingest_file(
    conn: sqlite3.Connection, file_path: Path, record_type: RecordType,
) -> int:
    """Ingest a single data file into the database.

    Args:
        conn: SQLite database connection.
        file_path: Path to the file.
        record_type: Type of records in the file.

    Returns:
        Number of rows ingested.

    """
    logger.info(
        "Processing %s as %s",
        file_path.name,
        record_type,
    )

    # Determine if file is zipped
    if file_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(file_path) as zf:
            # Assume single file in zip
            names = zf.namelist()
            if not names:
                logger.warning(
                    "Empty zip file: %s",
                    file_path.name,
                )
                return 0

            with zf.open(names[0]) as f:
                # Try to read with pipe delimiter
                try:
                    df = pd.read_csv(   # noqa: PD901
                        f,
                        sep="|",
                        dtype=str,
                        encoding="latin1",
                        on_bad_lines="warn",
                        engine="python",
                    )
                except (EmptyDataError, ParserError):
                    logger.exception("Error reading %s", file_path.name)
                    return 0
    else:
        # Read CSV/TXT directly
        try:
            df = pd.read_csv(   # noqa: PD901
                file_path,
                sep="|",
                dtype=str,
                encoding="latin1",
                on_bad_lines="warn",
                engine="python",
            )
        except (EmptyDataError, ParserError):
            logger.exception("Error reading %s", file_path.name)
            return 0

    if df.empty:
        logger.warning("No data in %s", file_path.name)
        return 0

    # Normalize column names to lowercase and replace invalid characters
    df.columns = [
        col.lower().strip().replace("-", "_").replace(".", "_")
        for col in df.columns
    ]

    # Add columns to table if needed
    add_columns_if_needed(conn, record_type, df.columns.tolist())

    # Compute row hashes
    df["row_hash"] = df.apply(compute_row_hash, axis=1)

    # Get existing row hashes to avoid duplicates
    cursor = conn.cursor()
    cursor.execute(f"SELECT row_hash FROM {record_type}")
    existing_hashes = {row[0] for row in cursor.fetchall()}

    # Filter out rows that already exist
    df_new = df[~df["row_hash"].isin(existing_hashes)]

    if df_new.empty:
        logger.info(
            "All rows from %s already exist in database",
            file_path.name,
        )
        return 0

    # Insert new rows
    df_new.to_sql(record_type, conn, if_exists="append", index=False)
    rows_added = len(df_new)

    logger.info(
        "Ingested %s new rows from %s",
        rows_added,
        file_path.name,
    )
    return rows_added


def log_ingestion(
    conn: sqlite3.Connection,
    file_name: str,
    file_hash: str,
    record_type: RecordType,
    rows_ingested: int,
) -> None:
    """Log successful ingestion to the ingestion_log table.

    Args:
        conn: SQLite database connection.
        file_name: Name of the file.
        file_hash: Hash of the file content.
        record_type: Type of records ingested.
        rows_ingested: Number of rows ingested.

    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO ingestion_log
        (file_name, file_hash, record_type, rows_ingested)
        VALUES (?, ?, ?, ?)
        """,
        (file_name, file_hash, record_type, rows_ingested),
    )
    conn.commit()


async def download_file_from_url(url: str) -> Path:
    """Downloads the datafile and returns the path of the downloaded file."""
    dest = CACHE_DIR / url.split("/")[-1]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        lambda: urllib.request.urlretrieve(url, filename=dest),
    )
    return dest


async def build_database() -> None:
    """Primary function to build the SQLite database from MAUDE files."""
    logger.info("Starting database build process")
    logger.info("Database path: %s", DB_PATH)

    # Ensure resources directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to database
    conn = sqlite3.connect(DB_PATH)

    # Download data files
    results = await asyncio.gather(
        *[download_file_from_url(url) for url in DATAFILE_URLS],
        return_exceptions=True,
    )

    data_files = []
    failed_downloads = 0

    for url, result in zip(DATAFILE_URLS, results):
        if isinstance(result, Path) and result.exists():
            data_files.append(result)
        elif isinstance(result, Exception):
            logger.error("Failed to download %s: %s", url, result)
            failed_downloads += 1
        else:
            logger.error(
                "Unexpected result when downloading %s: %r", url, result,
            )
            failed_downloads += 1

    logger.info("Found %i/%i data files", len(data_files), len(DATAFILE_URLS))
    if failed_downloads:
        logger.warning(
            "Failed to download %i/%i data files",
            failed_downloads,
            len(DATAFILE_URLS),
        )

    try:
        # Create tables
        create_tables(conn)

        # Process each file
        total_rows = 0
        files_processed = 0
        files_skipped = 0
        files_errored = 0

        for file_path in sorted(data_files):
            try:
                # Classify file
                record_type = classify_file(file_path.name)
                if record_type is None:
                    logger.warning(
                        "Could not classify file: %s", file_path.name,
                    )
                    files_skipped += 1
                    continue

                # Compute file hash
                file_hash = compute_file_hash(file_path)

                # Check if already ingested
                if is_file_ingested(conn, file_path.name, file_hash):
                    logger.info(
                        "File already ingested (no changes): %s",
                        file_path.name,
                    )
                    files_skipped += 1
                    continue

                # Ingest file
                rows_added = ingest_file(conn, file_path, record_type)

                # Log ingestion
                log_ingestion(conn, file_path.name, file_hash, record_type, rows_added)

                total_rows += rows_added
                files_processed += 1

            except (EmptyDataError, ParserError):
                logger.exception(
                    "Error processing %s", file_path.name,
                )
                files_errored += 1
                # Continue with next file

        # Print summary
        logger.info(
            "\n"
            + "=" * 60
            + "\nBuild Summary:\n"
            + "\n  Files processed: %s\n"
            + "  Files skipped (already ingested): %s\n"
            + "  Files with errors: %s\n"
            + "  Total rows ingested: %s\n"
            + "=" * 60,
            files_processed,
            files_skipped,
            files_errored,
            total_rows,
        )

        # Print table statistics
        cursor = conn.cursor()
        for table in ["device", "foitext", "foidev"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info("Table '%s': %s rows", table, count)

    finally:
        conn.close()

    logger.info("Database build complete")

def database_exists() -> bool:
    """Check if the local database exists.

    Returns:
        True if the database file exists, False otherwise.
    """
    return DB_PATH.exists()


def query_local_database(
    search_terms: list[list[str]],
    exclude_terms: list[list[str]] | None = None,
    search_field: str = "foi_text",
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Query the local historical MAUDE database.
    
    Args:
        search_terms: Term groups to search. Results must contain at least one
            match from EACH group (AND logic between groups, OR within groups).
        exclude_terms: Term groups to exclude. Results containing matches from
            ALL exclude groups will be filtered out.
        search_field: The field to search in. For foitext table, use 'foi_text'.
            For device tables, common fields include 'brand_name', 'generic_name'.
            Default is 'foi_text'.
        limit: Maximum number of results to return. None for no limit.
        
    Returns:
        List of dictionaries containing the query results.
    """
    if not database_exists():
        logger.warning(f"Local database not found at {DB_PATH}")
        return []
    
    # Determine which table(s) to query based on search field
    tables = []
    if search_field in ["foi_text", "mdr_text_key", "text_type_code"]:
        tables.append("foitext")
    elif search_field in ["brand_name", "generic_name", "manufacturer_d_name", 
                          "model_number", "device_report_product_code"]:
        tables.extend(["device", "foidev"])
    else:
        # Default: search all tables
        tables.extend(["device", "foitext", "foidev"])
    
    results = []
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        cursor = conn.cursor()
        
        for table in tables:
            # Check if the field exists in this table
            cursor.execute(f"PRAGMA table_info({table})")
            columns = {row[1] for row in cursor.fetchall()}
            
            if search_field not in columns:
                logger.debug(f"Field '{search_field}' not in table '{table}', skipping")
                continue
            
            # Build WHERE clause for search terms
            # Each group is OR'd internally, groups are AND'd together
            where_clauses = []
            params = []
            
            for group in search_terms:
                group_conditions = []
                for term in group:
                    group_conditions.append(f"{search_field} LIKE ?")
                    params.append(f"%{term}%")
                if group_conditions:
                    where_clauses.append(f"({' OR '.join(group_conditions)})")
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            # Build query
            query = f"SELECT * FROM {table} WHERE {where_sql}"
            if limit:
                query += f" LIMIT {limit}"
            
            logger.debug(f"Executing query on {table}: {query}")
            cursor.execute(query, params)
            
            # Fetch results and convert to dicts
            for row in cursor.fetchall():
                result_dict = dict(row)
                
                # Apply exclusion filtering in Python (simpler than complex SQL)
                if exclude_terms:
                    should_exclude = True
                    for exclude_group in exclude_terms:
                        # Check if ANY term in this group matches
                        group_matches = False
                        for term in exclude_group:
                            field_value = str(result_dict.get(search_field, "")).lower()
                            if term.lower() in field_value:
                                group_matches = True
                                break
                        # If this group doesn't match, don't exclude
                        if not group_matches:
                            should_exclude = False
                            break
                    
                    if should_exclude:
                        continue
                
                # Add table source for debugging
                result_dict["_source"] = f"local_db:{table}"
                results.append(result_dict)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error querying local database: {e}", exc_info=True)
    
    logger.info(f"Local database query returned {len(results)} results")
    return results


def get_table_stats() -> dict[str, int]:
    """Get row counts for each table in the database.

    Returns:
        Dictionary mapping table names to row counts.
    """
    if not database_exists():
        return {}

    stats = {}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            for table in ["device", "foitext", "foidev"]:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats[table] = count

    except Exception as e:
        logger.error(f"Error getting table stats: {e}")

    return stats
