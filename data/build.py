#!/usr/bin/env python3
"""Build script to ingest MAUDE CSV/ZIP files into a local SQLite database.

This script scans the data/ directory for MAUDE data files (.zip, .csv, .txt),
classifies them by record type (device, foitext, foidev), and ingests them into
a SQLite database at maudecli/resources/historical-incidents.sqlite3.

The process is idempotent - re-running on the same files will not create duplicates.
"""

import hashlib
import logging
import sqlite3
import sys
import zipfile
from pathlib import Path
from typing import Literal

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Database path relative to project root
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "maudecli" / "resources" / "historical-incidents.sqlite3"
DATA_DIR = PROJECT_ROOT / "data"

RecordType = Literal["device", "foitext", "foidev"]


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file for content-based deduplication.
    
    Args:
        file_path: Path to the file.
        
    Returns:
        Hexadecimal string of the file hash.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


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
    elif "foidev" in filename_lower:
        return "foidev"
    elif "device" in filename_lower:
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
    
    conn.commit()
    logger.info("Database tables initialized")


def is_file_ingested(conn: sqlite3.Connection, file_name: str, file_hash: str) -> bool:
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


def add_columns_if_needed(conn: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
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
                logger.debug(f"Sanitized column name '{col}' to '{safe_col}'")
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN [{safe_col}] TEXT")
                logger.debug(f"Added column '{safe_col}' to table '{table_name}'")
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not add column '{safe_col}' to '{table_name}': {e}")
    
    conn.commit()


def ingest_file(conn: sqlite3.Connection, file_path: Path, record_type: RecordType) -> int:
    """Ingest a single data file into the database.
    
    Args:
        conn: SQLite database connection.
        file_path: Path to the file.
        record_type: Type of records in the file.
        
    Returns:
        Number of rows ingested.
    """
    logger.info(f"Processing {file_path.name} as {record_type}")
    
    # Determine if file is zipped
    if file_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(file_path) as zf:
            # Assume single file in zip
            names = zf.namelist()
            if not names:
                logger.warning(f"Empty zip file: {file_path.name}")
                return 0
            
            with zf.open(names[0]) as f:
                # Try to read with pipe delimiter
                try:
                    df = pd.read_csv(
                        f, 
                        sep="|", 
                        dtype=str, 
                        encoding="latin1",
                        on_bad_lines="warn",
                        engine="python",
                    )
                except Exception as e:
                    logger.error(f"Error reading {file_path.name}: {e}")
                    return 0
    else:
        # Read CSV/TXT directly
        try:
            df = pd.read_csv(
                file_path,
                sep="|",
                dtype=str,
                encoding="latin1",
                on_bad_lines="warn",
                engine="python",
            )
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")
            return 0
    
    if df.empty:
        logger.warning(f"No data in {file_path.name}")
        return 0
    
    # Normalize column names to lowercase and replace invalid characters
    df.columns = [col.lower().strip().replace("-", "_").replace(".", "_") for col in df.columns]
    
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
        logger.info(f"All rows from {file_path.name} already exist in database")
        return 0
    
    # Insert new rows
    df_new.to_sql(record_type, conn, if_exists="append", index=False)
    rows_added = len(df_new)
    
    logger.info(f"Ingested {rows_added} new rows from {file_path.name}")
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


def build_database() -> None:
    """Main function to build the SQLite database from MAUDE files."""
    logger.info("Starting database build process")
    logger.info(f"Database path: {DB_PATH}")
    logger.info(f"Data directory: {DATA_DIR}")
    
    # Ensure resources directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Create tables
        create_tables(conn)
        
        # Find all data files
        data_files = []
        for pattern in ["*.zip", "*.csv", "*.txt"]:
            data_files.extend(DATA_DIR.glob(pattern))
        
        # Exclude README and build script
        data_files = [
            f for f in data_files 
            if "readme" not in f.name.lower() and f.name != "build.py"
        ]
        
        logger.info(f"Found {len(data_files)} data files")
        
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
                    logger.warning(f"Could not classify file: {file_path.name}")
                    files_skipped += 1
                    continue
                
                # Compute file hash
                file_hash = compute_file_hash(file_path)
                
                # Check if already ingested
                if is_file_ingested(conn, file_path.name, file_hash):
                    logger.info(f"File already ingested (no changes): {file_path.name}")
                    files_skipped += 1
                    continue
                
                # Ingest file
                rows_added = ingest_file(conn, file_path, record_type)
                
                # Log ingestion
                log_ingestion(conn, file_path.name, file_hash, record_type, rows_added)
                
                total_rows += rows_added
                files_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)
                files_errored += 1
                # Continue with next file
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Build Summary:")
        logger.info(f"  Files processed: {files_processed}")
        logger.info(f"  Files skipped (already ingested): {files_skipped}")
        logger.info(f"  Files with errors: {files_errored}")
        logger.info(f"  Total rows ingested: {total_rows}")
        logger.info("=" * 60)
        
        # Print table statistics
        cursor = conn.cursor()
        for table in ["device", "foitext", "foidev"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"Table '{table}': {count} rows")
        
    finally:
        conn.close()
    
    logger.info("Database build complete")


if __name__ == "__main__":
    try:
        build_database()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
