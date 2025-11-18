"""Query the local SQLite database for historical MAUDE data."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Database path
DB_PATH = Path(__file__).parent / "resources" / "historical-incidents.sqlite3"


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
