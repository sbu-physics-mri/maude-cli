# Local Database Query Examples

## Building the Database

The local SQLite database is built from historical MAUDE CSV/ZIP files stored in the `data/` directory.

```bash
# Build or update the database
uv run python data/build.py
```

The script is idempotent - running it multiple times will not create duplicate rows. It will only process files that have changed or new files.

### Build Process

The build script:
1. Scans `data/` directory for `.zip`, `.csv`, and `.txt` files
2. Classifies files by type: `device`, `foitext`, or `foidev`
3. Extracts and normalizes column names
4. Computes content hashes for deduplication
5. Inserts new rows into the appropriate table
6. Logs ingestion in the `ingestion_log` table

### Database Location

The database is created at: `maudecli/resources/historical-incidents.sqlite3`

## Querying the Database

### Programmatic Access

```python
from maudecli.db import query_local_database, get_table_stats

# Get database statistics
stats = get_table_stats()
print(f"Device records: {stats['device']}")
print(f"Text records: {stats['foitext']}")
print(f"FOI device records: {stats['foidev']}")

# Simple query - find MRI-related incidents
results = query_local_database(
    search_terms=[['MRI']],
    search_field='foi_text',
    limit=10
)

# Multi-term query - find records with MRI OR magnet
results = query_local_database(
    search_terms=[['MRI', 'magnet']],
    search_field='foi_text',
    limit=10
)

# Multi-group query - find records with MRI AND pacemaker
results = query_local_database(
    search_terms=[['MRI'], ['pacemaker']],
    search_field='foi_text',
    limit=10
)

# Query with exclusions - find MRI records but exclude artifacts
results = query_local_database(
    search_terms=[['MRI']],
    exclude_terms=[['artifact', 'shadow']],
    search_field='foi_text',
    limit=10
)

# Query device table by brand name
results = query_local_database(
    search_terms=[['pacemaker']],
    search_field='brand_name',
    limit=10
)
```

### CLI Integration

When you use the main CLI, it automatically queries both the openFDA API and the local database (if available):

```bash
# This will search both API and local database
maude-cli "MRI" "pacemaker"

# With exclusions
maude-cli "MRI" -x "artifact"

# Specify output format
maude-cli "MRI" -o json

# Limit results
maude-cli "MRI" -l 100
```

The local database is queried automatically when it exists at the expected path.

## Database Schema

### Tables

The database contains three main tables:

1. **device** - Device-related incident records (2000-2008)
   - Common columns: `brand_name`, `generic_name`, `manufacturer_d_name`, `model_number`, etc.

2. **foitext** - Freedom of Information Act text records (pre-2009)
   - Common columns: `foi_text`, `mdr_report_key`, `text_type_code`, `date_report`

3. **foidev** - FOI device records (1997-1998)
   - Common columns: `brand_name`, `generic_name`, `manufacturer_d_name`, `model_number`, etc.

4. **ingestion_log** - Tracks which files have been processed
   - Columns: `file_name`, `file_hash`, `record_type`, `rows_ingested`, `ingestion_timestamp`

### Column Names

All column names are normalized to lowercase with hyphens and periods replaced by underscores.
For example:
- `UDI-DI` becomes `udi_di`
- `MANUFACTURER.NAME` becomes `manufacturer_name`

### Data Types

All data columns are stored as TEXT to accommodate varying schemas across years and file types.

## Search Fields

Common search fields for queries:

### foitext table
- `foi_text` - The main text content of the incident report
- `mdr_report_key` - Report identifier
- `text_type_code` - Type of text entry

### device and foidev tables
- `brand_name` - Device brand name
- `generic_name` - Generic device name
- `manufacturer_d_name` - Manufacturer name
- `model_number` - Device model number
- `device_report_product_code` - Product code

## Notes

- The local database contains historical data (pre-2009) not available through the openFDA API
- Search is case-insensitive
- Multiple search terms within a group are OR'd together
- Multiple groups are AND'd together
- Exclusion terms work the same way as search terms
- The database file (~1.2 GB) is excluded from version control via `.gitignore`
