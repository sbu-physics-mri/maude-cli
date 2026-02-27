# MAUDE CLI

Command-line interface, written in pure Python, for searching the MAUDE (Manufacturer and User Facility Device Experience) database through the openFDA API and local historical data.
Search medical device adverse event reports directly from your terminal.

*Note: The openFDA API is updated weekly but does not cover incidents before 2009. This CLI includes support for querying a local SQLite database of historical pre-2009 data!*


### Features

- Command-line interface for searching the [MAUDE database](https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/search.cfm) via the [openFDA API](https://open.fda.gov/apis/try-the-api/).
- **Local database support for historical pre-2009 data** not available through the API (~2.6M records).
- Flexible term syntax: multiple groups combined with `AND`, terms within a group combined with `OR`.
- Exclusion filtering with the same logical syntax as search terms.
- Customizable search fields (default: `mdr_text.text`).
- Pagination control: specify maximum pages and results per page.
- Optional sorting of results using openFDA sort criteria.
- Multiple output formats: Org-mode, JSON, CSV, and plain text.
- Ability to select specific fields for output and define the item name field.
- Configurable Org-mode heading level for hierarchical reports.
- Direct file output with automatic format validation against file extension.
- Graceful handling of stdout when no output file is specified.
- Comprehensive logging for error reporting and debugging.
- Fully type-annotated, lint-clean code base ready for extension or integration.


## Installation

With uvx there's no need to install the tool before first use, just use:

```sh
uvx maude-cli --help
```

or to use uv as more traditional package manage, i.e. download the `maude-cli` locally before first use:

```sh
uv tool install maude-cli
```

or pip:

```sh
pip install maude-cli
```

## Usage

```sh
maude-cli [OPTIONS] TERM_GROUPS...
```

### Basic Example

Search for reports containing either `"MRI"` or `"MAGNET"` AND either `"STAPES"` or `"TYMP"`:

```sh
maude-cli "MRI,MAGNET" "STAPES,TYMP"
```

### Options

-   `-x, --exclude`: Term groups to exclude (comma-separated terms OR'd within groups, groups AND'd together). Example: 'ARTIFACT,SHADOW' 'METAL,SCREW'
-   `-f, --search-fields`: Fields to search (default: `mdr_text.text`)
-   `-p, --max-pages`: Maximum pages to retrieve (0=all)
-   `-l, --limit`: Results per page (default: 1000)
-   `-s, --sort`: Sort criteria
-   `-o, --format`: Output format (`org`, `json`, or `text`; default: `org`)
-   `-O, --output`: Output file. If not used then will print to standard out.
-   `-n, --name`: Field to use as item name (default: `report_number`)
-   `-F, --fields`: Comma-separated fields to include in output
-   `-L, --level`: Org heading level (default: 3)


### Output Formats

-   `org`: Org-mode formatted output (default)
-   `json`: Pretty-printed JSON
-   `text`: Simple text format
-   `csv` : CSV format.


### Exclusion Criteria

The CLI supports sophisticated exclusion filtering using the `-x=/``--exclude` option. Exclusion terms follow the same logic as search terms but filter OUT matching results:

-   Within each exclusion group: terms are OR'd (`term1 OR term2`)
-   Between exclusion groups: conditions are AND'd (`group1 AND group2`)
-   Results matching ALL exclusion groups will be filtered out

## Examples

*Note with all of these examples if you are using `uvx` (recommended) then you must prepend `uvx` before each command. That is `maude-cli --help` would become `uvx maude-cli --help`*

Search with specific fields and output format:

```
maude-cli "pacemaker" -f device.device_name -o json -F device.brand_name,patient.age
```

Search with pagination control:

```
maude-cli "defibrillator" -p 2 -l 50
```

Search for all entries that contain either "MRI" or "MAGNET" AND also containing either "CARDIAC" or "VALVE" and output as a csv file.

```
maude-cli "mri,magnet" "cardiac,valve" -o csv
```

#### Exclusion Examples

**Basic exclusion**
Find MRI-related reports but exclude artifacts:

``` sh
maude-cli "MRI,MAGNET" -x "ARTIFACT,SHADOW"
```

**Multi-group exclusion**
Find pacemaker reports but exclude both battery AND lead issues:

```
maude-cli "pacemaker" -x "BATTERY,POWER" "LEAD,WIRE"
```

*(Excludes reports containing ("BATTERY" OR "POWER") AND ("LEAD" OR "WIRE"))*

**Field-specific exclusion**
Search device names for "catheter" but exclude infection-related reports:

```
maude-cli "catheter" -f device.device_name -x "INFECTION,SEPSIS" -F patient.problem
```

**Manufacturer exclusion**
Find defibrillator reports but exclude two specific manufacturers:

```
maude-cli "defibrillator" -x "MEDTRONIC,ST_JUDE" "BOSTON_SCIENTIFIC"
```

**Report type filtering**
Find stent reports but exclude follow-ups and summaries:

```
maude-cli "stent" -x "FOLLOW-UP,SUMMARY,ADDITIONAL"
```

**Combined search/exclusion**
Find reports about "valve" in cardiac devices but exclude MRI-related artifacts:

```
maude-cli "valve" -f "device.device_name" -x "MRI,MAGNET" "ARTIFACT,SHADOW"
```

**Complex scenario**
Search for infusion pump issues but exclude:

-   Software-related problems (group 1)
-   Baxter-manufactured devices (group 2)
-   Reports without patient injury (group 3)

```
maude-cli "infusion,pump" -x "SOFTWARE,ALGORITHM" "BAXTER" "NO INJURY,"
```

## Local Historical Database

The CLI supports querying a local SQLite database containing historical MAUDE data from before 2009. The openFDA API only includes data from 2009 onwards, but the local database provides access to ~2.6 million historical incident reports.

The package ships with a pre-built local database, for information on building the database from the data files [see here](DATABASE_USAGE.md).


### Automatic Query Integration

When the local database exists, the CLI automatically queries both the API and the local database, combining results:

```bash
# Searches both API (2009+) and local DB (pre-2009)
maude-cli "MRI" "pacemaker"
```

No additional flags are needed - the integration is transparent to the user.

For more details on database usage, see [DATABASE_USAGE.md](DATABASE_USAGE.md).


## License

This code is licensed under the GNU GPL v3 license which can be found [here](LICENSE).
