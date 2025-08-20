# MAUDE CLI

Command-line interface, written in pure Python, for searching the MAUDE (Manufacturer and User Facility Device Experience) database through the openFDA API.
Search medical device adverse event reports directly from your terminal.


## Installation

With uv:

```sh
uvx maude-cli
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


## License

This code is licensed under the GNU GPL v3 license which can be found [here](LICENSE).
