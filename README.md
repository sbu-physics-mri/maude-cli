# MAUDE CLI

Command-line interface for searching the MAUDE (Manufacturer and User Facility Device Experience) database through the openFDA API. Search medical device adverse event reports directly from your terminal.


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

-   `-f, --search-fields`: Fields to search (default: `mdr_text.text`)
-   `-p, --max-pages`: Maximum pages to retrieve (0=all)
-   `-l, --limit`: Results per page (default: 1000)
-   `-s, --sort`: Sort criteria
-   `-o, --format`: Output format (`org`, `json`, or `text`; default: `org`)
-   `-n, --name`: Field to use as item name (default: `report_number`)
-   `-F, --fields`: Comma-separated fields to include in output
-   `-L, --level`: Org heading level (default: 3)


### Output Formats

-   `org`: Org-mode formatted output (default)
-   `json`: Pretty-printed JSON
-   `text`: Simple text format
-   `csv` : CSV format.



## Examples

Search with specific fields and output format:

```
maude-cli "pacemaker" -f device.device_name -o json -F device.brand_name,patient.age
```

Search with pagination control:

```
maude-cli "defibrillator" -p 2 -l 50
```
