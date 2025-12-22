"""MAUDE database search CLI."""

# ruff: noqa: T201

# Python imports
import asyncio
import argparse
import json
import logging
import sys
from pathlib import Path

# Local imports
from maudecli.api import fetch_results
from maudecli.db import build_database, database_exists, query_local_database
from maudecli.formatters import as_csv, as_org

logger = logging.getLogger(__name__)


def main() -> None:
    """Entry point for the MAUDE CLI."""
    parser = argparse.ArgumentParser(
        description="Search MAUDE database via openFDA API",
    )
    parser.add_argument(
        "term_groups",
        nargs="+",
        help=(
            "Term groups (comma-separated terms OR'd within groups, "
            "groups AND'd together). Example: 'MRI,MAGNET' 'STAPES,TYMP'"
        ),
    )
    parser.add_argument(
        "-x", "--exclude",
        nargs="+",
        help=(
            "Term groups to exclude (comma-separated terms OR'd within groups, "
            "groups AND'd together). Example: 'ARTIFACT,SHADOW' 'METAL,SCREW'"
        ),
    )
    parser.add_argument(
        "-f", "--search-fields",
        default="mdr_text.text",
        help="Fields to search (default: mdr_text.text)",
    )
    parser.add_argument(
        "-p", "--max-pages",
        type=int,
        default=0,
        help="Max pages to retrieve (0=all, default: 0)",
    )
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=1000,
        help="Results per page (default: 1000)",
    )
    parser.add_argument(
        "-s", "--sort",
        help="Sort criteria",
    )
    parser.add_argument(
        "-o", "--format",
        default="json",
        choices=["org", "json", "csv", "text"],
        help="Output format (default: json)",
    )
    parser.add_argument(
        "-O", "--output",
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "-n", "--name",
        default="report_number",
        help="Field to use as item name (default: report_number)",
    )
    parser.add_argument(
        "-F", "--fields",
        help="Comma-separated fields to include in output",
    )
    parser.add_argument(
        "-L", "--level",
        type=int,
        default=3,
        help="Org heading level (default: 3)",
    )

    args = parser.parse_args()

    # Process term groups into lists
    terms = [group.split(",") for group in args.term_groups]
    exclude_terms = (
        [group.split(",") for group in args.exclude]
        if args.exclude
        else None
    )

    # Builds the local database
    asyncio.run(build_database())

    # Fetch results from API
    results = fetch_results(
        *terms,
        exclude_terms=exclude_terms,
        search_fields=args.search_fields,
        max_pages=args.max_pages,
        limit=args.limit,
        sort=args.sort,
    )

    # Also query local database for pre-2009 data if available
    if database_exists():
        logger.info("Querying local database for historical data...")
        # Use the first search field for local DB query
        search_field = args.search_fields.split(",")[0] if "," in args.search_fields else args.search_fields
        # Map common API field names to local DB field names
        field_mapping = {
            "mdr_text.text": "foi_text",
            "device.device_name": "generic_name",
            "device.brand_name": "brand_name",
        }
        local_field = field_mapping.get(search_field, search_field.split(".")[-1])
        
        local_results = query_local_database(
            terms,
            exclude_terms=exclude_terms,
            search_field=local_field,
            limit=args.limit if args.max_pages == 1 else None,
        )
        if local_results:
            logger.info(f"Found {len(local_results)} results in local database")
            results.extend(local_results)
        else:
            logger.info("No results found in local database")
    else:
        logger.info(
            "Local database not found, skipping historical data query"
            " The local database is required to query results pre-2009!",
        )

    output = Path(args.output) if isinstance(args.output, str) else args.output
    if output and args.format != output.suffix[1:]:
        logger.error(
            "Format (%s) does not match file format (%s) setting format to %s",
            args.format,
            output.suffix[1:],
            output.suffix[1:],
        )
        args.format = output.suffix[1:]

    # Format output
    fields = args.fields.split(",") if args.fields else None
    match args.format:
        case "org":
            output_str = as_org(
                results, name=args.name, fields=fields, level=args.level,
            )
        case "json":
            output_str = json.dumps(results, indent=2)
        case "csv":
            output_str = as_csv(results, fields=fields)
        case _: # Text
            output_str = ""
            for i, r in enumerate(results, 1):
                output_str += f"\nResult {i}:"
                for k, v in r.items():
                    if fields is None or k in fields:
                        output_str += f"\n  {k}: {v}"

    # Ensure consistent trailing newline
    if output_str and not output_str.endswith("\n"):
        output_str += "\n"

    # Handle output destination
    try:
        with output.open("w") as fp:
            fp.write(output_str)
    except AttributeError:
        print(output_str, end="")

if __name__ == "__main__":
    main()
    sys.exit()
