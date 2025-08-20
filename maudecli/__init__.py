"""MAUDE database search CLI."""

# ruff: noqa: T201

# Python imports
import argparse
import json
import sys

# Local imports
from maudecli.api import fetch_results
from maudecli.formatters import as_csv, as_org


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
        default="org",
        choices=["org", "json", "csv", "text"],
        help="Output format (default: org)",
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

    # Fetch results
    results = fetch_results(
        *terms,
        search_fields=args.search_fields,
        max_pages=args.max_pages,
        limit=args.limit,
        sort=args.sort,
    )

    # Format output
    fields = args.fields.split(",") if args.fields else None
    if args.format == "org":
        print(as_org(results, name=args.name, fields=fields, level=args.level))
    elif args.format == "json":
        print(json.dumps(results, indent=2))
    elif args.format == "csv":
        print(as_csv(results, fields=fields))
    else:  # text
        for i, r in enumerate(results, 1):
            print(f"Result {i}:")
            for k, v in r.items():
                if fields is None or k in fields:
                    print(f"  {k}: {v}")

if __name__ == "__main__":
    sys.exit(main())
