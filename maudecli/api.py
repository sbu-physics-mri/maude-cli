"""Query the MAUDE database via the openfda API."""

# Typing
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

# Python imports
import json
import logging
import urllib.request
from typing import Iterable

# Local imports
from maudecli.errors import (APIConnectionError, APIRateLimitError,
                             APIResponseError, CantConvertToStringError)

logger = logging.getLogger(__name__)


def construct_url(
        base_url: str,
        query: str,
        *,
        limit: int = 1000,
        sort: str | None = None,
) -> str:
    """Construct a request URL from a base url and query."""
    sort = "" if sort is None else f"&sort={sort}"
    encoded_query = query.replace(" ", "+")
    return f"{base_url}:{encoded_query}&limit={limit}" + sort


def _validate_search_terms(
        terms: tuple[Iterable[str] | str, ...],
) -> list[list[str]]:
    keywords = []
    for idx, kw in enumerate(terms):
        if isinstance(kw, str):
            keywords.append([kw])
        elif isinstance(kw, Iterable):
            group = []
            for term in kw:
                try:
                    group.append(str(term))
                except Exception as e:  # noqa: PERF203
                    raise CantConvertToStringError(term) from e
            keywords.append(group)
        else:
            msg = f"Invalid keyword type at index {idx}: {type(kw)}"
            logger.critical(msg)
            raise TypeError(msg)
    return keywords


def fetch_results(
        *terms: Iterable[str] | str,
        exclude_terms: Sequence[Sequence[str]] | None = None,
        search_fields: Iterable[str] | str = "mdr_text.text",
        base_endpoint: str = "https://api.fda.gov/device/event.json",
        max_pages: int = 0,
        limit: int = 1000,
        sort: None | str = None,
) -> list[dict]:
    """Fetch and filter query results from the endpoint.

    Args:
        terms : Term groups to search. If multiple groups are provided,
            results must contain at least one match from EACH group.
            Example: ["MRI", "magnet"], ["stapes", "grommet"] returns matches
            containing ("MRI" OR "magnet") AND ("stapes" OR "grommet").
        exclude_terms : Term groups to exclude. Results containing matches
            from ALL exclude groups will be filtered out. Within each group,
            terms are OR'd together. Example: ["artifact", "shadow"],
            ["metal", "screw"] will exclude results containing
            ("artifact" OR "shadow") AND ("metal" OR "screw").
        search_fields : The fields to search for keyword matches.
                Can be a list of fields or a single field.
                Defaults to "mdr_text.text".
        base_endpoint : The base endpoint to construct the query.
                Defaults to "https://api.fda.gov/device/event.json".
        max_pages : Maximum number of pages to return. If 0 (default),
                will return all available pages.
        limit : Maximum number of results per query page.
            Defaults to 1000.
        sort : Sort criteria for results. Defaults to None.

    Returns:
        results : A list of dictionaries containing the filtered JSON results.

    Note:
        Exclusion filtering happens during result fetching (not post-processing),
        making it memory efficient. The API doesn't support negative matching
        natively, so this implementation filters results after retrieval.

    """
    keywords = _validate_search_terms(terms)
    search_fields = (
        [search_fields]
        if isinstance(search_fields, str)
        else search_fields
    )

    results = []
    pages = 0
    for sf in search_fields:
        base_url = f"{base_endpoint}?search={sf}"
        query = "+AND+".join(
            f"({'+OR+'.join(kw)})" for kw in keywords
        )
        url: str | None = construct_url(
            base_url, query, limit=limit, sort=sort,
        )

        while url:
            logger.debug("Sending request to %s", url)
            if not url.startswith(("http:", "https:")):
                msg = "URL must start with 'http:' or 'https:'"
                logger.critical("%s but got %s", msg, url)
                raise ValueError(msg)

            try:

                with urllib.request.urlopen(url) as response:
                    data = json.loads(response.read().decode())

                    if "error" in data:
                        error_msg = data["error"].get(
                            "message", "Unknown API error",
                        )
                        raise APIResponseError(response.status, error_msg)

                    if fr := filter_results(
                        data["results"], exclude_terms=exclude_terms, field=sf,
                    ):
                        results += fr
                    pages += 1

                    logger.info(
                        "Devices found: %i (%i %s)",
                        len(results),
                        pages,
                        "requests" if pages > 1 else "request",
                    )
                    meta = data["meta"]
                    if len(results) >= limit or (max_pages and pages >= max_pages):
                        logger.warning(
                            "Maximum pages recieved (%i) exiting...", pages,
                        )
                        break

                    link_header = response.getheader("Link")
                    if link_header:
                        start = link_header.find("<") + 1
                        end = link_header.find(">")
                        url = link_header[start:end]
                    else:
                        url = None
                    logger.debug("Next link found? %s", bool(url))

            except urllib.error.HTTPError as e:
                # Handle specific HTTP errors
                if e.code == 429:       # noqa: PLR2004
                    # Check for rate limit reset header
                    reset = e.headers.get("X-RateLimit-Reset")
                    reset_time = int(reset) if reset else None
                    raise APIRateLimitError(reset_time) from e
                error_msg = "Unknown error"
                try:
                    error_data = json.loads(e.read().decode())
                    error_msg = error_data.get("error", {}).get("message", error_msg)
                except Exception:
                    logger.exception()
                raise APIResponseError(e.code, error_msg) from e

            except urllib.error.URLError as e:
                raise APIConnectionError(str(e.reason)) from e

            except json.JSONDecodeError as e:
                raise APIResponseError(
                    200, f"Invalid JSON response: {e}",
                ) from e
            except Exception:
                logger.exception("Unexpected error during API request")
                raise
        else:
            logger.info("All results retrieved with search field %s", sf)
    logger.info("Query returned with the following meta data: %s", meta)
    return results


def _get_item_text(item: dict | list, field: str) -> list[str]:
    if not isinstance(item, list):
        item = [item]

    next_field, *rest = field.split(".")

    for obj in item:
        if rest:
            yield from _get_item_text(obj[next_field], ".".join(rest))
        else:
            yield obj[next_field]


def filter_results(
    results: Sequence[dict],
    exclude_terms: Sequence[Sequence[str]] | None,
    field: str,
) -> list[dict]:
    """Remove results that contain excluded terms in the field."""
    if exclude_terms is None or not exclude_terms:
        return results

    return [
        r for r in results
        if all(
            all(
                term.lower() not in " ".join(_get_item_text(r, field)).lower()
                for term in terms
            )
            for terms in exclude_terms
        )
    ]
