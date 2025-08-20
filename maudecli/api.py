"""Query the MAUDE database via the openfda API."""

from __future__ import annotations

# Python imports
import json
import logging
import urllib.request
from typing import Iterable

# Local imports
from maudecli.errors import CantConvertToStringError

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
        search_fields: Iterable[str] | str = "mdr_text.text",
        base_endpoint: str = "https://api.fda.gov/device/event.json",
        max_pages: int = 0,
        limit: int = 1000,
        sort: None | str = None,
) -> list[dict]:
    """Fetch the query from the endpoint.

    Args:
        terms : List of terms to search. If multiple keyword lists
            or strings are provided then there the results will contain
            on match from EACH of the terms. That is, terms of
            ["MRI, "magnet"], ["stapes", "grommet"] will return matches
            that contain one of "MRI" or "magnet"
            AND one of "stapes" or "grommet".
        search_fields : The fields to search for a keyword match.
                Can be a list of fields or a single field.
                Defaults to "mdr_text.text".
        base_endpoint : The base_endpoint to construct the query.
                Defaults to "https://api.fda.gov/device/event.json".
        max_pages : Number of pages to return. If 0 (default),
                will return all pages.
        limit : Limit the number of results returned in a single query.
            Defaults to 1000.
        sort : Sort criteria. Defaults to None.


    Returns:
        results : A list of dictionaries of the JSON results.

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

            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode())

                if data["results"]:
                    results += data["results"]
                pages += 1

                logger.info(
                    "Devices found: %i (%i %s)",
                    len(results),
                    pages,
                    "requests" if pages > 1 else "request",
                )
                meta = data["meta"]
                if max_pages and pages >= max_pages:
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
        else:
            logger.info("All results retrieved with search field %s", sf)
    logger.info("Query returned with the following meta data: %s", meta)
    return results
