"""Unit tests for API functionality of MAUDE CLI.

This module contains unittest cases for verifying the correctness of API-related
functions in the maudecli.api module, including URL construction, search term
validation, results filtering, and API integration.
"""

# ruff: noqa: PT009 PT027 SLF001

from __future__ import annotations

import json
import unittest
import urllib
from typing import Any
from unittest import mock

# Import project modules
from maudecli import api, errors


class TestURLConstruction(unittest.TestCase):
    """Test suite for URL construction functionality in the API module.

    Verifies that URL construction handles various parameters correctly,
    including base URLs, query terms, limits, and sorting parameters.
    """

    def test_basic_url(self) -> None:
        """Test construction of a basic URL with query and limit parameters.

        Verifies that a simple URL with query term and limit is properly formatted.
        """
        url: str = api.construct_url(
            "https://api.fda.gov/device/event.json",
            "mdr_text.text:mri",
            limit=100,
        )
        self.assertEqual(
            url,
            "https://api.fda.gov/device/event.json:mdr_text.text:mri&limit=100",
        )

    def test_url_with_sort(self) -> None:
        """Test URL construction with sort parameter.

        Verifies that sort parameters are correctly appended to the URL.
        """
        url: str = api.construct_url(
            "https://api.fda.gov/device/event.json",
            "mdr_text.text:mri",
            limit=100,
            sort="report_date:desc",
        )
        self.assertEqual(
            url,
            "https://api.fda.gov/device/event.json:mdr_text.text:mri&limit=100&sort=report_date:desc",
        )

    def test_url_with_spaces(self) -> None:
        """Test URL construction handles spaces correctly.

        Verifies that spaces in query terms are properly URL-encoded as '+'.
        """
        url: str = api.construct_url(
            "https://api.fda.gov/device/event.json",
            "mdr_text.text:mri machine",
            limit=100,
        )
        self.assertEqual(
            url,
            "https://api.fda.gov/device/event.json:mdr_text.text:mri+machine&limit=100",
        )


class TestSearchTermValidation(unittest.TestCase):
    """Test suite for search term validation functionality.

    Verifies that the _validate_search_terms function correctly processes
    various input types and structures, converting them to the expected
    internal representation.
    """

    def test_single_string_term(self) -> None:
        """Test validation of a single string search term.

        Verifies that a single string term is converted to a list of lists.
        """
        result: list[list[str]] = api._validate_search_terms(("mri",))
        self.assertEqual(result, [["mri"]])

    def test_multiple_string_terms(self) -> None:
        """Test validation of multiple string search terms.

        Verifies that multiple string terms are converted to separate term groups.
        """
        result: list[list[str]] = api._validate_search_terms(("mri", "pacemaker"))
        self.assertEqual(result, [["mri"], ["pacemaker"]])

    def test_list_term(self) -> None:
        """Test validation of a list of search terms.

        Verifies that a list of terms is treated as a single term group.
        """
        result: list[list[str]] = api._validate_search_terms((["mri", "magnet"],))
        self.assertEqual(result, [["mri", "magnet"]])

    def test_mixed_terms(self) -> None:
        """Test validation of mixed string and list search terms.

        Verifies proper handling of a combination of string terms and term groups.
        """
        result: list[list[str]] = api._validate_search_terms(
            ("mri", ["stapes", "tympanostomy"]),
        )
        self.assertEqual(result, [["mri"], ["stapes", "tympanostomy"]])

    def test_invalid_type(self) -> None:
        """Test validation rejects invalid term types.

        Verifies that non-string and non-iterable types raise TypeError.
        """
        with self.assertRaises(TypeError):
            api._validate_search_terms((123,))

    def test_cant_convert_to_string(self) -> None:
        """Test handling of objects that can't be converted to strings.

        Verifies that objects without proper string representation raise
        CantConvertToStringError.
        """

        class NoString:
            """Test class that cannot be converted to string."""

            def __str__(self) -> str:
                raise NotImplementedError

        with self.assertRaises(errors.CantConvertToStringError):
            api._validate_search_terms([(NoString(),)])


class TestResultsFiltering(unittest.TestCase):
    """Test suite for results filtering functionality.

    Verifies that the filter_results function correctly excludes items
    based on the specified exclusion terms and field paths.
    """

    def test_no_exclusion(self) -> None:
        """Test filtering with no exclusion terms.

        Verifies that all results are returned when no exclusion terms are provided.
        """
        results: list[dict[str, Any]] = [{"mdr_text": {"text": "MRI report"}}]
        filtered: list[dict[str, Any]] = api.filter_results(
            results,
            None,
            "mdr_text.text",
        )
        self.assertEqual(filtered, results)

    def test_single_exclusion_term(self) -> None:
        """Test filtering with a single exclusion term group.

        Verifies that items containing the exclusion term are properly filtered out.
        """
        results: list[dict[str, Any]] = [
            {"mdr_text": {"text": "MRI report"}},
            {"mdr_text": {"text": "Artifact in MRI"}},
        ]
        filtered: list[dict[str, Any]] = api.filter_results(
            results,
            [["artifact"]],
            "mdr_text.text",
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["mdr_text"]["text"], "MRI report")

    def test_multiple_exclusion_groups(self) -> None:
        """Test filtering with multiple exclusion term groups.

        Verifies that items matching all exclusion groups (AND logic between groups)
        are filtered out, while others are retained.
        """
        results: list[dict[str, Any]] = [
            {"mdr_text": {"text": "MRI report"}},
            {"mdr_text": {"text": "Artifact in MRI"}},
            {"mdr_text": {"text": "Artifact with metal"}},
            {"mdr_text": {"text": "Metal in MRI"}},
        ]
        # Exclude items with BOTH "artifact" AND "metal"
        filtered: list[dict[str, Any]] = api.filter_results(
            results,
            [["artifact"], ["metal"]],
            "mdr_text.text",
        )
        self.assertEqual(len(filtered), 1)
        texts: list[str] = [r["mdr_text"]["text"] for r in filtered]
        self.assertNotIn("Artifact with metal", texts)

    def test_case_insensitive_filtering(self) -> None:
        """Test exclusion filtering is case-insensitive.

        Verifies that exclusion terms match regardless of case in the source text.
        """
        results: list[dict[str, Any]] = [
            {"mdr_text": {"text": "MRI report"}},
            {"mdr_text": {"text": "ARTIFACT in MRI"}},
        ]
        filtered: list[dict[str, Any]] = api.filter_results(
            results,
            [["artifact"]],
            "mdr_text.text",
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["mdr_text"]["text"], "MRI report")

    def test_nested_field_extraction(self) -> None:
        """Test extraction from nested fields for filtering.

        Verifies that the filter can correctly access nested field structures.
        """
        results: list[dict[str, Any]] = [
            {"device": {"brand_name": "Pacemaker X"}},
            {"device": {"brand_name": "MRI Safe Device"}},
        ]
        filtered: list[dict[str, Any]] = api.filter_results(
            results,
            [["mri"]],
            "device.brand_name",
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["device"]["brand_name"], "Pacemaker X")


class TestItemTextExtraction(unittest.TestCase):
    """Test suite for item text extraction functionality.

    Verifies that the _get_item_text function correctly extracts text
    from various nested data structures using field path specifications.
    """

    def test_simple_extraction(self) -> None:
        """Test extracting text from simple nested structure.

        Verifies basic extraction from a single-level nested structure.
        """
        item: dict[str, Any] = {"mdr_text": {"text": "Sample report text"}}
        result: list[str] = list(api._get_item_text(item, "mdr_text.text"))
        self.assertEqual(result, ["Sample report text"])

    def test_list_extraction(self) -> None:
        """Test extracting text from list structure.

        Verifies extraction from a field containing a list of objects.
        """
        item: dict[str, Any] = {
            "mdr_text": [{"text": "First report"}, {"text": "Second report"}],
        }
        result: list[str] = list(api._get_item_text(item, "mdr_text.text"))
        self.assertEqual(result, ["First report", "Second report"])

    def test_multiple_nesting(self) -> None:
        """Test extracting text with multiple levels of nesting.

        Verifies extraction from deeply nested structures.
        """
        item: dict[str, Any] = {"reports": {"details": {"text": "Deeply nested text"}}}
        result: list[str] = list(api._get_item_text(item, "reports.details.text"))
        self.assertEqual(result, ["Deeply nested text"])


class TestAPIIntegration(unittest.TestCase):
    """Test suite for API integration functionality.

    Verifies that the fetch_results function correctly interacts with
    the API, processes responses, and applies filtering logic.
    """

    @mock.patch("urllib.request.urlopen")
    def test_fetch_results(self, mock_urlopen: mock.MagicMock) -> None:
        """Test basic fetch_results functionality with mocked API response.

        Verifies that results are correctly retrieved and parsed from API response.

        Args:
            mock_urlopen: Mocked urllib.request.urlopen function

        """
        # Mock API response
        mock_response = mock.Mock()
        mock_response.getheader.return_value = '<next_url>; rel="next"'
        mock_response.read.return_value = json.dumps(
            {
                "meta": {"results": {"total": 2}},
                "results": [
                    {"report_number": "R123", "mdr_text": {"text": "MRI report"}},
                ],
            },
        ).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Call the function
        results: list[dict[str, Any]] = api.fetch_results(["mri"], limit=1)

        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["report_number"], "R123")
        self.assertEqual(results[0]["mdr_text"]["text"], "MRI report")

    @mock.patch("urllib.request.urlopen")
    def test_fetch_results_with_exclusion(self, mock_urlopen: mock.MagicMock) -> None:
        """Test fetch_results with exclusion terms.

        Verifies that exclusion terms are properly applied during result fetching.

        Args:
            mock_urlopen: Mocked urllib.request.urlopen function

        """
        # Mock API response
        mock_response = mock.Mock()
        mock_response.getheader.return_value = None  # No next page
        mock_response.read.return_value = json.dumps(
            {
                "meta": {"results": {"total": 3}},
                "results": [
                    {"report_number": "R123", "mdr_text": {"text": "MRI report"}},
                    {"report_number": "R124", "mdr_text": {"text": "Artifact in MRI"}},
                    {
                        "report_number": "R125",
                        "mdr_text": {"text": "Artifact with metal"},
                    },
                ],
            },
        ).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Call the function with exclusion
        results: list[dict[str, Any]] = api.fetch_results(
            ["mri"],
            exclude_terms=[["artifact"], ["metal"]],
        )

        # Should exclude R124 (has artifact) and R125 (has artifact AND metal)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["report_number"], "R123")


class TestAPIErrorHandling(unittest.TestCase):
    """Test suite for API error handling functionality."""

    @mock.patch("urllib.request.urlopen")
    def test_rate_limit_error(self, mock_urlopen: mock.MagicMock) -> None:
        """Test handling of API rate limit errors (HTTP 429)."""
        # Mock rate limit response
        mock_response = mock.Mock()
        mock_response.status = 429
        mock_response.read.return_value = json.dumps(
            {"error": {"code": "429", "message": "Rate limit exceeded"}},
        ).encode("utf-8")
        mock_response.getheader.return_value = None
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "url", 429, "Rate limit", None, None,
        )

        with self.assertRaises(api.APIRateLimitError):
            api.fetch_results(["test"])

    @mock.patch("urllib.request.urlopen")
    def test_invalid_search_field(self) -> None:
        """Test validation of search fields before making API calls."""
        with self.assertRaises(errors.InvalidSearchFieldError) as context:
            api.fetch_results(["test"], search_fields="invalid.field")

        self.assertEqual(
            str(context.exception), "Invalid search field: 'invalid.field'",
        )

    @mock.patch("urllib.request.urlopen")
    def test_api_error_response(self, mock_urlopen: mock.MagicMock) -> None:
        """Test handling of API error responses with error messages."""
        # Mock API error response
        mock_response = mock.Mock()
        mock_response.status = 400
        mock_response.read.return_value = json.dumps(
            {"error": {"code": "400", "message": "Invalid query parameter"}},
        ).encode("utf-8")
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "url", 400, "Bad Request", None, None,
        )

        with self.assertRaises(api.APIResponseError) as context:
            api.fetch_results(["test"])

        self.assertEqual(
            str(context.exception), "API returned error 400: Invalid query parameter",
        )

    @mock.patch("urllib.request.urlopen")
    def test_network_error(self, mock_urlopen: mock.MagicMock) -> None:
        """Test handling of network connection errors."""
        # Mock network failure
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        with self.assertRaises(api.APIConnectionError) as context:
            api.fetch_results(["test"])

        self.assertEqual(
            str(context.exception), "Failed to connect to API: Connection refused",
        )

    @mock.patch("urllib.request.urlopen")
    def test_invalid_json_response(self, mock_urlopen: mock.MagicMock) -> None:
        """Test handling of invalid JSON responses from API."""
        # Mock invalid JSON response
        mock_response = mock.Mock()
        mock_response.read.return_value = b"Not JSON data"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        with self.assertRaises(api.APIResponseError) as context:
            api.fetch_results(["test"])

        self.assertIn("Invalid JSON response", str(context.exception))


if __name__ == "__main__":
    unittest.main()
