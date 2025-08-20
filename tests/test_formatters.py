"""Unit tests for output formatter functionality of MAUDE CLI.

This module contains unittest cases for verifying the correctness of
output formatting functions in the maudecli.formatters module,
including org-mode and CSV formatting.
"""

# ruff: noqa: PT009 PT027

import csv
import io
import unittest
from typing import Any

# Import project modules
from maudecli import formatters


class TestOrgFormatter(unittest.TestCase):
    """Test suite for org-mode formatter functionality.

    Verifies that the as_org function correctly formats results
    as org-mode documents with proper structure and formatting.
    """

    def test_simple_org_format(self) -> None:
        """Test basic org-mode formatting with simple data structure.

        Verifies that a simple result is correctly formatted as org-mode.
        """
        results: list[dict[str, Any]] = [
            {"report_number": "R123", "device": {"brand_name": "Pacemaker"}},
        ]
        output: str = formatters.as_org(results)

        self.assertIn("* TODO R123", output)
        self.assertIn(":DEVICE_BRAND_NAME: Pacemaker", output)

    def test_nested_structure(self) -> None:
        """Test org-mode formatting of nested data structures.

        Verifies proper handling of nested dictionaries and lists.
        """
        results: list[dict[str, Any]] = [
            {
                "report_number": "R123",
                "device": {"brand_name": "Pacemaker", "problems": ["battery", "lead"]},
            },
        ]
        output: str = formatters.as_org(results)

        self.assertIn(":DEVICE_BRAND_NAME: Pacemaker", output)
        self.assertIn(":DEVICE_PROBLEMS: battery", output)
        self.assertIn(":DEVICE_1_PROBLEMS: lead", output)

    def test_field_filtering(self) -> None:
        """Test org-mode formatting with field filtering.

        Verifies that only specified fields are included in the output.
        """
        results: list[dict[str, Any]] = [
            {
                "report_number": "R123",
                "device": {"brand_name": "Pacemaker", "id": "12345"},
            },
        ]
        output: str = formatters.as_org(results)

        self.assertIn(":DEVICE_BRAND_NAME: Pacemaker", output)
        self.assertNotIn("id", output)

    def test_custom_name_field(self) -> None:
        """Test org-mode formatting with custom name field.

        Verifies that a different field can be used as the item name.
        """
        results: list[dict[str, Any]] = [
            {"id": "R123", "device": {"brand_name": "Pacemaker"}},
        ]
        output: str = formatters.as_org(results, name="id")
        self.assertIn("* TODO R123", output)

    def test_custom_heading_level(self) -> None:
        """Test org-mode formatting with custom heading level.

        Verifies that the heading level parameter affects the output structure.
        """
        results: list[dict[str, Any]] = [{"report_number": "R123"}]
        output: str = formatters.as_org(results, level=2)
        self.assertIn("** TODO R123", output)
        self.assertNotIn("*** TODO R123", output)

    def test_empty_results(self) -> None:
        """Test org-mode formatting with empty results list.

        Verifies that empty input produces empty output.
        """
        output: str = formatters.as_org([])
        self.assertEqual(output, "")


class TestCSVFormatter(unittest.TestCase):
    """Test suite for CSV formatter functionality.

    Verifies that the as_csv function correctly formats results
    as CSV with proper headers and data alignment.
    """

    def test_simple_csv_format(self) -> None:
        """Test basic CSV formatting with simple data structure.

        Verifies that results are correctly converted to CSV format.
        """
        results: list[dict[str, Any]] = [
            {"report_number": "R123", "device.brand_name": "Pacemaker"},
            {"report_number": "R124", "device.brand_name": "Defibrillator"},
        ]
        output: str = formatters.as_csv(results)

        # Parse the CSV output
        reader = csv.DictReader(io.StringIO(output))
        rows: list[dict[str, str]] = list(reader)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["report_number"], "R123")
        self.assertEqual(rows[0]["device.brand_name"], "Pacemaker")

    def test_field_selection(self) -> None:
        """Test CSV formatting with field selection.

        Verifies that only specified fields are included in the CSV output.
        """
        results: list[dict[str, Any]] = [
            {
                "report_number": "R123",
                "device.brand_name": "Pacemaker",
                "patient.age": "65",
            },
        ]
        output: str = formatters.as_csv(
            results, fields=["report_number", "patient.age"],
        )

        # Parse the CSV output
        reader = csv.DictReader(io.StringIO(output))
        rows: list[dict[str, str]] = list(reader)

        self.assertEqual(len(rows[0]), 2)
        self.assertIn("report_number", rows[0])
        self.assertIn("patient.age", rows[0])
        self.assertNotIn("device.brand_name", rows[0])

    def test_empty_results(self) -> None:
        """Test CSV formatting with empty results list.

        Verifies that empty input produces empty output.
        """
        output: str = formatters.as_csv([])
        self.assertEqual(output, "")

    def test_mixed_data_types(self) -> None:
        """Test CSV formatting with mixed data types.

        Verifies proper handling of different data types in CSV output.
        """
        results: list[dict[str, Any]] = [
            {
                "report_number": "R123",
                "device_count": 5,
                "is_serious": True,
                "notes": None,
            },
        ]
        output: str = formatters.as_csv(results)

        # Parse the CSV output
        reader = csv.DictReader(io.StringIO(output))
        rows: list[dict[str, str]] = list(reader)

        self.assertEqual(rows[0]["report_number"], "R123")
        self.assertEqual(rows[0]["device_count"], "5")
        self.assertEqual(rows[0]["is_serious"], "True")
        self.assertEqual(rows[0]["notes"], "")

    def test_special_characters(self) -> None:
        """Test CSV formatting with special characters.

        Verifies proper handling of special characters like commas and quotes.
        """
        results: list[dict[str, Any]] = [
            {"description": 'Report with "quotes", and, commas'},
        ]
        output: str = formatters.as_csv(results)

        # Parse the CSV output to verify proper escaping
        reader = csv.DictReader(io.StringIO(output))
        rows: list[dict[str, str]] = list(reader)

        self.assertEqual(rows[0]["description"], 'Report with "quotes", and, commas')


if __name__ == "__main__":
    unittest.main()
