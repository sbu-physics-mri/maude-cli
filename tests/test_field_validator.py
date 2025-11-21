"""Unit tests for field validation functionality."""

# ruff: noqa: PT009 PT027

from __future__ import annotations

import json
import time
import unittest
from pathlib import Path
from unittest import mock

from maudecli import field_validator


class TestFieldValidation(unittest.TestCase):
    """Test suite for field validation functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Use a temporary cache for testing
        self.original_cache_file = field_validator.CACHE_FILE
        self.test_cache = Path("/tmp/test_maude_cache.json")
        field_validator.CACHE_FILE = self.test_cache
        
        # Clean up any existing test cache
        if self.test_cache.exists():
            self.test_cache.unlink()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Restore original cache file
        field_validator.CACHE_FILE = self.original_cache_file
        
        # Clean up test cache
        if self.test_cache.exists():
            self.test_cache.unlink()

    def test_validate_field_valid(self) -> None:
        """Test validation of a valid field."""
        # mdr_text.text is a known valid field
        is_valid, suggestion = field_validator.validate_field("mdr_text.text")
        self.assertTrue(is_valid)
        self.assertIsNone(suggestion)

    def test_validate_field_invalid(self) -> None:
        """Test validation of an invalid field."""
        is_valid, suggestion = field_validator.validate_field("invalid_field")
        self.assertFalse(is_valid)
        # May or may not have a suggestion depending on similarity

    def test_validate_field_with_suggestion(self) -> None:
        """Test that similar field names are suggested."""
        # Typo in a known field
        is_valid, suggestion = field_validator.validate_field("mdr_text.txt")
        self.assertFalse(is_valid)
        self.assertIsNotNone(suggestion)
        # Should suggest the correct field
        self.assertEqual(suggestion, "mdr_text.text")

    def test_validate_field_without_suggestion(self) -> None:
        """Test validation without suggestions."""
        is_valid, suggestion = field_validator.validate_field(
            "invalid_field",
            suggest_on_error=False,
        )
        self.assertFalse(is_valid)
        self.assertIsNone(suggestion)

    def test_validate_fields_all_valid(self) -> None:
        """Test validation of multiple valid fields."""
        all_valid, suggestions = field_validator.validate_fields(
            "mdr_text.text,device.brand_name,report_number",
        )
        self.assertTrue(all_valid)
        self.assertEqual(suggestions, {})

    def test_validate_fields_with_invalid(self) -> None:
        """Test validation of mixed valid and invalid fields."""
        all_valid, suggestions = field_validator.validate_fields(
            "mdr_text.text,invalid_field,report_number",
        )
        self.assertFalse(all_valid)
        self.assertIn("invalid_field", suggestions)

    def test_validate_fields_sequence(self) -> None:
        """Test validation with a sequence of fields."""
        all_valid, suggestions = field_validator.validate_fields(
            ["mdr_text.text", "device.brand_name"],
        )
        self.assertTrue(all_valid)
        self.assertEqual(suggestions, {})


class TestFieldCaching(unittest.TestCase):
    """Test suite for field caching functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Use a temporary cache for testing
        self.original_cache_file = field_validator.CACHE_FILE
        self.test_cache = Path("/tmp/test_maude_cache.json")
        field_validator.CACHE_FILE = self.test_cache
        
        # Clean up any existing test cache
        if self.test_cache.exists():
            self.test_cache.unlink()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Restore original cache file
        field_validator.CACHE_FILE = self.original_cache_file
        
        # Clean up test cache
        if self.test_cache.exists():
            self.test_cache.unlink()

    def test_load_cache_nonexistent(self) -> None:
        """Test loading cache when file doesn't exist."""
        cache = field_validator._load_cache()
        self.assertIsNone(cache)

    def test_save_and_load_cache(self) -> None:
        """Test saving and loading field cache."""
        test_fields = ["field1", "field2", "field3"]
        field_validator._save_cache(test_fields)
        
        # Verify file was created
        self.assertTrue(self.test_cache.exists())
        
        # Load and verify
        cache = field_validator._load_cache()
        self.assertIsNotNone(cache)
        self.assertEqual(cache["fields"], sorted(test_fields))

    def test_cache_expiry(self) -> None:
        """Test that expired cache is rejected."""
        test_fields = ["field1", "field2"]
        
        # Create an expired cache
        cache = {
            "timestamp": time.time() - (8 * 24 * 3600),  # 8 days old
            "fields": test_fields,
        }
        
        self.test_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.test_cache.open("w") as f:
            json.dump(cache, f)
        
        # Should return None due to expiration
        loaded_cache = field_validator._load_cache()
        self.assertIsNone(loaded_cache)

    def test_cache_not_expired(self) -> None:
        """Test that valid cache is loaded."""
        test_fields = ["field1", "field2"]
        
        # Create a fresh cache
        cache = {
            "timestamp": time.time() - (3 * 24 * 3600),  # 3 days old
            "fields": test_fields,
        }
        
        self.test_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.test_cache.open("w") as f:
            json.dump(cache, f)
        
        # Should load successfully
        loaded_cache = field_validator._load_cache()
        self.assertIsNotNone(loaded_cache)
        self.assertEqual(loaded_cache["fields"], test_fields)

    def test_refresh_cache(self) -> None:
        """Test forcing cache refresh."""
        # Mock the API fetch to return test fields
        test_fields = ["field1", "field2", "field3"]
        
        with mock.patch.object(
            field_validator,
            "_fetch_fields_from_api",
            return_value=test_fields,
        ):
            count = field_validator.refresh_cache()
            self.assertEqual(count, len(test_fields))
            
            # Verify cache was saved
            cache = field_validator._load_cache()
            self.assertIsNotNone(cache)
            self.assertEqual(cache["fields"], sorted(test_fields))


class TestAPIFieldFetching(unittest.TestCase):
    """Test suite for API field fetching functionality."""

    @mock.patch("urllib.request.urlopen")
    def test_fetch_fields_from_api_success(
        self,
        mock_urlopen: mock.MagicMock,
    ) -> None:
        """Test successful field fetching from API."""
        # Mock API response
        mock_response = mock.Mock()
        mock_response.read.return_value = json.dumps({
            "results": [
                {
                    "report_number": "12345",
                    "mdr_text": {"text": "Sample text"},
                    "device": {"brand_name": "Test Device"},
                },
            ],
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response
        
        # Fetch fields
        fields = field_validator._fetch_fields_from_api()
        
        # Verify fields were extracted
        self.assertIsNotNone(fields)
        self.assertIn("report_number", fields)
        self.assertIn("mdr_text", fields)
        self.assertIn("mdr_text.text", fields)
        self.assertIn("device", fields)
        self.assertIn("device.brand_name", fields)

    @mock.patch("urllib.request.urlopen")
    def test_fetch_fields_from_api_failure(
        self,
        mock_urlopen: mock.MagicMock,
    ) -> None:
        """Test handling of API fetch failure."""
        # Mock network error
        import urllib.error
        mock_urlopen.side_effect = urllib.error.URLError("Connection failed")
        
        # Should return None on failure
        fields = field_validator._fetch_fields_from_api()
        self.assertIsNone(fields)

    @mock.patch("urllib.request.urlopen")
    def test_fetch_fields_from_api_empty_results(
        self,
        mock_urlopen: mock.MagicMock,
    ) -> None:
        """Test handling of empty API results."""
        # Mock empty response
        mock_response = mock.Mock()
        mock_response.read.return_value = json.dumps({
            "results": [],
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response
        
        # Should return None for empty results
        fields = field_validator._fetch_fields_from_api()
        self.assertIsNone(fields)


class TestGetValidFields(unittest.TestCase):
    """Test suite for get_valid_fields functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Use a temporary cache for testing
        self.original_cache_file = field_validator.CACHE_FILE
        self.test_cache = Path("/tmp/test_maude_cache.json")
        field_validator.CACHE_FILE = self.test_cache
        
        # Clean up any existing test cache
        if self.test_cache.exists():
            self.test_cache.unlink()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Restore original cache file
        field_validator.CACHE_FILE = self.original_cache_file
        
        # Clean up test cache
        if self.test_cache.exists():
            self.test_cache.unlink()

    @mock.patch.object(field_validator, "_fetch_fields_from_api")
    def test_get_valid_fields_from_api(
        self,
        mock_fetch: mock.MagicMock,
    ) -> None:
        """Test getting fields when cache is empty (fetch from API)."""
        test_fields = ["field1", "field2", "field3"]
        mock_fetch.return_value = test_fields
        
        fields = field_validator.get_valid_fields()
        
        # Should have fetched from API
        mock_fetch.assert_called_once()
        self.assertEqual(fields, sorted(test_fields))

    @mock.patch.object(field_validator, "_fetch_fields_from_api")
    def test_get_valid_fields_fallback_to_known(
        self,
        mock_fetch: mock.MagicMock,
    ) -> None:
        """Test fallback to known fields when API fails."""
        mock_fetch.return_value = None  # Simulate API failure
        
        fields = field_validator.get_valid_fields()
        
        # Should fall back to known fields
        self.assertEqual(fields, field_validator.KNOWN_FIELDS)

    def test_get_valid_fields_from_cache(self) -> None:
        """Test getting fields from cache when available."""
        test_fields = ["field1", "field2", "field3"]
        
        # Pre-populate cache
        cache = {
            "timestamp": time.time(),
            "fields": sorted(test_fields),
        }
        self.test_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.test_cache.open("w") as f:
            json.dump(cache, f)
        
        # Should load from cache without fetching
        with mock.patch.object(
            field_validator,
            "_fetch_fields_from_api",
        ) as mock_fetch:
            fields = field_validator.get_valid_fields()
            mock_fetch.assert_not_called()
            self.assertEqual(fields, sorted(test_fields))


if __name__ == "__main__":
    unittest.main()
