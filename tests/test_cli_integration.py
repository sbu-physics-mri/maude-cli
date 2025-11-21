"""Integration tests for field validation in CLI."""

# ruff: noqa: PT009 PT027

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from maudecli import field_validator, main


class TestCLIFieldValidation(unittest.TestCase):
    """Test CLI integration with field validation."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Use a temporary cache for testing
        self.original_cache_file = field_validator.CACHE_FILE
        self.test_cache_dir = tempfile.mkdtemp()
        self.test_cache = Path(self.test_cache_dir) / "test_maude_cli_integration.json"
        field_validator.CACHE_FILE = self.test_cache
        
        # Pre-populate cache with known fields to avoid API calls
        cache = {
            "timestamp": time.time(),
            "fields": field_validator.KNOWN_FIELDS,
        }
        self.test_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.test_cache.open("w") as f:
            json.dump(cache, f)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Restore original cache file
        field_validator.CACHE_FILE = self.original_cache_file
        
        # Clean up test cache
        import shutil
        if Path(self.test_cache_dir).exists():
            shutil.rmtree(self.test_cache_dir)

    def test_cli_with_invalid_field(self) -> None:
        """Test CLI exits with error for invalid field."""
        # Mock sys.argv and sys.exit
        with mock.patch.object(sys, "argv", ["maude-cli", "test", "-f", "invalid_field"]):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 1)

    def test_cli_with_typo_field(self) -> None:
        """Test CLI suggests correct field for typo."""
        # Capture logging output
        import logging
        
        # Create a handler to capture log records
        log_records = []
        
        class TestHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                log_records.append(record)
        
        handler = TestHandler()
        logger = logging.getLogger("maudecli")
        logger.addHandler(handler)
        
        try:
            with mock.patch.object(sys, "argv", ["maude-cli", "test", "-f", "mdr_text.txt"]):
                with self.assertRaises(SystemExit) as cm:
                    main()
                self.assertEqual(cm.exception.code, 1)
            
            # Check that a suggestion was logged
            error_logs = [r for r in log_records if r.levelno == logging.ERROR]
            self.assertTrue(any("Did you mean" in r.getMessage() for r in error_logs))
            self.assertTrue(any("mdr_text.text" in r.getMessage() for r in error_logs))
        finally:
            logger.removeHandler(handler)

    def test_cli_with_mixed_valid_invalid_fields(self) -> None:
        """Test CLI rejects when one of multiple fields is invalid."""
        with mock.patch.object(
            sys,
            "argv",
            ["maude-cli", "test", "-f", "mdr_text.text,invalid_field"],
        ):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 1)


class TestCacheRefreshOnValidation(unittest.TestCase):
    """Test that cache refreshes appropriately."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Use a temporary cache for testing
        self.original_cache_file = field_validator.CACHE_FILE
        self.test_cache_dir = tempfile.mkdtemp()
        self.test_cache = Path(self.test_cache_dir) / "test_maude_cache_refresh.json"
        field_validator.CACHE_FILE = self.test_cache

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Restore original cache file
        field_validator.CACHE_FILE = self.original_cache_file
        
        # Clean up test cache
        import shutil
        if Path(self.test_cache_dir).exists():
            shutil.rmtree(self.test_cache_dir)

    def test_expired_cache_triggers_refresh(self) -> None:
        """Test that expired cache triggers a refresh attempt."""
        # Create an expired cache
        expired_cache = {
            "timestamp": time.time() - (8 * 24 * 3600),  # 8 days old
            "fields": ["old_field"],
        }
        
        self.test_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.test_cache.open("w") as f:
            json.dump(expired_cache, f)
        
        # Mock API to return new fields
        new_fields = ["new_field_1", "new_field_2"]
        with mock.patch.object(
            field_validator,
            "_fetch_fields_from_api",
            return_value=new_fields,
        ):
            # Calling get_valid_fields should refresh cache
            fields = field_validator.get_valid_fields()
            
            # Should have new fields
            self.assertEqual(fields, sorted(new_fields))
            
            # Cache should be updated
            cache = field_validator._load_cache()
            self.assertIsNotNone(cache)
            self.assertEqual(cache["fields"], sorted(new_fields))

    def test_invalid_field_uses_existing_cache(self) -> None:
        """Test that invalid field validation uses existing cache."""
        # Create a valid cache
        cache_fields = field_validator.KNOWN_FIELDS[:10]
        cache = {
            "timestamp": time.time(),
            "fields": sorted(cache_fields),
        }
        
        self.test_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.test_cache.open("w") as f:
            json.dump(cache, f)
        
        # Validation should use cached fields
        is_valid, suggestion = field_validator.validate_field("invalid_test_field")
        
        # Should be invalid
        self.assertFalse(is_valid)
        
        # Should have used cache (no API call needed)
        # Cache should not be modified
        loaded_cache = field_validator._load_cache()
        self.assertIsNotNone(loaded_cache)
        self.assertEqual(loaded_cache["fields"], sorted(cache_fields))


if __name__ == "__main__":
    unittest.main()
