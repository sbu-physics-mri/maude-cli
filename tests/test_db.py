"""Unit tests for local database functionality."""

import asyncio
import gc
import http.server
import shutil
import sqlite3
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from maudecli import db
from maudecli.db import classify_file, compute_file_hash, compute_row_hash


class TestDatabaseQueries(unittest.TestCase):
    """Test suite for database query functionality."""
    
    def setUp(self):
        """Set up test database."""
        # Create a temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.db')
        self.temp_db_path = Path(self.temp_db.name)
        self.temp_db.close()
        
        # Patch DB_PATH to use temp database
        self.original_db_path = db.DB_PATH
        db.DB_PATH = self.temp_db_path
        
        # Create test database with sample data
        conn = sqlite3.connect(self.temp_db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE foitext (
                row_hash TEXT PRIMARY KEY,
                mdr_report_key TEXT,
                foi_text TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE device (
                row_hash TEXT PRIMARY KEY,
                brand_name TEXT,
                generic_name TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE foidev (
                row_hash TEXT PRIMARY KEY,
                brand_name TEXT,
                generic_name TEXT
            )
        """)
        
        # Insert test data
        cursor.execute("""
            INSERT INTO foitext VALUES 
            ('hash1', 'R001', 'MRI scan revealed artifact'),
            ('hash2', 'R002', 'Patient received pacemaker'),
            ('hash3', 'R003', 'MRI compatible pacemaker installed'),
            ('hash4', 'R004', 'Device malfunction during procedure')
        """)
        
        cursor.execute("""
            INSERT INTO device VALUES
            ('hash5', 'Brand A MRI Scanner', 'MRI Device'),
            ('hash6', 'Brand B Pacemaker', 'Cardiac Pacemaker')
        """)
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up test database."""
        db.DB_PATH = self.original_db_path

        # Force garbage collection
        gc.collect()

        self.temp_db_path.unlink(missing_ok=True)

    def test_database_exists(self):
        """Test database existence check."""
        self.assertTrue(db.database_exists())
        
        # Test with non-existent database
        db.DB_PATH = Path("/nonexistent/path/db.sqlite3")
        self.assertFalse(db.database_exists())
        
        # Restore for other tests
        db.DB_PATH = self.temp_db_path
    
    def test_get_table_stats(self):
        """Test getting table statistics."""
        stats = db.get_table_stats()
        self.assertEqual(stats['foitext'], 4)
        self.assertEqual(stats['device'], 2)
        self.assertEqual(stats['foidev'], 0)
    
    def test_simple_query(self):
        """Test simple query with single search term."""
        results = db.query_local_database([['MRI']], search_field='foi_text')
        self.assertEqual(len(results), 2)  # Should find 2 MRI-related records
        
        # Check that results contain expected text
        texts = [r['foi_text'] for r in results]
        self.assertTrue(any('artifact' in t for t in texts))
        self.assertTrue(any('compatible' in t for t in texts))
    
    def test_multi_term_query(self):
        """Test query with multiple search terms (OR within group)."""
        results = db.query_local_database(
            [['MRI', 'pacemaker']], 
            search_field='foi_text'
        )
        # Should find all records with MRI OR pacemaker
        self.assertEqual(len(results), 3)
    
    def test_multi_group_query(self):
        """Test query with multiple groups (AND between groups)."""
        results = db.query_local_database(
            [['MRI'], ['pacemaker']], 
            search_field='foi_text'
        )
        # Should find only records with MRI AND pacemaker
        self.assertEqual(len(results), 1)
        self.assertIn('compatible', results[0]['foi_text'])
    
    def test_exclusion_filtering(self):
        """Test query with exclusion terms."""
        results = db.query_local_database(
            [['MRI']],
            exclude_terms=[['artifact']],
            search_field='foi_text'
        )
        # Should find MRI records but exclude those with 'artifact'
        self.assertEqual(len(results), 1)
        self.assertIn('compatible', results[0]['foi_text'])
    
    def test_multi_exclusion_filtering(self):
        """Test query with multiple exclusion groups."""
        results = db.query_local_database(
            [['device', 'MRI', 'pacemaker']],
            exclude_terms=[['artifact'], ['malfunction']],
            search_field='foi_text'
        )
        # Should exclude records with artifact AND malfunction
        # Only "Device malfunction" has malfunction, only "MRI scan" has artifact
        # So both individual exclusions work, but not the AND
        self.assertGreater(len(results), 0)
    
    def test_limit(self):
        """Test query with result limit."""
        results = db.query_local_database(
            [['MRI', 'pacemaker', 'device']], 
            search_field='foi_text',
            limit=2
        )
        self.assertLessEqual(len(results), 2)
    
    def test_case_insensitive_search(self):
        """Test that search is case-insensitive."""
        results_lower = db.query_local_database([['mri']], search_field='foi_text')
        results_upper = db.query_local_database([['MRI']], search_field='foi_text')
        self.assertEqual(len(results_lower), len(results_upper))
    
    def test_device_table_query(self):
        """Test querying device table."""
        results = db.query_local_database(
            [['MRI']], 
            search_field='brand_name'
        )
        self.assertEqual(len(results), 1)
        self.assertIn('Scanner', results[0]['brand_name'])
    
    def test_nonexistent_field(self):
        """Test query with non-existent field."""
        # Should not raise an error, just return empty results
        results = db.query_local_database(
            [['test']], 
            search_field='nonexistent_field'
        )
        self.assertEqual(len(results), 0)
    
    def test_empty_search_terms(self):
        """Test query with empty search terms."""
        results = db.query_local_database([], search_field='foi_text', limit=10)
        # Should return all records (up to limit)
        self.assertGreater(len(results), 0)


class TestBuildScriptFunctions(unittest.TestCase):
    """Test suite for build script helper functions."""
    
    def test_classify_file(self):
        """Test file classification."""
        self.assertEqual(classify_file('device2000.zip'), 'device')
        self.assertEqual(classify_file('DEVICE2000.ZIP'), 'device')
        self.assertEqual(classify_file('foitext1996.zip'), 'foitext')
        self.assertEqual(classify_file('foidev1998.zip'), 'foidev')
        self.assertEqual(classify_file('readme.txt'), None)
        self.assertEqual(classify_file('unknown.csv'), None)
    
    def test_compute_file_hash(self):
        """Test file hash computation."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test content\n")
            temp_path = Path(f.name)
        
        try:
            hash1 = compute_file_hash(temp_path)
            self.assertEqual(len(hash1), 64)  # SHA256 produces 64 hex chars
            
            # Same file should produce same hash
            hash2 = compute_file_hash(temp_path)
            self.assertEqual(hash1, hash2)
        finally:
            temp_path.unlink()
    
    def test_compute_row_hash(self):
        """Test row hash computation."""
        import pandas as pd

        # Create test series
        row1 = pd.Series({'a': 'value1', 'b': 'value2', 'c': None})
        row2 = pd.Series({'a': 'value1', 'b': 'value2', 'c': None})
        row3 = pd.Series({'a': 'value1', 'b': 'different', 'c': None})
        
        hash1 = compute_row_hash(row1)
        hash2 = compute_row_hash(row2)
        hash3 = compute_row_hash(row3)
        
        # Same content should produce same hash
        self.assertEqual(hash1, hash2)
        # Different content should produce different hash
        self.assertNotEqual(hash1, hash3)


class TestDownloadFileFromUrl(unittest.TestCase):
    """Test suite for download_file_from_url function."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary cache directory
        self.temp_cache = tempfile.mkdtemp()
        self.original_cache_dir = db.CACHE_DIR
        db.CACHE_DIR = Path(self.temp_cache)

    def tearDown(self):
        """Clean up test environment."""
        db.CACHE_DIR = self.original_cache_dir
        shutil.rmtree(self.temp_cache, ignore_errors=True)

    def test_download_creates_cache_directory(self):
        """Test that download creates cache directory if it doesn't exist."""
        # Remove cache directory
        shutil.rmtree(self.temp_cache)
        self.assertFalse(Path(self.temp_cache).exists())

        # Mock the download to avoid actual network call
        with patch('urllib.request.urlretrieve') as mock_urlretrieve:
            mock_urlretrieve.return_value = None
            
            # Run download
            result = asyncio.run(db.download_file_from_url(
                "https://example.com/test.zip"
            ))

            # Verify cache directory was created
            self.assertTrue(db.CACHE_DIR.exists())
            self.assertTrue(result.parent.exists())

    def test_download_returns_correct_path(self):
        """Test that download returns the correct file path."""
        with patch('urllib.request.urlretrieve') as mock_urlretrieve:
            mock_urlretrieve.return_value = None
            
            result = asyncio.run(db.download_file_from_url(
                "https://example.com/testfile.zip"
            ))

            expected_path = db.CACHE_DIR / "testfile.zip"
            self.assertEqual(result, expected_path)

    def test_download_extracts_filename_from_url(self):
        """Test that filename is correctly extracted from URL."""
        with patch('urllib.request.urlretrieve') as mock_urlretrieve:
            mock_urlretrieve.return_value = None
            
            # Test with URL containing path
            result = asyncio.run(db.download_file_from_url(
                "https://example.com/path/to/datafile.zip"
            ))

            # Should extract just the filename from the last part of the path
            self.assertEqual(result.name, "datafile.zip")

    def test_download_calls_urlretrieve_correctly(self):
        """Test that urllib.request.urlretrieve is called with correct arguments."""
        with patch('urllib.request.urlretrieve') as mock_urlretrieve:
            mock_urlretrieve.return_value = None
            
            url = "https://example.com/test.zip"
            result = asyncio.run(db.download_file_from_url(url))

            # Verify urlretrieve was called
            mock_urlretrieve.assert_called_once()
            # Check that it was called with the URL and destination
            args = mock_urlretrieve.call_args[0]
            self.assertEqual(args[0], url)

    def test_download_handles_network_error(self):
        """Test that download handles network errors appropriately."""
        with patch('urllib.request.urlretrieve') as mock_urlretrieve:
            # Simulate a network error
            mock_urlretrieve.side_effect = Exception("Network error")
            
            # Should raise the exception
            with self.assertRaises(Exception) as context:
                asyncio.run(db.download_file_from_url(
                    "https://example.com/test.zip"
                ))
            
            self.assertIn("Network error", str(context.exception))


class TestBuildDatabase(unittest.TestCase):
    """Test suite for build_database function."""

    def setUp(self):
        """Set up test environment."""
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.temp_cache = tempfile.mkdtemp()
        
        # Patch DB_PATH and CACHE_DIR
        self.original_db_path = db.DB_PATH
        self.original_cache_dir = db.CACHE_DIR
        self.original_datafile_urls = db.DATAFILE_URLS
        
        db.DB_PATH = Path(self.temp_dir) / "test.db"
        db.CACHE_DIR = Path(self.temp_cache)

    def tearDown(self):
        """Clean up test environment."""
        db.DB_PATH = self.original_db_path
        db.CACHE_DIR = self.original_cache_dir
        db.DATAFILE_URLS = self.original_datafile_urls
        
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.temp_cache, ignore_errors=True)

    def _create_test_zip(self, filename: str, record_type: str) -> Path:
        """Create a test zip file with sample CSV data.
        
        Args:
            filename: Name of the zip file
            record_type: Type of record (device, foitext, foidev)
            
        Returns:
            Path to the created zip file
        """
        zip_path = db.CACHE_DIR / filename
        csv_filename = filename.replace('.zip', '.txt')
        
        # Create sample CSV content based on record type
        if record_type == 'device':
            csv_content = "BRAND_NAME|GENERIC_NAME\nTest Device|Generic Device\n"
        elif record_type == 'foitext':
            csv_content = "MDR_REPORT_KEY|FOI_TEXT\nR001|Sample text\n"
        elif record_type == 'foidev':
            csv_content = "MDR_REPORT_KEY|BRAND_NAME\nR001|Test Brand\n"
        else:
            csv_content = "COL1|COL2\nval1|val2\n"
        
        # Create zip file
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr(csv_filename, csv_content)
        
        return zip_path

    def test_build_database_creates_database_file(self):
        """Test that build_database creates a database file."""
        # Set minimal URLs for testing
        db.DATAFILE_URLS = ()
        
        asyncio.run(db.build_database())
        
        # Verify database file was created
        self.assertTrue(db.DB_PATH.exists())

    def test_build_database_creates_parent_directory(self):
        """Test that build_database creates parent directories."""
        # Set DB_PATH to a nested location
        nested_path = Path(self.temp_dir) / "nested" / "dir" / "test.db"
        db.DB_PATH = nested_path
        
        self.assertFalse(nested_path.parent.exists())
        
        # Set minimal URLs for testing
        db.DATAFILE_URLS = ()
        
        asyncio.run(db.build_database())
        
        # Verify parent directory was created
        self.assertTrue(nested_path.parent.exists())

    def test_build_database_creates_tables(self):
        """Test that build_database creates required tables."""
        db.DATAFILE_URLS = ()
        
        asyncio.run(db.build_database())
        
        # Verify tables exist
        conn = sqlite3.connect(db.DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        self.assertIn('device', tables)
        self.assertIn('foitext', tables)
        self.assertIn('foidev', tables)
        self.assertIn('ingestion_log', tables)
        
        conn.close()

    def test_build_database_downloads_files(self):
        """Test that build_database downloads files from URLs."""
        test_urls = (
            "https://example.com/device2000.zip",
            "https://example.com/foitext2000.zip",
        )
        db.DATAFILE_URLS = test_urls
        
        with patch('maudecli.db.download_file_from_url') as mock_download:
            # Mock successful downloads
            async def mock_download_func(url):
                filename = url.split("/")[-1]
                return self._create_test_zip(filename, classify_file(filename))
            
            mock_download.side_effect = mock_download_func
            
            asyncio.run(db.build_database())
            
            # Verify download was called for each URL
            self.assertEqual(mock_download.call_count, len(test_urls))

    def test_build_database_handles_download_failures(self):
        """Test that build_database handles download failures gracefully."""
        test_urls = (
            "https://example.com/device2000.zip",
            "https://example.com/foitext2000.zip",
        )
        db.DATAFILE_URLS = test_urls
        
        with patch('maudecli.db.download_file_from_url') as mock_download:
            # First download succeeds, second fails
            async def mock_download_func(url):
                if "device" in url:
                    filename = url.split("/")[-1]
                    return self._create_test_zip(filename, "device")
                else:
                    raise Exception("Download failed")
            
            mock_download.side_effect = mock_download_func
            
            # Should not raise exception
            asyncio.run(db.build_database())
            
            # Database should still be created
            self.assertTrue(db.DB_PATH.exists())

    def test_build_database_processes_files(self):
        """Test that build_database processes downloaded files."""
        test_urls = ("https://example.com/device2000.zip",)
        db.DATAFILE_URLS = test_urls
        
        with patch('maudecli.db.download_file_from_url') as mock_download:
            async def mock_download_func(url):
                filename = url.split("/")[-1]
                return self._create_test_zip(filename, "device")
            
            mock_download.side_effect = mock_download_func
            
            asyncio.run(db.build_database())
            
            # Verify data was ingested
            conn = sqlite3.connect(db.DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM device")
            count = cursor.fetchone()[0]
            
            # Should have ingested at least one row
            self.assertGreater(count, 0)
            
            conn.close()

    def test_build_database_idempotency(self):
        """Test that build_database is idempotent (no duplicates on re-run)."""
        test_urls = ("https://example.com/device2000.zip",)
        db.DATAFILE_URLS = test_urls
        
        with patch('maudecli.db.download_file_from_url') as mock_download:
            async def mock_download_func(url):
                filename = url.split("/")[-1]
                return self._create_test_zip(filename, "device")
            
            mock_download.side_effect = mock_download_func
            
            # Run build twice
            asyncio.run(db.build_database())
            
            # Get count after first run
            conn = sqlite3.connect(db.DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM device")
            count_first = cursor.fetchone()[0]
            conn.close()
            
            # Run build again
            asyncio.run(db.build_database())
            
            # Get count after second run
            conn = sqlite3.connect(db.DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM device")
            count_second = cursor.fetchone()[0]
            conn.close()
            
            # Counts should be the same (no duplicates)
            self.assertEqual(count_first, count_second)

    def test_build_database_handles_malformed_files(self):
        """Test that build_database handles malformed CSV files gracefully."""
        test_urls = ("https://example.com/device2000.zip",)
        db.DATAFILE_URLS = test_urls
        
        with patch('maudecli.db.download_file_from_url') as mock_download:
            async def mock_download_func(url):
                # Create a malformed zip/CSV
                filename = url.split("/")[-1]
                zip_path = db.CACHE_DIR / filename
                
                with zipfile.ZipFile(zip_path, 'w') as zf:
                    # Write malformed CSV
                    zf.writestr('device2000.txt', 'malformed|||data\n')
                
                return zip_path
            
            mock_download.side_effect = mock_download_func
            
            # Should not raise exception
            asyncio.run(db.build_database())
            
            # Database should still exist
            self.assertTrue(db.DB_PATH.exists())

    def test_build_database_skips_unrecognized_files(self):
        """Test that build_database skips unrecognized file types."""
        test_urls = ("https://example.com/unknown_file.zip",)
        db.DATAFILE_URLS = test_urls
        
        with patch('maudecli.db.download_file_from_url') as mock_download:
            async def mock_download_func(url):
                filename = url.split("/")[-1]
                zip_path = db.CACHE_DIR / filename
                
                with zipfile.ZipFile(zip_path, 'w') as zf:
                    zf.writestr('unknown.txt', 'some data\n')
                
                return zip_path
            
            mock_download.side_effect = mock_download_func
            
            # Should complete without errors
            asyncio.run(db.build_database())
            
            # Database should exist
            self.assertTrue(db.DB_PATH.exists())

    def test_build_database_closes_connection(self):
        """Test that build_database properly closes database connection."""
        db.DATAFILE_URLS = ()
        
        asyncio.run(db.build_database())
        
        # Should be able to connect to database without conflicts
        conn = sqlite3.connect(db.DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM device")
        conn.close()
        
        # If connection wasn't closed properly, this would fail


if __name__ == '__main__':
    unittest.main()
