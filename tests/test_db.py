"""Unit tests for local database functionality."""

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from maudecli import db


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
        from data.build import classify_file
        
        self.assertEqual(classify_file('device2000.zip'), 'device')
        self.assertEqual(classify_file('DEVICE2000.ZIP'), 'device')
        self.assertEqual(classify_file('foitext1996.zip'), 'foitext')
        self.assertEqual(classify_file('foidev1998.zip'), 'foidev')
        self.assertEqual(classify_file('readme.txt'), None)
        self.assertEqual(classify_file('unknown.csv'), None)
    
    def test_compute_file_hash(self):
        """Test file hash computation."""
        from data.build import compute_file_hash
        
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
        from data.build import compute_row_hash
        
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


if __name__ == '__main__':
    unittest.main()
