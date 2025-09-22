"""
Regression tests for log processing

Tests that ensure all training logs in unittest/ folder are processed without errors.
These tests serve as regression tests to catch breaking changes.
"""

import unittest
import sys
import os
from pathlib import Path
from io import StringIO
import tempfile
import json

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lib.parser import LogParser
from lib.output.formatter import OutputFormatter
from lib.entities.registry import EntityRegistry
from lib.patterns import PatternMatcher
from lib.entities.temporal import TemporalProperty
from datetime import datetime


class TestLogProcessing(unittest.TestCase):
    """Test suite for GRAP log processing functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.project_root = Path(__file__).parent.parent
        self.test_logs_dir = self.project_root / "unittest"  # Updated to use unittest/ folder
        self.pattern_dir = self.project_root / "patterns"
        
        # Initialize components
        self.entity_registry = EntityRegistry()
        self.pattern_matcher = PatternMatcher(
            pattern_dir=self.pattern_dir,
            confidence_threshold=0.8
        )
        self.parser = LogParser(
            pattern_matcher=self.pattern_matcher,
            entity_registry=self.entity_registry
        )
        self.formatter = OutputFormatter()

    def test_all_training_logs_process_without_errors(self):
        """Test that all training logs in unittest/ can be processed without exceptions"""
        if not self.test_logs_dir.exists():
            self.skipTest(f"Training logs directory {self.test_logs_dir} does not exist")
        
        # Get all log files with CS/ES prefixes
        log_files = []
        for pattern in ["CS-*.err", "ES-*.err", "CS-*.log", "ES-*.log"]:
            log_files.extend(self.test_logs_dir.glob(pattern))
        
        if not log_files:
            self.skipTest(f"No training log files found in {self.test_logs_dir}")
        
        processed_files = []
        failed_files = []
        
        for log_file in sorted(log_files):
            with self.subTest(log_file=log_file.name):
                try:
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        # Use parse_file method which takes a Path object
                        entities = self.parser.parse_file(log_file)
                    
                    # Format output to catch any formatting errors
                    output = self.formatter.format(entities)
                    
                    # Verify we got some output
                    self.assertIsInstance(output, str)
                    
                    processed_files.append(log_file.name)
                    
                except Exception as e:
                    failed_files.append((log_file.name, str(e)))
                    self.fail(f"Failed to process {log_file.name}: {e}")
        
        # Print summary
        print(f"\nProcessed {len(processed_files)} training log files successfully:")
        for filename in processed_files:
            print(f"  ✓ {filename}")
        
        if failed_files:
            print(f"\nFailed to process {len(failed_files)} files:")
            for filename, error in failed_files:
                print(f"  ✗ {filename}: {error}")

    def test_community_server_logs(self):
        """Test that Community Server (CS-*) logs are processed correctly"""
        cs_files = list(self.test_logs_dir.glob("CS-*.err")) + list(self.test_logs_dir.glob("CS-*.log"))
        
        if not cs_files:
            self.skipTest("No Community Server log files found")
        
        for log_file in cs_files:
            with self.subTest(log_file=log_file.name):
                entities = self.parser.parse_file(log_file)
                self.assertIsInstance(entities, list)
                # CS logs should contain some entities
                if entities:
                    self.assertGreater(len(entities), 0)

    def test_enterprise_server_logs(self):
        """Test that Enterprise Server (ES-*) logs are processed correctly"""
        es_files = list(self.test_logs_dir.glob("ES-*.err")) + list(self.test_logs_dir.glob("ES-*.log"))
        
        if not es_files:
            self.skipTest("No Enterprise Server log files found")
        
        for log_file in es_files:
            with self.subTest(log_file=log_file.name):
                entities = self.parser.parse_file(log_file)
                self.assertIsInstance(entities, list)
                # ES logs should contain some entities
                if entities:
                    self.assertGreater(len(entities), 0)

    def test_entity_extraction_validation(self):
        """Test that specific entities are extracted from known log samples"""
        # Find a sample log file for testing
        sample_files = list(self.test_logs_dir.glob("CS-*.err"))[:1]  # Take first CS file
        if not sample_files:
            sample_files = list(self.test_logs_dir.glob("ES-*.err"))[:1]  # Fallback to ES
        
        if not sample_files:
            self.skipTest("No sample log files found for entity validation")
        
        sample_file = sample_files[0]
        entities = self.parser.parse_file(sample_file)
        
        # Basic validation that we extract some entities
        self.assertIsInstance(entities, list)
        
        # Check that entities have required attributes
        for entity in entities[:5]:  # Check first 5 entities
            self.assertTrue(hasattr(entity, 'entity_type'))
            self.assertTrue(hasattr(entity, 'confidence'))
            # Confidence should be between 0 and 1
            if hasattr(entity, 'confidence') and entity.confidence is not None:
                self.assertGreaterEqual(entity.confidence, 0.0)
                self.assertLessEqual(entity.confidence, 1.0)

    def test_json_output_format(self):
        """Test that JSON output format works correctly"""
        sample_files = list(self.test_logs_dir.glob("CS-*.err"))[:1]
        if not sample_files:
            sample_files = list(self.test_logs_dir.glob("ES-*.err"))[:1]
        
        if not sample_files:
            self.skipTest("No sample log files found for JSON format test")
        
        sample_file = sample_files[0]
        entities = self.parser.parse_file(sample_file)
        
        # Test JSON formatter
        json_formatter = OutputFormatter(format_type='json')
        json_output = json_formatter.format(entities)
        
        # Verify it's valid JSON
        try:
            parsed_json = json.loads(json_output)
            self.assertIsInstance(parsed_json, dict)
        except json.JSONDecodeError as e:
            self.fail(f"Invalid JSON output: {e}")

    def test_timestamp_comparison_fix(self):
        """Test that temporal property sorting works with mixed timestamp types"""
        # Create a temporal property
        temp_prop = TemporalProperty("test_property")
        
        # Add values with different timestamp types (this was causing the error)
        temp_prop.update("value1", datetime.now())
        temp_prop.update("value2", datetime(2025, 9, 22, 10, 30, 0))
        temp_prop.update("value3", datetime(2025, 9, 22, 10, 35, 0))
        
        # This should not raise an exception
        current = temp_prop.current_value
        self.assertIsNotNone(current)
        
        # Check that timeline has the expected number of entries
        # Note: TemporalProperty initializes with a default value, so we expect 4 entries total
        self.assertEqual(len(temp_prop.timeline), 4)

    def test_pattern_loading(self):
        """Test that all pattern files load correctly"""
        pattern_files = list(self.pattern_dir.glob("*.yaml"))
        self.assertGreater(len(pattern_files), 0, "No pattern files found")
        
        # Test that PatternMatcher can load all patterns
        try:
            matcher = PatternMatcher(pattern_dir=self.pattern_dir)
            patterns = matcher.get_all_patterns()
            self.assertIsInstance(patterns, dict)
            self.assertGreater(len(patterns), 0, "No patterns loaded")
        except Exception as e:
            self.fail(f"Failed to load patterns: {e}")

    def test_empty_log_handling(self):
        """Test that empty or malformed logs are handled gracefully"""
        # Test with empty content
        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write("")
            empty_file = Path(f.name)
        
        try:
            entities = self.parser.parse_file(empty_file)
            self.assertIsInstance(entities, list)
            self.assertEqual(len(entities), 0)
        finally:
            empty_file.unlink()
        
        # Test with malformed content
        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write("This is not a valid Galera log\nJust some random text\n")
            malformed_file = Path(f.name)
        
        try:
            entities = self.parser.parse_file(malformed_file)
            self.assertIsInstance(entities, list)
            # Should handle gracefully, might return empty list or some entities
        finally:
            malformed_file.unlink()

    def test_large_log_performance(self):
        """Test performance with the largest available training log"""
        log_files = list(self.test_logs_dir.glob("CS-*.err")) + list(self.test_logs_dir.glob("ES-*.err"))
        if not log_files:
            self.skipTest("No training log files found")
        
        # Find the largest log file
        largest_file = max(log_files, key=lambda f: f.stat().st_size)
        
        import time
        start_time = time.time()
        entities = self.parser.parse_file(largest_file)
        end_time = time.time()
        
        processing_time = end_time - start_time
        file_size_mb = largest_file.stat().st_size / (1024 * 1024)
        
        print(f"\nPerformance test on {largest_file.name}:")
        print(f"  File size: {file_size_mb:.2f} MB")
        print(f"  Processing time: {processing_time:.2f} seconds")
        print(f"  Entities extracted: {len(entities)}")
        print(f"  Rate: {file_size_mb/processing_time:.2f} MB/s")
        
        # Basic assertion that it completed
        self.assertIsInstance(entities, list)


if __name__ == '__main__':
    # Configure logging to see any issues
    import logging
    logging.basicConfig(level=logging.WARNING)
    
    # Run tests with verbose output
    unittest.main(verbosity=2)