"""
Unit tests for pattern matching system

Tests for the PatternMatcher class and pattern loading functionality.
"""

import unittest
import tempfile
import os
from pathlib import Path
from lib.patterns import PatternMatcher
from lib.entities import EntityType


class TestPatternMatcher(unittest.TestCase):
    """Test PatternMatcher functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create temporary directory for test patterns
        self.temp_dir = tempfile.mkdtemp()
        self.pattern_dir = Path(self.temp_dir)
        
        # Create test pattern file
        test_pattern_content = """
version: "10.6"
description: "Test patterns"

patterns:
  NODE:
    - name: "simple_node_state"
      description: "Simple node state pattern"
      confidence: 0.9
      regex: 'Node (?P<node_id>\\w+) state: (?P<current_state>\\w+)'
      required_fields: ["node_id", "current_state"]
      test_cases:
        - input: "Node abc123 state: SYNCED"
          expected:
            node_id: "abc123"
            current_state: "SYNCED"
            
  STATE_TRANSFER:
    - name: "sst_request"
      description: "SST request pattern"
      confidence: 0.8
      regex: 'SST (?P<transfer_method>\\w+) (?P<transfer_status>\\w+)'
      field_mappings:
        transfer_method: "transfer_method"
        transfer_status: "transfer_status"
      required_fields: ["transfer_status"]
"""
        
        pattern_file = self.pattern_dir / "test_patterns.yaml"
        with open(pattern_file, 'w') as f:
            f.write(test_pattern_content)
            
    def tearDown(self):
        """Clean up test fixtures"""
        # Remove temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)
        
    def test_pattern_matcher_initialization(self):
        """Test PatternMatcher initialization"""
        matcher = PatternMatcher(
            pattern_dir=self.pattern_dir,
            confidence_threshold=0.7
        )
        
        self.assertEqual(matcher.pattern_dir, self.pattern_dir)
        self.assertEqual(matcher.confidence_threshold, 0.7)
        
        # Check that patterns were loaded
        patterns = matcher.get_all_patterns()
        self.assertIn(EntityType.NODE, patterns)
        self.assertIn(EntityType.STATE_TRANSFER, patterns)
        
    def test_pattern_loading(self):
        """Test pattern loading from YAML files"""
        matcher = PatternMatcher(pattern_dir=self.pattern_dir)
        
        node_patterns = matcher.get_patterns(EntityType.NODE)
        self.assertEqual(len(node_patterns), 1)
        self.assertEqual(node_patterns[0].name, "simple_node_state")
        
        sst_patterns = matcher.get_patterns(EntityType.STATE_TRANSFER)
        self.assertEqual(len(sst_patterns), 1)
        self.assertEqual(sst_patterns[0].name, "sst_request")
        
    def test_line_matching(self):
        """Test matching lines against patterns"""
        matcher = PatternMatcher(
            pattern_dir=self.pattern_dir,
            confidence_threshold=0.5
        )
        
        # Test node state line
        line = "Node abc123 state: SYNCED"
        entities = matcher.match_line(line)
        
        self.assertEqual(len(entities), 1)
        entity = entities[0]
        self.assertEqual(entity.entity_type, EntityType.NODE)
        
        # Test SST line
        line = "SST mariabackup started"
        entities = matcher.match_line(line)
        
        self.assertEqual(len(entities), 1)
        entity = entities[0]
        self.assertEqual(entity.entity_type, EntityType.STATE_TRANSFER)
        
    def test_confidence_threshold_filtering(self):
        """Test confidence threshold filtering"""
        # High threshold - should filter out low confidence patterns
        matcher = PatternMatcher(
            pattern_dir=self.pattern_dir,
            confidence_threshold=0.95
        )
        
        line = "SST mariabackup started"  # confidence 0.8 < 0.95
        entities = matcher.match_line(line)
        
        self.assertEqual(len(entities), 0)  # Should be filtered out
        
    def test_entity_type_filtering(self):
        """Test filtering by entity types"""
        matcher = PatternMatcher(pattern_dir=self.pattern_dir)
        
        line = "Node abc123 state: SYNCED"
        
        # Match only NODE entities
        entities = matcher.match_line(line, [EntityType.NODE])
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].entity_type, EntityType.NODE)
        
        # Match only STATE_TRANSFER entities (should not match)
        entities = matcher.match_line(line, [EntityType.STATE_TRANSFER])
        self.assertEqual(len(entities), 0)
        
    def test_pattern_validation(self):
        """Test pattern validation functionality"""
        matcher = PatternMatcher(pattern_dir=self.pattern_dir)
        
        results = matcher.validate_patterns()
        
        self.assertIn('total_patterns', results)
        self.assertIn('passed_patterns', results)
        self.assertIn('failed_patterns', results)
        self.assertIn('details', results)
        
        # Check that our test patterns pass validation
        self.assertGreater(results['passed_patterns'], 0)


if __name__ == '__main__':
    unittest.main()