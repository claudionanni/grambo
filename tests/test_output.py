"""
Unit tests for output formatting

Tests for the OutputFormatter class and various output formats.
"""

import unittest
import json
from datetime import datetime
from lib.output import OutputFormatter
from lib.entities import NodeEntity, StateTransferEntity, ViewEntity, EntityType, NodeState, StateTransferType


class TestOutputFormatter(unittest.TestCase):
    """Test OutputFormatter functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_entities = [
            NodeEntity(
                node_id="node-123",
                current_state=NodeState.SYNCED,
                previous_state=NodeState.JOINER,
                node_address="192.168.1.100:4567",
                timestamp=datetime(2024, 9, 15, 10, 30, 45),
                line_number=100,
                raw_line="Test node state line",
                confidence=0.9,
                pattern_name="node_state_change"
            ),
            StateTransferEntity(
                transfer_type=StateTransferType.SST,
                donor_node="donor-456",
                joiner_node="joiner-789",
                transfer_status="completed",
                timestamp=datetime(2024, 9, 15, 10, 32, 15),
                line_number=150,
                raw_line="Test SST completion line",
                confidence=0.95,
                pattern_name="sst_completion"
            ),
            ViewEntity(
                view_id="view-abc",
                cluster_state="PRIMARY",
                members=["node1", "node2", "node3"],
                timestamp=datetime(2024, 9, 15, 10, 25, 30),
                line_number=50,
                raw_line="Test view change line",
                confidence=0.85,
                pattern_name="view_change"
            )
        ]
        
    def test_json_formatting(self):
        """Test JSON output formatting"""
        formatter = OutputFormatter(format_type="json")
        
        output = formatter.format(self.test_entities)
        data = json.loads(output)
        
        # Check structure
        self.assertIn('metadata', data)
        self.assertIn('entities', data)
        
        # Check metadata
        self.assertEqual(data['metadata']['generator'], 'grap')
        self.assertEqual(data['metadata']['total_entities'], 3)
        
        # Check entities
        self.assertEqual(len(data['entities']), 3)
        
        # Check first entity (NodeEntity)
        node_entity = data['entities'][0]
        self.assertEqual(node_entity['entity_type'], 'NODE')
        self.assertEqual(node_entity['node_id'], 'node-123')
        self.assertEqual(node_entity['current_state'], 'SYNCED')
        
    def test_text_formatting(self):
        """Test text output formatting"""
        formatter = OutputFormatter(format_type="text")
        
        output = formatter.format(self.test_entities)
        
        # Check that output contains expected sections
        self.assertIn('GRAP Entity Extraction Results', output)
        self.assertIn('Total entities: 3', output)
        self.assertIn('NODE (1 entities)', output)
        self.assertIn('STATE_TRANSFER (1 entities)', output)
        self.assertIn('VIEW (1 entities)', output)
        
    def test_compact_json_formatting(self):
        """Test compact JSON formatting"""
        formatter = OutputFormatter(format_type="json", compact=True)
        
        output = formatter.format(self.test_entities)
        
        # Compact JSON should not have indentation
        self.assertNotIn('\n  ', output)
        
        # But should still be valid JSON
        data = json.loads(output)
        self.assertIn('metadata', data)
        self.assertIn('entities', data)
        
    def test_statistics_inclusion(self):
        """Test inclusion of statistics in output"""
        stats = {
            'total_lines': 1000,
            'matched_lines': 150,
            'extracted_entities': 3,
            'match_rate': 0.15
        }
        
        formatter = OutputFormatter(format_type="json", show_stats=True)
        output = formatter.format(self.test_entities, stats)
        
        data = json.loads(output)
        self.assertIn('statistics', data)
        self.assertEqual(data['statistics']['total_lines'], 1000)
        self.assertEqual(data['statistics']['match_rate'], 0.15)
        
    def test_compatible_formatting(self):
        """Test backward-compatible formatting"""
        formatter = OutputFormatter(format_type="json")
        
        compatible_data = formatter.format_compatible(self.test_entities)
        
        # Check structure compatible with existing grambo tools
        self.assertIn('metadata', compatible_data)
        self.assertIn('detailed_events', compatible_data)
        self.assertIn('cluster_events', compatible_data)
        self.assertIn('summary', compatible_data)
        
        # Check detailed_events
        self.assertEqual(len(compatible_data['detailed_events']), 3)
        
        # Check cluster_events (should include significant events)
        self.assertGreater(len(compatible_data['cluster_events']), 0)
        
        # Check summary
        summary = compatible_data['summary']
        self.assertIn('node_count', summary)
        self.assertIn('state_transfers', summary)
        self.assertIn('view_changes', summary)
        
    def test_entity_grouping(self):
        """Test entity grouping by type"""
        formatter = OutputFormatter(format_type="text")
        
        grouped = formatter._group_entities_by_type(self.test_entities)
        
        self.assertIn('NODE', grouped)
        self.assertIn('STATE_TRANSFER', grouped)
        self.assertIn('VIEW', grouped)
        
        self.assertEqual(len(grouped['NODE']), 1)
        self.assertEqual(len(grouped['STATE_TRANSFER']), 1)
        self.assertEqual(len(grouped['VIEW']), 1)
        
    def test_event_significance_calculation(self):
        """Test event significance calculation"""
        formatter = OutputFormatter(format_type="json")
        
        # Test different entity types
        node_sig = formatter._calculate_event_significance(self.test_entities[0])
        sst_sig = formatter._calculate_event_significance(self.test_entities[1])
        view_sig = formatter._calculate_event_significance(self.test_entities[2])
        
        # View events should be most significant
        self.assertGreater(view_sig, sst_sig)
        self.assertGreater(sst_sig, node_sig)
        
        # All should be between 0 and 1
        for sig in [node_sig, sst_sig, view_sig]:
            self.assertGreaterEqual(sig, 0.0)
            self.assertLessEqual(sig, 1.0)


if __name__ == '__main__':
    unittest.main()