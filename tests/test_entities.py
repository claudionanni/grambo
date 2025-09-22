"""
Unit tests for entity classes

Tests for the base entity classes and core entity implementations.
"""

import unittest
from datetime import datetime
from lib.entities import (
    Entity, Event, Pattern, EntityRegistry, EntityType, ConfidenceLevel,
    NodeEntity, StateTransferEntity, ViewEntity, NodeState, StateTransferType,
    create_default_registry
)


class TestEntityBase(unittest.TestCase):
    """Test base Entity class functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.registry = create_default_registry()
        
    def test_node_entity_creation(self):
        """Test creating a NodeEntity"""
        node = NodeEntity(
            node_id="test-node-123",
            current_state=NodeState.SYNCED,
            node_address="192.168.1.100:4567"
        )
        
        self.assertEqual(node.entity_type, EntityType.NODE)
        self.assertEqual(node.node_id, "test-node-123")
        self.assertEqual(node.current_state, NodeState.SYNCED)
        self.assertEqual(node.node_address, "192.168.1.100:4567")
        
    def test_node_entity_validation(self):
        """Test NodeEntity validation"""
        # Valid node
        node = NodeEntity(node_id="test-node")
        self.assertTrue(node.validate())
        
        # Invalid node (no identifiers)
        with self.assertRaises(ValueError):
            NodeEntity()
            
    def test_node_state_transition(self):
        """Test node state transitions"""
        node = NodeEntity(
            node_id="test-node",
            current_state=NodeState.SYNCED
        )
        
        # Valid transition
        node.update_state(NodeState.DONOR)
        self.assertEqual(node.previous_state, NodeState.SYNCED)
        self.assertEqual(node.current_state, NodeState.DONOR)
        
    def test_state_transfer_entity_creation(self):
        """Test creating a StateTransferEntity"""
        sst = StateTransferEntity(
            transfer_type=StateTransferType.SST,
            donor_node="donor-123",
            joiner_node="joiner-456",
            transfer_status="completed"
        )
        
        self.assertEqual(sst.entity_type, EntityType.STATE_TRANSFER)
        self.assertEqual(sst.transfer_type, StateTransferType.SST)
        self.assertEqual(sst.donor_node, "donor-123")
        self.assertEqual(sst.joiner_node, "joiner-456")
        self.assertTrue(sst.is_successful())
        
    def test_view_entity_creation(self):
        """Test creating a ViewEntity"""
        view = ViewEntity(
            view_id="view-123",
            cluster_state="PRIMARY",
            members=["node1", "node2", "node3"]
        )
        
        self.assertEqual(view.entity_type, EntityType.VIEW)
        self.assertEqual(view.view_id, "view-123")
        self.assertEqual(view.cluster_state, "PRIMARY")
        self.assertEqual(view.get_member_count(), 3)
        self.assertTrue(view.is_primary_view())
        
    def test_entity_serialization(self):
        """Test entity to_dict and from_dict"""
        original_node = NodeEntity(
            node_id="test-node-123",
            current_state=NodeState.SYNCED,
            node_address="192.168.1.100:4567",
            timestamp=datetime(2024, 9, 15, 10, 30, 45)
        )
        
        # Serialize
        node_dict = original_node.to_dict()
        
        # Deserialize
        restored_node = NodeEntity.from_dict(node_dict)
        
        # Compare
        self.assertEqual(original_node.node_id, restored_node.node_id)
        self.assertEqual(original_node.current_state, restored_node.current_state)
        self.assertEqual(original_node.node_address, restored_node.node_address)
        
    def test_entity_registry(self):
        """Test EntityRegistry functionality"""
        registry = EntityRegistry()
        
        # Register entity class
        registry.register_entity_class(EntityType.NODE, NodeEntity)
        
        # Test entity creation
        node = registry.create_entity(
            EntityType.NODE,
            node_id="test-node",
            current_state=NodeState.SYNCED
        )
        
        self.assertIsInstance(node, NodeEntity)
        self.assertEqual(node.entity_type, EntityType.NODE)
        self.assertEqual(node.node_id, "test-node")


class TestPatternMatching(unittest.TestCase):
    """Test Pattern class functionality"""
    
    def test_pattern_creation(self):
        """Test creating a Pattern"""
        pattern = Pattern(
            name="test_pattern",
            entity_type=EntityType.NODE,
            regex=r"Node (?P<node_id>\w+) state: (?P<current_state>\w+)",
            field_mappings={"node_id": "node_id", "current_state": "current_state"},
            confidence=0.9
        )
        
        self.assertEqual(pattern.name, "test_pattern")
        self.assertEqual(pattern.entity_type, EntityType.NODE)
        self.assertEqual(pattern.confidence, 0.9)
        self.assertIsNotNone(pattern.compiled_regex)
        
    def test_pattern_matching(self):
        """Test pattern matching functionality"""
        pattern = Pattern(
            name="node_state_pattern",
            entity_type=EntityType.NODE,
            regex=r"Node (?P<node_id>\w+) state: (?P<current_state>\w+)",
            required_fields=["node_id", "current_state"]
        )
        
        # Test successful match
        line = "Node abc123 state: SYNCED"
        result = pattern.match(line)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["node_id"], "abc123")
        self.assertEqual(result["current_state"], "SYNCED")
        
        # Test failed match
        line = "This is not a node state line"
        result = pattern.match(line)
        self.assertIsNone(result)
        
    def test_pattern_field_mapping(self):
        """Test pattern field mapping"""
        pattern = Pattern(
            name="mapped_pattern",
            entity_type=EntityType.NODE,
            regex=r"Node (?P<uuid>\w+) is (?P<state>\w+)",
            field_mappings={"uuid": "node_id", "state": "current_state"},
            required_fields=["node_id", "current_state"]
        )
        
        line = "Node abc123 is SYNCED"
        result = pattern.match(line)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["node_id"], "abc123")  # Mapped from uuid
        self.assertEqual(result["current_state"], "SYNCED")  # Mapped from state
        
    def test_pattern_test_cases(self):
        """Test pattern test case functionality"""
        pattern = Pattern(
            name="test_pattern",
            entity_type=EntityType.NODE,
            regex=r"Node (?P<node_id>\w+) state: (?P<current_state>\w+)",
            test_cases=[
                {
                    "input": "Node abc123 state: SYNCED",
                    "expected": {"node_id": "abc123", "current_state": "SYNCED"}
                },
                {
                    "input": "Invalid line format",
                    "expected": {}
                }
            ]
        )
        
        results = pattern.test()
        
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0]["passed"])
        self.assertTrue(results[1]["passed"])


if __name__ == '__main__':
    unittest.main()