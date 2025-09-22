#!/usr/bin/env python3
"""
Tests for temporal properties and session management features.

This module provides comprehensive tests for the Phase 1.5 enhancements:
- TemporalProperty functionality
- Tempo    def test_start_event_classific    def test_progress_event_classif    def test_error_event_classification(self):
        """Test SST error event classification with real enterprise log patterns"""
        error_lines = [
            # Real error patterns from enterprise logs
            "2025-09-15 13:48:11 0 [Note] WSREP: SST sending failed: -32",
            "2025-09-19 18:33:48 0 [ERROR] WSREP: Process completed with error: wsrep_sst_mariabackup",
            "2025-09-19 18:33:48 0 [ERROR] WSREP: Failed to read from: wsrep_sst_mariabackup",
            "2025-09-19 18:33:48 0 [ERROR] WSREP: Command did not run: wsrep_sst_mariabackup",
        ]
        
        for line in error_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.ERROR, f"Failed for: {line}")f):
        """Test SST progress event classification with real enterprise log patterns"""
        progress_lines = [
            # Real progress patterns that might appear in enterprise logs
            "WSREP_SST: [INFO] Waiting for SST streaming to complete! (20250919 18:17:18.825)",
            "WSREP_SST: [INFO] Disabling all progress/rate-limiting (20250915 13:45:56.473)",
            "2025-09-15 10:32:00 12348 [Note] WSREP: Progress 45.5% transferred",
        ]
        
        for line in progress_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.PROGRESS, f"Failed for: {line}"):
        """Test SST start event classification with real enterprise log lines"""
        start_lines = [
            # MariaDB 10.6 patterns from enterprise logs
            "2025-09-15 13:45:56 0 [Note] WSREP: Running: 'wsrep_sst_mariabackup --role 'donor'",
            "WSREP_SST: [INFO] mariabackup SST started on donor (20250915 13:45:56.276)",
            "WSREP_SST: [INFO] Streaming with mbstream (20250915 13:45:56.485)",
            
            # MariaDB 11.4 patterns from enterprise logs  
            "2025-09-19 18:17:17 0 [Note] WSREP: Initiating SST/IST transfer on DONOR side",
        ]
        
        for line in start_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.START, f"Failed for: {line}")nsferEntity behavior
- SessionManager lifecycle management
- SST event classification
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.entities.temporal import TemporalProperty, TemporalPropertySet
from lib.entities.temporal_entities import TemporalStateTransferEntity
from lib.entities.session_manager import SessionManager
from lib.entities.sst_classifier import SSTEventClassifier, SSTEventType
from lib.entities.core import StateTransferType, StateTransferMethod
from lib.parser import LogParser


class TestTemporalProperty(unittest.TestCase):
    """Test TemporalProperty functionality"""
    
    def test_basic_functionality(self):
        """Test basic temporal property operations"""
        # Create property with initial value
        prop = TemporalProperty("initial", datetime(2025, 1, 1, 10, 0, 0))
        
        self.assertEqual(prop.current_value, "initial")
        self.assertEqual(prop.initial_value, "initial")
        self.assertEqual(prop.change_count(), 1)
        self.assertFalse(prop.has_changed())
        
        # Add a change
        prop.update("updated", datetime(2025, 1, 1, 10, 5, 0))
        
        self.assertEqual(prop.current_value, "updated")
        self.assertEqual(prop.initial_value, "initial")
        self.assertEqual(prop.change_count(), 2)
        self.assertTrue(prop.has_changed())
    
    def test_temporal_queries(self):
        """Test temporal query functionality"""
        prop = TemporalProperty("start", datetime(2025, 1, 1, 10, 0, 0))
        prop.update("middle", datetime(2025, 1, 1, 10, 5, 0))
        prop.update("end", datetime(2025, 1, 1, 10, 10, 0))
        
        # Test value_at queries
        self.assertEqual(prop.value_at(datetime(2025, 1, 1, 9, 59, 0)), None)
        self.assertEqual(prop.value_at(datetime(2025, 1, 1, 10, 2, 0)), "start")
        self.assertEqual(prop.value_at(datetime(2025, 1, 1, 10, 7, 0)), "middle")
        self.assertEqual(prop.value_at(datetime(2025, 1, 1, 10, 15, 0)), "end")
        
        # Test changes_between
        changes = prop.changes_between(
            datetime(2025, 1, 1, 10, 3, 0),
            datetime(2025, 1, 1, 10, 8, 0)
        )
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0][1], "middle")
    
    def test_timeline_ordering(self):
        """Test that timeline maintains chronological order"""
        prop = TemporalProperty()
        
        # Add values out of order
        prop.update("third", datetime(2025, 1, 1, 10, 10, 0))
        prop.update("first", datetime(2025, 1, 1, 10, 0, 0))
        prop.update("second", datetime(2025, 1, 1, 10, 5, 0))
        
        timeline = prop.get_timeline()
        self.assertEqual(len(timeline), 3)
        self.assertEqual(timeline[0][1], "first")
        self.assertEqual(timeline[1][1], "second")
        self.assertEqual(timeline[2][1], "third")


class TestTemporalPropertySet(unittest.TestCase):
    """Test TemporalPropertySet functionality"""
    
    def test_property_management(self):
        """Test property set management"""
        prop_set = TemporalPropertySet()
        
        # Add properties
        status_prop = prop_set.add_property("status", "started", datetime.now())
        bytes_prop = prop_set.add_property("bytes", 0, datetime.now())
        
        self.assertEqual(len(prop_set.get_property_names()), 2)
        self.assertTrue(prop_set.has_property("status"))
        self.assertTrue(prop_set.has_property("bytes"))
        
        # Update properties
        prop_set.update_property("status", "in_progress")
        prop_set.update_property("bytes", 1024)
        
        current_values = prop_set.get_current_values()
        self.assertEqual(current_values["status"], "in_progress")
        self.assertEqual(current_values["bytes"], 1024)


class TestTemporalStateTransferEntity(unittest.TestCase):
    """Test TemporalStateTransferEntity functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.start_time = datetime.now()
        self.entity = TemporalStateTransferEntity(
            transfer_id="test_sst_1",
            start_timestamp=self.start_time,
            transfer_method=StateTransferMethod.MARIABACKUP,
            donor_node="node1",
            joiner_node="node2",
            transfer_status="started"
        )
    
    def test_initialization(self):
        """Test entity initialization"""
        self.assertEqual(self.entity.entity_id, "test_sst_1")
        self.assertEqual(self.entity.start_timestamp, self.start_time)
        self.assertEqual(self.entity.transfer_method, StateTransferMethod.MARIABACKUP)
        self.assertEqual(self.entity.donor_node, "node1")
        self.assertEqual(self.entity.joiner_node, "node2")
        self.assertEqual(self.entity.current_status, "started")
        self.assertTrue(self.entity.is_active)
    
    def test_temporal_updates(self):
        """Test temporal property updates"""
        # Update progress
        self.entity.update_property("transferred_bytes", 1024)
        self.entity.update_property("progress_percentage", 25.0)
        
        self.assertEqual(self.entity.current_bytes, 1024)
        
        # Update status
        self.entity.update_property("transfer_status", "in_progress")
        self.assertEqual(self.entity.current_status, "in_progress")
        
        # Check timeline
        timeline = self.entity.get_timeline()
        self.assertIn("transferred_bytes", timeline)
        self.assertIn("transfer_status", timeline)
        
        # Should have at least 2 status changes (initial + update)
        self.assertGreaterEqual(len(timeline["transfer_status"]), 2)
    
    def test_completion(self):
        """Test transfer completion"""
        end_time = self.start_time + timedelta(minutes=5)
        
        self.entity.complete_transfer("completed", end_time)
        
        self.assertEqual(self.entity.current_status, "completed")
        self.assertEqual(self.entity.end_timestamp, end_time)
        self.assertFalse(self.entity.is_active)
        self.assertEqual(self.entity.duration, 300.0)  # 5 minutes
    
    def test_identity_key(self):
        """Test identity key generation"""
        identity = self.entity.get_identity_key()
        self.assertIn("node1", identity)
        self.assertIn("node2", identity)
        self.assertIn(self.start_time.strftime("%Y%m%d"), identity)
    
    def test_validation(self):
        """Test entity validation"""
        # Should validate successfully
        self.assertTrue(self.entity.validate())
        
        # Test with minimal entity
        minimal_entity = TemporalStateTransferEntity(
            transfer_id="minimal",
            start_timestamp=datetime.now(),
            transfer_status="started"
        )
        self.assertTrue(minimal_entity.validate())
    
    def test_from_dict(self):
        """Test entity creation from dictionary"""
        entity_dict = self.entity.to_dict()
        
        # Create new entity from dictionary
        restored_entity = TemporalStateTransferEntity.from_dict(entity_dict)
        
        self.assertEqual(restored_entity.entity_id, self.entity.entity_id)
        self.assertEqual(restored_entity.donor_node, self.entity.donor_node)
        self.assertEqual(restored_entity.joiner_node, self.entity.joiner_node)
        self.assertEqual(restored_entity.transfer_method, self.entity.transfer_method)


class TestSSTEventClassifier(unittest.TestCase):
    """Test SST event classification"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.classifier = SSTEventClassifier()
    
    def test_start_event_classification(self):
        """Test SST start event classification with real enterprise log lines"""
        start_lines = [
            # MariaDB 10.6 patterns from enterprise logs
            "2025-09-15 13:45:56 0 [Note] WSREP: Running: 'wsrep_sst_mariabackup --role 'donor'",
            "WSREP_SST: [INFO] mariabackup SST started on donor (20250915 13:45:56.276)",
            "WSREP_SST: [INFO] Streaming with mbstream (20250915 13:45:56.485)",
            
            # MariaDB 11.4 patterns from enterprise logs  
            "2025-09-19 18:17:17 0 [Note] WSREP: Initiating SST/IST transfer on DONOR side",
        ]
        
        for line in start_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.START, f"Failed for: {line}")
        
        for line in start_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.START, f"Failed for: {line}")
    
    def test_progress_event_classification(self):
        """Test SST progress event classification for MariaDB 10.6+"""
        progress_lines = [
            "2025-09-15 10:32:00 12348 [Note] WSREP: Progress 45.5% transferred",
            "2025-09-15 10:32:15 12349 [Note] mariabackup: Sending 1024 MB at rate 50.5 MB/s",
            "2025-09-15 10:32:30 12350 [Note] WSREP: Transferred 1024 of 2048 bytes",
        ]
        
        for line in progress_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.PROGRESS, f"Failed for: {line}")
    
    def test_end_event_classification(self):
        """Test SST end event classification"""
        end_lines = [
            "2025-09-15 10:35:20 12350 [Note] WSREP: SST completed with seqno 12345",
            "2025-09-15 10:35:21 12351 [Note] WSREP: SST finished successfully",
            "2025-09-15 10:35:22 12352 [Note] WSREP: State transfer complete",
        ]
        
        for line in end_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.END, f"Failed for: {line}")
    
    def test_error_event_classification(self):
        """Test SST error event classification"""
        error_lines = [
            "2025-09-15 10:33:30 12349 [ERROR] WSREP: SST failed 2: mariabackup process failed",
            "2025-09-15 10:33:31 12350 [ERROR] WSREP: SST timeout",
            "2025-09-15 10:33:32 12351 [ERROR] WSREP: SST abort due to connection error",
        ]
        
        for line in error_lines:
            event_type = self.classifier.classify_sst_event(line)
            self.assertEqual(event_type, SSTEventType.ERROR, f"Failed for: {line}")
    
    def test_detail_extraction(self):
        """Test detail extraction from SST events"""
        # Test start event details
        start_line = "2025-09-15 10:30:45 12345 [Note] WSREP: Requesting SST: mariabackup"
        details = self.classifier.extract_sst_details(start_line, SSTEventType.START)
        
        self.assertEqual(details['event_type'], 'SST_START')
        self.assertEqual(details['transfer_method'], 'mariabackup')
        self.assertEqual(details['transfer_status'], 'started')
        
        # Test progress event details
        progress_line = "2025-09-15 10:32:00 12348 [Note] WSREP: Progress 45.5% transferred"
        details = self.classifier.extract_sst_details(progress_line, SSTEventType.PROGRESS)
        
        self.assertEqual(details['event_type'], 'SST_PROGRESS')
        self.assertEqual(details['progress_percentage'], 45.5)
        self.assertEqual(details['transfer_status'], 'in_progress')
    
    def test_sst_related_detection(self):
        """Test SST-related line detection"""
        sst_lines = [
            "2025-09-15 10:30:45 12345 [Note] WSREP: SST request",
            "2025-09-15 10:30:46 12346 [Note] mariabackup: Starting backup",
            "2025-09-15 10:30:47 12347 [Note] WSREP: State transfer initiated",
        ]
        
        non_sst_lines = [
            "2025-09-15 10:30:48 12348 [Note] WSREP: Cluster membership changed",
            "2025-09-15 10:30:49 12349 [Note] MySQL: Query executed",
        ]
        
        for line in sst_lines:
            self.assertTrue(self.classifier.is_sst_related(line), f"Should be SST-related: {line}")
        
        for line in non_sst_lines:
            self.assertFalse(self.classifier.is_sst_related(line), f"Should not be SST-related: {line}")


class TestSessionManager(unittest.TestCase):
    """Test SessionManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.session_manager = SessionManager()
        self.test_timestamp = datetime.now()
    
    def test_sst_session_lifecycle(self):
        """Test complete SST session lifecycle"""
        # Start SST
        start_line = "2025-09-15 10:30:45 12345 [Note] WSREP: Requesting SST: mariabackup"
        start_data = {"transfer_method": "mariabackup", "donor_node": "node1", "joiner_node": "node2"}
        
        entity = self.session_manager.process_sst_event(start_line, self.test_timestamp, start_data)
        
        self.assertIsNotNone(entity)
        self.assertIsInstance(entity, TemporalStateTransferEntity)
        self.assertEqual(entity.current_status, "started")
        self.assertTrue(entity.is_active)
        
        # Update progress
        progress_line = "2025-09-15 10:32:00 12348 [Note] WSREP: Progress 50% transferred"
        progress_data = {"progress_percentage": 50.0, "transferred_bytes": 512*1024*1024}
        
        updated_entity = self.session_manager.process_sst_event(
            progress_line, 
            self.test_timestamp + timedelta(minutes=2), 
            progress_data
        )
        
        self.assertEqual(updated_entity.entity_id, entity.entity_id)
        self.assertEqual(updated_entity.current_status, "in_progress")
        
        # Complete SST
        end_line = "2025-09-15 10:35:20 12350 [Note] WSREP: SST completed"
        end_data = {"transfer_status": "completed"}
        
        completed_entity = self.session_manager.process_sst_event(
            end_line,
            self.test_timestamp + timedelta(minutes=5),
            end_data
        )
        
        self.assertEqual(completed_entity.entity_id, entity.entity_id)
        self.assertEqual(completed_entity.current_status, "completed")
        self.assertFalse(completed_entity.is_active)
        
        # Check session manager state
        active_sessions = self.session_manager.get_active_sessions()
        completed_sessions = self.session_manager.get_completed_sessions()
        
        self.assertEqual(len(active_sessions.get('SST', [])), 0)
        self.assertEqual(len(completed_sessions), 1)
    
    def test_orphaned_progress_event(self):
        """Test handling of orphaned progress events"""
        # Send progress without start
        progress_line = "2025-09-15 10:32:00 12348 [Note] WSREP: Progress 50% transferred"
        progress_data = {"progress_percentage": 50.0}
        
        entity = self.session_manager.process_sst_event(progress_line, self.test_timestamp, progress_data)
        
        # Should create implied session
        self.assertIsNotNone(entity)
        self.assertIsInstance(entity, TemporalStateTransferEntity)
    
    def test_sequential_sst_handling(self):
        """Test sequential SST session handling"""
        # Start first SST
        start_data1 = {"transfer_method": "mariabackup", "donor_node": "node1", "joiner_node": "node2"}
        entity1 = self.session_manager.process_sst_event(
            "SST request 1", self.test_timestamp, start_data1
        )
        
        # Start second SST before first completes (should auto-complete first)
        start_data2 = {"transfer_method": "mariabackup", "donor_node": "node3", "joiner_node": "node4"}
        entity2 = self.session_manager.process_sst_event(
            "SST request 2", self.test_timestamp + timedelta(minutes=1), start_data2
        )
        
        # Should have different entities
        self.assertNotEqual(entity1.entity_id, entity2.entity_id)
        
        # First entity should be auto-completed
        completed_sessions = self.session_manager.get_completed_sessions()
        self.assertEqual(len(completed_sessions), 1)
        self.assertIn("interrupted_by_new_sst", completed_sessions[0].current_status)
    
    def test_session_statistics(self):
        """Test session statistics generation"""
        # Create and complete a session
        start_data = {"transfer_method": "mariabackup", "donor_node": "node1", "joiner_node": "node2"}
        entity = self.session_manager.process_sst_event(
            "SST start", self.test_timestamp, start_data
        )
        
        self.session_manager.process_sst_event(
            "SST complete", self.test_timestamp + timedelta(minutes=3), {"transfer_status": "completed"}
        )
        
        stats = self.session_manager.get_session_statistics()
        
        self.assertIn('completed_sessions', stats)
        self.assertIn('total_sessions_created', stats)
        self.assertEqual(stats['completed_sessions'], 1)
        self.assertEqual(stats['total_sessions_created']['SST'], 1)


class TestLogParserIntegration(unittest.TestCase):
    """Test integration with LogParser"""
    
    def test_parser_with_session_manager(self):
        """Test that LogParser correctly uses SessionManager"""
        parser = LogParser()
        
        # Verify session manager is initialized
        self.assertIsNotNone(parser.session_manager)
        
        # Test getting all entities includes session-managed entities
        all_entities = parser.get_all_entities()
        self.assertIsInstance(all_entities, list)
        
        # Test session statistics retrieval
        session_stats = parser.get_session_statistics()
        self.assertIsInstance(session_stats, dict)


if __name__ == '__main__':
    # Set up logging to avoid noise during tests
    import logging
    logging.getLogger().setLevel(logging.WARNING)
    
    # Run tests
    unittest.main(verbosity=2)