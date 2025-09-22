#!/usr/bin/env python3
"""
Sequential Session Manager for tracking entity lifecycles.

This module provides session management for entities that have temporal
lifecycles (start/progress/end), particularly useful for SST operations.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict

from .temporal_entities import TemporalStateTransferEntity
from .sst_classifier import SSTEventClassifier, SSTEventType
from .base import EntityType


class SessionManager:
    """
    Manages entity sessions with temporal lifecycles.
    
    This manager tracks active sessions (ongoing processes like SST) and
    manages their lifecycle from start to completion, ensuring proper
    temporal flow and unique session identification.
    """
    
    def __init__(self):
        """Initialize the session manager."""
        self.active_sessions: Dict[str, Dict[str, Any]] = {}  # session_type -> {session_id -> entity}
        self.completed_sessions: List[Any] = []
        self.session_counters: Dict[str, int] = defaultdict(int)
        
        # SST-specific components
        self.sst_classifier = SSTEventClassifier()
        
        # Logging
        self.logger = logging.getLogger(__name__)
    
    def process_sst_event(self, log_line: str, timestamp: datetime, 
                         extracted_data: Dict[str, Any]) -> Optional[TemporalStateTransferEntity]:
        """
        Process an SST-related log line and manage SST session lifecycle.
        
        Args:
            log_line: The original log line
            timestamp: When the log line was written
            extracted_data: Data extracted from pattern matching
            
        Returns:
            TemporalStateTransferEntity: The SST entity (active or completed)
        """
        # First check if this is SST-related
        if not self.sst_classifier.is_sst_related(log_line):
            return None
        
        # Classify the SST event type
        event_type = self.sst_classifier.classify_sst_event(log_line)
        
        if event_type == SSTEventType.UNKNOWN:
            return None
        
        # Extract additional details from the classified event
        sst_details = self.sst_classifier.extract_sst_details(log_line, event_type)
        
        # Merge extracted data with classified details
        combined_data = {**extracted_data, **sst_details}
        
        # Handle based on event type
        if event_type == SSTEventType.START:
            return self._start_sst_session(combined_data, timestamp, log_line)
        elif event_type == SSTEventType.PROGRESS:
            return self._update_sst_session(combined_data, timestamp, log_line)
        elif event_type in [SSTEventType.END, SSTEventType.ERROR]:
            return self._complete_sst_session(combined_data, timestamp, log_line)
        
        return None
    
    def _start_sst_session(self, data: Dict[str, Any], timestamp: datetime, 
                          log_line: str) -> TemporalStateTransferEntity:
        """Start a new SST session."""
        
        # Close any existing SST session first (sequential assumption)
        existing_sst = self._get_active_sst_session()
        if existing_sst:
            self.logger.warning(f"Starting new SST while another is active. "
                              f"Auto-completing previous SST: {existing_sst.get_identity_key()}")
            self._force_complete_sst(existing_sst, "interrupted_by_new_sst", timestamp)
        
        # Generate unique session ID
        self.session_counters['SST'] += 1
        session_id = f"sst_{self.session_counters['SST']}"
        
        # Create temporal SST entity
        sst_entity = TemporalStateTransferEntity(
            transfer_id=session_id,
            start_timestamp=timestamp,
            transfer_method=data.get('transfer_method'),
            donor_node=data.get('donor_node', ''),
            joiner_node=data.get('joiner_node', ''),
            donor_address=data.get('donor_address', ''),
            joiner_address=data.get('joiner_address', ''),
            uuid=data.get('uuid', ''),
            transfer_status=data.get('transfer_status', 'started')
        )
        
        # Store as active session
        if 'SST' not in self.active_sessions:
            self.active_sessions['SST'] = {}
        self.active_sessions['SST'][session_id] = sst_entity
        
        self.logger.info(f"Started SST session: {sst_entity.get_identity_key()}")
        return sst_entity
    
    def _update_sst_session(self, data: Dict[str, Any], timestamp: datetime,
                           log_line: str) -> Optional[TemporalStateTransferEntity]:
        """Update the current active SST session."""
        
        current_sst = self._get_active_sst_session()
        
        if not current_sst:
            # Orphaned progress event - create implied SST session
            self.logger.warning("Found SST progress without active session. Creating implied session.")
            return self._start_sst_session(data, timestamp, log_line)
        
        # Update temporal properties
        for property_name, value in data.items():
            if property_name in ['progress_percentage', 'transferred_bytes', 'transfer_rate', 
                               'transfer_status', 'error_message']:
                if value is not None:
                    current_sst.update_property(property_name, value, timestamp)
        
        self.logger.debug(f"Updated SST session: {current_sst.get_identity_key()} - "
                         f"Status: {current_sst.current_status}")
        return current_sst
    
    def _complete_sst_session(self, data: Dict[str, Any], timestamp: datetime,
                             log_line: str) -> Optional[TemporalStateTransferEntity]:
        """Complete the current active SST session."""
        
        current_sst = self._get_active_sst_session()
        
        if not current_sst:
            # Orphaned completion event - log but don't create entity
            self.logger.warning("Found SST completion without active session.")
            return None
        
        # Update final properties
        final_status = data.get('transfer_status', 'completed')
        current_sst.complete_transfer(final_status, timestamp)
        
        # Update any final data
        for property_name, value in data.items():
            if value is not None and property_name in ['total_bytes', 'error_message']:
                current_sst.update_property(property_name, value, timestamp)
        
        # Move to completed sessions
        session_id = current_sst.entity_id
        self.completed_sessions.append(current_sst)
        del self.active_sessions['SST'][session_id]
        
        # Clean up empty session type
        if not self.active_sessions['SST']:
            del self.active_sessions['SST']
        
        self.logger.info(f"Completed SST session: {current_sst.get_identity_key()} - "
                        f"Final status: {final_status}, Duration: {current_sst.duration:.1f}s")
        return current_sst
    
    def _get_active_sst_session(self) -> Optional[TemporalStateTransferEntity]:
        """Get the current active SST session (assuming only one active at a time)."""
        if 'SST' not in self.active_sessions or not self.active_sessions['SST']:
            return None
        
        # Return the first (and should be only) active SST
        session_id = list(self.active_sessions['SST'].keys())[0]
        return self.active_sessions['SST'][session_id]
    
    def _force_complete_sst(self, sst_entity: TemporalStateTransferEntity, 
                           reason: str, timestamp: datetime) -> None:
        """Force complete an SST session (for cleanup)."""
        sst_entity.complete_transfer(f"auto_completed_{reason}", timestamp)
        sst_entity.update_property('error_message', f"Auto-completed: {reason}", timestamp)
        
        # Move to completed
        session_id = sst_entity.entity_id
        self.completed_sessions.append(sst_entity)
        if 'SST' in self.active_sessions and session_id in self.active_sessions['SST']:
            del self.active_sessions['SST'][session_id]
    
    def get_active_sessions(self) -> Dict[str, List[Any]]:
        """Get all currently active sessions."""
        active = {}
        for session_type, sessions in self.active_sessions.items():
            active[session_type] = list(sessions.values())
        return active
    
    def get_completed_sessions(self) -> List[Any]:
        """Get all completed sessions."""
        return self.completed_sessions.copy()
    
    def get_all_entities(self) -> List[Any]:
        """Get all entities (active and completed)."""
        all_entities = []
        
        # Add active entities
        for session_type, sessions in self.active_sessions.items():
            all_entities.extend(sessions.values())
        
        # Add completed entities
        all_entities.extend(self.completed_sessions)
        
        return all_entities
    
    def get_session_statistics(self) -> Dict[str, Any]:
        """Get statistics about session management."""
        stats = {
            'active_sessions': {
                session_type: len(sessions) 
                for session_type, sessions in self.active_sessions.items()
            },
            'completed_sessions': len(self.completed_sessions),
            'total_sessions_created': dict(self.session_counters),
        }
        
        # SST-specific statistics
        if self.completed_sessions:
            sst_entities = [e for e in self.completed_sessions 
                          if isinstance(e, TemporalStateTransferEntity)]
            if sst_entities:
                durations = [e.duration for e in sst_entities if e.duration]
                stats['sst_statistics'] = {
                    'total_sst_sessions': len(sst_entities),
                    'average_duration': sum(durations) / len(durations) if durations else 0,
                    'successful_transfers': len([e for e in sst_entities if e.current_status == 'completed']),
                    'failed_transfers': len([e for e in sst_entities if 'failed' in e.current_status]),
                }
        
        return stats
    
    def cleanup_stale_sessions(self, max_age_hours: int = 24) -> int:
        """
        Clean up stale active sessions that have been running too long.
        
        Args:
            max_age_hours: Maximum age in hours before considering a session stale
            
        Returns:
            Number of sessions cleaned up
        """
        cleaned = 0
        current_time = datetime.now()
        stale_threshold = max_age_hours * 3600  # convert to seconds
        
        for session_type in list(self.active_sessions.keys()):
            sessions = self.active_sessions[session_type]
            stale_sessions = []
            
            for session_id, entity in sessions.items():
                if hasattr(entity, 'start_timestamp'):
                    age = (current_time - entity.start_timestamp).total_seconds()
                    if age > stale_threshold:
                        stale_sessions.append((session_id, entity))
            
            # Clean up stale sessions
            for session_id, entity in stale_sessions:
                self.logger.warning(f"Cleaning up stale session: {session_id} "
                                  f"(age: {age/3600:.1f}h)")
                self._force_complete_sst(entity, "stale_cleanup", current_time)
                cleaned += 1
        
        return cleaned