#!/usr/bin/env python3
"""
Enhanced StateTransferEntity with temporal properties and session management.

This module provides an enhanced version of StateTransferEntity that uses
temporal properties to track changes over time during SST/IST operations.
"""

import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, field

from .base import Entity, Event, EntityType
from .core import StateTransferType, StateTransferMethod
from .temporal import TemporalProperty, TemporalPropertySet


class TemporalStateTransferEntity(Entity):
    """
    Enhanced StateTransferEntity with temporal property tracking.
    
    This entity represents an SST/IST operation and tracks property changes
    over time, allowing reconstruction of the transfer timeline.
    """
    
    def __init__(self, transfer_id: str, start_timestamp: datetime, **kwargs):
        """
        Initialize temporal state transfer entity.
        
        Args:
            transfer_id: Unique identifier for this transfer session
            start_timestamp: When the transfer started
            **kwargs: Initial property values
        """
        # Initialize temporal properties BEFORE calling super() to avoid validation issues
        self.temporal_properties = TemporalPropertySet()
        
        # Call parent constructor (this will call validate())
        super().__init__(transfer_id, start_timestamp)
        self.entity_type = EntityType.STATE_TRANSFER
        
        # Static properties (set once during initialization)
        self.transfer_type = kwargs.get('transfer_type', StateTransferType.SST)
        self.transfer_method = kwargs.get('transfer_method')
        self.donor_node = kwargs.get('donor_node', '')
        self.joiner_node = kwargs.get('joiner_node', '')
        self.donor_address = kwargs.get('donor_address', '')
        self.joiner_address = kwargs.get('joiner_address', '')
        self.uuid = kwargs.get('uuid', '')
        
        # Temporal properties (can change during the transfer)
        self.temporal_properties = TemporalPropertySet()
        
        # Initialize temporal properties with starting values
        self.temporal_properties.add_property(
            'transfer_status', 
            kwargs.get('transfer_status', 'initiated'), 
            start_timestamp
        )
        self.temporal_properties.add_property(
            'transferred_bytes', 
            kwargs.get('transferred_bytes', 0), 
            start_timestamp
        )
        self.temporal_properties.add_property('transfer_rate', None, start_timestamp)
        self.temporal_properties.add_property('error_message', '', start_timestamp)
        self.temporal_properties.add_property('seqno_start', kwargs.get('seqno_start'), start_timestamp)
        self.temporal_properties.add_property('seqno_end', kwargs.get('seqno_end'), start_timestamp)
        self.temporal_properties.add_property('progress_percentage', 0, start_timestamp)
        
        # Lifecycle tracking
        self.start_timestamp = start_timestamp
        self.end_timestamp: Optional[datetime] = None
        self.is_active = True
    
    def update_property(self, property_name: str, value: Any, timestamp: Optional[datetime] = None) -> None:
        """
        Update a temporal property.
        
        Args:
            property_name: Name of the property to update
            value: New value
            timestamp: When the change occurred (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        self.temporal_properties.update_property(property_name, value, timestamp)
        
        # Also update main entity properties for key fields used by formatter
        if property_name == 'transfer_method':
            self.transfer_method = value
    
    def complete_transfer(self, final_status: str, end_timestamp: Optional[datetime] = None) -> None:
        """
        Mark the transfer as completed.
        
        Args:
            final_status: Final transfer status (completed, failed, cancelled)
            end_timestamp: When the transfer ended (defaults to now)
        """
        if end_timestamp is None:
            end_timestamp = datetime.now()
            
        self.end_timestamp = end_timestamp
        self.is_active = False
        self.update_property('transfer_status', final_status, end_timestamp)
    
    @property
    def current_status(self) -> str:
        """Get current transfer status."""
        status_prop = self.temporal_properties.get_property('transfer_status')
        return status_prop.current_value if status_prop else 'unknown'
    
    @property
    def current_bytes(self) -> int:
        """Get current transferred bytes."""
        bytes_prop = self.temporal_properties.get_property('transferred_bytes')
        return bytes_prop.current_value if bytes_prop else 0
    
    @property
    def current_rate(self) -> Optional[float]:
        """Get current transfer rate."""
        rate_prop = self.temporal_properties.get_property('transfer_rate')
        return rate_prop.current_value if rate_prop else None
    
    @property
    def duration(self) -> Optional[float]:
        """Get transfer duration in seconds."""
        if self.end_timestamp:
            return (self.end_timestamp - self.start_timestamp).total_seconds()
        elif self.is_active:
            return (datetime.now() - self.start_timestamp).total_seconds()
        return None
    
    def get_status_at(self, timestamp: datetime) -> str:
        """Get transfer status at a specific time."""
        status_prop = self.temporal_properties.get_property('transfer_status')
        return status_prop.value_at(timestamp) if status_prop else 'unknown'
    
    def get_timeline(self) -> Dict[str, list]:
        """Get complete timeline of all property changes."""
        timeline = {}
        for prop_name in self.temporal_properties.get_property_names():
            prop = self.temporal_properties.get_property(prop_name)
            if prop is not None:
                timeline[prop_name] = prop.get_timeline()
        return timeline
    
    def get_identity_key(self) -> str:
        """Get unique identifier for this transfer session."""
        donor = self.donor_node or 'unknown_donor'
        joiner = self.joiner_node or 'unknown_joiner'
        timestamp = self.start_timestamp.strftime('%Y%m%d_%H%M%S')
        return f"{donor}→{joiner}@{timestamp}"
    
    def validate(self) -> bool:
        """Validate state transfer entity."""
        # Call parent validation
        super().validate()
        
        # Must have at least minimal identifying information
        donor_node = getattr(self, 'donor_node', '')
        joiner_node = getattr(self, 'joiner_node', '')
        current_status = self.current_status
        transfer_type = getattr(self, 'transfer_type', None)
        
        if (not donor_node and not joiner_node and 
            not current_status and not transfer_type):
            raise ValueError("State transfer must have at least one identifying field")
        
        # Validate temporal property values
        current_bytes = self.current_bytes
        if current_bytes is not None and current_bytes < 0:
            raise ValueError("Transferred bytes cannot be negative")
        
        current_rate = self.current_rate
        if current_rate is not None and current_rate < 0:
            raise ValueError("Transfer rate cannot be negative")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with temporal property information."""
        base_dict = super().to_dict()
        
        # Add static properties
        base_dict.update({
            'transfer_type': self.transfer_type.value if hasattr(self.transfer_type, 'value') else str(self.transfer_type),
            'transfer_method': self.transfer_method.value if self.transfer_method and hasattr(self.transfer_method, 'value') else str(self.transfer_method) if self.transfer_method else None,
            'donor_node': self.donor_node,
            'joiner_node': self.joiner_node,
            'donor_address': self.donor_address,
            'joiner_address': self.joiner_address,
            'uuid': self.uuid,
            'start_timestamp': self.start_timestamp.isoformat(),
            'end_timestamp': self.end_timestamp.isoformat() if self.end_timestamp else None,
            'is_active': self.is_active,
            'duration_seconds': self.duration
        })
        
        # Add current values of temporal properties
        current_values = self.temporal_properties.get_current_values()
        base_dict.update({f'current_{k}': v for k, v in current_values.items()})
        
        # Add timeline information
        base_dict['property_timeline'] = self.get_timeline()
        base_dict['identity_key'] = self.get_identity_key()
        
        return base_dict
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TemporalStateTransferEntity':
        """
        Create TemporalStateTransferEntity from dictionary representation.
        
        Args:
            data: Dictionary containing entity data
            
        Returns:
            TemporalStateTransferEntity: Reconstructed entity instance
        """
        # Extract required parameters
        transfer_id = data.get('entity_id', data.get('transfer_id', str(uuid.uuid4())))
        start_timestamp = data.get('start_timestamp', datetime.now())
        
        if isinstance(start_timestamp, str):
            start_timestamp = datetime.fromisoformat(start_timestamp)
        
        # Create entity with basic data
        entity = cls(
            transfer_id=transfer_id,
            start_timestamp=start_timestamp,
            transfer_type=data.get('transfer_type'),
            transfer_method=data.get('transfer_method'),
            donor_node=data.get('donor_node', ''),
            joiner_node=data.get('joiner_node', ''),
            donor_address=data.get('donor_address', ''),
            joiner_address=data.get('joiner_address', ''),
            uuid=data.get('uuid', ''),
            transfer_status=data.get('transfer_status', 'initiated')
        )
        
        # Restore temporal properties if available
        if 'property_timeline' in data:
            timeline_data = data['property_timeline']
            for prop_name, timeline in timeline_data.items():
                if timeline:  # If there are timeline entries
                    # Clear existing property and rebuild from timeline
                    entity.temporal_properties.properties[prop_name] = TemporalProperty()
                    for timestamp_str, value in timeline:
                        if isinstance(timestamp_str, str):
                            timestamp_obj = datetime.fromisoformat(timestamp_str)
                        else:
                            timestamp_obj = timestamp_str
                        entity.temporal_properties.properties[prop_name].update(value, timestamp_obj)
        
        # Set other attributes
        if 'end_timestamp' in data and data['end_timestamp']:
            entity.end_timestamp = datetime.fromisoformat(data['end_timestamp']) if isinstance(data['end_timestamp'], str) else data['end_timestamp']
        
        entity.is_active = data.get('is_active', True)
        
        return entity
    
    def __str__(self) -> str:
        """String representation of the temporal state transfer."""
        duration_str = f"{self.duration:.1f}s" if self.duration else "ongoing"
        return (f"TemporalStateTransfer({self.get_identity_key()}, "
                f"status={self.current_status}, duration={duration_str})")